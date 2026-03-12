pub mod duckdb_client;

use duckdb::{Connection, params};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::server::protocol::ColumnMeta;

// ---------------------------------------------------------------------------
// DuckDbParquetEngine — lazy on-demand view registration
// ---------------------------------------------------------------------------

pub struct DuckDbParquetEngine {
    conn: Connection,
    parquet_dir: PathBuf,
    /// table_name → list of parquet file paths (built at startup, cheap)
    file_index: HashMap<String, Vec<PathBuf>>,
    /// Which views have already been registered in DuckDB
    registered: HashSet<String>,
}

impl DuckDbParquetEngine {
    pub fn new(parquet_dir: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open_in_memory()?;
        let canonical = std::fs::canonicalize(parquet_dir)?;

        // Only build the file index — no CREATE VIEW calls
        let file_index = build_file_index(&canonical)?;
        let total = file_index.len();
        eprintln!("Indexed {} tables from {}", total, canonical.display());

        Ok(DuckDbParquetEngine {
            conn,
            parquet_dir: canonical,
            file_index,
            registered: HashSet::new(),
        })
    }

    pub fn table_count(&self) -> usize {
        self.file_index.len()
    }

    pub fn registered_count(&self) -> usize {
        self.registered.len()
    }

    pub fn parquet_dir(&self) -> &Path {
        &self.parquet_dir
    }

    /// Register all views eagerly (for server mode where startup cost is amortized).
    pub fn register_all(&mut self) -> usize {
        let names: Vec<String> = self.file_index.keys().cloned().collect();
        for name in &names {
            self.register_view(name);
        }
        self.registered.len()
    }

    /// Register a single view by table name. Returns true if newly registered.
    fn register_view(&mut self, table_name: &str) -> bool {
        if self.registered.contains(table_name) {
            return false;
        }
        if let Some(files) = self.file_index.get(table_name) {
            let sql = if files.len() == 1 {
                format!(
                    "CREATE VIEW \"{}\" AS SELECT * FROM read_parquet('{}')",
                    table_name,
                    files[0].display()
                )
            } else {
                let file_list: Vec<String> = files
                    .iter()
                    .map(|f| format!("'{}'", f.display()))
                    .collect();
                format!(
                    "CREATE VIEW \"{}\" AS SELECT * FROM read_parquet([{}])",
                    table_name,
                    file_list.join(",")
                )
            };

            match self.conn.execute(&sql, params![]) {
                Ok(_) => {
                    self.registered.insert(table_name.to_string());
                    true
                }
                Err(e) => {
                    eprintln!("Warning: failed to register {}: {}", table_name, e);
                    false
                }
            }
        } else {
            false
        }
    }

    /// Execute a SQL query and return JSON rows with column metadata.
    /// On "table not found" errors, lazily registers the missing view and retries.
    pub fn query_json(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<ColumnMeta>, Vec<Value>), Box<dyn std::error::Error>> {
        // Try up to 10 times (each attempt may discover one missing table)
        for _attempt in 0..10 {
            match self.execute_query(sql) {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let msg = e.to_string();
                    if let Some(table_name) = extract_missing_table(&msg) {
                        if self.register_view(&table_name) {
                            continue; // retry with newly registered view
                        }
                    }
                    return Err(e);
                }
            }
        }
        Err("Too many missing table retries".into())
    }

    fn execute_query(
        &self,
        sql: &str,
    ) -> Result<(Vec<ColumnMeta>, Vec<Value>), Box<dyn std::error::Error>> {
        use duckdb::arrow::array::Array;

        let mut stmt = self.conn.prepare(sql)?;
        let arrow_result = stmt.query_arrow(params![])?;

        let schema = arrow_result.get_schema();
        let columns_meta: Vec<ColumnMeta> = schema
            .fields()
            .iter()
            .map(|f| ColumnMeta {
                name: f.name().clone(),
                data_type: format!("{}", f.data_type()),
            })
            .collect();

        let batches: Vec<_> = arrow_result.collect();

        let mut rows_out = Vec::new();
        for batch in &batches {
            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();
            for row_idx in 0..num_rows {
                let mut obj = serde_json::Map::new();
                for col_idx in 0..num_cols {
                    let col = batch.column(col_idx);
                    let field = schema.field(col_idx);
                    let val = arrow_value_to_json(col.as_ref(), row_idx, field.data_type());
                    obj.insert(field.name().clone(), val);
                }
                rows_out.push(Value::Object(obj));
            }
        }

        Ok((columns_meta, rows_out))
    }
}

/// Extract table name from DuckDB "Catalog Error" messages.
/// e.g. "Catalog Error: Table with name foo does not exist!"
/// Also handles: "Table \"foo\" does not exist"
fn extract_missing_table(err: &str) -> Option<String> {
    // Pattern 1: "Table with name X does not exist"
    if let Some(start) = err.find("Table with name ") {
        let rest = &err[start + 16..];
        if let Some(end) = rest.find(" does not exist") {
            return Some(rest[..end].to_string());
        }
    }
    // Pattern 2: 'Table "X" does not exist'
    if let Some(start) = err.find("Table \"") {
        let rest = &err[start + 7..];
        if let Some(end) = rest.find("\" does not exist") {
            return Some(rest[..end].to_string());
        }
    }
    None
}

fn arrow_value_to_json(
    col: &dyn duckdb::arrow::array::Array,
    row: usize,
    dt: &duckdb::arrow::datatypes::DataType,
) -> Value {
    use duckdb::arrow::array::*;
    use duckdb::arrow::datatypes::DataType;

    if col.is_null(row) {
        return Value::Null;
    }

    match dt {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(row))
        }
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::Number(arr.value(row).into())
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Number(arr.value(row).into())
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(arr.value(row).into())
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(arr.value(row).into())
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::Number(arr.value(row).into())
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Number(arr.value(row).into())
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Number(arr.value(row).into())
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(arr.value(row).into())
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = arr.value(row) as f64;
            serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::String(v.to_string()))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(row);
            serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::String(v.to_string()))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(arr.value(row).to_string())
        }
        _ => {
            // Fallback: use Display
            Value::String(format!("{}", duckdb::arrow::util::display::ArrayFormatter::try_new(col, &Default::default())
                .map(|f| f.value(row).to_string())
                .unwrap_or_else(|_| "?".to_string())))
        }
    }
}

/// Build a file index mapping table names to their parquet file paths.
/// This is a fast directory scan — no DuckDB calls.
fn build_file_index(
    parquet_dir: &Path,
) -> Result<HashMap<String, Vec<PathBuf>>, Box<dyn std::error::Error>> {
    let entries: Vec<_> = std::fs::read_dir(parquet_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
        })
        .collect();

    let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();

    for entry in &entries {
        let path = entry.path();
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();

        // Robo mode: "ClassName_chunk0" -> group by "ClassName"
        let base = if let Some(pos) = stem.rfind("_chunk") {
            stem[..pos].to_string()
        } else {
            stem.clone()
        };

        let table_name = sanitize_table_name(&base);
        groups.entry(table_name).or_default().push(path);
    }

    Ok(groups)
}

/// Sanitize a parquet base name into a valid DuckDB table name.
fn sanitize_table_name(name: &str) -> String {
    name.replace('.', "_").replace('-', "_").replace('$', "_D_")
}

// ---------------------------------------------------------------------------
// Socket path computation (shared between server + client)
// ---------------------------------------------------------------------------

/// Compute the socket path for a parquet directory.
/// `/tmp/heappy-serve-<sha256_hex8>.sock`
pub fn socket_path_for(parquet_dir: &Path) -> PathBuf {
    let canonical = parquet_dir.to_string_lossy();
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let hash = hasher.finalize();
    let hex8 = format!("{:02x}{:02x}{:02x}{:02x}", hash[0], hash[1], hash[2], hash[3]);
    PathBuf::from(format!("/tmp/heappy-serve-{}.sock", hex8))
}

// ---------------------------------------------------------------------------
// EngineVariant — dispatch between inline and remote
// ---------------------------------------------------------------------------

pub enum EngineVariant {
    Inline(DuckDbParquetEngine),
    Remote(duckdb_client::DuckDbClientEngine),
}

impl EngineVariant {
    /// Execute a SQL query and return JSON rows with column metadata.
    pub fn query_json(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<ColumnMeta>, Vec<Value>), Box<dyn std::error::Error>> {
        match self {
            EngineVariant::Inline(engine) => engine.query_json(sql),
            EngineVariant::Remote(client) => client.query_json(sql),
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, EngineVariant::Remote(_))
    }
}

/// Create an engine, preferring a running server if available.
pub fn parquet_engine(dir: &Path) -> Result<EngineVariant, Box<dyn std::error::Error>> {
    let canonical = std::fs::canonicalize(dir)?;
    let sock = socket_path_for(&canonical);

    if sock.exists() {
        match duckdb_client::DuckDbClientEngine::try_connect(&sock) {
            Ok(client) => return Ok(EngineVariant::Remote(client)),
            Err(_) => {
                // Stale socket — remove it
                let _ = std::fs::remove_file(&sock);
            }
        }
    }

    Ok(EngineVariant::Inline(DuckDbParquetEngine::new(&canonical)?))
}

/// Create an engine with auto-start for large directories.
pub fn parquet_engine_auto_serve(dir: &Path) -> Result<EngineVariant, Box<dyn std::error::Error>> {
    let canonical = std::fs::canonicalize(dir)?;
    let sock = socket_path_for(&canonical);

    if !sock.exists() && count_parquet_files(&canonical) > 100 {
        auto_start_server(&canonical)?;
        wait_for_socket(&sock, std::time::Duration::from_secs(30))?;
    }

    parquet_engine(dir)
}

fn count_parquet_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "parquet")
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
}

fn auto_start_server(dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let exe = std::env::current_exe()?;
    std::process::Command::new(exe)
        .arg("serve")
        .arg(dir)
        .arg("--background")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    Ok(())
}

pub fn wait_for_socket_pub(sock: &Path, timeout: std::time::Duration) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_socket(sock, timeout)
}

fn wait_for_socket(sock: &Path, timeout: std::time::Duration) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if sock.exists() {
            // Try connecting to confirm it's ready
            if std::os::unix::net::UnixStream::connect(sock).is_ok() {
                return Ok(());
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    Err("Timeout waiting for server to start".into())
}
