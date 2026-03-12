mod commands;
mod hprof_index;
mod util;

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use jvm_hprof::parse_hprof;

use HeapDumpStarDiver::engine;
use HeapDumpStarDiver::server::HeappyServer;

#[derive(Parser)]
#[command(name = "heappy", about = "JVM heap dump analyzer — HPROF to Parquet and DuckDB query engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Display Object (and other associated) heap dump subrecords to stdout
    DumpObjects {
        #[arg(short, long, required = true, value_name = "FILE")]
        file: String,
    },
    /// Display the number of each of the top level hprof record types
    CountRecords {
        #[arg(short, long, required = true, value_name = "FILE")]
        file: String,
    },
    /// Parses and dumps objects in the heap dump to parquet files
    DumpObjectsToParquet {
        #[arg(short, long, required = true, value_name = "FILE")]
        file: String,
        /// Number of rows to accumulate before flushing to disk
        #[arg(long, default_value = "500000")]
        flush_rows: usize,
        /// LLM-optimized output: bare IDs for references, separate type index file
        #[arg(long)]
        robo_mode: bool,
    },
    /// Query parquet files using DuckDB SQL
    QueryParquet {
        /// Path to Parquet export directory
        parquet_dir: PathBuf,
        /// SQL query to execute
        #[arg(long)]
        sql: String,
        /// Output as JSON (LLM-friendly)
        #[arg(long)]
        json: bool,
        /// Force inline mode (skip server, for benchmarking)
        #[arg(long)]
        no_server: bool,
    },
    /// Start persistent query server for a Parquet directory
    Serve {
        /// Path to Parquet export directory
        parquet_dir: PathBuf,
        /// Run in background (detach from terminal)
        #[arg(long)]
        background: bool,
        /// Stop a running server
        #[arg(long)]
        stop: bool,
        /// Show server status
        #[arg(long)]
        status: bool,
        /// Idle timeout in seconds (default 300)
        #[arg(long, default_value = "300")]
        idle_timeout: u64,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::DumpObjects { file } => {
            let hprof = load_hprof(&file);
            commands::dump_objects(&hprof);
        }
        Commands::CountRecords { file } => {
            let hprof = load_hprof(&file);
            commands::count_records(&hprof);
        }
        Commands::DumpObjectsToParquet {
            file,
            flush_rows,
            robo_mode,
        } => {
            let hprof = load_hprof(&file);
            commands::dump_objects_to_parquet(&hprof, flush_rows, robo_mode);
        }
        Commands::QueryParquet {
            parquet_dir,
            sql,
            json,
            no_server,
        } => {
            cmd_query_parquet(&parquet_dir, &sql, json, no_server);
        }
        Commands::Serve {
            parquet_dir,
            background,
            stop,
            status,
            idle_timeout,
        } => {
            cmd_serve(&parquet_dir, background, stop, status, idle_timeout);
        }
    }
}

fn load_hprof(file_path: &str) -> jvm_hprof::Hprof<'static> {
    let file =
        fs::File::open(file_path).unwrap_or_else(|_| panic!("Could not open file at path: {}", file_path));
    let memmap = unsafe { memmap::MmapOptions::new().map(&file) }.unwrap();
    // Leak the mmap to get a 'static lifetime — the process owns it until exit
    let memmap: &'static [u8] = Box::leak(Box::new(memmap));
    parse_hprof(memmap).unwrap()
}

fn cmd_query_parquet(parquet_dir: &PathBuf, sql: &str, json_output: bool, no_server: bool) {
    let mut engine = if no_server {
        let canonical = std::fs::canonicalize(parquet_dir)
            .unwrap_or_else(|e| {
                eprintln!("Error: cannot resolve path {}: {}", parquet_dir.display(), e);
                std::process::exit(1);
            });
        match engine::DuckDbParquetEngine::new(&canonical) {
            Ok(e) => engine::EngineVariant::Inline(e),
            Err(e) => {
                eprintln!("Error creating engine: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        match engine::parquet_engine_auto_serve(parquet_dir) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    };

    if engine.is_remote() {
        eprintln!("Connected to running server");
    }

    let t0 = std::time::Instant::now();
    let (columns, rows) = match engine.query_json(sql) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Query error: {}", e);
            std::process::exit(1);
        }
    };
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

    if json_output {
        let output = serde_json::json!({
            "columns": columns,
            "rows": rows,
            "row_count": rows.len(),
            "query_time_ms": elapsed_ms,
        });
        println!("{}", serde_json::to_string_pretty(&output).unwrap());
    } else {
        // Pretty-print as table
        print_table(&columns, &rows);
        eprintln!(
            "\n{} row(s) in {:.1}ms{}",
            rows.len(),
            elapsed_ms,
            if engine.is_remote() { " (via server)" } else { "" }
        );
    }
}

fn print_table(columns: &[HeapDumpStarDiver::server::protocol::ColumnMeta], rows: &[serde_json::Value]) {
    if rows.is_empty() {
        println!("(empty result)");
        return;
    }

    let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    // Compute column widths
    let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
    for row in rows {
        if let serde_json::Value::Object(map) = row {
            for (i, name) in col_names.iter().enumerate() {
                let val_str = format_json_value(map.get(*name).unwrap_or(&serde_json::Value::Null));
                widths[i] = widths[i].max(val_str.len());
            }
        }
    }

    // Header
    let header: Vec<String> = col_names
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{:width$}", n, width = widths[i]))
        .collect();
    println!("{}", header.join(" | "));
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("-+-"));

    // Rows
    for row in rows {
        if let serde_json::Value::Object(map) = row {
            let cells: Vec<String> = col_names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let val = format_json_value(map.get(*name).unwrap_or(&serde_json::Value::Null));
                    format!("{:width$}", val, width = widths[i])
                })
                .collect();
            println!("{}", cells.join(" | "));
        }
    }
}

fn format_json_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}

fn cmd_serve(parquet_dir: &PathBuf, background: bool, stop: bool, status: bool, idle_timeout: u64) {
    let canonical = std::fs::canonicalize(parquet_dir).unwrap_or_else(|e| {
        eprintln!("Error: cannot resolve path {}: {}", parquet_dir.display(), e);
        std::process::exit(1);
    });
    let sock = engine::socket_path_for(&canonical);

    if stop {
        if !sock.exists() {
            eprintln!("No server running for {}", canonical.display());
            std::process::exit(1);
        }
        match engine::duckdb_client::DuckDbClientEngine::try_connect(&sock) {
            Ok(client) => {
                client.shutdown().unwrap_or_else(|e| {
                    eprintln!("Shutdown error: {}", e);
                });
                eprintln!("Server shutdown requested");
            }
            Err(_) => {
                eprintln!("Stale socket, cleaning up");
                let _ = std::fs::remove_file(&sock);
                let _ = std::fs::remove_file(sock.with_extension("pid"));
            }
        }
        return;
    }

    if status {
        if !sock.exists() {
            eprintln!("No server running for {}", canonical.display());
            std::process::exit(1);
        }
        match engine::duckdb_client::DuckDbClientEngine::try_connect(&sock) {
            Ok(client) => match client.status() {
                Ok((table_count, dir, uptime)) => {
                    println!("Server running");
                    println!("  Parquet dir: {}", dir);
                    println!("  Tables: {}", table_count);
                    println!("  Uptime: {}s", uptime);
                    println!("  Socket: {}", sock.display());
                }
                Err(e) => {
                    eprintln!("Status error: {}", e);
                    std::process::exit(1);
                }
            },
            Err(_) => {
                eprintln!("Stale socket, no running server");
                std::process::exit(1);
            }
        }
        return;
    }

    if background {
        // Daemonize: fork, setsid, then run server in child
        unsafe {
            let pid = libc::fork();
            if pid < 0 {
                eprintln!("Failed to fork");
                std::process::exit(1);
            }
            if pid > 0 {
                // Parent: wait for socket to appear, then exit
                // Use write() directly since we redirected stdio in child
                let msg = format!("Server starting in background (PID {})\n", pid);
                libc::write(2, msg.as_ptr() as *const _, msg.len());
                let timeout = Duration::from_secs(30);
                match engine::wait_for_socket_pub(&sock, timeout) {
                    Ok(()) => {
                        let msg = format!("Server ready at {}\n", sock.display());
                        libc::write(2, msg.as_ptr() as *const _, msg.len());
                    }
                    Err(e) => {
                        let msg = format!("Warning: {}\n", e);
                        libc::write(2, msg.as_ptr() as *const _, msg.len());
                    }
                }
                libc::_exit(0);
            }
            // Child: create new session, redirect stdio
            libc::setsid();
            // Redirect all stdio to /dev/null
            let devnull = libc::open(b"/dev/null\0".as_ptr() as *const _, libc::O_RDWR);
            if devnull >= 0 {
                libc::dup2(devnull, 0);
                libc::dup2(devnull, 1);
                libc::dup2(devnull, 2);
                if devnull > 2 {
                    libc::close(devnull);
                }
            }
        }
        // Child continues to foreground server start below
    }

    // Foreground mode
    HeappyServer::start(&canonical, Duration::from_secs(idle_timeout)).unwrap_or_else(|e| {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    });
}
