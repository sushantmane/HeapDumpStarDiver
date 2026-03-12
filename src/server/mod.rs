pub mod protocol;

use crate::engine::DuckDbParquetEngine;
use protocol::{read_message, write_message, Request, Response};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub struct HeappyServer {
    engine: DuckDbParquetEngine,
    socket_path: PathBuf,
    pid_path: PathBuf,
    idle_timeout: Duration,
}

impl HeappyServer {
    pub fn start(parquet_dir: &Path, idle_timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
        let canonical = std::fs::canonicalize(parquet_dir)?;
        let socket_path = crate::engine::socket_path_for(&canonical);
        let pid_path = socket_path.with_extension("pid");

        // Clean stale socket if exists
        if socket_path.exists() {
            // Try connecting to see if it's alive
            if std::os::unix::net::UnixStream::connect(&socket_path).is_ok() {
                return Err(format!(
                    "Server already running for {}",
                    canonical.display()
                )
                .into());
            }
            let _ = std::fs::remove_file(&socket_path);
        }

        eprintln!("Registering parquet tables from {}...", canonical.display());
        let engine = DuckDbParquetEngine::new(&canonical)?;
        eprintln!(
            "Registered {} tables. Starting server on {}",
            engine.table_count(),
            socket_path.display()
        );

        let server = HeappyServer {
            engine,
            socket_path: socket_path.clone(),
            pid_path: pid_path.clone(),
            idle_timeout,
        };

        // Write PID file
        std::fs::write(&pid_path, std::process::id().to_string())?;

        let listener = UnixListener::bind(&socket_path)?;
        listener.set_nonblocking(true)?;

        let start_time = Instant::now();
        let mut last_activity = Instant::now();

        eprintln!(
            "Server ready. Idle timeout: {}s",
            idle_timeout.as_secs()
        );

        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    last_activity = Instant::now();
                    if server.handle_connection(stream, start_time) {
                        // Shutdown requested
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if last_activity.elapsed() > idle_timeout {
                        eprintln!("Idle timeout reached, shutting down");
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    eprintln!("Accept error: {}", e);
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }

        server.cleanup();
        Ok(())
    }

    /// Returns true if shutdown was requested. Never returns Err — connection
    /// errors are logged and the server continues accepting.
    fn handle_connection(
        &self,
        stream: std::os::unix::net::UnixStream,
        start_time: Instant,
    ) -> bool {
        let mut stream = stream;
        if let Err(e) = stream.set_nonblocking(false) {
            eprintln!("set_nonblocking error: {}", e);
            return false;
        }
        if let Err(e) = stream.set_read_timeout(Some(Duration::from_secs(30))) {
            eprintln!("set_read_timeout error: {}", e);
            return false;
        }

        let data = match read_message(&mut stream) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Read error: {}", e);
                return false;
            }
        };

        let request: Request = match serde_json::from_slice(&data) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::Error {
                    message: format!("Invalid request: {}", e),
                };
                let _ = serde_json::to_vec(&resp)
                    .map(|v| write_message(&mut stream, &v));
                return false;
            }
        };

        match request {
            Request::Query { sql } => {
                let t0 = Instant::now();
                let resp = match self.engine.query_json(&sql) {
                    Ok((columns, rows)) => Response::Result {
                        row_count: rows.len(),
                        columns,
                        rows,
                        query_time_ms: t0.elapsed().as_secs_f64() * 1000.0,
                    },
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                };
                let _ = serde_json::to_vec(&resp)
                    .map(|v| write_message(&mut stream, &v));
                false
            }
            Request::Ping => {
                let _ = serde_json::to_vec(&Response::Pong)
                    .map(|v| write_message(&mut stream, &v));
                false
            }
            Request::Status => {
                let resp = Response::Status {
                    table_count: self.engine.table_count(),
                    parquet_dir: self
                        .engine
                        .parquet_dir()
                        .to_string_lossy()
                        .into_owned(),
                    uptime_secs: start_time.elapsed().as_secs(),
                };
                let _ = serde_json::to_vec(&resp)
                    .map(|v| write_message(&mut stream, &v));
                false
            }
            Request::Shutdown => {
                let _ = serde_json::to_vec(&Response::Pong)
                    .map(|v| write_message(&mut stream, &v));
                eprintln!("Shutdown requested");
                true
            }
        }
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_file(&self.pid_path);
        eprintln!("Server stopped, socket cleaned up");
    }
}

impl Drop for HeappyServer {
    fn drop(&mut self) {
        self.cleanup();
    }
}
