use crate::server::protocol::{self, ColumnMeta, Request, Response};
use serde_json::Value;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct DuckDbClientEngine {
    socket_path: PathBuf,
}

impl DuckDbClientEngine {
    /// Try to connect to a running server. Returns error if connection fails.
    pub fn try_connect(socket_path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let mut stream = UnixStream::connect(socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        // Verify with a ping
        protocol::send_request(&mut stream, &Request::Ping)?;
        match protocol::recv_response(&mut stream)? {
            Response::Pong => Ok(DuckDbClientEngine {
                socket_path: socket_path.to_path_buf(),
            }),
            other => Err(format!("Unexpected response to ping: {:?}", other).into()),
        }
    }

    pub fn query_json(
        &self,
        sql: &str,
    ) -> Result<(Vec<ColumnMeta>, Vec<Value>), Box<dyn std::error::Error>> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(60)))?;

        protocol::send_request(&mut stream, &Request::Query { sql: sql.to_string() })?;

        match protocol::recv_response(&mut stream)? {
            Response::Result {
                columns,
                rows,
                row_count: _,
                query_time_ms: _,
            } => Ok((columns, rows)),
            Response::Error { message } => Err(message.into()),
            other => Err(format!("Unexpected response: {:?}", other).into()),
        }
    }

    pub fn status(
        &self,
    ) -> Result<(usize, String, u64), Box<dyn std::error::Error>> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        protocol::send_request(&mut stream, &Request::Status)?;

        match protocol::recv_response(&mut stream)? {
            Response::Status {
                table_count,
                parquet_dir,
                uptime_secs,
            } => Ok((table_count, parquet_dir, uptime_secs)),
            other => Err(format!("Unexpected response: {:?}", other).into()),
        }
    }

    pub fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        protocol::send_request(&mut stream, &Request::Shutdown)?;
        // Server may close connection immediately
        let _ = protocol::recv_response(&mut stream);
        Ok(())
    }
}
