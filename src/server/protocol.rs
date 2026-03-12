use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    #[serde(rename = "query")]
    Query { sql: String },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "shutdown")]
    Shutdown,
    #[serde(rename = "status")]
    Status,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    #[serde(rename = "result")]
    Result {
        columns: Vec<ColumnMeta>,
        rows: Vec<Value>,
        row_count: usize,
        query_time_ms: f64,
    },
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "status")]
    Status {
        table_count: usize,
        parquet_dir: String,
        uptime_secs: u64,
    },
}

// ---------------------------------------------------------------------------
// Length-prefixed framing: [4 bytes u32 BE length][UTF-8 JSON]
// ---------------------------------------------------------------------------

pub fn read_message(stream: &mut UnixStream) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message too large: {} bytes", len),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn write_message(stream: &mut UnixStream, data: &[u8]) -> io::Result<()> {
    let len = (data.len() as u32).to_be_bytes();
    stream.write_all(&len)?;
    stream.write_all(data)?;
    stream.flush()
}

pub fn send_request(stream: &mut UnixStream, req: &Request) -> io::Result<()> {
    let json = serde_json::to_vec(req).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    write_message(stream, &json)
}

pub fn recv_response(stream: &mut UnixStream) -> io::Result<Response> {
    let data = read_message(stream)?;
    serde_json::from_slice(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
