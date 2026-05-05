//! K2I real-time read protocol types.
//!
//! The socket transport frames these payloads with a small K2I header before
//! bincode encoding. Keep these structs append-only and version the frame when
//! making breaking changes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Magic bytes at the start of every RPC frame.
pub const FRAME_MAGIC: [u8; 4] = *b"K2IR";

/// Current protocol major version.
pub const PROTOCOL_VERSION: u16 = 1;

/// Current payload codec.
pub const CODEC_BINCODE: u8 = 1;

/// Default maximum frame size: 64 MiB.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

const FRAME_HEADER_LEN: usize = 4 + 2 + 1;

/// A K2I table visible to read clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSummary {
    /// Database/namespace name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Latest table-scoped read LSN known by K2I.
    pub current_lsn: u64,
    /// Current number of committed data files tracked for reads.
    pub data_file_count: usize,
}

/// Per-partition offset watermark visible in a read state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartitionWatermark {
    /// Kafka topic.
    pub topic: String,
    /// Kafka partition.
    pub partition: i32,
    /// Highest committed offset visible for this partition.
    pub committed_offset: i64,
}

/// Data file visible to a read client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataFileRef {
    /// Minimum table LSN contained in this file.
    pub min_lsn: u64,
    /// Maximum table LSN contained in this file.
    pub lsn: u64,
    /// Database/namespace name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Absolute local path or object-store URI.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Number of rows in the file.
    pub row_count: u64,
    /// Kafka topic for this file.
    pub topic: String,
    /// Kafka partition for this file.
    pub partition: i32,
    /// Minimum Kafka offset in this file.
    pub min_offset: i64,
    /// Maximum Kafka offset in this file.
    pub max_offset: i64,
    /// Iceberg snapshot ID that committed this file.
    pub snapshot_id: i64,
}

/// Position delete placeholder for future upsert/delete semantics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PositionDelete {
    /// File path containing the deleted row.
    pub file_path: String,
    /// Zero-based row position in the file.
    pub row_id: u64,
}

/// Puffin deletion-vector placeholder for future upsert/delete semantics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeletionVectorRef {
    /// File path for the deletion vector.
    pub path: String,
    /// Referenced data file path.
    pub data_file_path: String,
}

/// Consistent read state returned by `scan_table_begin`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadState {
    /// Opaque scan identifier. Clients must pass it to `scan_table_end`.
    pub scan_id: u64,
    /// LSN at which this read state is consistent.
    pub lsn: u64,
    /// Database/namespace name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Iceberg/Parquet files visible at `lsn`.
    pub data_files: Vec<DataFileRef>,
    /// Hot buffer rows serialized as Arrow IPC stream bytes.
    pub hot_arrow_ipc: Option<Vec<u8>>,
    /// Partition watermarks used for hot/cold visibility.
    pub partition_watermarks: Vec<PartitionWatermark>,
    /// Position deletes. Empty for append-only v1.
    pub position_deletes: Vec<PositionDelete>,
    /// Puffin deletion vectors. Empty for append-only v1.
    pub deletion_vectors: Vec<DeletionVectorRef>,
    /// Time the read state was created.
    pub created_at: DateTime<Utc>,
}

/// Client request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RpcRequest {
    /// Check server health.
    Health,
    /// List tables.
    ListTables,
    /// Get an Arrow IPC stream containing only the table schema.
    GetTableSchema { database: String, table: String },
    /// Begin a scan and return a consistent read state.
    ScanTableBegin {
        database: String,
        table: String,
        lsn_bound: Option<u64>,
    },
    /// End a scan and release any pins.
    ScanTableEnd {
        database: String,
        table: String,
        scan_id: u64,
    },
}

/// Server response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RpcResponse {
    /// Health response.
    Health {
        ok: bool,
        current_lsn: u64,
        message: Option<String>,
    },
    /// Table summaries.
    Tables(Vec<TableSummary>),
    /// Arrow IPC schema bytes.
    TableSchema(Vec<u8>),
    /// Consistent read state.
    ReadState(ReadState),
    /// Empty success.
    Ack,
    /// Request failed.
    Error(RpcErrorBody),
}

/// Serializable RPC error.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcErrorBody {
    /// Stable-ish error code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
}

impl RpcErrorBody {
    /// Create a new error body.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Local codec errors.
#[derive(Debug, Error)]
pub enum CodecError {
    /// Frame has an invalid magic header.
    #[error("invalid K2I RPC frame magic")]
    InvalidMagic,
    /// Unsupported frame version.
    #[error("unsupported K2I RPC version {0}")]
    UnsupportedVersion(u16),
    /// Unsupported codec.
    #[error("unsupported K2I RPC codec {0}")]
    UnsupportedCodec(u8),
    /// Frame exceeds configured limit.
    #[error("frame exceeds maximum size: {actual} > {max}")]
    FrameTooLarge { actual: usize, max: usize },
    /// Bincode encode/decode error.
    #[error("bincode error: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),
}

/// Encode a request frame.
pub fn encode_request(request: &RpcRequest) -> Result<Vec<u8>, CodecError> {
    encode_payload(request)
}

/// Decode a request frame.
pub fn decode_request(frame: &[u8], max_frame_bytes: usize) -> Result<RpcRequest, CodecError> {
    decode_payload(frame, max_frame_bytes)
}

/// Encode a response frame.
pub fn encode_response(response: &RpcResponse) -> Result<Vec<u8>, CodecError> {
    encode_payload(response)
}

/// Decode a response frame.
pub fn decode_response(frame: &[u8], max_frame_bytes: usize) -> Result<RpcResponse, CodecError> {
    decode_payload(frame, max_frame_bytes)
}

fn encode_payload<T: Serialize>(payload: &T) -> Result<Vec<u8>, CodecError> {
    let payload = bincode::serialize(payload)?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&FRAME_MAGIC);
    frame.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    frame.push(CODEC_BINCODE);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn decode_payload<T: for<'de> Deserialize<'de>>(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<T, CodecError> {
    if frame.len() > max_frame_bytes {
        return Err(CodecError::FrameTooLarge {
            actual: frame.len(),
            max: max_frame_bytes,
        });
    }
    if frame.len() < FRAME_HEADER_LEN {
        return Err(CodecError::InvalidMagic);
    }
    if frame[0..4] != FRAME_MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    let version = u16::from_le_bytes([frame[4], frame[5]]);
    if version != PROTOCOL_VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let codec = frame[6];
    if codec != CODEC_BINCODE {
        return Err(CodecError::UnsupportedCodec(codec));
    }

    Ok(bincode::deserialize(&frame[FRAME_HEADER_LEN..])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    const SCAN_BEGIN_REQUEST_HEX: &str = "4b32495201000103000000030000000000000072617706000000000000006576656e7473012a00000000000000";
    const READ_STATE_RESPONSE_HEX: &str = "4b3249520100010300000007000000000000002a00000000000000030000000000000072617706000000000000006576656e7473010000000000000028000000000000002a00000000000000030000000000000072617706000000000000006576656e747331000000000000002f746d702f6b32692f77617265686f7573652f646174612f7261772f6576656e74732f706172742d312e70617271756574d204000000000000030000000000000006000000000000006576656e74730000000064000000000000006600000000000000292300000000000001040000000000000001020304010000000000000006000000000000006576656e7473000000006600000000000000000000000000000000000000000000001400000000000000323032332d31312d31345432323a31333a32305a";

    #[test]
    fn request_round_trip() {
        let request = RpcRequest::ScanTableBegin {
            database: "raw".to_string(),
            table: "events".to_string(),
            lsn_bound: Some(42),
        };

        let encoded = encode_request(&request).unwrap();
        let decoded = decode_request(&encoded, DEFAULT_MAX_FRAME_BYTES).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn response_round_trip() {
        let response = RpcResponse::Tables(vec![TableSummary {
            database: "raw".to_string(),
            table: "events".to_string(),
            current_lsn: 7,
            data_file_count: 2,
        }]);

        let encoded = encode_response(&response).unwrap();
        let decoded = decode_response(&encoded, DEFAULT_MAX_FRAME_BYTES).unwrap();

        assert_eq!(decoded, response);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut encoded = encode_request(&RpcRequest::Health).unwrap();
        encoded[0] = b'X';

        let err = decode_request(&encoded, DEFAULT_MAX_FRAME_BYTES).unwrap_err();
        assert!(matches!(err, CodecError::InvalidMagic));
    }

    #[test]
    fn scan_begin_request_golden_fixture() {
        let request = RpcRequest::ScanTableBegin {
            database: "raw".to_string(),
            table: "events".to_string(),
            lsn_bound: Some(42),
        };

        let encoded = encode_request(&request).unwrap();
        assert_eq!(hex(&encoded), SCAN_BEGIN_REQUEST_HEX);
        assert_eq!(
            decode_request(&unhex(SCAN_BEGIN_REQUEST_HEX), DEFAULT_MAX_FRAME_BYTES).unwrap(),
            request
        );
    }

    #[test]
    fn read_state_response_golden_fixture() {
        let response = RpcResponse::ReadState(ReadState {
            scan_id: 7,
            lsn: 42,
            database: "raw".to_string(),
            table: "events".to_string(),
            data_files: vec![DataFileRef {
                min_lsn: 40,
                lsn: 42,
                database: "raw".to_string(),
                table: "events".to_string(),
                path: "/tmp/k2i/warehouse/data/raw/events/part-1.parquet".to_string(),
                size_bytes: 1234,
                row_count: 3,
                topic: "events".to_string(),
                partition: 0,
                min_offset: 100,
                max_offset: 102,
                snapshot_id: 9001,
            }],
            hot_arrow_ipc: Some(vec![1, 2, 3, 4]),
            partition_watermarks: vec![PartitionWatermark {
                topic: "events".to_string(),
                partition: 0,
                committed_offset: 102,
            }],
            position_deletes: vec![],
            deletion_vectors: vec![],
            created_at: Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
        });

        let encoded = encode_response(&response).unwrap();
        assert_eq!(hex(&encoded), READ_STATE_RESPONSE_HEX);
        assert_eq!(
            decode_response(&unhex(READ_STATE_RESPONSE_HEX), DEFAULT_MAX_FRAME_BYTES).unwrap(),
            response
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{:02x}", byte)).collect()
    }

    fn unhex(input: &str) -> Vec<u8> {
        assert_eq!(input.len() % 2, 0);
        (0..input.len())
            .step_by(2)
            .map(|idx| u8::from_str_radix(&input[idx..idx + 2], 16).unwrap())
            .collect()
    }
}
