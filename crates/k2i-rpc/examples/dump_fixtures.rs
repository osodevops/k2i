use chrono::{TimeZone, Utc};
use k2i_rpc::{
    encode_request, encode_response, DataFileRef, PartitionWatermark, ReadState, RpcRequest,
    RpcResponse,
};

fn main() {
    let request = RpcRequest::ScanTableBegin {
        database: "raw".to_string(),
        table: "events".to_string(),
        lsn_bound: Some(42),
    };
    print_hex("scan_begin_request", &encode_request(&request).unwrap());

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
    print_hex("read_state_response", &encode_response(&response).unwrap());
}

fn print_hex(name: &str, bytes: &[u8]) {
    print!("{}=", name);
    for byte in bytes {
        print!("{:02x}", byte);
    }
    println!();
}
