//! Iceberg REST API types and protocol implementation.
//!
//! This module implements the Apache Iceberg REST Catalog API specification.
//! See: https://iceberg.apache.org/spec/#iceberg-rest-catalog-api

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Catalog configuration response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Default configuration values
    #[serde(default)]
    pub defaults: HashMap<String, String>,
    /// Override configuration values
    #[serde(default)]
    pub overrides: HashMap<String, String>,
}

/// List namespaces response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    /// List of namespace identifiers
    pub namespaces: Vec<Vec<String>>,
    /// Continuation token for pagination
    #[serde(rename = "next-page-token")]
    pub next_page_token: Option<String>,
}

/// Create namespace request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Namespace identifier (multi-part)
    pub namespace: Vec<String>,
    /// Namespace properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Namespace response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceResponse {
    /// Namespace identifier
    pub namespace: Vec<String>,
    /// Namespace properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// List tables response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTablesResponse {
    /// List of table identifiers
    pub identifiers: Vec<TableIdentifier>,
    /// Continuation token for pagination
    #[serde(rename = "next-page-token")]
    pub next_page_token: Option<String>,
}

/// Table identifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIdentifier {
    /// Namespace
    pub namespace: Vec<String>,
    /// Table name
    pub name: String,
}

/// Create table request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableRequest {
    /// Table name
    pub name: String,
    /// Table location (optional, catalog assigns if not provided)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Table schema
    pub schema: Schema,
    /// Partition spec (optional)
    #[serde(skip_serializing_if = "Option::is_none", rename = "partition-spec")]
    pub partition_spec: Option<PartitionSpec>,
    /// Write order (optional)
    #[serde(skip_serializing_if = "Option::is_none", rename = "write-order")]
    pub write_order: Option<SortOrder>,
    /// Stage create (optional)
    #[serde(skip_serializing_if = "Option::is_none", rename = "stage-create")]
    pub stage_create: Option<bool>,
    /// Table properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Load table response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTableResponse {
    /// Metadata location
    #[serde(rename = "metadata-location")]
    pub metadata_location: Option<String>,
    /// Table metadata
    pub metadata: TableMetadata,
    /// Configuration overrides
    #[serde(default)]
    pub config: HashMap<String, String>,
}

/// Table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Format version (1 or 2)
    #[serde(rename = "format-version")]
    pub format_version: i32,
    /// Table UUID
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    /// Table location
    pub location: String,
    /// Last updated timestamp (ms)
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: i64,
    /// Last column ID assigned
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,
    /// Table schema (current)
    pub schema: Schema,
    /// All schemas (version history)
    #[serde(default)]
    pub schemas: Vec<Schema>,
    /// Current schema ID
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
    /// Partition specs
    #[serde(default, rename = "partition-specs")]
    pub partition_specs: Vec<PartitionSpec>,
    /// Default spec ID
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
    /// Last partition ID assigned
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,
    /// Table properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// Current snapshot ID
    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: Option<i64>,
    /// Snapshots
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    /// Snapshot log
    #[serde(default, rename = "snapshot-log")]
    pub snapshot_log: Vec<SnapshotLogEntry>,
    /// Sort orders
    #[serde(default, rename = "sort-orders")]
    pub sort_orders: Vec<SortOrder>,
    /// Default sort order ID
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,
}

/// Iceberg schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema ID
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    /// Schema type (always "struct")
    #[serde(rename = "type", default = "default_struct_type")]
    pub r#type: String,
    /// Schema fields
    pub fields: Vec<SchemaField>,
    /// Identifier field IDs (for upsert support)
    #[serde(default, rename = "identifier-field-ids")]
    pub identifier_field_ids: Vec<i32>,
}

fn default_struct_type() -> String {
    "struct".to_string()
}

/// Schema field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field ID
    pub id: i32,
    /// Field name
    pub name: String,
    /// Field type (primitive or nested)
    #[serde(rename = "type")]
    pub field_type: serde_json::Value,
    /// Whether field is required
    pub required: bool,
    /// Documentation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

/// Partition spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// Spec ID
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    /// Partition fields
    #[serde(default)]
    pub fields: Vec<PartitionField>,
}

/// Partition field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    /// Source field ID
    #[serde(rename = "source-id")]
    pub source_id: i32,
    /// Field ID in partition spec
    #[serde(rename = "field-id")]
    pub field_id: i32,
    /// Partition field name
    pub name: String,
    /// Transform (identity, bucket, truncate, year, month, day, hour)
    pub transform: String,
}

/// Sort order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrder {
    /// Sort order ID
    #[serde(rename = "order-id")]
    pub order_id: i32,
    /// Sort fields
    #[serde(default)]
    pub fields: Vec<SortField>,
}

/// Sort field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortField {
    /// Transform
    pub transform: String,
    /// Source field ID
    #[serde(rename = "source-id")]
    pub source_id: i32,
    /// Sort direction
    pub direction: String,
    /// Null ordering
    #[serde(rename = "null-order")]
    pub null_order: String,
}

/// Snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot ID
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Parent snapshot ID
    #[serde(skip_serializing_if = "Option::is_none", rename = "parent-snapshot-id")]
    pub parent_snapshot_id: Option<i64>,
    /// Sequence number
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    /// Timestamp (ms)
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Manifest list location
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,
    /// Snapshot summary
    #[serde(default)]
    pub summary: HashMap<String, String>,
    /// Schema ID
    #[serde(skip_serializing_if = "Option::is_none", rename = "schema-id")]
    pub schema_id: Option<i32>,
}

/// Snapshot log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    /// Snapshot ID
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Timestamp (ms)
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

/// Commit table request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitTableRequest {
    /// Identifier (optional for v1)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<TableIdentifier>,
    /// Requirements for optimistic concurrency
    pub requirements: Vec<TableRequirement>,
    /// Updates to apply
    pub updates: Vec<TableUpdate>,
}

/// Table requirement for optimistic concurrency.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TableRequirement {
    /// Assert table exists
    #[serde(rename = "assert-create")]
    AssertCreate,
    /// Assert table UUID matches
    #[serde(rename = "assert-table-uuid")]
    AssertTableUuid { uuid: String },
    /// Assert ref snapshot ID (for branches/tags)
    #[serde(rename = "assert-ref-snapshot-id")]
    AssertRefSnapshotId {
        #[serde(rename = "ref")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    /// Assert last assigned field ID
    #[serde(rename = "assert-last-assigned-field-id")]
    AssertLastAssignedFieldId {
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32,
    },
    /// Assert current schema ID
    #[serde(rename = "assert-current-schema-id")]
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32,
    },
    /// Assert last assigned partition ID
    #[serde(rename = "assert-last-assigned-partition-id")]
    AssertLastAssignedPartitionId {
        #[serde(rename = "last-assigned-partition-id")]
        last_assigned_partition_id: i32,
    },
    /// Assert default spec ID
    #[serde(rename = "assert-default-spec-id")]
    AssertDefaultSpecId {
        #[serde(rename = "default-spec-id")]
        default_spec_id: i32,
    },
    /// Assert default sort order ID
    #[serde(rename = "assert-default-sort-order-id")]
    AssertDefaultSortOrderId {
        #[serde(rename = "default-sort-order-id")]
        default_sort_order_id: i32,
    },
}

/// Table update operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum TableUpdate {
    /// Assign UUID
    #[serde(rename = "assign-uuid")]
    AssignUuid { uuid: String },
    /// Upgrade format version
    #[serde(rename = "upgrade-format-version")]
    UpgradeFormatVersion {
        #[serde(rename = "format-version")]
        format_version: i32,
    },
    /// Add schema
    #[serde(rename = "add-schema")]
    AddSchema {
        schema: Schema,
        #[serde(rename = "last-column-id")]
        last_column_id: Option<i32>,
    },
    /// Set current schema
    #[serde(rename = "set-current-schema")]
    SetCurrentSchema {
        #[serde(rename = "schema-id")]
        schema_id: i32,
    },
    /// Add partition spec
    #[serde(rename = "add-spec")]
    AddSpec { spec: PartitionSpec },
    /// Set default spec
    #[serde(rename = "set-default-spec")]
    SetDefaultSpec {
        #[serde(rename = "spec-id")]
        spec_id: i32,
    },
    /// Add sort order
    #[serde(rename = "add-sort-order")]
    AddSortOrder {
        #[serde(rename = "sort-order")]
        sort_order: SortOrder,
    },
    /// Set default sort order
    #[serde(rename = "set-default-sort-order")]
    SetDefaultSortOrder {
        #[serde(rename = "sort-order-id")]
        sort_order_id: i32,
    },
    /// Add snapshot
    #[serde(rename = "add-snapshot")]
    AddSnapshot { snapshot: Snapshot },
    /// Set snapshot ref (branch or tag)
    #[serde(rename = "set-snapshot-ref")]
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "type")]
        ref_type: String,
        #[serde(skip_serializing_if = "Option::is_none", rename = "max-ref-age-ms")]
        max_ref_age_ms: Option<i64>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            rename = "max-snapshot-age-ms"
        )]
        max_snapshot_age_ms: Option<i64>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            rename = "min-snapshots-to-keep"
        )]
        min_snapshots_to_keep: Option<i32>,
    },
    /// Remove snapshot ref
    #[serde(rename = "remove-snapshot-ref")]
    RemoveSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
    },
    /// Remove snapshots
    #[serde(rename = "remove-snapshots")]
    RemoveSnapshots {
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },
    /// Set location
    #[serde(rename = "set-location")]
    SetLocation { location: String },
    /// Set properties
    #[serde(rename = "set-properties")]
    SetProperties { updates: HashMap<String, String> },
    /// Remove properties
    #[serde(rename = "remove-properties")]
    RemoveProperties { removals: Vec<String> },
}

/// Commit table response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitTableResponse {
    /// Metadata location
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    /// Updated metadata
    pub metadata: TableMetadata,
}

/// Data file for adding to snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    /// Content type (data, position-deletes, equality-deletes)
    #[serde(default = "default_content_type")]
    pub content: String,
    /// File path
    #[serde(rename = "file-path")]
    pub file_path: String,
    /// File format (parquet, avro, orc)
    #[serde(rename = "file-format")]
    pub file_format: String,
    /// Partition data (JSON-encoded values)
    #[serde(default)]
    pub partition: HashMap<String, serde_json::Value>,
    /// Record count
    #[serde(rename = "record-count")]
    pub record_count: i64,
    /// File size in bytes
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: i64,
    /// Column sizes (field ID -> size)
    #[serde(skip_serializing_if = "Option::is_none", rename = "column-sizes")]
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// Value counts (field ID -> count)
    #[serde(skip_serializing_if = "Option::is_none", rename = "value-counts")]
    pub value_counts: Option<HashMap<i32, i64>>,
    /// Null value counts (field ID -> count)
    #[serde(skip_serializing_if = "Option::is_none", rename = "null-value-counts")]
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// NaN value counts (field ID -> count)
    #[serde(skip_serializing_if = "Option::is_none", rename = "nan-value-counts")]
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    /// Lower bounds (field ID -> encoded value)
    #[serde(skip_serializing_if = "Option::is_none", rename = "lower-bounds")]
    pub lower_bounds: Option<HashMap<i32, String>>,
    /// Upper bounds (field ID -> encoded value)
    #[serde(skip_serializing_if = "Option::is_none", rename = "upper-bounds")]
    pub upper_bounds: Option<HashMap<i32, String>>,
    /// Sort order ID
    #[serde(skip_serializing_if = "Option::is_none", rename = "sort-order-id")]
    pub sort_order_id: Option<i32>,
    /// Split offsets
    #[serde(skip_serializing_if = "Option::is_none", rename = "split-offsets")]
    pub split_offsets: Option<Vec<i64>>,
}

fn default_content_type() -> String {
    "data".to_string()
}

/// Error response from REST API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Error type
    #[serde(rename = "type")]
    pub error_type: String,
    /// HTTP status code
    pub code: i32,
    /// Error message
    pub message: String,
    /// Stack trace (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<Vec<String>>,
}

/// OAuth token request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokenRequest {
    /// Grant type
    pub grant_type: String,
    /// Client ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// Client secret
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    /// Scope
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// OAuth token response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokenResponse {
    /// Access token
    pub access_token: String,
    /// Token type
    pub token_type: String,
    /// Expires in (seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<i64>,
    /// Issued token type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issued_token_type: Option<String>,
    /// Scope
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_serialization() {
        let schema = Schema {
            schema_id: 0,
            r#type: "struct".to_string(),
            fields: vec![
                SchemaField {
                    id: 1,
                    name: "id".to_string(),
                    field_type: serde_json::json!("long"),
                    required: true,
                    doc: None,
                },
                SchemaField {
                    id: 2,
                    name: "data".to_string(),
                    field_type: serde_json::json!("string"),
                    required: false,
                    doc: Some("Event data".to_string()),
                },
            ],
            identifier_field_ids: vec![1],
        };

        let json = serde_json::to_string_pretty(&schema).unwrap();
        assert!(json.contains("schema-id"));
        assert!(json.contains("identifier-field-ids"));

        let parsed: Schema = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.schema_id, 0);
        assert_eq!(parsed.fields.len(), 2);
    }

    #[test]
    fn test_table_requirement_serialization() {
        let req = TableRequirement::AssertRefSnapshotId {
            ref_name: "main".to_string(),
            snapshot_id: Some(123456789),
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("assert-ref-snapshot-id"));
        assert!(json.contains("123456789"));
    }

    #[test]
    fn test_table_update_serialization() {
        let update = TableUpdate::AddSnapshot {
            snapshot: Snapshot {
                snapshot_id: 100,
                parent_snapshot_id: None,
                sequence_number: 1,
                timestamp_ms: 1704672000000,
                manifest_list: "s3://bucket/metadata/snap-100-uuid.avro".to_string(),
                summary: HashMap::from([
                    ("operation".to_string(), "append".to_string()),
                    ("added-data-files".to_string(), "1".to_string()),
                ]),
                schema_id: Some(0),
            },
        };

        let json = serde_json::to_string(&update).unwrap();
        assert!(json.contains("add-snapshot"));
        assert!(json.contains("snapshot-id"));
    }

    #[test]
    fn test_commit_request_serialization() {
        let request = CommitTableRequest {
            identifier: None,
            requirements: vec![TableRequirement::AssertRefSnapshotId {
                ref_name: "main".to_string(),
                snapshot_id: Some(99),
            }],
            updates: vec![TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_id: 100,
                ref_type: "branch".to_string(),
                max_ref_age_ms: None,
                max_snapshot_age_ms: None,
                min_snapshots_to_keep: None,
            }],
        };

        let json = serde_json::to_string_pretty(&request).unwrap();
        assert!(json.contains("requirements"));
        assert!(json.contains("updates"));
    }

    #[test]
    fn test_data_file_serialization() {
        let file = DataFile {
            content: "data".to_string(),
            file_path: "s3://bucket/data/file.parquet".to_string(),
            file_format: "parquet".to_string(),
            partition: HashMap::new(),
            record_count: 1000,
            file_size_in_bytes: 1048576,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            sort_order_id: None,
            split_offsets: None,
        };

        let json = serde_json::to_string(&file).unwrap();
        assert!(json.contains("file-path"));
        assert!(json.contains("record-count"));
    }

    #[test]
    fn test_error_response_deserialization() {
        let json = r#"{
            "type": "NoSuchTableException",
            "code": 404,
            "message": "Table not found: db.table"
        }"#;

        let error: ErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(error.code, 404);
        assert!(error.message.contains("Table not found"));
    }
}
