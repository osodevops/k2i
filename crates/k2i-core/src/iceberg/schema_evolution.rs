//! Schema evolution for automatic schema detection and updates.
//!
//! This module provides functionality for:
//! - Inferring schema from incoming JSON messages
//! - Detecting schema differences between existing and incoming schemas
//! - Determining safe type promotions
//! - Applying schema evolution (add columns only, for safety)

use crate::iceberg::factory::{SchemaFieldInfo, TableSchema};
use crate::{Error, IcebergError, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

/// Represents a field inferred from incoming data.
#[derive(Debug, Clone, PartialEq)]
pub struct InferredField {
    /// Field name
    pub name: String,
    /// Inferred Iceberg type
    pub field_type: IcebergType,
    /// Whether the field appears to be required (seen in all records)
    pub required: bool,
    /// Number of times this field was seen
    pub seen_count: usize,
    /// Total records analyzed
    pub total_records: usize,
}

/// Iceberg primitive and complex types.
#[derive(Debug, Clone, PartialEq)]
pub enum IcebergType {
    /// Boolean type
    Boolean,
    /// 32-bit integer
    Int,
    /// 64-bit integer
    Long,
    /// 32-bit float
    Float,
    /// 64-bit double
    Double,
    /// String type
    String,
    /// Binary data
    Binary,
    /// Date (days since epoch)
    Date,
    /// Time (microseconds since midnight)
    Time,
    /// Timestamp with timezone
    TimestampTz,
    /// Timestamp without timezone
    Timestamp,
    /// UUID type
    Uuid,
    /// Unknown/mixed type
    Unknown,
}

impl IcebergType {
    /// Convert to Iceberg type string.
    pub fn to_iceberg_string(&self) -> &'static str {
        match self {
            IcebergType::Boolean => "boolean",
            IcebergType::Int => "int",
            IcebergType::Long => "long",
            IcebergType::Float => "float",
            IcebergType::Double => "double",
            IcebergType::String => "string",
            IcebergType::Binary => "binary",
            IcebergType::Date => "date",
            IcebergType::Time => "time",
            IcebergType::TimestampTz => "timestamptz",
            IcebergType::Timestamp => "timestamp",
            IcebergType::Uuid => "uuid",
            IcebergType::Unknown => "string", // Default to string for unknown
        }
    }

    /// Parse from Iceberg type string.
    pub fn from_iceberg_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "boolean" => IcebergType::Boolean,
            "int" | "integer" => IcebergType::Int,
            "long" | "bigint" => IcebergType::Long,
            "float" => IcebergType::Float,
            "double" => IcebergType::Double,
            "string" => IcebergType::String,
            "binary" | "bytes" => IcebergType::Binary,
            "date" => IcebergType::Date,
            "time" => IcebergType::Time,
            "timestamptz" | "timestamp_tz" => IcebergType::TimestampTz,
            "timestamp" => IcebergType::Timestamp,
            "uuid" => IcebergType::Uuid,
            _ => IcebergType::Unknown,
        }
    }
}

/// Result of schema comparison.
#[derive(Debug, Clone)]
pub struct SchemaDiff {
    /// New fields that need to be added
    pub new_fields: Vec<InferredField>,
    /// Fields with type changes
    pub type_changes: Vec<TypeChange>,
    /// Fields that exist in the table but not in incoming data
    pub missing_in_incoming: Vec<String>,
    /// Whether the schema is compatible (can be evolved)
    pub is_compatible: bool,
}

/// A type change between existing and incoming schema.
#[derive(Debug, Clone)]
pub struct TypeChange {
    /// Field name
    pub name: String,
    /// Existing type in table
    pub existing_type: String,
    /// Type inferred from incoming data
    pub incoming_type: IcebergType,
    /// Whether this is a safe promotion
    pub is_safe_promotion: bool,
}

/// Schema inference engine.
pub struct SchemaInference;

impl SchemaInference {
    /// Infer schema from a batch of JSON values.
    pub fn infer_from_json(records: &[Value]) -> Vec<InferredField> {
        if records.is_empty() {
            return Vec::new();
        }

        let mut field_stats: HashMap<String, FieldStats> = HashMap::new();
        let total_records = records.len();

        for record in records {
            if let Value::Object(map) = record {
                for (key, value) in map {
                    let entry = field_stats
                        .entry(key.clone())
                        .or_insert_with(|| FieldStats {
                            types_seen: Vec::new(),
                            null_count: 0,
                            seen_count: 0,
                        });

                    entry.seen_count += 1;
                    let inferred_type = Self::infer_type_from_value(value);
                    if inferred_type == IcebergType::Unknown && value.is_null() {
                        entry.null_count += 1;
                    } else {
                        entry.types_seen.push(inferred_type);
                    }
                }
            }
        }

        field_stats
            .into_iter()
            .map(|(name, stats)| {
                let field_type = Self::resolve_type(&stats.types_seen);
                let required = stats.seen_count == total_records && stats.null_count == 0;

                InferredField {
                    name,
                    field_type,
                    required,
                    seen_count: stats.seen_count,
                    total_records,
                }
            })
            .collect()
    }

    /// Infer type from a single JSON value.
    fn infer_type_from_value(value: &Value) -> IcebergType {
        match value {
            Value::Null => IcebergType::Unknown,
            Value::Bool(_) => IcebergType::Boolean,
            Value::Number(n) => {
                if n.is_i64() {
                    let v = n.as_i64().unwrap();
                    if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
                        IcebergType::Int
                    } else {
                        IcebergType::Long
                    }
                } else if n.is_f64() {
                    IcebergType::Double
                } else {
                    IcebergType::Long
                }
            }
            Value::String(s) => {
                // Try to detect special types
                if Self::looks_like_timestamp(s) {
                    IcebergType::TimestampTz
                } else if Self::looks_like_date(s) {
                    IcebergType::Date
                } else if Self::looks_like_uuid(s) {
                    IcebergType::Uuid
                } else {
                    IcebergType::String
                }
            }
            Value::Array(_) => IcebergType::String, // Serialize arrays as JSON strings
            Value::Object(_) => IcebergType::String, // Serialize objects as JSON strings
        }
    }

    /// Check if string looks like a timestamp.
    fn looks_like_timestamp(s: &str) -> bool {
        // ISO 8601 timestamp patterns
        s.contains('T') && (s.contains('Z') || s.contains('+') || s.contains('-')) && s.len() >= 19
    }

    /// Check if string looks like a date.
    fn looks_like_date(s: &str) -> bool {
        // YYYY-MM-DD pattern
        s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-')
    }

    /// Check if string looks like a UUID.
    fn looks_like_uuid(s: &str) -> bool {
        s.len() == 36
            && s.chars().nth(8) == Some('-')
            && s.chars().nth(13) == Some('-')
            && s.chars().nth(18) == Some('-')
            && s.chars().nth(23) == Some('-')
    }

    /// Resolve the final type from multiple observed types.
    fn resolve_type(types: &[IcebergType]) -> IcebergType {
        if types.is_empty() {
            return IcebergType::String; // Default to string for null-only fields
        }

        // If all types are the same, use that type
        let first = &types[0];
        if types.iter().all(|t| t == first) {
            return first.clone();
        }

        // Check for numeric promotion
        let has_int = types.iter().any(|t| *t == IcebergType::Int);
        let has_long = types.iter().any(|t| *t == IcebergType::Long);
        let has_float = types.iter().any(|t| *t == IcebergType::Float);
        let has_double = types.iter().any(|t| *t == IcebergType::Double);

        if has_double || (has_float && (has_int || has_long)) {
            return IcebergType::Double;
        }
        if has_float {
            return IcebergType::Float;
        }
        if has_long || has_int {
            return IcebergType::Long;
        }

        // Default to string for mixed types
        IcebergType::String
    }
}

/// Internal stats for type inference.
struct FieldStats {
    types_seen: Vec<IcebergType>,
    null_count: usize,
    seen_count: usize,
}

/// Schema comparison and evolution logic.
pub struct SchemaEvolution;

impl SchemaEvolution {
    /// Compare inferred schema with existing table schema.
    pub fn compare(existing: &TableSchema, incoming: &[InferredField]) -> SchemaDiff {
        let existing_fields: HashMap<&str, &SchemaFieldInfo> = existing
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f))
            .collect();

        let incoming_names: HashSet<&str> = incoming.iter().map(|f| f.name.as_str()).collect();

        let mut new_fields = Vec::new();
        let mut type_changes = Vec::new();
        let mut is_compatible = true;

        for field in incoming {
            match existing_fields.get(field.name.as_str()) {
                None => {
                    // New field
                    new_fields.push(field.clone());
                    debug!(field = %field.name, field_type = %field.field_type.to_iceberg_string(), "Detected new field");
                }
                Some(existing_field) => {
                    // Check type compatibility
                    let existing_type =
                        IcebergType::from_iceberg_string(&existing_field.field_type);
                    if existing_type != field.field_type {
                        // First check if incoming data fits in existing type (no change needed)
                        if Self::fits_in_existing(&existing_type, &field.field_type) {
                            // Incoming data can be stored in existing schema, no change needed
                            debug!(
                                field = %field.name,
                                existing = %existing_field.field_type,
                                incoming = %field.field_type.to_iceberg_string(),
                                "Incoming type fits in existing schema type"
                            );
                            continue;
                        }

                        // Check if we can safely promote the existing type
                        let is_safe = Self::is_safe_promotion(&existing_type, &field.field_type);
                        if !is_safe {
                            is_compatible = false;
                        }
                        type_changes.push(TypeChange {
                            name: field.name.clone(),
                            existing_type: existing_field.field_type.clone(),
                            incoming_type: field.field_type.clone(),
                            is_safe_promotion: is_safe,
                        });
                    }
                }
            }
        }

        // Find fields in existing schema that are not in incoming data
        let missing_in_incoming: Vec<String> = existing
            .fields
            .iter()
            .filter(|f| !incoming_names.contains(f.name.as_str()))
            .map(|f| f.name.clone())
            .collect();

        SchemaDiff {
            new_fields,
            type_changes,
            missing_in_incoming,
            is_compatible,
        }
    }

    /// Check if a type promotion is safe (widening, not narrowing).
    pub fn is_safe_promotion(from: &IcebergType, to: &IcebergType) -> bool {
        use IcebergType::*;

        matches!(
            (from, to),
            // Integer promotions
            (Int, Long) |
            // Float promotions
            (Float, Double) |
            // Integer to float (with potential precision loss, but no data loss)
            (Int, Float) | (Int, Double) |
            (Long, Double) |
            // Any type to string (always safe)
            (_, String)
        )
    }

    /// Check if incoming type fits in existing type (no schema change needed).
    ///
    /// This is true when incoming data can be stored in the existing schema without modification.
    pub fn fits_in_existing(existing: &IcebergType, incoming: &IcebergType) -> bool {
        if existing == incoming {
            return true;
        }

        use IcebergType::*;

        matches!(
            (existing, incoming),
            // Wider types can hold narrower types
            (Long, Int) |
            (Double, Float) |
            (Double, Int) | (Double, Long) |
            (Float, Int) |
            // String can hold anything that was serialized as string
            (String, _)
        )
    }

    /// Generate new schema fields to add.
    ///
    /// Only returns fields that are safe to add (new columns).
    pub fn fields_to_add(diff: &SchemaDiff, next_field_id: i32) -> Vec<SchemaFieldInfo> {
        diff.new_fields
            .iter()
            .enumerate()
            .map(|(i, field)| SchemaFieldInfo {
                id: next_field_id + i as i32,
                name: field.name.clone(),
                field_type: field.field_type.to_iceberg_string().to_string(),
                required: false, // Always make new columns nullable for safety
                doc: Some(format!("Auto-added field from schema evolution")),
            })
            .collect()
    }

    /// Validate that schema evolution is possible.
    pub fn validate_evolution(diff: &SchemaDiff) -> Result<()> {
        // Check for incompatible type changes
        let unsafe_changes: Vec<_> = diff
            .type_changes
            .iter()
            .filter(|c| !c.is_safe_promotion)
            .collect();

        if !unsafe_changes.is_empty() {
            let changes_desc: Vec<String> = unsafe_changes
                .iter()
                .map(|c| {
                    format!(
                        "{}: {} -> {}",
                        c.name,
                        c.existing_type,
                        c.incoming_type.to_iceberg_string()
                    )
                })
                .collect();

            return Err(Error::Iceberg(IcebergError::SchemaMismatch {
                expected: "compatible types".to_string(),
                actual: format!("incompatible type changes: {}", changes_desc.join(", ")),
            }));
        }

        Ok(())
    }
}

/// Configuration for schema evolution behavior.
#[derive(Debug, Clone)]
pub struct SchemaEvolutionConfig {
    /// Enable automatic field addition
    pub auto_add_fields: bool,
    /// Allow safe type promotions
    pub allow_type_promotion: bool,
    /// Maximum number of fields to add in a single evolution
    pub max_fields_per_evolution: usize,
    /// Log schema changes to transaction log
    pub log_changes: bool,
}

impl Default for SchemaEvolutionConfig {
    fn default() -> Self {
        Self {
            auto_add_fields: true,
            allow_type_promotion: true,
            max_fields_per_evolution: 50,
            log_changes: true,
        }
    }
}

/// Schema evolver that coordinates schema changes.
pub struct SchemaEvolver {
    config: SchemaEvolutionConfig,
}

impl SchemaEvolver {
    /// Create a new schema evolver.
    pub fn new(config: SchemaEvolutionConfig) -> Self {
        Self { config }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(SchemaEvolutionConfig::default())
    }

    /// Process incoming data and determine required schema changes.
    pub fn process_batch(
        &self,
        existing_schema: &TableSchema,
        records: &[Value],
    ) -> Result<Option<SchemaEvolutionPlan>> {
        if records.is_empty() {
            return Ok(None);
        }

        // Infer schema from incoming data
        let inferred = SchemaInference::infer_from_json(records);

        if inferred.is_empty() {
            return Ok(None);
        }

        // Compare with existing schema
        let diff = SchemaEvolution::compare(existing_schema, &inferred);

        // Check if there are any changes needed
        if diff.new_fields.is_empty() && diff.type_changes.is_empty() {
            return Ok(None);
        }

        // Validate evolution is possible
        SchemaEvolution::validate_evolution(&diff)?;

        // Check limits
        if diff.new_fields.len() > self.config.max_fields_per_evolution {
            warn!(
                new_fields = diff.new_fields.len(),
                max_allowed = self.config.max_fields_per_evolution,
                "Too many new fields detected, limiting evolution"
            );
            return Err(Error::Iceberg(IcebergError::Other(format!(
                "Schema evolution would add {} fields, exceeds limit of {}",
                diff.new_fields.len(),
                self.config.max_fields_per_evolution
            ))));
        }

        // Calculate next field ID
        let max_existing_id = existing_schema
            .fields
            .iter()
            .map(|f| f.id)
            .max()
            .unwrap_or(0);
        let next_field_id = max_existing_id + 1;

        // Generate fields to add
        let fields_to_add = if self.config.auto_add_fields {
            SchemaEvolution::fields_to_add(&diff, next_field_id)
        } else {
            Vec::new()
        };

        // Generate safe type promotions
        let type_promotions = if self.config.allow_type_promotion {
            diff.type_changes
                .iter()
                .filter(|c| c.is_safe_promotion)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };

        if fields_to_add.is_empty() && type_promotions.is_empty() {
            return Ok(None);
        }

        info!(
            fields_added = fields_to_add.len(),
            type_promotions = type_promotions.len(),
            "Schema evolution plan generated"
        );

        Ok(Some(SchemaEvolutionPlan {
            fields_to_add,
            type_promotions,
            new_schema_id: existing_schema.schema_id + 1,
        }))
    }
}

/// A plan for schema evolution.
#[derive(Debug, Clone)]
pub struct SchemaEvolutionPlan {
    /// New fields to add
    pub fields_to_add: Vec<SchemaFieldInfo>,
    /// Safe type promotions to apply
    pub type_promotions: Vec<TypeChange>,
    /// New schema ID after evolution
    pub new_schema_id: i32,
}

impl SchemaEvolutionPlan {
    /// Check if this plan has any changes.
    pub fn has_changes(&self) -> bool {
        !self.fields_to_add.is_empty() || !self.type_promotions.is_empty()
    }

    /// Get the names of fields being added.
    pub fn added_field_names(&self) -> Vec<&str> {
        self.fields_to_add.iter().map(|f| f.name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_infer_from_json_basic() {
        let records = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob", "age": 25}),
        ];

        let fields = SchemaInference::infer_from_json(&records);

        assert_eq!(fields.len(), 2);

        let name_field = fields.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.field_type, IcebergType::String);
        assert!(name_field.required);

        let age_field = fields.iter().find(|f| f.name == "age").unwrap();
        assert_eq!(age_field.field_type, IcebergType::Int);
        assert!(age_field.required);
    }

    #[test]
    fn test_infer_nullable_field() {
        let records = vec![
            json!({"name": "Alice", "email": "alice@example.com"}),
            json!({"name": "Bob", "email": null}),
        ];

        let fields = SchemaInference::infer_from_json(&records);

        let email_field = fields.iter().find(|f| f.name == "email").unwrap();
        assert!(!email_field.required);
    }

    #[test]
    fn test_infer_missing_field() {
        let records = vec![
            json!({"name": "Alice", "email": "alice@example.com"}),
            json!({"name": "Bob"}), // Missing email
        ];

        let fields = SchemaInference::infer_from_json(&records);

        let email_field = fields.iter().find(|f| f.name == "email").unwrap();
        assert!(!email_field.required);
        assert_eq!(email_field.seen_count, 1);
    }

    #[test]
    fn test_infer_timestamp() {
        let records = vec![json!({"created_at": "2024-01-15T10:30:00Z"})];

        let fields = SchemaInference::infer_from_json(&records);

        let ts_field = fields.iter().find(|f| f.name == "created_at").unwrap();
        assert_eq!(ts_field.field_type, IcebergType::TimestampTz);
    }

    #[test]
    fn test_infer_date() {
        let records = vec![json!({"birth_date": "1990-05-15"})];

        let fields = SchemaInference::infer_from_json(&records);

        let date_field = fields.iter().find(|f| f.name == "birth_date").unwrap();
        assert_eq!(date_field.field_type, IcebergType::Date);
    }

    #[test]
    fn test_infer_uuid() {
        let records = vec![json!({"id": "550e8400-e29b-41d4-a716-446655440000"})];

        let fields = SchemaInference::infer_from_json(&records);

        let uuid_field = fields.iter().find(|f| f.name == "id").unwrap();
        assert_eq!(uuid_field.field_type, IcebergType::Uuid);
    }

    #[test]
    fn test_infer_numeric_promotion() {
        let records = vec![
            json!({"value": 100}),           // Int
            json!({"value": 5000000000i64}), // Long (exceeds i32)
        ];

        let fields = SchemaInference::infer_from_json(&records);

        let value_field = fields.iter().find(|f| f.name == "value").unwrap();
        assert_eq!(value_field.field_type, IcebergType::Long);
    }

    #[test]
    fn test_schema_diff_new_fields() {
        let existing = TableSchema {
            schema_id: 1,
            fields: vec![SchemaFieldInfo {
                id: 1,
                name: "id".to_string(),
                field_type: "long".to_string(),
                required: true,
                doc: None,
            }],
        };

        let incoming = vec![
            InferredField {
                name: "id".to_string(),
                field_type: IcebergType::Long,
                required: true,
                seen_count: 1,
                total_records: 1,
            },
            InferredField {
                name: "name".to_string(),
                field_type: IcebergType::String,
                required: true,
                seen_count: 1,
                total_records: 1,
            },
        ];

        let diff = SchemaEvolution::compare(&existing, &incoming);

        assert_eq!(diff.new_fields.len(), 1);
        assert_eq!(diff.new_fields[0].name, "name");
        assert!(diff.is_compatible);
    }

    #[test]
    fn test_schema_diff_type_change() {
        let existing = TableSchema {
            schema_id: 1,
            fields: vec![SchemaFieldInfo {
                id: 1,
                name: "value".to_string(),
                field_type: "int".to_string(),
                required: true,
                doc: None,
            }],
        };

        let incoming = vec![InferredField {
            name: "value".to_string(),
            field_type: IcebergType::Long, // Promotion from int to long
            required: true,
            seen_count: 1,
            total_records: 1,
        }];

        let diff = SchemaEvolution::compare(&existing, &incoming);

        assert_eq!(diff.type_changes.len(), 1);
        assert!(diff.type_changes[0].is_safe_promotion);
        assert!(diff.is_compatible);
    }

    #[test]
    fn test_schema_diff_incompatible_type_change() {
        // Test case: existing schema has int, incoming has string (incompatible)
        let existing = TableSchema {
            schema_id: 1,
            fields: vec![SchemaFieldInfo {
                id: 1,
                name: "value".to_string(),
                field_type: "int".to_string(),
                required: true,
                doc: None,
            }],
        };

        let incoming = vec![InferredField {
            name: "value".to_string(),
            field_type: IcebergType::Boolean, // Boolean cannot be stored as int
            required: true,
            seen_count: 1,
            total_records: 1,
        }];

        let diff = SchemaEvolution::compare(&existing, &incoming);

        assert_eq!(diff.type_changes.len(), 1);
        assert!(!diff.type_changes[0].is_safe_promotion);
        assert!(!diff.is_compatible);
    }

    #[test]
    fn test_schema_diff_incoming_fits_existing() {
        // Test case: existing schema has long, incoming data fits in int
        // This should NOT be flagged as a type change because int fits in long
        let existing = TableSchema {
            schema_id: 1,
            fields: vec![SchemaFieldInfo {
                id: 1,
                name: "value".to_string(),
                field_type: "long".to_string(),
                required: true,
                doc: None,
            }],
        };

        let incoming = vec![InferredField {
            name: "value".to_string(),
            field_type: IcebergType::Int, // Int fits in long
            required: true,
            seen_count: 1,
            total_records: 1,
        }];

        let diff = SchemaEvolution::compare(&existing, &incoming);

        // No type changes should be reported - int fits in long
        assert_eq!(diff.type_changes.len(), 0);
        assert!(diff.is_compatible);
    }

    #[test]
    fn test_safe_promotion_rules() {
        // Safe promotions
        assert!(SchemaEvolution::is_safe_promotion(
            &IcebergType::Int,
            &IcebergType::Long
        ));
        assert!(SchemaEvolution::is_safe_promotion(
            &IcebergType::Float,
            &IcebergType::Double
        ));
        assert!(SchemaEvolution::is_safe_promotion(
            &IcebergType::Int,
            &IcebergType::String
        ));

        // Unsafe promotions
        assert!(!SchemaEvolution::is_safe_promotion(
            &IcebergType::Long,
            &IcebergType::Int
        ));
        assert!(!SchemaEvolution::is_safe_promotion(
            &IcebergType::Double,
            &IcebergType::Float
        ));
        assert!(!SchemaEvolution::is_safe_promotion(
            &IcebergType::String,
            &IcebergType::Int
        ));
    }

    #[test]
    fn test_fields_to_add() {
        let diff = SchemaDiff {
            new_fields: vec![
                InferredField {
                    name: "email".to_string(),
                    field_type: IcebergType::String,
                    required: true,
                    seen_count: 10,
                    total_records: 10,
                },
                InferredField {
                    name: "age".to_string(),
                    field_type: IcebergType::Int,
                    required: false,
                    seen_count: 8,
                    total_records: 10,
                },
            ],
            type_changes: vec![],
            missing_in_incoming: vec![],
            is_compatible: true,
        };

        let new_fields = SchemaEvolution::fields_to_add(&diff, 10);

        assert_eq!(new_fields.len(), 2);
        assert_eq!(new_fields[0].id, 10);
        assert_eq!(new_fields[0].name, "email");
        assert!(!new_fields[0].required); // Always nullable
        assert_eq!(new_fields[1].id, 11);
        assert_eq!(new_fields[1].name, "age");
    }

    #[test]
    fn test_schema_evolver_no_changes() {
        let existing = TableSchema {
            schema_id: 1,
            fields: vec![
                SchemaFieldInfo {
                    id: 1,
                    name: "id".to_string(),
                    field_type: "long".to_string(),
                    required: true,
                    doc: None,
                },
                SchemaFieldInfo {
                    id: 2,
                    name: "name".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    doc: None,
                },
            ],
        };

        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];

        let evolver = SchemaEvolver::with_defaults();
        let plan = evolver.process_batch(&existing, &records).unwrap();

        assert!(plan.is_none());
    }

    #[test]
    fn test_schema_evolver_with_new_field() {
        let existing = TableSchema {
            schema_id: 1,
            fields: vec![SchemaFieldInfo {
                id: 1,
                name: "id".to_string(),
                field_type: "long".to_string(),
                required: true,
                doc: None,
            }],
        };

        let records = vec![
            json!({"id": 1, "email": "alice@example.com"}),
            json!({"id": 2, "email": "bob@example.com"}),
        ];

        let evolver = SchemaEvolver::with_defaults();
        let plan = evolver.process_batch(&existing, &records).unwrap();

        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.fields_to_add.len(), 1);
        assert_eq!(plan.fields_to_add[0].name, "email");
        assert_eq!(plan.new_schema_id, 2);
    }

    #[test]
    fn test_schema_evolver_respects_config() {
        let existing = TableSchema {
            schema_id: 1,
            fields: vec![SchemaFieldInfo {
                id: 1,
                name: "id".to_string(),
                field_type: "long".to_string(),
                required: true,
                doc: None,
            }],
        };

        let records = vec![json!({"id": 1, "email": "alice@example.com"})];

        let config = SchemaEvolutionConfig {
            auto_add_fields: false, // Disable auto-add
            ..Default::default()
        };

        let evolver = SchemaEvolver::new(config);
        let plan = evolver.process_batch(&existing, &records).unwrap();

        // Plan should be None because auto_add_fields is disabled
        assert!(plan.is_none());
    }

    #[test]
    fn test_validate_evolution_compatible() {
        let diff = SchemaDiff {
            new_fields: vec![],
            type_changes: vec![TypeChange {
                name: "value".to_string(),
                existing_type: "int".to_string(),
                incoming_type: IcebergType::Long,
                is_safe_promotion: true,
            }],
            missing_in_incoming: vec![],
            is_compatible: true,
        };

        let result = SchemaEvolution::validate_evolution(&diff);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_evolution_incompatible() {
        let diff = SchemaDiff {
            new_fields: vec![],
            type_changes: vec![TypeChange {
                name: "value".to_string(),
                existing_type: "long".to_string(),
                incoming_type: IcebergType::Int,
                is_safe_promotion: false,
            }],
            missing_in_incoming: vec![],
            is_compatible: false,
        };

        let result = SchemaEvolution::validate_evolution(&diff);
        assert!(result.is_err());
    }
}
