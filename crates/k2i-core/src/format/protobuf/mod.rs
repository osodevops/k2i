//! Protobuf decoding for Confluent Schema Registry payloads.
//!
//! This module is intentionally split between Confluent frame parsing,
//! schema-registry IO, descriptor-to-Arrow mapping, and row decoding. The
//! seams keep the production HTTP path testable without requiring Kafka or a
//! live registry in unit tests.

use super::{
    is_reserved_column, DecodedBatch, MessageDecoder, COL_KEY, COL_MESSAGE_TYPE, COL_OFFSET,
    COL_PARTITION, COL_READ_LSN, COL_SCHEMA_ID, COL_TIMESTAMP, COL_TOPIC, COL_VALUE,
};
use crate::config::{ProtobufFormatConfig, ProtobufSubjectStrategy};
use crate::iceberg::{SchemaFieldInfo, TableSchema};
use crate::kafka::KafkaMessage;
use crate::{BufferError, Error, Result};
use arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_json::ReaderBuilder;
use async_trait::async_trait;
use base64::Engine;
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor,
    Value,
};
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

pub mod frame;
pub mod registry;

use frame::ConfluentProtobufFrame;
use registry::{RegisteredSchema, SchemaRegistryClient};

/// Fully resolved Protobuf schema and its Arrow projection.
#[derive(Debug, Clone)]
pub struct ResolvedProtobufSchema {
    /// Registry schema ID.
    pub schema_id: i32,
    /// Fully-qualified message type.
    pub message_type: String,
    /// Protobuf descriptor pool.
    pub pool: DescriptorPool,
    /// Message descriptor to decode.
    pub message: MessageDescriptor,
    /// Arrow schema containing user fields plus K2I metadata columns.
    pub arrow_schema: SchemaRef,
    /// Iceberg-compatible table schema projection.
    pub table_schema: TableSchema,
}

/// Builds Arrow/Iceberg schemas from Protobuf descriptors.
#[derive(Debug, Default, Clone)]
pub struct ProtobufSchemaResolver;

impl ProtobufSchemaResolver {
    /// Resolve a message from encoded `FileDescriptorSet` bytes.
    ///
    /// Confluent Schema Registry stores `.proto` source text. The production
    /// registry client boundary keeps that acquisition separate from this
    /// descriptor-based resolver, which is also how generated producers expose
    /// descriptor sets in tests and codegen-heavy deployments.
    pub fn resolve_descriptor_set(
        schema_id: i32,
        descriptor_set_bytes: &[u8],
        message_type: &str,
    ) -> Result<ResolvedProtobufSchema> {
        let pool = DescriptorPool::decode(descriptor_set_bytes).map_err(|e| {
            Error::Serialization(format!("failed to decode protobuf descriptor set: {}", e))
        })?;
        Self::resolve_pool(schema_id, pool, message_type)
    }

    /// Resolve a message from an already-built descriptor pool.
    pub fn resolve_pool(
        schema_id: i32,
        pool: DescriptorPool,
        message_type: &str,
    ) -> Result<ResolvedProtobufSchema> {
        let message = pool.get_message_by_name(message_type).ok_or_else(|| {
            Error::Serialization(format!(
                "protobuf message type '{}' not found in descriptor pool",
                message_type
            ))
        })?;

        let arrow_schema = Arc::new(Schema::new(Self::arrow_fields(&message)?));
        let table_schema = Self::table_schema_from_arrow(schema_id, &arrow_schema);

        Ok(ResolvedProtobufSchema {
            schema_id,
            message_type: message.full_name().to_string(),
            pool,
            message,
            arrow_schema,
            table_schema,
        })
    }

    fn arrow_fields(message: &MessageDescriptor) -> Result<Vec<Field>> {
        let mut fields = Vec::new();
        let oneof_field_numbers = Self::non_synthetic_oneof_field_numbers(message);

        for field in message.fields() {
            if oneof_field_numbers.contains(&field.number()) {
                continue;
            }
            if is_reserved_column(field.name()) {
                return Err(Error::Config(format!(
                    "protobuf field '{}' collides with a reserved K2I column",
                    field.name()
                )));
            }
            fields.push(Self::arrow_field(&field)?);
        }

        for oneof in message.oneofs().filter(|oneof| !oneof.is_synthetic()) {
            let name = format!("{}_json", oneof.name());
            if is_reserved_column(&name) {
                return Err(Error::Config(format!(
                    "protobuf oneof '{}' collides with a reserved K2I column",
                    oneof.name()
                )));
            }
            fields.push(
                Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([
                    (
                        "k2i.protobuf.oneof".to_string(),
                        oneof.full_name().to_string(),
                    ),
                    ("k2i.protobuf.encoding".to_string(), "json".to_string()),
                ])),
            );
        }

        fields.extend(metadata_fields());
        Ok(fields)
    }

    fn arrow_field(field: &FieldDescriptor) -> Result<Field> {
        let data_type = Self::arrow_type(field)?;
        let nullable = field.supports_presence() || field.cardinality() != Cardinality::Required;
        let mut metadata = HashMap::new();
        metadata.insert(
            "k2i.protobuf.field_number".to_string(),
            field.number().to_string(),
        );
        metadata.insert(
            "k2i.protobuf.full_name".to_string(),
            field.full_name().to_string(),
        );
        Ok(Field::new(field.name(), data_type, nullable).with_metadata(metadata))
    }

    fn arrow_type(field: &FieldDescriptor) -> Result<DataType> {
        if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                return Err(Error::Serialization(format!(
                    "protobuf map field '{}' did not resolve to a map entry message",
                    field.full_name()
                )));
            };
            let key = entry.map_entry_key_field();
            let value = entry.map_entry_value_field();
            let entries = Field::new_struct(
                "entries",
                vec![
                    Arc::new(Field::new(
                        "key",
                        Self::arrow_type_for_kind(&key.kind())?,
                        false,
                    )),
                    Arc::new(Field::new(
                        "value",
                        Self::arrow_type_for_kind(&value.kind())?,
                        true,
                    )),
                ],
                false,
            );
            return Ok(DataType::Map(Arc::new(entries), false));
        }

        if field.is_list() {
            let item = Field::new("element", Self::arrow_type_for_kind(&field.kind())?, true);
            return Ok(DataType::List(Arc::new(item)));
        }

        Self::arrow_type_for_kind(&field.kind())
    }

    fn arrow_type_for_kind(kind: &Kind) -> Result<DataType> {
        Ok(match kind {
            Kind::Double => DataType::Float64,
            Kind::Float => DataType::Float32,
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => DataType::Int32,
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => DataType::Int64,
            Kind::Uint32 | Kind::Fixed32 => DataType::UInt32,
            Kind::Uint64 | Kind::Fixed64 => DataType::UInt64,
            Kind::Bool => DataType::Boolean,
            Kind::String => DataType::Utf8,
            Kind::Bytes => DataType::Binary,
            // Arrow JSON does not decode Dictionary directly, so v1 stores enum
            // names as Utf8 while retaining enum metadata on the field.
            Kind::Enum(_) => DataType::Utf8,
            Kind::Message(message) if message.full_name() == "google.protobuf.Timestamp" => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            Kind::Message(message) if message.full_name() == "google.protobuf.Duration" => {
                DataType::Duration(TimeUnit::Microsecond)
            }
            Kind::Message(message) => {
                let fields = message
                    .fields()
                    .filter(|nested| nested.containing_oneof().is_none())
                    .map(|nested| Self::arrow_field(&nested).map(Arc::new))
                    .collect::<Result<Vec<_>>>()?;
                DataType::Struct(Fields::from(fields))
            }
        })
    }

    fn non_synthetic_oneof_field_numbers(message: &MessageDescriptor) -> BTreeSet<u32> {
        message
            .oneofs()
            .filter(|oneof| !oneof.is_synthetic())
            .flat_map(|oneof| {
                oneof
                    .fields()
                    .map(|field| field.number())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Convert the Arrow schema into K2I's current Iceberg schema projection.
    pub fn table_schema_from_arrow(schema_id: i32, schema: &Schema) -> TableSchema {
        let mut next_field_id = 1;
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                let id = next_field_id;
                next_field_id += 1;
                SchemaFieldInfo {
                    id,
                    name: field.name().clone(),
                    field_type: iceberg_type_string(field.data_type(), &mut next_field_id),
                    required: !field.is_nullable(),
                    doc: field
                        .metadata()
                        .get("k2i.protobuf.full_name")
                        .map(|name| format!("Decoded from Protobuf field {}", name)),
                }
            })
            .collect();
        TableSchema { schema_id, fields }
    }

    /// Resolve a table schema from a local `.proto` source file.
    pub fn resolve_proto_file(
        schema_id: i32,
        path: &std::path::Path,
        message_type: &str,
    ) -> Result<ResolvedProtobufSchema> {
        let source = std::fs::read_to_string(path)?;
        let root_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::Config(format!("invalid protobuf path {}", path.display())))?;
        let descriptor_set =
            compile_proto_sources(root_name, HashMap::from([(root_name.to_string(), source)]))?;
        Self::resolve_descriptor_set(schema_id, &descriptor_set, message_type)
    }
}

/// Diff result for Protobuf schema evolution.
#[derive(Debug, Clone)]
pub struct ProtobufSchemaDiff {
    /// New fields that can be added automatically.
    pub additive_fields: Vec<SchemaFieldInfo>,
    /// Breaking changes requiring ingest pause/failure.
    pub breaking_changes: Vec<String>,
}

impl ProtobufSchemaDiff {
    /// True when no changes are required.
    pub fn is_empty(&self) -> bool {
        self.additive_fields.is_empty() && self.breaking_changes.is_empty()
    }

    /// True when all changes are additive.
    pub fn is_additive_only(&self) -> bool {
        self.breaking_changes.is_empty()
    }
}

/// Compare existing and incoming table schemas with additive-only policy.
pub fn diff_table_schema(existing: &TableSchema, incoming: &TableSchema) -> ProtobufSchemaDiff {
    let existing_by_name: BTreeMap<&str, &SchemaFieldInfo> = existing
        .fields
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect();
    let incoming_by_name: BTreeMap<&str, &SchemaFieldInfo> = incoming
        .fields
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect();

    let mut additive_fields = Vec::new();
    let mut breaking_changes = Vec::new();

    for incoming_field in &incoming.fields {
        match existing_by_name.get(incoming_field.name.as_str()) {
            None if incoming_field.required => {
                breaking_changes.push(format!(
                    "new required field '{}' is not additive",
                    incoming_field.name
                ));
            }
            None => additive_fields.push(incoming_field.clone()),
            Some(existing_field)
                if !field_types_compatible(
                    &existing_field.field_type,
                    &incoming_field.field_type,
                ) =>
            {
                breaking_changes.push(format!(
                    "field '{}' changed type from '{}' to '{}'",
                    incoming_field.name, existing_field.field_type, incoming_field.field_type
                ));
            }
            Some(existing_field) if existing_field.required != incoming_field.required => {
                breaking_changes.push(format!(
                    "field '{}' changed requiredness from {} to {}",
                    incoming_field.name, existing_field.required, incoming_field.required
                ));
            }
            Some(_) => {}
        }
    }

    for existing_field in &existing.fields {
        if !incoming_by_name.contains_key(existing_field.name.as_str()) {
            breaking_changes.push(format!("field '{}' was removed", existing_field.name));
        }
    }

    ProtobufSchemaDiff {
        additive_fields,
        breaking_changes,
    }
}

fn field_types_compatible(existing: &str, incoming: &str) -> bool {
    normalized_field_type(existing) == normalized_field_type(incoming)
}

fn normalized_field_type(field_type: &str) -> JsonValue {
    match serde_json::from_str::<JsonValue>(field_type) {
        Ok(mut value) => {
            strip_generated_iceberg_ids(&mut value);
            value
        }
        Err(_) => JsonValue::String(field_type.to_string()),
    }
}

fn strip_generated_iceberg_ids(value: &mut JsonValue) {
    match value {
        JsonValue::Object(map) => {
            map.remove("id");
            map.remove("element-id");
            map.remove("key-id");
            map.remove("value-id");
            map.remove("doc");
            for child in map.values_mut() {
                strip_generated_iceberg_ids(child);
            }
        }
        JsonValue::Array(values) => {
            for child in values {
                strip_generated_iceberg_ids(child);
            }
        }
        _ => {}
    }
}

/// Protobuf decoder backed by an in-process descriptor cache.
pub struct ProtobufDecoder {
    registry: Arc<dyn SchemaRegistryClient>,
    config: ProtobufFormatConfig,
    resolved: parking_lot::RwLock<HashMap<i32, Arc<ResolvedProtobufSchema>>>,
}

impl std::fmt::Debug for ProtobufDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufDecoder")
            .field("config", &self.config)
            .field("resolved_schema_count", &self.resolved.read().len())
            .finish()
    }
}

impl ProtobufDecoder {
    /// Create a decoder with an injected registry client.
    pub fn new(registry: Arc<dyn SchemaRegistryClient>, config: ProtobufFormatConfig) -> Self {
        Self {
            registry,
            config,
            resolved: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Resolve and cache the latest schema for the configured subject.
    pub async fn resolve_latest_table_schema(&self, topic: &str) -> Result<TableSchema> {
        let subject = self.subject_for_topic(topic)?;
        let registered = self.registry.get_latest(&subject).await?;
        Ok(self
            .resolve_registered_schema(registered)
            .await?
            .table_schema
            .clone())
    }

    /// Build the Schema Registry subject for the configured strategy.
    pub fn subject_for_topic(&self, topic: &str) -> Result<String> {
        match self.config.subject_strategy {
            ProtobufSubjectStrategy::TopicName => Ok(format!("{}-value", topic)),
            ProtobufSubjectStrategy::RecordName => {
                self.config.message_type.clone().ok_or_else(|| {
                    Error::Config(
                        "kafka.format.message_type is required for record_name subject strategy"
                            .to_string(),
                    )
                })
            }
            ProtobufSubjectStrategy::TopicRecordName => {
                let message_type = self.config.message_type.as_ref().ok_or_else(|| {
                    Error::Config(
                        "kafka.format.message_type is required for topic_record_name subject strategy"
                            .to_string(),
                    )
                })?;
                Ok(format!("{}-{}", topic, message_type))
            }
        }
    }

    async fn resolve_schema(&self, schema_id: i32) -> Result<Arc<ResolvedProtobufSchema>> {
        if let Some(schema) = self.resolved.read().get(&schema_id) {
            return Ok(schema.clone());
        }

        let registered = self.registry.get_by_id(schema_id).await?;
        self.resolve_registered_schema(registered).await
    }

    async fn resolve_registered_schema(
        &self,
        registered: RegisteredSchema,
    ) -> Result<Arc<ResolvedProtobufSchema>> {
        if !registered.schema_type.eq_ignore_ascii_case("PROTOBUF") {
            return Err(Error::Serialization(format!(
                "schema ID {} has schemaType '{}', expected PROTOBUF",
                registered.id, registered.schema_type
            )));
        }

        let descriptor_bytes = match &registered.descriptor_set_bytes {
            Some(bytes) => bytes.clone(),
            None => self.compile_registry_schema(&registered).await?,
        };
        let pool = DescriptorPool::decode(descriptor_bytes.as_slice()).map_err(|e| {
            Error::Serialization(format!("failed to decode protobuf descriptor set: {}", e))
        })?;
        let message_type = self
            .message_type(&registered)
            .unwrap_or_else(|| infer_single_message_type(&pool));
        let resolved = Arc::new(ProtobufSchemaResolver::resolve_pool(
            registered.id,
            pool,
            &message_type?,
        )?);
        self.resolved
            .write()
            .insert(registered.id, resolved.clone());
        Ok(resolved)
    }

    async fn compile_registry_schema(&self, registered: &RegisteredSchema) -> Result<Vec<u8>> {
        let root_name = registered
            .subject
            .as_deref()
            .filter(|subject| subject.ends_with(".proto"))
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("schema-{}.proto", registered.id));
        let mut sources = HashMap::from([(root_name.clone(), registered.schema.clone())]);
        let mut stack = vec![registered.clone()];
        let mut seen = BTreeSet::new();

        while let Some(schema) = stack.pop() {
            for reference in schema.references {
                let key = (reference.subject.clone(), reference.version);
                if !seen.insert(key) {
                    continue;
                }
                let child = self
                    .registry
                    .get_version(&reference.subject, reference.version)
                    .await?;
                sources
                    .entry(reference.name.clone())
                    .or_insert_with(|| child.schema.clone());
                stack.push(child);
            }
        }

        compile_proto_sources(&root_name, sources)
    }

    fn message_type(&self, registered: &RegisteredSchema) -> Option<Result<String>> {
        if let Some(message_type) = &self.config.message_type {
            return Some(Ok(message_type.clone()));
        }
        registered.message_type.clone().map(Ok)
    }

    fn decode_rows_projected(
        target_schema: &ResolvedProtobufSchema,
        resolved_by_id: &HashMap<i32, Arc<ResolvedProtobufSchema>>,
        messages: &[KafkaMessage],
        frames: &[ConfluentProtobufFrame<'_>],
        read_lsns: &[u64],
    ) -> Result<RecordBatch> {
        let mut rows = Vec::with_capacity(messages.len());
        let mut key_builder = BinaryBuilder::new();
        let mut value_builder = BinaryBuilder::new();
        let mut topic_builder = StringBuilder::new();
        let mut partition_builder = Int32Builder::new();
        let mut offset_builder = Int64Builder::new();
        let mut timestamp_builder = Int64Builder::new();
        let mut read_lsn_builder = UInt64Builder::new();
        let mut schema_id_builder = Int32Builder::new();
        let mut message_type_builder = StringBuilder::new();

        for ((msg, frame), read_lsn) in messages.iter().zip(frames).zip(read_lsns) {
            let schema = resolved_by_id.get(&frame.schema_id).ok_or_else(|| {
                Error::Serialization(format!(
                    "protobuf schema ID {} was parsed but not resolved",
                    frame.schema_id
                ))
            })?;
            let dynamic =
                DynamicMessage::decode(schema.message.clone(), frame.payload).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to decode protobuf payload at {}:{}:{} with schema ID {}: {}",
                        msg.topic, msg.partition, msg.offset, frame.schema_id, e
                    ))
                })?;

            rows.push(message_to_json(&schema.message, &dynamic)?);

            match &msg.key {
                Some(key) => key_builder.append_value(key),
                None => key_builder.append_null(),
            }
            match &msg.value {
                Some(value) => value_builder.append_value(value),
                None => value_builder.append_null(),
            }
            topic_builder.append_value(&msg.topic);
            partition_builder.append_value(msg.partition);
            offset_builder.append_value(msg.offset);
            timestamp_builder.append_value(msg.timestamp);
            read_lsn_builder.append_value(*read_lsn);
            schema_id_builder.append_value(frame.schema_id);
            message_type_builder.append_value(&schema.message_type);
        }

        let user_schema = Arc::new(Schema::new(
            target_schema
                .arrow_schema
                .fields()
                .iter()
                .filter(|field| !is_k2i_metadata_field(field.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let mut decoder = ReaderBuilder::new(user_schema)
            .with_coerce_primitive(true)
            .build_decoder()
            .map_err(|e| Error::Buffer(BufferError::ArrowConversion(e.to_string())))?;
        decoder
            .serialize(&rows)
            .map_err(|e| Error::Buffer(BufferError::ArrowConversion(e.to_string())))?;
        let user_batch = decoder
            .flush()
            .map_err(|e| Error::Buffer(BufferError::ArrowConversion(e.to_string())))?
            .ok_or(Error::Buffer(BufferError::Empty))?;

        let mut arrays = user_batch.columns().to_vec();
        arrays.extend(vec![
            Arc::new(key_builder.finish()) as _,
            Arc::new(value_builder.finish()) as _,
            Arc::new(topic_builder.finish()) as _,
            Arc::new(partition_builder.finish()) as _,
            Arc::new(offset_builder.finish()) as _,
            Arc::new(timestamp_builder.finish()) as _,
            Arc::new(read_lsn_builder.finish()) as _,
            Arc::new(schema_id_builder.finish()) as _,
            Arc::new(message_type_builder.finish()) as _,
        ]);

        RecordBatch::try_new(target_schema.arrow_schema.clone(), arrays)
            .map_err(|e| Error::Buffer(BufferError::ArrowConversion(e.to_string())))
    }
}

fn infer_single_message_type(pool: &DescriptorPool) -> Result<String> {
    let messages = pool
        .all_messages()
        .filter(|message| !message.is_map_entry())
        .collect::<Vec<_>>();
    match messages.as_slice() {
        [message] => Ok(message.full_name().to_string()),
        [] => Err(Error::Config(
            "protobuf schema contains no message types".to_string(),
        )),
        _ => Err(Error::Config(
            "kafka.format.message_type is required when a protobuf schema contains multiple message types".to_string(),
        )),
    }
}

#[derive(Debug, Clone)]
struct SourceMapResolver {
    sources: HashMap<String, String>,
}

impl FileResolver for SourceMapResolver {
    fn resolve_path(&self, path: &Path) -> Option<String> {
        let name = path.to_string_lossy().to_string();
        self.sources.contains_key(&name).then_some(name)
    }

    fn open_file(&self, name: &str) -> std::result::Result<File, protox::Error> {
        self.sources
            .get(name)
            .ok_or_else(|| protox::Error::file_not_found(name))
            .and_then(|source| File::from_source(name, source))
    }
}

fn compile_proto_sources(root_name: &str, sources: HashMap<String, String>) -> Result<Vec<u8>> {
    let mut chain = ChainFileResolver::new();
    chain.add(SourceMapResolver { sources });
    chain.add(GoogleFileResolver::new());

    let mut compiler = protox::Compiler::with_file_resolver(chain);
    compiler.include_imports(true);
    compiler
        .open_file(root_name)
        .map_err(|e| Error::Serialization(format!("failed to compile protobuf schema: {}", e)))?;
    Ok(compiler.encode_file_descriptor_set())
}

#[async_trait]
impl MessageDecoder for ProtobufDecoder {
    fn format_name(&self) -> &'static str {
        "protobuf"
    }

    async fn decode_batch(
        &self,
        messages: &[KafkaMessage],
        read_lsns: &[u64],
    ) -> Result<DecodedBatch> {
        if messages.len() != read_lsns.len() {
            return Err(Error::Buffer(BufferError::SchemaMismatch {
                expected: format!("{} read LSNs", messages.len()),
                actual: format!("{} read LSNs", read_lsns.len()),
            }));
        }

        if messages.is_empty() {
            let schema = Arc::new(Schema::empty());
            return Ok(DecodedBatch::new(
                RecordBatch::new_empty(schema),
                "protobuf",
                vec![],
                None,
            ));
        }

        let frames = messages
            .iter()
            .map(|msg| {
                let payload = msg.value.as_deref().ok_or_else(|| {
                    Error::Serialization(format!(
                        "protobuf message at {}:{}:{} has null Kafka value",
                        msg.topic, msg.partition, msg.offset
                    ))
                })?;
                ConfluentProtobufFrame::parse(payload)
            })
            .collect::<Result<Vec<_>>>()?;

        let schema_ids: BTreeSet<i32> = frames.iter().map(|frame| frame.schema_id).collect();
        let mut resolved_by_id = HashMap::new();
        for schema_id in &schema_ids {
            resolved_by_id.insert(*schema_id, self.resolve_schema(*schema_id).await?);
        }

        let target_schema_id = *schema_ids.iter().next_back().expect("non-empty schema IDs");
        let target_schema = resolved_by_id
            .get(&target_schema_id)
            .expect("target schema was resolved")
            .clone();
        let batch = Self::decode_rows_projected(
            &target_schema,
            &resolved_by_id,
            messages,
            &frames,
            read_lsns,
        )?;
        Ok(DecodedBatch::new(
            batch,
            self.format_name(),
            schema_ids.into_iter().collect(),
            Some(target_schema.table_schema.clone()),
        ))
    }
}

fn metadata_fields() -> Vec<Field> {
    vec![
        Field::new(COL_KEY, DataType::Binary, true),
        Field::new(COL_VALUE, DataType::Binary, true),
        Field::new(COL_TOPIC, DataType::Utf8, false),
        Field::new(COL_PARTITION, DataType::Int32, false),
        Field::new(COL_OFFSET, DataType::Int64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new(COL_READ_LSN, DataType::UInt64, false),
        Field::new(COL_SCHEMA_ID, DataType::Int32, false),
        Field::new(COL_MESSAGE_TYPE, DataType::Utf8, false),
    ]
}

fn is_k2i_metadata_field(name: &str) -> bool {
    matches!(
        name,
        COL_KEY
            | COL_VALUE
            | COL_TOPIC
            | COL_PARTITION
            | COL_OFFSET
            | COL_TIMESTAMP
            | COL_READ_LSN
            | COL_SCHEMA_ID
            | COL_MESSAGE_TYPE
    )
}

fn iceberg_type_string(data_type: &DataType, next_field_id: &mut i32) -> String {
    match iceberg_type_value(data_type, next_field_id) {
        JsonValue::String(value) => value,
        value => value.to_string(),
    }
}

fn iceberg_type_value(data_type: &DataType, next_field_id: &mut i32) -> JsonValue {
    match data_type {
        DataType::Boolean => json!("boolean"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => json!("int"),
        DataType::Int64 => json!("long"),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => json!("int"),
        DataType::UInt64 => json!("long"),
        DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => json!("binary"),
        DataType::Timestamp(_, Some(_)) => json!("timestamptz"),
        DataType::Timestamp(_, None) => json!("timestamp"),
        DataType::Duration(_) => json!("long"),
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| iceberg_nested_field(field, next_field_id))
                .collect::<Vec<_>>();
            json!({
                "type": "struct",
                "fields": fields,
            })
        }
        DataType::List(field) | DataType::LargeList(field) => json!({
            "type": "list",
            "element-id": allocate_field_id(next_field_id),
            "element": iceberg_type_value(field.data_type(), next_field_id),
            "element-required": !field.is_nullable(),
        }),
        DataType::Map(entries, _) => {
            let (key_type, value_type, value_required) = match entries.data_type() {
                DataType::Struct(fields) => {
                    let key = fields
                        .iter()
                        .find(|field| field.name() == "key")
                        .map(|field| iceberg_type_value(field.data_type(), next_field_id))
                        .unwrap_or_else(|| json!("string"));
                    let value = fields
                        .iter()
                        .find(|field| field.name() == "value")
                        .map(|field| {
                            (
                                iceberg_type_value(field.data_type(), next_field_id),
                                !field.is_nullable(),
                            )
                        })
                        .unwrap_or_else(|| (json!("string"), false));
                    (key, value.0, value.1)
                }
                _ => (json!("string"), json!("string"), false),
            };
            json!({
                "type": "map",
                "key-id": allocate_field_id(next_field_id),
                "key": key_type,
                "value-id": allocate_field_id(next_field_id),
                "value": value_type,
                "value-required": value_required,
            })
        }
        _ => json!("string"),
    }
}

fn iceberg_nested_field(field: &Field, next_field_id: &mut i32) -> JsonValue {
    json!({
        "id": allocate_field_id(next_field_id),
        "name": field.name(),
        "required": !field.is_nullable(),
        "type": iceberg_type_value(field.data_type(), next_field_id),
        "doc": field.metadata().get("k2i.protobuf.full_name"),
    })
}

fn allocate_field_id(next_field_id: &mut i32) -> i32 {
    let id = *next_field_id;
    *next_field_id += 1;
    id
}

fn message_to_json(descriptor: &MessageDescriptor, message: &DynamicMessage) -> Result<JsonValue> {
    let mut row = JsonMap::new();
    let oneof_field_numbers = ProtobufSchemaResolver::non_synthetic_oneof_field_numbers(descriptor);

    for field in descriptor.fields() {
        if oneof_field_numbers.contains(&field.number()) {
            continue;
        }
        let value = field_value_to_json(message, &field)?;
        row.insert(field.name().to_string(), value);
    }

    for oneof in descriptor.oneofs().filter(|oneof| !oneof.is_synthetic()) {
        let mut encoded = None;
        for field in oneof.fields() {
            if message.has_field(&field) {
                let value = message.get_field(&field);
                encoded = Some(json!({
                    "field": field.name(),
                    "field_number": field.number(),
                    "value": value_to_json(&field, value.as_ref())?,
                }));
                break;
            }
        }
        row.insert(
            format!("{}_json", oneof.name()),
            encoded
                .map(|value| JsonValue::String(value.to_string()))
                .unwrap_or(JsonValue::Null),
        );
    }

    Ok(JsonValue::Object(row))
}

fn field_value_to_json(message: &DynamicMessage, field: &FieldDescriptor) -> Result<JsonValue> {
    if field.supports_presence() && !message.has_field(field) {
        return Ok(JsonValue::Null);
    }
    let value = message.get_field(field);
    value_to_json(field, value.as_ref())
}

fn value_to_json(field: &FieldDescriptor, value: &Value) -> Result<JsonValue> {
    if field.is_map() {
        return map_to_json(value);
    }
    if field.is_list() {
        return list_to_json(field, value);
    }
    scalar_value_to_json(&field.kind(), value)
}

fn scalar_value_to_json(kind: &Kind, value: &Value) -> Result<JsonValue> {
    Ok(match (kind, value) {
        (_, Value::Bool(v)) => JsonValue::Bool(*v),
        (_, Value::I32(v)) => json!(*v),
        (_, Value::I64(v)) => json!(*v),
        (_, Value::U32(v)) => json!(*v),
        (_, Value::U64(v)) => json!(*v),
        (_, Value::F32(v)) => json!(*v),
        (_, Value::F64(v)) => json!(*v),
        (_, Value::String(v)) => JsonValue::String(v.clone()),
        (_, Value::Bytes(v)) => {
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(v))
        }
        (Kind::Enum(enum_desc), Value::EnumNumber(number)) => enum_desc
            .get_value(*number)
            .map(|value| JsonValue::String(value.name().to_string()))
            .unwrap_or_else(|| JsonValue::String(number.to_string())),
        (Kind::Message(message_desc), Value::Message(message))
            if message_desc.full_name() == "google.protobuf.Timestamp" =>
        {
            timestamp_to_json(message)?
        }
        (Kind::Message(message_desc), Value::Message(message))
            if message_desc.full_name() == "google.protobuf.Duration" =>
        {
            duration_to_json(message)?
        }
        (Kind::Message(message_desc), Value::Message(message)) => {
            message_to_json(message_desc, message)?
        }
        (_, Value::List(_)) | (_, Value::Map(_)) => {
            return Err(Error::Serialization(
                "nested repeated/map value reached scalar conversion".to_string(),
            ));
        }
        _ => JsonValue::Null,
    })
}

fn list_to_json(field: &FieldDescriptor, value: &Value) -> Result<JsonValue> {
    let Value::List(values) = value else {
        return Ok(JsonValue::Array(Vec::new()));
    };
    let items = values
        .iter()
        .map(|value| scalar_value_to_json(&field.kind(), value))
        .collect::<Result<Vec<_>>>()?;
    Ok(JsonValue::Array(items))
}

fn map_to_json(value: &Value) -> Result<JsonValue> {
    let Value::Map(values) = value else {
        return Ok(JsonValue::Object(JsonMap::new()));
    };
    let mut entries = JsonMap::new();
    for (key, value) in values {
        entries.insert(map_key_to_string(key), primitive_json(value));
    }
    Ok(JsonValue::Object(entries))
}

fn map_key_to_string(key: &MapKey) -> String {
    match key {
        MapKey::Bool(v) => v.to_string(),
        MapKey::I32(v) => v.to_string(),
        MapKey::I64(v) => v.to_string(),
        MapKey::U32(v) => v.to_string(),
        MapKey::U64(v) => v.to_string(),
        MapKey::String(v) => v.clone(),
    }
}

fn primitive_json(value: &Value) -> JsonValue {
    match value {
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::I32(v) => json!(*v),
        Value::I64(v) => json!(*v),
        Value::U32(v) => json!(*v),
        Value::U64(v) => json!(*v),
        Value::F32(v) => json!(*v),
        Value::F64(v) => json!(*v),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Bytes(v) => JsonValue::String(base64::engine::general_purpose::STANDARD.encode(v)),
        Value::EnumNumber(v) => json!(*v),
        Value::Message(_) | Value::List(_) | Value::Map(_) => {
            JsonValue::String(format!("{:?}", value))
        }
    }
}

fn timestamp_to_json(message: &DynamicMessage) -> Result<JsonValue> {
    let seconds = match message.get_field_by_name("seconds").as_deref() {
        Some(Value::I64(seconds)) => *seconds,
        _ => 0,
    };
    let nanos = match message.get_field_by_name("nanos").as_deref() {
        Some(Value::I32(nanos)) => *nanos,
        _ => 0,
    };
    let datetime = chrono::DateTime::from_timestamp(seconds, nanos as u32).ok_or_else(|| {
        Error::Serialization(format!(
            "invalid google.protobuf.Timestamp seconds={} nanos={}",
            seconds, nanos
        ))
    })?;
    Ok(JsonValue::String(datetime.to_rfc3339()))
}

fn duration_to_json(message: &DynamicMessage) -> Result<JsonValue> {
    let seconds = match message.get_field_by_name("seconds").as_deref() {
        Some(Value::I64(seconds)) => *seconds,
        _ => 0,
    };
    let nanos = match message.get_field_by_name("nanos").as_deref() {
        Some(Value::I32(nanos)) => *nanos,
        _ => 0,
    };
    Ok(json!(
        seconds.saturating_mul(1_000_000) + i64::from(nanos / 1_000)
    ))
}

#[cfg(test)]
mod tests {
    use super::frame::encode_signed_varint;
    use super::registry::{InMemorySchemaRegistryClient, RegisteredSchema};
    use super::*;
    use arrow::array::{Array, Float64Array, Int32Array, StringArray, UInt64Array};
    use prost::Message;
    use prost_reflect::MapKey;
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        MessageOptions, OneofDescriptorProto,
    };

    fn field(
        name: &str,
        number: i32,
        proto_type: Type,
        type_name: Option<&str>,
        label: Label,
    ) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            label: Some(label as i32),
            r#type: Some(proto_type as i32),
            type_name: type_name.map(ToString::to_string),
            json_name: Some(name.to_string()),
            ..Default::default()
        }
    }

    fn descriptor_set(include_team: bool, breaking_lap_string: bool) -> FileDescriptorSet {
        let telemetry = DescriptorProto {
            name: Some("Telemetry".to_string()),
            field: vec![
                field("speed", 1, Type::Double, None, Label::Optional),
                field("gear", 2, Type::Int32, None, Label::Optional),
            ],
            ..Default::default()
        };

        let counters_entry = DescriptorProto {
            name: Some("CountersEntry".to_string()),
            field: vec![
                field("key", 1, Type::String, None, Label::Optional),
                field("value", 2, Type::Int32, None, Label::Optional),
            ],
            options: Some(MessageOptions {
                map_entry: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };

        let mut derived_fields = vec![
            field("driver", 1, Type::String, None, Label::Optional),
            field(
                "lap",
                2,
                if breaking_lap_string {
                    Type::String
                } else {
                    Type::Int32
                },
                None,
                Label::Optional,
            ),
            field("sector_time", 3, Type::Double, None, Label::Optional),
            field(
                "telemetry",
                4,
                Type::Message,
                Some(".f1.timing.v1.Telemetry"),
                Label::Optional,
            ),
            field("samples", 5, Type::Double, None, Label::Repeated),
            field(
                "counters",
                6,
                Type::Message,
                Some(".f1.timing.v1.DerivedState.CountersEntry"),
                Label::Repeated,
            ),
        ];

        let mut status_text = field("status_text", 7, Type::String, None, Label::Optional);
        status_text.oneof_index = Some(0);
        let mut status_code = field("status_code", 8, Type::Int32, None, Label::Optional);
        status_code.oneof_index = Some(0);
        derived_fields.push(status_text);
        derived_fields.push(status_code);

        if include_team {
            derived_fields.push(field("team", 9, Type::String, None, Label::Optional));
        }

        let derived = DescriptorProto {
            name: Some("DerivedState".to_string()),
            field: derived_fields,
            nested_type: vec![counters_entry],
            oneof_decl: vec![OneofDescriptorProto {
                name: Some("status".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };

        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("derived_state.proto".to_string()),
                package: Some("f1.timing.v1".to_string()),
                message_type: vec![telemetry, derived],
                syntax: Some("proto3".to_string()),
                ..Default::default()
            }],
        }
    }

    fn encoded_descriptor_set(include_team: bool, breaking_lap_string: bool) -> Vec<u8> {
        descriptor_set(include_team, breaking_lap_string).encode_to_vec()
    }

    fn encoded_payload(descriptor_set: &FileDescriptorSet) -> Vec<u8> {
        let pool = DescriptorPool::from_file_descriptor_set(descriptor_set.clone()).unwrap();
        let derived = pool
            .get_message_by_name("f1.timing.v1.DerivedState")
            .unwrap();
        let telemetry = pool.get_message_by_name("f1.timing.v1.Telemetry").unwrap();

        let mut telemetry_msg = DynamicMessage::new(telemetry);
        telemetry_msg
            .try_set_field_by_name("speed", Value::F64(318.4))
            .unwrap();
        telemetry_msg
            .try_set_field_by_name("gear", Value::I32(7))
            .unwrap();

        let has_team = derived.get_field_by_name("team").is_some();
        let mut msg = DynamicMessage::new(derived);
        msg.try_set_field_by_name("driver", Value::String("VER".to_string()))
            .unwrap();
        msg.try_set_field_by_name("lap", Value::I32(44)).unwrap();
        msg.try_set_field_by_name("sector_time", Value::F64(28.313))
            .unwrap();
        msg.try_set_field_by_name("telemetry", Value::Message(telemetry_msg))
            .unwrap();
        msg.try_set_field_by_name(
            "samples",
            Value::List(vec![Value::F64(1.1), Value::F64(2.2)]),
        )
        .unwrap();
        msg.try_set_field_by_name(
            "counters",
            Value::Map(HashMap::from([(
                MapKey::String("sector_1_ms".to_string()),
                Value::I32(28_313),
            )])),
        )
        .unwrap();
        msg.try_set_field_by_name("status_text", Value::String("green".to_string()))
            .unwrap();
        if has_team {
            msg.try_set_field_by_name("team", Value::String("Red Bull".to_string()))
                .unwrap();
        }

        msg.encode_to_vec()
    }

    fn confluent_payload(schema_id: i32, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0];
        out.extend(schema_id.to_be_bytes());
        out.extend(encode_signed_varint(0));
        out.extend(payload);
        out
    }

    fn kafka_message(value: Vec<u8>) -> KafkaMessage {
        KafkaMessage {
            key: Some(b"driver:VER".to_vec()),
            value: Some(value),
            topic: "f1.timing.derived_state".to_string(),
            partition: 0,
            offset: 12,
            timestamp: 1_700_000_000_000,
            headers: vec![],
        }
    }

    fn protobuf_config() -> ProtobufFormatConfig {
        ProtobufFormatConfig {
            schema_registry_url: "http://registry:8081".to_string(),
            subject_strategy: crate::config::ProtobufSubjectStrategy::TopicName,
            message_type: Some("f1.timing.v1.DerivedState".to_string()),
            cache_ttl_seconds: 300,
            latest_on_startup: true,
        }
    }

    #[test]
    fn descriptor_maps_to_arrow_schema() {
        let resolved = ProtobufSchemaResolver::resolve_descriptor_set(
            101,
            &encoded_descriptor_set(false, false),
            "f1.timing.v1.DerivedState",
        )
        .unwrap();

        let schema = resolved.arrow_schema;
        assert_eq!(
            schema.field_with_name("driver").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("lap").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            schema.field_with_name("sector_time").unwrap().data_type(),
            &DataType::Float64
        );
        assert!(matches!(
            schema.field_with_name("telemetry").unwrap().data_type(),
            DataType::Struct(_)
        ));
        assert!(matches!(
            schema.field_with_name("samples").unwrap().data_type(),
            DataType::List(_)
        ));
        assert!(matches!(
            schema.field_with_name("counters").unwrap().data_type(),
            DataType::Map(_, _)
        ));
        assert_eq!(
            schema.field_with_name("status_json").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(schema.field_with_name(COL_SCHEMA_ID).is_ok());

        let telemetry_type = &resolved
            .table_schema
            .fields
            .iter()
            .find(|field| field.name == "telemetry")
            .unwrap()
            .field_type;
        assert!(telemetry_type.contains(r#""type":"struct""#));
        assert!(telemetry_type.contains(r#""name":"speed""#));
    }

    #[tokio::test]
    async fn decoder_builds_arrow_batch_with_metadata() {
        let descriptor_set = descriptor_set(false, false);
        let payload = encoded_payload(&descriptor_set);
        let registry = Arc::new(InMemorySchemaRegistryClient::default());
        registry.insert(RegisteredSchema {
            id: 101,
            subject: Some("f1.timing.derived_state-value".to_string()),
            version: Some(1),
            schema_type: "PROTOBUF".to_string(),
            schema: "descriptor-set fixture".to_string(),
            references: vec![],
            descriptor_set_bytes: Some(descriptor_set.encode_to_vec()),
            message_type: Some("f1.timing.v1.DerivedState".to_string()),
        });
        let decoder = ProtobufDecoder::new(registry, protobuf_config());
        let msg = kafka_message(confluent_payload(101, &payload));

        let decoded = decoder.decode_batch(&[msg], &[55]).await.unwrap();
        assert_eq!(decoded.schema_ids, vec![101]);
        assert_eq!(decoded.batch.num_rows(), 1);

        let driver = decoded
            .batch
            .column_by_name("driver")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(driver.value(0), "VER");

        let lap = decoded
            .batch
            .column_by_name("lap")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(lap.value(0), 44);

        let sector_time = decoded
            .batch
            .column_by_name("sector_time")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sector_time.value(0), 28.313);

        let status = decoded
            .batch
            .column_by_name("status_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(status.value(0).contains("status_text"));

        let read_lsn = decoded
            .batch
            .column_by_name(COL_READ_LSN)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(read_lsn.value(0), 55);
    }

    #[tokio::test]
    async fn decoder_projects_mixed_additive_schema_ids_to_newest_schema() {
        let descriptor_set_v1 = descriptor_set(false, false);
        let descriptor_set_v2 = descriptor_set(true, false);
        let registry = Arc::new(InMemorySchemaRegistryClient::default());
        registry.insert(RegisteredSchema {
            id: 101,
            subject: Some("f1.timing.derived_state-value".to_string()),
            version: Some(1),
            schema_type: "PROTOBUF".to_string(),
            schema: "descriptor-set fixture".to_string(),
            references: vec![],
            descriptor_set_bytes: Some(descriptor_set_v1.encode_to_vec()),
            message_type: Some("f1.timing.v1.DerivedState".to_string()),
        });
        registry.insert(RegisteredSchema {
            id: 102,
            subject: Some("f1.timing.derived_state-value".to_string()),
            version: Some(2),
            schema_type: "PROTOBUF".to_string(),
            schema: "descriptor-set fixture".to_string(),
            references: vec![],
            descriptor_set_bytes: Some(descriptor_set_v2.encode_to_vec()),
            message_type: Some("f1.timing.v1.DerivedState".to_string()),
        });
        let decoder = ProtobufDecoder::new(registry, protobuf_config());

        let mut msg_v1 =
            kafka_message(confluent_payload(101, &encoded_payload(&descriptor_set_v1)));
        msg_v1.offset = 12;
        let mut msg_v2 =
            kafka_message(confluent_payload(102, &encoded_payload(&descriptor_set_v2)));
        msg_v2.offset = 13;

        let decoded = decoder
            .decode_batch(&[msg_v1, msg_v2], &[55, 56])
            .await
            .unwrap();

        assert_eq!(decoded.schema_ids, vec![101, 102]);
        assert_eq!(decoded.batch.num_rows(), 2);
        assert!(decoded
            .table_schema
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .any(|field| field.name == "team"));

        let team = decoded
            .batch
            .column_by_name("team")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(team.is_null(0));
        assert_eq!(team.value(1), "Red Bull");
    }

    #[tokio::test]
    async fn decoder_compiles_schema_registry_proto_source() {
        let descriptor_set = descriptor_set(false, false);
        let payload = encoded_payload(&descriptor_set);
        let registry = Arc::new(InMemorySchemaRegistryClient::default());
        registry.insert(RegisteredSchema {
            id: 202,
            subject: Some("derived_state.proto".to_string()),
            version: Some(1),
            schema_type: "PROTOBUF".to_string(),
            schema: r#"
                syntax = "proto3";
                package f1.timing.v1;
                message DerivedState {
                  string driver = 1;
                  int32 lap = 2;
                  double sector_time = 3;
                }
            "#
            .to_string(),
            references: vec![],
            descriptor_set_bytes: None,
            message_type: Some("f1.timing.v1.DerivedState".to_string()),
        });
        let decoder = ProtobufDecoder::new(registry, protobuf_config());
        let msg = kafka_message(confluent_payload(202, &payload));

        let decoded = decoder.decode_batch(&[msg], &[77]).await.unwrap();
        assert_eq!(decoded.batch.num_rows(), 1);
        let driver = decoded
            .batch
            .column_by_name("driver")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(driver.value(0), "VER");
    }

    #[tokio::test]
    async fn decoder_resolves_latest_subject_schema_on_startup() {
        let descriptor_set = descriptor_set(false, false);
        let registry = Arc::new(InMemorySchemaRegistryClient::default());
        registry.insert(RegisteredSchema {
            id: 303,
            subject: Some("f1.timing.derived_state-value".to_string()),
            version: Some(7),
            schema_type: "PROTOBUF".to_string(),
            schema: "descriptor-set fixture".to_string(),
            references: vec![],
            descriptor_set_bytes: Some(descriptor_set.encode_to_vec()),
            message_type: Some("f1.timing.v1.DerivedState".to_string()),
        });

        let decoder = ProtobufDecoder::new(registry, protobuf_config());
        let schema = decoder
            .resolve_latest_table_schema("f1.timing.derived_state")
            .await
            .unwrap();

        assert_eq!(schema.schema_id, 303);
        assert!(schema.fields.iter().any(|field| field.name == "driver"));
    }

    #[test]
    fn subject_strategy_builds_expected_subjects() {
        let decoder = ProtobufDecoder::new(
            Arc::new(InMemorySchemaRegistryClient::default()),
            protobuf_config(),
        );
        assert_eq!(decoder.subject_for_topic("events").unwrap(), "events-value");

        let mut config = protobuf_config();
        config.subject_strategy = ProtobufSubjectStrategy::RecordName;
        let decoder =
            ProtobufDecoder::new(Arc::new(InMemorySchemaRegistryClient::default()), config);
        assert_eq!(
            decoder.subject_for_topic("events").unwrap(),
            "f1.timing.v1.DerivedState"
        );

        let mut config = protobuf_config();
        config.subject_strategy = ProtobufSubjectStrategy::TopicRecordName;
        let decoder =
            ProtobufDecoder::new(Arc::new(InMemorySchemaRegistryClient::default()), config);
        assert_eq!(
            decoder.subject_for_topic("events").unwrap(),
            "events-f1.timing.v1.DerivedState"
        );
    }

    #[test]
    fn schema_diff_allows_additive_optional_field() {
        let v1 = ProtobufSchemaResolver::resolve_descriptor_set(
            101,
            &encoded_descriptor_set(false, false),
            "f1.timing.v1.DerivedState",
        )
        .unwrap();
        let v2 = ProtobufSchemaResolver::resolve_descriptor_set(
            102,
            &encoded_descriptor_set(true, false),
            "f1.timing.v1.DerivedState",
        )
        .unwrap();

        let diff = diff_table_schema(&v1.table_schema, &v2.table_schema);
        assert!(diff.is_additive_only());
        assert_eq!(
            diff.additive_fields
                .iter()
                .filter(|f| f.name == "team")
                .count(),
            1
        );
    }

    #[test]
    fn schema_diff_ignores_generated_nested_iceberg_field_ids() {
        let existing = r#"{"type":"struct","fields":[{"id":17,"name":"speed","required":false,"type":"double"},{"id":18,"name":"gear","required":false,"type":"int"}]}"#;
        let incoming = r#"{"type":"struct","fields":[{"id":5,"name":"speed","required":false,"type":"double"},{"id":6,"name":"gear","required":false,"type":"int"}]}"#;
        let existing_list =
            r#"{"type":"list","element-id":19,"element":"double","element-required":false}"#;
        let incoming_list =
            r#"{"type":"list","element-id":8,"element":"double","element-required":false}"#;
        let existing_map = r#"{"type":"map","key-id":20,"key":"string","value-id":21,"value":"int","value-required":false}"#;
        let incoming_map = r#"{"type":"map","key-id":10,"key":"string","value-id":11,"value":"int","value-required":false}"#;

        assert!(field_types_compatible(existing, incoming));
        assert!(field_types_compatible(existing_list, incoming_list));
        assert!(field_types_compatible(existing_map, incoming_map));
    }

    #[test]
    fn schema_diff_rejects_breaking_type_change() {
        let v1 = ProtobufSchemaResolver::resolve_descriptor_set(
            101,
            &encoded_descriptor_set(false, false),
            "f1.timing.v1.DerivedState",
        )
        .unwrap();
        let v2 = ProtobufSchemaResolver::resolve_descriptor_set(
            102,
            &encoded_descriptor_set(false, true),
            "f1.timing.v1.DerivedState",
        )
        .unwrap();

        let diff = diff_table_schema(&v1.table_schema, &v2.table_schema);
        assert!(!diff.is_additive_only());
        assert!(diff
            .breaking_changes
            .iter()
            .any(|change| change.contains("lap") && change.contains("int")));
    }
}
