//! Metadata cache with TTL-based invalidation.
//!
//! Reduces catalog queries by caching frequently accessed metadata:
//! - Snapshot ID: 1 second TTL (very volatile)
//! - Schema: 5 minutes TTL (rarely changes)
//! - Partition spec: 5 minutes TTL (rarely changes)
//! - Manifest entries: 1 minute TTL (for compaction decisions)

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default TTL values per PRD specification.
pub mod ttl {
    use std::time::Duration;

    /// Snapshot ID TTL: 1 second (very volatile)
    pub const SNAPSHOT_ID: Duration = Duration::from_secs(1);

    /// Schema TTL: 5 minutes (rarely changes)
    pub const SCHEMA: Duration = Duration::from_secs(300);

    /// Partition spec TTL: 5 minutes (rarely changes)
    pub const PARTITION_SPEC: Duration = Duration::from_secs(300);

    /// Manifest entries TTL: 1 minute (for compaction decisions)
    pub const MANIFEST: Duration = Duration::from_secs(60);

    /// Table location TTL: 10 minutes (essentially static)
    pub const TABLE_LOCATION: Duration = Duration::from_secs(600);
}

/// A cached value with expiration tracking.
#[derive(Debug, Clone)]
pub struct CachedValue<T> {
    /// The cached value
    pub value: T,
    /// When the value was cached
    pub cached_at: Instant,
    /// Time-to-live for this value
    pub ttl: Duration,
}

impl<T> CachedValue<T> {
    /// Create a new cached value.
    pub fn new(value: T, ttl: Duration) -> Self {
        Self {
            value,
            cached_at: Instant::now(),
            ttl,
        }
    }

    /// Check if the cached value has expired.
    pub fn is_expired(&self) -> bool {
        self.cached_at.elapsed() > self.ttl
    }

    /// Get the age of the cached value.
    pub fn age(&self) -> Duration {
        self.cached_at.elapsed()
    }

    /// Get time remaining before expiration.
    pub fn time_remaining(&self) -> Duration {
        self.ttl.saturating_sub(self.cached_at.elapsed())
    }
}

/// Cached schema information.
#[derive(Debug, Clone)]
pub struct CachedSchema {
    /// Schema ID
    pub schema_id: i32,
    /// Field names and types
    pub fields: Vec<CachedField>,
}

/// Cached field information.
#[derive(Debug, Clone)]
pub struct CachedField {
    /// Field ID
    pub id: i32,
    /// Field name
    pub name: String,
    /// Field type (as string)
    pub field_type: String,
    /// Whether the field is required
    pub required: bool,
}

/// Cached partition specification.
#[derive(Debug, Clone)]
pub struct CachedPartitionSpec {
    /// Spec ID
    pub spec_id: i32,
    /// Partition fields
    pub fields: Vec<CachedPartitionField>,
}

/// Cached partition field.
#[derive(Debug, Clone)]
pub struct CachedPartitionField {
    /// Source field ID
    pub source_id: i32,
    /// Field name
    pub name: String,
    /// Transform type
    pub transform: String,
}

/// Cached manifest entry for compaction decisions.
#[derive(Debug, Clone)]
pub struct CachedManifestEntry {
    /// File path
    pub file_path: String,
    /// File size in bytes
    pub file_size_bytes: u64,
    /// Record count
    pub record_count: u64,
    /// Partition values
    pub partition_values: HashMap<String, String>,
}

/// Configuration for the metadata cache.
#[derive(Debug, Clone)]
pub struct MetadataCacheConfig {
    /// TTL for snapshot ID
    pub snapshot_id_ttl: Duration,
    /// TTL for schema
    pub schema_ttl: Duration,
    /// TTL for partition spec
    pub partition_spec_ttl: Duration,
    /// TTL for manifest entries
    pub manifest_ttl: Duration,
    /// TTL for table location
    pub table_location_ttl: Duration,
    /// Maximum number of manifest entries to cache
    pub max_manifest_entries: usize,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            snapshot_id_ttl: ttl::SNAPSHOT_ID,
            schema_ttl: ttl::SCHEMA,
            partition_spec_ttl: ttl::PARTITION_SPEC,
            manifest_ttl: ttl::MANIFEST,
            table_location_ttl: ttl::TABLE_LOCATION,
            max_manifest_entries: 10000,
        }
    }
}

/// Metadata cache for Iceberg table metadata.
///
/// Thread-safe cache using RwLock for concurrent read access
/// with exclusive write access for updates.
pub struct MetadataCache {
    /// Configuration
    config: MetadataCacheConfig,

    /// Cached snapshot ID
    snapshot_id: RwLock<Option<CachedValue<i64>>>,

    /// Cached schema
    schema: RwLock<Option<CachedValue<CachedSchema>>>,

    /// Cached partition spec
    partition_spec: RwLock<Option<CachedValue<CachedPartitionSpec>>>,

    /// Cached manifest entries
    manifest_entries: RwLock<Option<CachedValue<Vec<CachedManifestEntry>>>>,

    /// Cached table location
    table_location: RwLock<Option<CachedValue<String>>>,

    /// Cache statistics
    stats: CacheStats,
}

/// Cache statistics for monitoring.
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Total cache hits
    pub hits: AtomicU64,
    /// Total cache misses
    pub misses: AtomicU64,
    /// Total invalidations
    pub invalidations: AtomicU64,
    /// Total refreshes
    pub refreshes: AtomicU64,
}

impl CacheStats {
    /// Get the hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Record a cache hit.
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an invalidation.
    pub fn record_invalidation(&self) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a refresh.
    pub fn record_refresh(&self) {
        self.refreshes.fetch_add(1, Ordering::Relaxed);
    }
}

impl MetadataCache {
    /// Create a new metadata cache with default configuration.
    pub fn new() -> Self {
        Self::with_config(MetadataCacheConfig::default())
    }

    /// Create a new metadata cache with custom configuration.
    pub fn with_config(config: MetadataCacheConfig) -> Self {
        Self {
            config,
            snapshot_id: RwLock::new(None),
            schema: RwLock::new(None),
            partition_spec: RwLock::new(None),
            manifest_entries: RwLock::new(None),
            table_location: RwLock::new(None),
            stats: CacheStats::default(),
        }
    }

    /// Get the cached snapshot ID if valid.
    pub fn get_snapshot_id(&self) -> Option<i64> {
        let guard = self.snapshot_id.read();
        if let Some(cached) = guard.as_ref() {
            if !cached.is_expired() {
                self.stats.record_hit();
                return Some(cached.value);
            }
        }
        self.stats.record_miss();
        None
    }

    /// Set the cached snapshot ID.
    pub fn set_snapshot_id(&self, snapshot_id: i64) {
        let mut guard = self.snapshot_id.write();
        *guard = Some(CachedValue::new(snapshot_id, self.config.snapshot_id_ttl));
        self.stats.record_refresh();
    }

    /// Get the cached schema if valid.
    pub fn get_schema(&self) -> Option<CachedSchema> {
        let guard = self.schema.read();
        if let Some(cached) = guard.as_ref() {
            if !cached.is_expired() {
                self.stats.record_hit();
                return Some(cached.value.clone());
            }
        }
        self.stats.record_miss();
        None
    }

    /// Set the cached schema.
    pub fn set_schema(&self, schema: CachedSchema) {
        let mut guard = self.schema.write();
        *guard = Some(CachedValue::new(schema, self.config.schema_ttl));
        self.stats.record_refresh();
    }

    /// Get the cached partition spec if valid.
    pub fn get_partition_spec(&self) -> Option<CachedPartitionSpec> {
        let guard = self.partition_spec.read();
        if let Some(cached) = guard.as_ref() {
            if !cached.is_expired() {
                self.stats.record_hit();
                return Some(cached.value.clone());
            }
        }
        self.stats.record_miss();
        None
    }

    /// Set the cached partition spec.
    pub fn set_partition_spec(&self, spec: CachedPartitionSpec) {
        let mut guard = self.partition_spec.write();
        *guard = Some(CachedValue::new(spec, self.config.partition_spec_ttl));
        self.stats.record_refresh();
    }

    /// Get the cached manifest entries if valid.
    pub fn get_manifest_entries(&self) -> Option<Vec<CachedManifestEntry>> {
        let guard = self.manifest_entries.read();
        if let Some(cached) = guard.as_ref() {
            if !cached.is_expired() {
                self.stats.record_hit();
                return Some(cached.value.clone());
            }
        }
        self.stats.record_miss();
        None
    }

    /// Set the cached manifest entries.
    pub fn set_manifest_entries(&self, entries: Vec<CachedManifestEntry>) {
        let mut guard = self.manifest_entries.write();
        // Limit entries to max configured
        let entries = if entries.len() > self.config.max_manifest_entries {
            entries
                .into_iter()
                .take(self.config.max_manifest_entries)
                .collect()
        } else {
            entries
        };
        *guard = Some(CachedValue::new(entries, self.config.manifest_ttl));
        self.stats.record_refresh();
    }

    /// Get the cached table location if valid.
    pub fn get_table_location(&self) -> Option<String> {
        let guard = self.table_location.read();
        if let Some(cached) = guard.as_ref() {
            if !cached.is_expired() {
                self.stats.record_hit();
                return Some(cached.value.clone());
            }
        }
        self.stats.record_miss();
        None
    }

    /// Set the cached table location.
    pub fn set_table_location(&self, location: String) {
        let mut guard = self.table_location.write();
        *guard = Some(CachedValue::new(location, self.config.table_location_ttl));
        self.stats.record_refresh();
    }

    /// Invalidate all cached data.
    pub fn invalidate_all(&self) {
        {
            let mut guard = self.snapshot_id.write();
            *guard = None;
        }
        {
            let mut guard = self.schema.write();
            *guard = None;
        }
        {
            let mut guard = self.partition_spec.write();
            *guard = None;
        }
        {
            let mut guard = self.manifest_entries.write();
            *guard = None;
        }
        {
            let mut guard = self.table_location.write();
            *guard = None;
        }
        self.stats.record_invalidation();
    }

    /// Invalidate only the snapshot ID (after a commit).
    pub fn invalidate_snapshot(&self) {
        let mut guard = self.snapshot_id.write();
        *guard = None;
        self.stats.record_invalidation();
    }

    /// Invalidate manifest entries (after compaction or new files).
    pub fn invalidate_manifest(&self) {
        let mut guard = self.manifest_entries.write();
        *guard = None;
        self.stats.record_invalidation();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Check if any cache entry is valid (for health checks).
    pub fn has_valid_entries(&self) -> bool {
        self.get_snapshot_id().is_some()
            || self.get_schema().is_some()
            || self.get_partition_spec().is_some()
            || self.get_table_location().is_some()
    }

    /// Get a summary of cache state.
    pub fn summary(&self) -> CacheSummary {
        CacheSummary {
            snapshot_id_cached: self
                .snapshot_id
                .read()
                .as_ref()
                .map(|c| !c.is_expired())
                .unwrap_or(false),
            schema_cached: self
                .schema
                .read()
                .as_ref()
                .map(|c| !c.is_expired())
                .unwrap_or(false),
            partition_spec_cached: self
                .partition_spec
                .read()
                .as_ref()
                .map(|c| !c.is_expired())
                .unwrap_or(false),
            manifest_entries_cached: self
                .manifest_entries
                .read()
                .as_ref()
                .map(|c| !c.is_expired())
                .unwrap_or(false),
            table_location_cached: self
                .table_location
                .read()
                .as_ref()
                .map(|c| !c.is_expired())
                .unwrap_or(false),
            hit_rate: self.stats.hit_rate(),
            total_hits: self.stats.hits.load(Ordering::Relaxed),
            total_misses: self.stats.misses.load(Ordering::Relaxed),
        }
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of cache state for monitoring.
#[derive(Debug, Clone)]
pub struct CacheSummary {
    /// Whether snapshot ID is cached and valid
    pub snapshot_id_cached: bool,
    /// Whether schema is cached and valid
    pub schema_cached: bool,
    /// Whether partition spec is cached and valid
    pub partition_spec_cached: bool,
    /// Whether manifest entries are cached and valid
    pub manifest_entries_cached: bool,
    /// Whether table location is cached and valid
    pub table_location_cached: bool,
    /// Current hit rate
    pub hit_rate: f64,
    /// Total cache hits
    pub total_hits: u64,
    /// Total cache misses
    pub total_misses: u64,
}

/// Thread-safe metadata cache wrapper.
pub type SharedMetadataCache = Arc<MetadataCache>;

/// Create a new shared metadata cache.
pub fn create_shared_cache() -> SharedMetadataCache {
    Arc::new(MetadataCache::new())
}

/// Create a new shared metadata cache with custom config.
pub fn create_shared_cache_with_config(config: MetadataCacheConfig) -> SharedMetadataCache {
    Arc::new(MetadataCache::with_config(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_cache_creation() {
        let cache = MetadataCache::new();
        assert!(cache.get_snapshot_id().is_none());
        assert!(cache.get_schema().is_none());
    }

    #[test]
    fn test_snapshot_id_caching() {
        let cache = MetadataCache::new();

        // Initially empty
        assert!(cache.get_snapshot_id().is_none());

        // Set and get
        cache.set_snapshot_id(12345);
        assert_eq!(cache.get_snapshot_id(), Some(12345));

        // Should still be valid (within 1 second TTL)
        assert_eq!(cache.get_snapshot_id(), Some(12345));
    }

    #[test]
    fn test_snapshot_id_expiration() {
        let config = MetadataCacheConfig {
            snapshot_id_ttl: Duration::from_millis(50),
            ..Default::default()
        };
        let cache = MetadataCache::with_config(config);

        cache.set_snapshot_id(12345);
        assert_eq!(cache.get_snapshot_id(), Some(12345));

        // Wait for expiration
        thread::sleep(Duration::from_millis(100));
        assert!(cache.get_snapshot_id().is_none());
    }

    #[test]
    fn test_schema_caching() {
        let cache = MetadataCache::new();

        let schema = CachedSchema {
            schema_id: 1,
            fields: vec![
                CachedField {
                    id: 1,
                    name: "id".to_string(),
                    field_type: "long".to_string(),
                    required: true,
                },
                CachedField {
                    id: 2,
                    name: "name".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                },
            ],
        };

        cache.set_schema(schema.clone());
        let cached = cache.get_schema().unwrap();
        assert_eq!(cached.schema_id, 1);
        assert_eq!(cached.fields.len(), 2);
    }

    #[test]
    fn test_partition_spec_caching() {
        let cache = MetadataCache::new();

        let spec = CachedPartitionSpec {
            spec_id: 0,
            fields: vec![CachedPartitionField {
                source_id: 1,
                name: "event_date".to_string(),
                transform: "day".to_string(),
            }],
        };

        cache.set_partition_spec(spec);
        let cached = cache.get_partition_spec().unwrap();
        assert_eq!(cached.spec_id, 0);
        assert_eq!(cached.fields.len(), 1);
    }

    #[test]
    fn test_manifest_entries_caching() {
        let cache = MetadataCache::new();

        let entries = vec![
            CachedManifestEntry {
                file_path: "s3://bucket/data/file1.parquet".to_string(),
                file_size_bytes: 1024 * 1024,
                record_count: 1000,
                partition_values: HashMap::new(),
            },
            CachedManifestEntry {
                file_path: "s3://bucket/data/file2.parquet".to_string(),
                file_size_bytes: 2048 * 1024,
                record_count: 2000,
                partition_values: HashMap::new(),
            },
        ];

        cache.set_manifest_entries(entries);
        let cached = cache.get_manifest_entries().unwrap();
        assert_eq!(cached.len(), 2);
    }

    #[test]
    fn test_manifest_entries_limit() {
        let config = MetadataCacheConfig {
            max_manifest_entries: 2,
            ..Default::default()
        };
        let cache = MetadataCache::with_config(config);

        let entries: Vec<CachedManifestEntry> = (0..10)
            .map(|i| CachedManifestEntry {
                file_path: format!("file{}.parquet", i),
                file_size_bytes: 1024,
                record_count: 100,
                partition_values: HashMap::new(),
            })
            .collect();

        cache.set_manifest_entries(entries);
        let cached = cache.get_manifest_entries().unwrap();
        assert_eq!(cached.len(), 2); // Limited to max
    }

    #[test]
    fn test_table_location_caching() {
        let cache = MetadataCache::new();

        cache.set_table_location("s3://bucket/warehouse/db/table".to_string());
        assert_eq!(
            cache.get_table_location(),
            Some("s3://bucket/warehouse/db/table".to_string())
        );
    }

    #[test]
    fn test_invalidate_all() {
        let cache = MetadataCache::new();

        cache.set_snapshot_id(123);
        cache.set_table_location("s3://bucket".to_string());

        assert!(cache.get_snapshot_id().is_some());
        assert!(cache.get_table_location().is_some());

        cache.invalidate_all();

        assert!(cache.get_snapshot_id().is_none());
        assert!(cache.get_table_location().is_none());
    }

    #[test]
    fn test_invalidate_snapshot() {
        let cache = MetadataCache::new();

        cache.set_snapshot_id(123);
        cache.set_table_location("s3://bucket".to_string());

        cache.invalidate_snapshot();

        assert!(cache.get_snapshot_id().is_none());
        assert!(cache.get_table_location().is_some()); // Not invalidated
    }

    #[test]
    fn test_cache_stats() {
        let cache = MetadataCache::new();

        // Miss
        cache.get_snapshot_id();
        assert_eq!(cache.stats().misses.load(Ordering::Relaxed), 1);

        // Set and hit
        cache.set_snapshot_id(123);
        cache.get_snapshot_id();
        assert_eq!(cache.stats().hits.load(Ordering::Relaxed), 1);

        // Hit rate
        assert!(cache.stats().hit_rate() > 0.0);
    }

    #[test]
    fn test_cache_summary() {
        let cache = MetadataCache::new();

        cache.set_snapshot_id(123);
        cache.set_table_location("s3://bucket".to_string());

        let summary = cache.summary();
        assert!(summary.snapshot_id_cached);
        assert!(summary.table_location_cached);
        assert!(!summary.schema_cached);
    }

    #[test]
    fn test_cached_value_time_remaining() {
        let cached = CachedValue::new(42, Duration::from_secs(10));

        // Should have ~10 seconds remaining initially
        assert!(cached.time_remaining() > Duration::from_secs(9));
        assert!(!cached.is_expired());
    }

    #[test]
    fn test_shared_cache() {
        let cache = create_shared_cache();

        cache.set_snapshot_id(999);
        assert_eq!(cache.get_snapshot_id(), Some(999));
    }

    #[test]
    fn test_concurrent_access() {
        let cache = Arc::new(MetadataCache::new());
        let mut handles = vec![];

        // Multiple readers
        for _ in 0..10 {
            let cache_clone = cache.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _ = cache_clone.get_snapshot_id();
                }
            }));
        }

        // One writer
        let cache_clone = cache.clone();
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                cache_clone.set_snapshot_id(i);
            }
        }));

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have some value
        // (may or may not be valid depending on timing)
    }

    #[test]
    fn test_has_valid_entries() {
        let cache = MetadataCache::new();

        assert!(!cache.has_valid_entries());

        cache.set_snapshot_id(123);
        assert!(cache.has_valid_entries());
    }
}
