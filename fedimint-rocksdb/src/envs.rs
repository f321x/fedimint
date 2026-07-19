// Env variable to TODO
pub const FM_ROCKSDB_WRITE_BUFFER_SIZE_ENV: &str = "FM_ROCKSDB_WRITE_BUFFER_SIZE";
pub const FM_ROCKSDB_BLOCK_CACHE_SIZE_ENV: &str = "FM_ROCKSDB_BLOCK_CACHE_SIZE";

/// Minimum interval in milliseconds between `fsync`-ed commits.
///
/// Unset or `0` (the default) preserves upstream behaviour: every commit is
/// `fsync`-ed individually. Any larger value relaxes that to at most one
/// `fsync`-ed commit per interval, trading a bounded window of durability for
/// a large reduction in physical disk writes.
///
/// See `docs/low-io-patch.md`.
pub const FM_ROCKSDB_WAL_SYNC_INTERVAL_MS_ENV: &str = "FM_ROCKSDB_WAL_SYNC_INTERVAL_MS";
