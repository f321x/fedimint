#![deny(clippy::pedantic)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::needless_lifetimes)]

pub mod envs;

use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use async_trait::async_trait;
use fedimint_core::db::{
    DatabaseError, DatabaseResult, IDatabaseTransactionOps, IDatabaseTransactionOpsCore,
    IRawDatabase, IRawDatabaseTransaction, PrefixStream,
};
use fedimint_core::task::block_in_place;
use fedimint_db_locked::{Locked, LockedBuilder};
use futures::stream;
pub use rocksdb;
use rocksdb::{
    DBRecoveryMode, OptimisticTransactionDB, OptimisticTransactionOptions, WriteOptions,
};
use tracing::{debug, warn};

use crate::envs::{
    FM_ROCKSDB_BLOCK_CACHE_SIZE_ENV, FM_ROCKSDB_WAL_SYNC_INTERVAL_MS_ENV,
    FM_ROCKSDB_WRITE_BUFFER_SIZE_ENV,
};

// turn an `iter` into a `Stream` where every `next` is ran inside
// `block_in_place` to offload the blocking calls
fn convert_to_async_stream<'i, I>(iter: I) -> impl futures::Stream<Item = I::Item> + use<I>
where
    I: Iterator + Send + 'i,
    I::Item: Send,
{
    stream::unfold(iter, |mut iter| async {
        fedimint_core::runtime::block_in_place(|| {
            let item = iter.next();
            item.map(|item| (item, iter))
        })
    })
}

/// Decides which commits get an `fsync`.
///
/// Upstream `fsync`s every commit. Because `AlephBFT` persists one unit per DB
/// transaction on the consensus critical path (a few dozen per second, forever,
/// even while the federation is idle), that turns into a constant stream of
/// tiny synchronous writes. On copy-on-write filesystems each one costs a full
/// extent allocation plus checksum updates, amplifying ~150 byte records into
/// megabytes per second of physical writes.
///
/// When an interval is configured, commits are written without `fsync` and a
/// `flush_wal(true)` is issued at most once per interval instead. That call
/// syncs *every* live WAL file, so it also makes all preceding un-synced
/// commits durable — this is group commit, not "durability off".
///
/// Un-synced commits still reach the OS page cache, so they survive process
/// death, `SIGKILL`, OOM-kill and container restarts. A power loss or kernel
/// panic can discard them, back to the last synced commit. (That assumes the
/// host page cache is the last buffer in the stack; a hypervisor or network
/// block device caching underneath it can widen the window.)
///
/// The sync is claimed at *commit* time, not when a transaction is opened.
/// `begin_transaction` also serves read-only transactions, which are dropped
/// without committing — letting one claim the interval would mean the `fsync`
/// was accounted for but never performed, leaving the window unbounded.
#[derive(Debug)]
struct WalSyncPolicy {
    /// `None` means "`fsync` every commit" (upstream behaviour).
    interval: Option<Duration>,
    epoch: Instant,
    /// Millis since `epoch` from which the next commit may claim the sync slot.
    /// Starts at 0 so the first commit after opening is always `fsync`-ed.
    next_sync_millis: AtomicU64,
}

impl WalSyncPolicy {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self::new(parse_env_wal_sync_interval()?))
    }

    fn new(interval: Option<Duration>) -> Self {
        Self {
            interval,
            epoch: Instant::now(),
            next_sync_millis: AtomicU64::new(0),
        }
    }

    /// Claim this interval's explicit sync, if one is due.
    ///
    /// Returns `true` for exactly one caller per interval; that caller must then
    /// actually perform the sync. Call this only *after* a commit succeeds — a
    /// claim that does not result in an `fsync` silently widens the window.
    ///
    /// Always `false` when no interval is configured: those commits were already
    /// `fsync`-ed by `WriteOptions`, so an extra flush would be a second,
    /// redundant `fsync` on the default path.
    fn claim_sync(&self) -> bool {
        let Some(interval) = self.interval else {
            return false;
        };

        // Saturating conversions: a `u64` of milliseconds cannot realistically
        // overflow within a process lifetime (~584 million years).
        let now = u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX);
        let interval = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
        let deadline = self.next_sync_millis.load(Ordering::Relaxed);

        if now < deadline {
            return false;
        }

        // Compare-and-swap so that exactly one of several concurrent
        // transactions claims the sync for this interval.
        self.next_sync_millis
            .compare_exchange(
                deadline,
                now.saturating_add(interval),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }
}

#[derive(Debug)]
pub struct RocksDb {
    db: rocksdb::OptimisticTransactionDB,
    wal_sync: WalSyncPolicy,
}

pub struct RocksDbTransaction<'a>(
    rocksdb::Transaction<'a, rocksdb::OptimisticTransactionDB>,
    &'a RocksDb,
);

#[bon::bon]
impl RocksDb {
    /// Open the database using blocking IO
    #[builder(start_fn = build)]
    #[builder(finish_fn = open_blocking)]
    pub fn open_blocking(
        #[builder(start_fn)] db_path: impl AsRef<Path>,
        /// Relaxed consistency allows opening the database
        /// even if the wal got corrupted.
        relaxed_consistency: Option<bool>,
    ) -> anyhow::Result<Locked<RocksDb>> {
        let db_path = db_path.as_ref();

        block_in_place(|| {
            std::fs::create_dir_all(
                db_path
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("db path must have a base dir"))?,
            )?;
            LockedBuilder::new(db_path)?.with_db(|| {
                Self::open_blocking_unlocked(db_path, relaxed_consistency.unwrap_or_default())
            })
        })
    }
}

impl<I1, S> RocksDbOpenBlockingBuilder<I1, S>
where
    S: rocks_db_open_blocking_builder::State,
    I1: std::convert::AsRef<std::path::Path>,
{
    /// Open the database
    #[allow(clippy::unused_async)]
    pub async fn open(self) -> anyhow::Result<Locked<RocksDb>> {
        block_in_place(|| self.open_blocking())
    }
}

impl RocksDb {
    fn open_blocking_unlocked(
        db_path: &Path,
        relaxed_consistency: bool,
    ) -> anyhow::Result<RocksDb> {
        let mut opts = get_default_options()?;
        let wal_sync = WalSyncPolicy::from_env()?;

        if wal_sync.interval.is_some() {
            // `PointInTime` stops replaying at the first gap, yielding a
            // consistent prefix of committed transactions.
            //
            // `TolerateCorruptedTailRecords` is NOT sufficient here. RocksDB
            // keeps a deque of WAL files and rotates on every memtable switch,
            // so between two syncs there can be several un-synced WAL files at
            // once. Kernel writeback across separate files is unordered, so a
            // retired WAL can lose its tail while a newer one is fully
            // persisted. `TolerateCorruptedTailRecords` silently stops reading
            // the torn file and then replays the newer one in full, producing a
            // database with a *hole* rather than a shorter prefix. Consensus
            // replay tolerates having fewer items than the federation; it does
            // not tolerate missing items in the middle.
            opts.set_wal_recovery_mode(DBRecoveryMode::PointInTime);
        } else if relaxed_consistency {
            // https://github.com/fedimint/fedimint/issues/8072
            opts.set_wal_recovery_mode(DBRecoveryMode::TolerateCorruptedTailRecords);
        } else {
            // Since we turned synchronous writes one we should never encounter a corrupted
            // WAL and should rather fail in this case
            opts.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);
        }

        if let Some(interval) = wal_sync.interval {
            // Warn, not debug: this is a durability relaxation that applies to
            // every process reading the env var, and it must be visible at
            // default log levels.
            warn!(
                target: "fedimint-rocksdb",
                interval_ms = interval.as_millis(),
                path = %db_path.display(),
                "Durability relaxed: commits are not individually fsynced"
            );
        }

        let db: rocksdb::OptimisticTransactionDB =
            rocksdb::OptimisticTransactionDB::<rocksdb::SingleThreaded>::open(&opts, db_path)?;
        Ok(RocksDb { db, wal_sync })
    }

    pub fn inner(&self) -> &rocksdb::OptimisticTransactionDB {
        &self.db
    }
}

// TODO: Remove this and inline it in the places where it's used.
fn is_power_of_two(num: usize) -> bool {
    num.is_power_of_two()
}

impl fmt::Debug for RocksDbReadOnlyTransaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RocksDbTransaction")
    }
}

impl fmt::Debug for RocksDbTransaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RocksDbTransaction")
    }
}

#[test]
fn is_power_of_two_sanity() {
    assert!(!is_power_of_two(0));
    assert!(is_power_of_two(1));
    assert!(is_power_of_two(2));
    assert!(!is_power_of_two(3));
    assert!(is_power_of_two(4));
    assert!(!is_power_of_two(5));
    assert!(is_power_of_two(2 << 10));
    assert!(!is_power_of_two((2 << 10) + 1));
}

/// Default write buffer size: 2 MiB (`RocksDB` default is 64 MiB)
const DEFAULT_WRITE_BUFFER_SIZE: usize = 2 * 1024 * 1024;

/// Default block cache size: 2 MiB (`RocksDB` default is 8 MiB).
/// Index/filter blocks are placed in this cache too
/// (`set_cache_index_and_filter_blocks`), so everything is bounded.
/// We only need correctness, not throughput, so we keep this minimal.
const DEFAULT_BLOCK_CACHE_SIZE: usize = 2 * 1024 * 1024;

/// Default max open files: 256 (`RocksDB` default is unlimited which
/// consumes memory for each open file handle and associated metadata)
const DEFAULT_MAX_OPEN_FILES: i32 = 256;

fn parse_env_size(env_name: &str) -> anyhow::Result<Option<usize>> {
    let Ok(var) = std::env::var(env_name) else {
        return Ok(None);
    };
    let size: usize =
        FromStr::from_str(&var).with_context(|| format!("Could not parse {env_name}"))?;
    if !is_power_of_two(size) {
        bail!("{env_name} is not a power of 2");
    }
    Ok(Some(size))
}

/// Upper bound on the configurable sync interval. Beyond this the setting
/// stops being "bounded durability" and becomes "effectively never sync", which
/// is much more likely to be a typo or a seconds/milliseconds mix-up than an
/// intent.
const MAX_WAL_SYNC_INTERVAL_MS: u64 = 60_000;

/// Parse [`FM_ROCKSDB_WAL_SYNC_INTERVAL_MS_ENV`].
///
/// Unset or `0` yields `None`, meaning every commit is `fsync`-ed.
fn parse_env_wal_sync_interval() -> anyhow::Result<Option<Duration>> {
    parse_env_wal_sync_interval_from(
        std::env::var(FM_ROCKSDB_WAL_SYNC_INTERVAL_MS_ENV)
            .ok()
            .as_deref(),
    )
}

/// Env-independent core of [`parse_env_wal_sync_interval`], so it is testable
/// without mutating process-global state.
fn parse_env_wal_sync_interval_from(var: Option<&str>) -> anyhow::Result<Option<Duration>> {
    let Some(var) = var else {
        return Ok(None);
    };
    let millis: u64 = FromStr::from_str(var.trim()).with_context(|| {
        format!("Could not parse {FM_ROCKSDB_WAL_SYNC_INTERVAL_MS_ENV} as milliseconds")
    })?;
    if millis > MAX_WAL_SYNC_INTERVAL_MS {
        bail!(
            "{FM_ROCKSDB_WAL_SYNC_INTERVAL_MS_ENV} is {millis}ms, refusing anything above \
             {MAX_WAL_SYNC_INTERVAL_MS}ms; a larger value would leave writes un-synced almost \
             indefinitely"
        );
    }
    Ok((millis != 0).then(|| Duration::from_millis(millis)))
}

fn get_default_options() -> anyhow::Result<rocksdb::Options> {
    let mut opts = rocksdb::Options::default();

    let write_buffer_size =
        parse_env_size(FM_ROCKSDB_WRITE_BUFFER_SIZE_ENV)?.unwrap_or(DEFAULT_WRITE_BUFFER_SIZE);
    opts.set_write_buffer_size(write_buffer_size);

    // Keep at most 2 write buffers (1 active + 1 flushing)
    opts.set_max_write_buffer_number(2);

    let block_cache_size =
        parse_env_size(FM_ROCKSDB_BLOCK_CACHE_SIZE_ENV)?.unwrap_or(DEFAULT_BLOCK_CACHE_SIZE);
    let cache = rocksdb::Cache::new_lru_cache(block_cache_size);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);
    // Put index and filter blocks into the block cache so they are
    // bounded by the same memory budget instead of growing unbounded.
    block_opts.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&block_opts);

    opts.set_max_open_files(DEFAULT_MAX_OPEN_FILES);

    debug!(
        write_buffer_size,
        block_cache_size,
        max_open_files = DEFAULT_MAX_OPEN_FILES,
        "RocksDB memory options"
    );

    opts.create_if_missing(true);
    Ok(opts)
}

#[derive(Debug)]
pub struct RocksDbReadOnly(rocksdb::DB);

pub struct RocksDbReadOnlyTransaction<'a>(&'a rocksdb::DB);

impl RocksDbReadOnly {
    #[allow(clippy::unused_async)]
    pub async fn open_read_only(db_path: impl AsRef<Path>) -> anyhow::Result<RocksDbReadOnly> {
        let db_path = db_path.as_ref();
        block_in_place(|| Self::open_read_only_blocking(db_path))
    }

    pub fn open_read_only_blocking(db_path: &Path) -> anyhow::Result<RocksDbReadOnly> {
        let opts = get_default_options()?;
        // Note: rocksdb is OK if one process has write access, and other read-access
        let db = rocksdb::DB::open_for_read_only(&opts, db_path, false)?;
        Ok(RocksDbReadOnly(db))
    }
}

impl From<rocksdb::OptimisticTransactionDB> for RocksDb {
    fn from(db: OptimisticTransactionDB) -> Self {
        // This conversion cannot report errors, so a malformed interval falls
        // back to the conservative "fsync every commit" behaviour. The path
        // fedimintd actually uses (`open_blocking_unlocked`) surfaces the error
        // at startup instead.
        let wal_sync = WalSyncPolicy::from_env().unwrap_or_else(|err| {
            warn!(
                target: "fedimint-rocksdb",
                err = %err,
                "Ignoring invalid WAL sync interval, syncing every commit"
            );
            WalSyncPolicy::new(None)
        });
        RocksDb { db, wal_sync }
    }
}

impl From<RocksDb> for rocksdb::OptimisticTransactionDB {
    fn from(db: RocksDb) -> Self {
        db.db
    }
}

// When finding by prefix iterating in Reverse order, we need to start from
// "prefix+1" instead of "prefix", using lexicographic ordering. See the tests
// below.
// Will return None if there is no next prefix (i.e prefix is already the last
// possible/max one)
fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next_prefix = prefix.to_vec();
    let mut is_last_prefix = true;
    for i in (0..next_prefix.len()).rev() {
        next_prefix[i] = next_prefix[i].wrapping_add(1);
        if next_prefix[i] > 0 {
            is_last_prefix = false;
            break;
        }
    }
    if is_last_prefix {
        // The given prefix is already the last/max prefix, so there is no next prefix,
        // return None to represent that
        None
    } else {
        Some(next_prefix)
    }
}

#[async_trait]
impl IRawDatabase for RocksDb {
    type Transaction<'a> = RocksDbTransaction<'a>;
    async fn begin_transaction<'a>(&'a self) -> RocksDbTransaction {
        let mut optimistic_options = OptimisticTransactionOptions::default();
        optimistic_options.set_snapshot(true);

        let mut write_options = WriteOptions::default();
        // Make sure we never lose data on unclean shutdown, unless the operator
        // explicitly opted into a bounded window via
        // `FM_ROCKSDB_WAL_SYNC_INTERVAL_MS`. See `WalSyncPolicy`.
        //
        // Deliberately NOT decided here: this is also the entry point for
        // read-only transactions (`begin_transaction_nc`), which are dropped
        // without ever committing. Deciding here would let a reader consume the
        // interval's sync and never perform it. The sync is issued explicitly
        // after a successful commit instead.
        write_options.set_sync(self.wal_sync.interval.is_none());

        RocksDbTransaction(
            self.db.transaction_opt(&write_options, &optimistic_options),
            self,
        )
    }

    fn checkpoint(&self, backup_path: &Path) -> DatabaseResult<()> {
        let checkpoint =
            rocksdb::checkpoint::Checkpoint::new(&self.db).map_err(DatabaseError::backend)?;
        checkpoint
            .create_checkpoint(backup_path)
            .map_err(DatabaseError::backend)?;
        Ok(())
    }
}

#[async_trait]
impl IRawDatabase for RocksDbReadOnly {
    type Transaction<'a> = RocksDbReadOnlyTransaction<'a>;
    async fn begin_transaction<'a>(&'a self) -> RocksDbReadOnlyTransaction<'a> {
        RocksDbReadOnlyTransaction(&self.0)
    }

    fn checkpoint(&self, backup_path: &Path) -> DatabaseResult<()> {
        let checkpoint =
            rocksdb::checkpoint::Checkpoint::new(&self.0).map_err(DatabaseError::backend)?;
        checkpoint
            .create_checkpoint(backup_path)
            .map_err(DatabaseError::backend)?;
        Ok(())
    }
}

#[async_trait]
impl IDatabaseTransactionOpsCore for RocksDbTransaction<'_> {
    async fn raw_insert_bytes(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> DatabaseResult<Option<Vec<u8>>> {
        fedimint_core::runtime::block_in_place(|| {
            let val = self.0.snapshot().get(key).unwrap();
            self.0.put(key, value).map_err(DatabaseError::backend)?;
            Ok(val)
        })
    }

    async fn raw_get_bytes(&mut self, key: &[u8]) -> DatabaseResult<Option<Vec<u8>>> {
        fedimint_core::runtime::block_in_place(|| {
            self.0.snapshot().get(key).map_err(DatabaseError::backend)
        })
    }

    async fn raw_remove_entry(&mut self, key: &[u8]) -> DatabaseResult<Option<Vec<u8>>> {
        fedimint_core::runtime::block_in_place(|| {
            let val = self.0.snapshot().get(key).unwrap();
            self.0.delete(key).map_err(DatabaseError::backend)?;
            Ok(val)
        })
    }

    async fn raw_find_by_prefix(&mut self, key_prefix: &[u8]) -> DatabaseResult<PrefixStream<'_>> {
        Ok(fedimint_core::runtime::block_in_place(|| {
            let prefix = key_prefix.to_vec();
            let mut options = rocksdb::ReadOptions::default();
            options.set_iterate_range(rocksdb::PrefixRange(prefix.clone()));
            let iter = self.0.snapshot().iterator_opt(
                rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                options,
            );
            let rocksdb_iter = iter.map_while(move |res| {
                let (key_bytes, value_bytes) = res.expect("Error reading from RocksDb");
                key_bytes
                    .starts_with(&prefix)
                    .then_some((key_bytes.to_vec(), value_bytes.to_vec()))
            });
            Box::pin(convert_to_async_stream(rocksdb_iter))
        }))
    }

    async fn raw_find_by_range(&mut self, range: Range<&[u8]>) -> DatabaseResult<PrefixStream<'_>> {
        Ok(fedimint_core::runtime::block_in_place(|| {
            let range = Range {
                start: range.start.to_vec(),
                end: range.end.to_vec(),
            };
            let mut options = rocksdb::ReadOptions::default();
            options.set_iterate_range(range.clone());
            let iter = self.0.snapshot().iterator_opt(
                rocksdb::IteratorMode::From(&range.start, rocksdb::Direction::Forward),
                options,
            );
            let rocksdb_iter = iter.map_while(move |res| {
                let (key_bytes, value_bytes) = res.expect("Error reading from RocksDb");
                (key_bytes.as_ref() < range.end.as_slice())
                    .then_some((key_bytes.to_vec(), value_bytes.to_vec()))
            });
            Box::pin(convert_to_async_stream(rocksdb_iter))
        }))
    }

    async fn raw_remove_by_prefix(&mut self, key_prefix: &[u8]) -> DatabaseResult<()> {
        fedimint_core::runtime::block_in_place(|| {
            // Note: delete_range is not supported in Transactions :/
            let mut options = rocksdb::ReadOptions::default();
            options.set_iterate_range(rocksdb::PrefixRange(key_prefix.to_owned()));
            let iter = self
                .0
                .snapshot()
                .iterator_opt(
                    rocksdb::IteratorMode::From(key_prefix, rocksdb::Direction::Forward),
                    options,
                )
                .map_while(|res| {
                    res.map(|(key_bytes, _)| {
                        key_bytes
                            .starts_with(key_prefix)
                            .then_some(key_bytes.to_vec())
                    })
                    .transpose()
                });

            for item in iter {
                let key = item.map_err(DatabaseError::backend)?;
                self.0.delete(key).map_err(DatabaseError::backend)?;
            }

            Ok(())
        })
    }

    async fn raw_find_by_prefix_sorted_descending(
        &mut self,
        key_prefix: &[u8],
    ) -> DatabaseResult<PrefixStream<'_>> {
        let prefix = key_prefix.to_vec();
        let next_prefix = next_prefix(&prefix);
        let iterator_mode = if let Some(next_prefix) = &next_prefix {
            rocksdb::IteratorMode::From(next_prefix, rocksdb::Direction::Reverse)
        } else {
            rocksdb::IteratorMode::End
        };
        Ok(fedimint_core::runtime::block_in_place(|| {
            let mut options = rocksdb::ReadOptions::default();
            options.set_iterate_range(rocksdb::PrefixRange(prefix.clone()));
            let iter = self.0.snapshot().iterator_opt(iterator_mode, options);
            let rocksdb_iter = iter.map_while(move |res| {
                let (key_bytes, value_bytes) = res.expect("Error reading from RocksDb");
                key_bytes
                    .starts_with(&prefix)
                    .then_some((key_bytes.to_vec(), value_bytes.to_vec()))
            });
            Box::pin(convert_to_async_stream(rocksdb_iter))
        }))
    }
}

impl IDatabaseTransactionOps for RocksDbTransaction<'_> {}

#[async_trait]
impl IRawDatabaseTransaction for RocksDbTransaction<'_> {
    async fn commit_tx(self) -> DatabaseResult<()> {
        let db = self.1;
        fedimint_core::runtime::block_in_place(|| {
            match self.0.commit() {
                Ok(()) => {
                    // Only a commit that actually succeeded may claim the
                    // interval's sync, so read-only and rolled-back
                    // transactions cannot consume it without performing it.
                    if db.wal_sync.claim_sync() {
                        // Syncs every live WAL, so this also makes all
                        // preceding un-synced commits durable.
                        if let Err(err) = db.db.flush_wal(true) {
                            return Err(DatabaseError::backend(err));
                        }
                    }
                    Ok(())
                }
                Err(err) => {
                    // RocksDB's OptimisticTransactionDB can return Busy/TryAgain errors
                    // when concurrent transactions conflict on the same keys.
                    // These are retriable - return WriteConflict so autocommit retries.
                    // See: https://github.com/fedimint/fedimint/issues/8077
                    match err.kind() {
                        rocksdb::ErrorKind::Busy
                        | rocksdb::ErrorKind::TryAgain
                        | rocksdb::ErrorKind::MergeInProgress
                        | rocksdb::ErrorKind::TimedOut => Err(DatabaseError::WriteConflict),
                        _ => Err(DatabaseError::backend(err)),
                    }
                }
            }
        })
    }
}

#[async_trait]
impl IDatabaseTransactionOpsCore for RocksDbReadOnlyTransaction<'_> {
    async fn raw_insert_bytes(
        &mut self,
        _key: &[u8],
        _value: &[u8],
    ) -> DatabaseResult<Option<Vec<u8>>> {
        panic!("Cannot insert into a read only transaction");
    }

    async fn raw_get_bytes(&mut self, key: &[u8]) -> DatabaseResult<Option<Vec<u8>>> {
        fedimint_core::runtime::block_in_place(|| {
            self.0.snapshot().get(key).map_err(DatabaseError::backend)
        })
    }

    async fn raw_remove_entry(&mut self, _key: &[u8]) -> DatabaseResult<Option<Vec<u8>>> {
        panic!("Cannot remove from a read only transaction");
    }

    async fn raw_find_by_range(&mut self, range: Range<&[u8]>) -> DatabaseResult<PrefixStream<'_>> {
        Ok(fedimint_core::runtime::block_in_place(|| {
            let range = Range {
                start: range.start.to_vec(),
                end: range.end.to_vec(),
            };
            let mut options = rocksdb::ReadOptions::default();
            options.set_iterate_range(range.clone());
            let iter = self.0.snapshot().iterator_opt(
                rocksdb::IteratorMode::From(&range.start, rocksdb::Direction::Forward),
                options,
            );
            let rocksdb_iter = iter.map_while(move |res| {
                let (key_bytes, value_bytes) = res.expect("Error reading from RocksDb");
                (key_bytes.as_ref() < range.end.as_slice())
                    .then_some((key_bytes.to_vec(), value_bytes.to_vec()))
            });
            Box::pin(convert_to_async_stream(rocksdb_iter))
        }))
    }

    async fn raw_find_by_prefix(&mut self, key_prefix: &[u8]) -> DatabaseResult<PrefixStream<'_>> {
        Ok(fedimint_core::runtime::block_in_place(|| {
            let prefix = key_prefix.to_vec();
            let mut options = rocksdb::ReadOptions::default();
            options.set_iterate_range(rocksdb::PrefixRange(prefix.clone()));
            let iter = self.0.snapshot().iterator_opt(
                rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                options,
            );
            let rocksdb_iter = iter.map_while(move |res| {
                let (key_bytes, value_bytes) = res.expect("Error reading from RocksDb");
                key_bytes
                    .starts_with(&prefix)
                    .then_some((key_bytes.to_vec(), value_bytes.to_vec()))
            });
            Box::pin(convert_to_async_stream(rocksdb_iter))
        }))
    }

    async fn raw_remove_by_prefix(&mut self, _key_prefix: &[u8]) -> DatabaseResult<()> {
        panic!("Cannot remove from a read only transaction");
    }

    async fn raw_find_by_prefix_sorted_descending(
        &mut self,
        key_prefix: &[u8],
    ) -> DatabaseResult<PrefixStream<'_>> {
        let prefix = key_prefix.to_vec();
        let next_prefix = next_prefix(&prefix);
        let iterator_mode = if let Some(next_prefix) = &next_prefix {
            rocksdb::IteratorMode::From(next_prefix, rocksdb::Direction::Reverse)
        } else {
            rocksdb::IteratorMode::End
        };
        Ok(fedimint_core::runtime::block_in_place(|| {
            let mut options = rocksdb::ReadOptions::default();
            options.set_iterate_range(rocksdb::PrefixRange(prefix.clone()));
            let iter = self.0.snapshot().iterator_opt(iterator_mode, options);
            let rocksdb_iter = iter.map_while(move |res| {
                let (key_bytes, value_bytes) = res.expect("Error reading from RocksDb");
                key_bytes
                    .starts_with(&prefix)
                    .then_some((key_bytes.to_vec(), value_bytes.to_vec()))
            });
            Box::pin(stream::iter(rocksdb_iter))
        }))
    }
}

impl IDatabaseTransactionOps for RocksDbReadOnlyTransaction<'_> {}

#[async_trait]
impl IRawDatabaseTransaction for RocksDbReadOnlyTransaction<'_> {
    async fn commit_tx(self) -> DatabaseResult<()> {
        panic!("Cannot commit a read only transaction");
    }
}

#[cfg(test)]
mod wal_sync_policy_tests {
    use super::*;

    /// With no interval configured, `WriteOptions` already `fsync`s every
    /// commit, so the commit path must not issue a second, redundant flush.
    #[test]
    fn no_explicit_sync_when_disabled() {
        let policy = WalSyncPolicy::new(None);
        for _ in 0..100 {
            assert!(
                !policy.claim_sync(),
                "default path must not add an extra fsync on top of set_sync(true)"
            );
        }
    }

    #[test]
    fn syncs_first_commit_then_rate_limits() {
        let policy = WalSyncPolicy::new(Some(Duration::from_secs(30)));

        // The commit right after opening must be durable.
        assert!(policy.claim_sync());

        // Everything within the interval rides on that fsync.
        for _ in 0..100 {
            assert!(!policy.claim_sync());
        }
    }

    #[test]
    fn syncs_again_once_interval_elapses() {
        let policy = WalSyncPolicy::new(Some(Duration::from_millis(50)));

        assert!(policy.claim_sync());
        assert!(!policy.claim_sync());

        std::thread::sleep(Duration::from_millis(75));

        // A new interval means a new fsync, which also makes every commit
        // skipped above durable.
        assert!(policy.claim_sync());
        assert!(!policy.claim_sync());
    }

    #[test]
    fn only_one_concurrent_commit_claims_the_sync() {
        let policy = std::sync::Arc::new(WalSyncPolicy::new(Some(Duration::from_secs(30))));
        let claims = std::sync::Arc::new(AtomicU64::new(0));

        let threads: Vec<_> = (0..16)
            .map(|_| {
                let policy = policy.clone();
                let claims = claims.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        if policy.claim_sync() {
                            claims.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for thread in threads {
            thread.join().expect("thread panicked");
        }

        // Racing transactions must not each take the slot for one interval.
        assert_eq!(claims.load(Ordering::Relaxed), 1);
    }

    /// Regression test: opening transactions must not consume the interval's
    /// sync. `begin_transaction` also serves read-only transactions, which are
    /// dropped without committing, so claiming there would account for an
    /// `fsync` that never happens and leave the un-synced window unbounded.
    #[tokio::test(flavor = "multi_thread")]
    async fn read_only_transactions_do_not_consume_the_sync() {
        let dir = tempfile::Builder::new()
            .prefix("fm-walsync")
            .tempdir()
            .expect("tempdir");

        let db = RocksDb::open_blocking_unlocked(&dir.path().join("db"), false).expect("open");
        // Simulate a configured interval without touching process-global env.
        let db = RocksDb {
            wal_sync: WalSyncPolicy::new(Some(Duration::from_millis(1))),
            ..db
        };

        // Well past the interval, so any claim would be granted.
        std::thread::sleep(Duration::from_millis(5));

        for _ in 0..50 {
            let tx = db.begin_transaction().await;
            drop(tx);
        }

        assert_eq!(
            db.wal_sync.next_sync_millis.load(Ordering::Relaxed),
            0,
            "opening (and dropping) transactions must leave the sync unclaimed"
        );

        // A real commit does claim it.
        let mut tx = db.begin_transaction().await;
        tx.raw_insert_bytes(b"k", b"v").await.expect("insert");
        tx.commit_tx().await.expect("commit");

        assert_ne!(
            db.wal_sync.next_sync_millis.load(Ordering::Relaxed),
            0,
            "a committed transaction must claim the sync"
        );
    }

    #[test]
    fn rejects_absurd_intervals() {
        assert!(
            parse_env_wal_sync_interval_from(Some(&MAX_WAL_SYNC_INTERVAL_MS.to_string())).is_ok()
        );
        assert!(
            parse_env_wal_sync_interval_from(Some(&(MAX_WAL_SYNC_INTERVAL_MS + 1).to_string()))
                .is_err(),
            "an interval past the cap must be rejected, not silently disable syncing"
        );
        assert!(parse_env_wal_sync_interval_from(Some(&u64::MAX.to_string())).is_err());
    }

    #[test]
    fn zero_and_unset_mean_sync_every_commit() {
        // Guards the default: the patch must be inert unless opted into.
        assert_eq!(parse_env_wal_sync_interval_from(None).expect("valid"), None);
        assert_eq!(
            parse_env_wal_sync_interval_from(Some("0")).expect("valid"),
            None
        );
        assert_eq!(
            parse_env_wal_sync_interval_from(Some(" 5000 ")).expect("valid"),
            Some(Duration::from_secs(5))
        );
        assert!(parse_env_wal_sync_interval_from(Some("nonsense")).is_err());
    }
}

#[cfg(test)]
mod fedimint_rocksdb_tests {
    use fedimint_core::db::{Database, IDatabaseTransactionOpsCoreTyped};
    use fedimint_core::encoding::{Decodable, Encodable};
    use fedimint_core::module::registry::{ModuleDecoderRegistry, ModuleRegistry};
    use fedimint_core::{impl_db_lookup, impl_db_record};
    use futures::StreamExt;

    use super::*;

    fn open_temp_db(temp_path: &str) -> Database {
        let path = tempfile::Builder::new()
            .prefix(temp_path)
            .tempdir()
            .unwrap();

        Database::new(
            RocksDb::build(path.as_ref()).open_blocking().unwrap(),
            ModuleDecoderRegistry::default(),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_insert_elements() {
        fedimint_core::db::verify_insert_elements(open_temp_db("fcb-rocksdb-test-insert-elements"))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_remove_nonexisting() {
        fedimint_core::db::verify_remove_nonexisting(open_temp_db(
            "fcb-rocksdb-test-remove-nonexisting",
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_remove_existing() {
        fedimint_core::db::verify_remove_existing(open_temp_db("fcb-rocksdb-test-remove-existing"))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_read_own_writes() {
        fedimint_core::db::verify_read_own_writes(open_temp_db("fcb-rocksdb-test-read-own-writes"))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_prevent_dirty_reads() {
        fedimint_core::db::verify_prevent_dirty_reads(open_temp_db(
            "fcb-rocksdb-test-prevent-dirty-reads",
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_find_by_range() {
        fedimint_core::db::verify_find_by_range(open_temp_db("fcb-rocksdb-test-find-by-range"))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_find_by_prefix() {
        fedimint_core::db::verify_find_by_prefix(open_temp_db("fcb-rocksdb-test-find-by-prefix"))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_commit() {
        fedimint_core::db::verify_commit(open_temp_db("fcb-rocksdb-test-commit")).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_prevent_nonrepeatable_reads() {
        fedimint_core::db::verify_prevent_nonrepeatable_reads(open_temp_db(
            "fcb-rocksdb-test-prevent-nonrepeatable-reads",
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_snapshot_isolation() {
        fedimint_core::db::verify_snapshot_isolation(open_temp_db(
            "fcb-rocksdb-test-snapshot-isolation",
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_phantom_entry() {
        fedimint_core::db::verify_phantom_entry(open_temp_db("fcb-rocksdb-test-phantom-entry"))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_write_conflict() {
        fedimint_core::db::expect_write_conflict(open_temp_db("fcb-rocksdb-test-write-conflict"))
            .await;
    }

    /// Test that concurrent transaction conflicts are handled gracefully
    /// with autocommit retry logic instead of panicking.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_transaction_conflict_with_autocommit() {
        use std::sync::Arc;

        let db = Arc::new(open_temp_db("fcb-rocksdb-test-concurrent-conflict"));

        // Spawn multiple concurrent tasks that all write to the same key
        // This will trigger optimistic transaction conflicts
        let mut handles = Vec::new();

        for i in 0u64..10 {
            let db_clone = Arc::clone(&db);
            let handle =
                fedimint_core::runtime::spawn("rocksdb-transient-error-test", async move {
                    for j in 0u64..10 {
                        // Use autocommit which handles retriable errors with retry logic
                        let result = db_clone
                            .autocommit::<_, _, anyhow::Error>(
                                |dbtx, _| {
                                    #[allow(clippy::cast_possible_truncation)]
                                    let val = (i * 100 + j) as u8;
                                    Box::pin(async move {
                                        // All transactions write to the same key to force conflicts
                                        dbtx.insert_entry(&TestKey(vec![0]), &TestVal(vec![val]))
                                            .await;
                                        Ok(())
                                    })
                                },
                                None, // unlimited retries
                            )
                            .await;

                        // Should succeed after retries, must NOT panic with "Resource busy"
                        assert!(
                            result.is_ok(),
                            "Transaction should succeed after retries, got: {result:?}",
                        );
                    }
                });
            handles.push(handle);
        }

        // Wait for all tasks - none should panic
        for handle in handles {
            handle.await.expect("Task should not panic");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dbtx_remove_by_prefix() {
        fedimint_core::db::verify_remove_by_prefix(open_temp_db(
            "fcb-rocksdb-test-remove-by-prefix",
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_module_dbtx() {
        fedimint_core::db::verify_module_prefix(open_temp_db("fcb-rocksdb-test-module-prefix"))
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_module_db() {
        let module_instance_id = 1;
        let path = tempfile::Builder::new()
            .prefix("fcb-rocksdb-test-module-db-prefix")
            .tempdir()
            .unwrap();

        let module_db = Database::new(
            RocksDb::build(path.as_ref()).open_blocking().unwrap(),
            ModuleDecoderRegistry::default(),
        );

        fedimint_core::db::verify_module_db(
            open_temp_db("fcb-rocksdb-test-module-db"),
            module_db.with_prefix_module_id(module_instance_id).0,
        )
        .await;
    }

    #[test]
    fn test_next_prefix() {
        // Note: although we are testing the general case of a vector with N elements,
        // the prefixes currently use N = 1
        assert_eq!(next_prefix(&[1, 2, 3]).unwrap(), vec![1, 2, 4]);
        assert_eq!(next_prefix(&[1, 2, 254]).unwrap(), vec![1, 2, 255]);
        assert_eq!(next_prefix(&[1, 2, 255]).unwrap(), vec![1, 3, 0]);
        assert_eq!(next_prefix(&[1, 255, 255]).unwrap(), vec![2, 0, 0]);
        // this is a "max" prefix
        assert!(next_prefix(&[255, 255, 255]).is_none());
        // these are the common case
        assert_eq!(next_prefix(&[0]).unwrap(), vec![1]);
        assert_eq!(next_prefix(&[254]).unwrap(), vec![255]);
        assert!(next_prefix(&[255]).is_none()); // this is a "max" prefix
    }

    #[repr(u8)]
    #[derive(Clone)]
    pub enum TestDbKeyPrefix {
        Test = 254,
        MaxTest = 255,
    }

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Encodable, Decodable)]
    pub(super) struct TestKey(pub Vec<u8>);

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Encodable, Decodable)]
    pub(super) struct TestVal(pub Vec<u8>);

    #[derive(Debug, Encodable, Decodable)]
    struct DbPrefixTestPrefix;

    impl_db_record!(
        key = TestKey,
        value = TestVal,
        db_prefix = TestDbKeyPrefix::Test,
        notify_on_modify = true,
    );
    impl_db_lookup!(key = TestKey, query_prefix = DbPrefixTestPrefix);

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Encodable, Decodable)]
    pub(super) struct TestKey2(pub Vec<u8>);

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Encodable, Decodable)]
    pub(super) struct TestVal2(pub Vec<u8>);

    #[derive(Debug, Encodable, Decodable)]
    struct DbPrefixTestPrefixMax;

    impl_db_record!(
        key = TestKey2,
        value = TestVal2,
        db_prefix = TestDbKeyPrefix::MaxTest, // max/last prefix
        notify_on_modify = true,
    );
    impl_db_lookup!(key = TestKey2, query_prefix = DbPrefixTestPrefixMax);

    #[tokio::test(flavor = "multi_thread")]
    async fn test_retrieve_descending_order() {
        let path = tempfile::Builder::new()
            .prefix("fcb-rocksdb-test-descending-order")
            .tempdir()
            .unwrap();
        {
            let db = Database::new(
                RocksDb::build(&path).open().await.unwrap(),
                ModuleDecoderRegistry::default(),
            );
            let mut dbtx = db.begin_transaction().await;
            dbtx.insert_entry(&TestKey(vec![0]), &TestVal(vec![3]))
                .await;
            dbtx.insert_entry(&TestKey(vec![254]), &TestVal(vec![1]))
                .await;
            dbtx.insert_entry(&TestKey(vec![255]), &TestVal(vec![2]))
                .await;
            dbtx.insert_entry(&TestKey2(vec![0]), &TestVal2(vec![3]))
                .await;
            dbtx.insert_entry(&TestKey2(vec![254]), &TestVal2(vec![1]))
                .await;
            dbtx.insert_entry(&TestKey2(vec![255]), &TestVal2(vec![2]))
                .await;
            let query = dbtx
                .find_by_prefix_sorted_descending(&DbPrefixTestPrefix)
                .await
                .collect::<Vec<_>>()
                .await;
            assert_eq!(
                query,
                vec![
                    (TestKey(vec![255]), TestVal(vec![2])),
                    (TestKey(vec![254]), TestVal(vec![1])),
                    (TestKey(vec![0]), TestVal(vec![3]))
                ]
            );
            let query = dbtx
                .find_by_prefix_sorted_descending(&DbPrefixTestPrefixMax)
                .await
                .collect::<Vec<_>>()
                .await;
            assert_eq!(
                query,
                vec![
                    (TestKey2(vec![255]), TestVal2(vec![2])),
                    (TestKey2(vec![254]), TestVal2(vec![1])),
                    (TestKey2(vec![0]), TestVal2(vec![3]))
                ]
            );
            dbtx.commit_tx().await;
        }
        // Test readonly implementation
        let db_readonly = RocksDbReadOnly::open_read_only(path).await.unwrap();
        let db_readonly = Database::new(db_readonly, ModuleRegistry::default());
        let mut dbtx = db_readonly.begin_transaction_nc().await;
        let query = dbtx
            .find_by_prefix_sorted_descending(&DbPrefixTestPrefix)
            .await
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            query,
            vec![
                (TestKey(vec![255]), TestVal(vec![2])),
                (TestKey(vec![254]), TestVal(vec![1])),
                (TestKey(vec![0]), TestVal(vec![3]))
            ]
        );
        let query = dbtx
            .find_by_prefix_sorted_descending(&DbPrefixTestPrefixMax)
            .await
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            query,
            vec![
                (TestKey2(vec![255]), TestVal2(vec![2])),
                (TestKey2(vec![254]), TestVal2(vec![1])),
                (TestKey2(vec![0]), TestVal2(vec![3]))
            ]
        );
    }
}
