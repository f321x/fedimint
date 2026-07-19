# Low-IO patch (`FM_ROCKSDB_WAL_SYNC_INTERVAL_MS`)

Out-of-tree patch for <https://github.com/fedimint/fedimint/issues/8660>. Not upstream.

## What it does

Upstream `fsync`s every RocksDB commit (`fedimint-rocksdb/src/lib.rs`, `set_sync(true)`).
AlephBFT persists one unit per DB transaction, for every peer, every round — a few dozen
tiny synchronous writes per second, forever, even while the federation is idle. On
copy-on-write filesystems each `fsync` costs a fresh extent plus checksum updates, turning
~150 byte records into ~1.7 MB/s of physical writes.

This patch adds `FM_ROCKSDB_WAL_SYNC_INTERVAL_MS`. When set, commits are written without
`fsync` and a `flush_wal(true)` is issued at most once per interval instead. That call
syncs every live WAL file, so it also makes all preceding commits durable — this is group
commit, not "durability off".

**Unset or `0` is the default and behaves exactly like upstream.** The patch is inert
until you opt in. Values above 60000 are rejected at startup.

## Enabling it

```ini
# in the [Container] section of your quadlet
Environment=FM_ROCKSDB_WAL_SYNC_INTERVAL_MS=5000
```

Start at 5000 (5s). **Measure rather than assume the benefit**: the interval only
suppresses WAL `fsync`s, and RocksDB still writes and `fsync`s an SST plus a MANIFEST
update on every memtable flush, independent of this setting. With fedimint's small 2 MiB
write buffer those flushes are frequent, so they set a floor on how far the write rate can
drop. Compare `pidstat -d -C fedimintd 60 5` before and after.

## What you give up

Un-`fsync`-ed commits sit in the OS page cache, so they survive process death, `SIGKILL`,
OOM-kill, `podman restart` and crashes of fedimintd itself. **Only a power loss or kernel
panic can discard them**, and only back to the last `fsync`-ed commit. (That assumes the
host page cache is the last buffer in the stack — a hypervisor disk cache or a network
block device underneath it widens the window.)

If that happens, up to `interval` of writes are lost and the guardian restarts with a
consistent-but-shorter database. Two consequences:

- Consensus state replays. Having *fewer* items than the federation is explicitly
  supported; the node refetches the rest from peers.
- The AlephBFT unit backup may be missing units the node already broadcast, so it can
  re-create different units for those rounds. Peers detect this, ignore the node's units
  for the remainder of that session (~3 min), and it rejoins on the next one.

The second point consumes your federation's entire Byzantine fault budget (1 of 4) for
those ~3 minutes. If a *second* guardian fails in that same window the federation stalls
until yours recovers. This is the real cost — accept it deliberately.

The patch also switches WAL recovery to `DBRecoveryMode::PointInTime` whenever an interval
is set. This matters more than it looks. `AbsoluteConsistency` (the default, chosen
precisely because writes are synced) would refuse to open a torn WAL and leave the node
permanently down. But `TolerateCorruptedTailRecords` is *also* wrong here: RocksDB rotates
to a new WAL file on every memtable flush, so between two syncs several un-synced WAL files
can be live at once, and kernel writeback across separate files is unordered. A retired WAL
can lose its tail while a newer one is fully persisted — and that mode would silently stop
reading the torn file, then replay the newer one in full, leaving a *hole* in the middle.
`PointInTime` stops at the first gap, which is what makes "consistent prefix" true rather
than merely hoped for.

## Scope

The env var is process-global and read by every binary that opens a fedimint RocksDB —
`fedimint-cli`, the gateway, `fedimint-dbtool`, and the test harnesses. Set it only in the
fedimintd unit, not in a shared shell profile or compose-wide environment. In particular do
not have it set while running `fedimint-dbtool` writes. Startup logs a `WARN` naming the
database path whenever it is active.

## Building the container

```sh
scripts/build-lowio-container.sh
```

Requires `nix` (flakes enabled) and `podman`. First build is slow; the
`fedimint.cachix.org` substituter declared in `flake.nix` supplies prebuilt dependencies.

The script refuses to build if the patch is not committed — Nix flakes only see
git-tracked files, so an unstaged patch silently builds *unpatched* source.

## Rebasing onto a new release

The branch itself is the artifact — push it to your fork (`git push fork lowio/v0.11.1`)
so it survives losing this checkout. To move it to a new release:

```sh
git fetch origin --tags
git rebase --onto vX.Y.Z v0.11.1 lowio/v0.11.1
git switch -c lowio/vX.Y.Z            # name it after the new tag
scripts/build-lowio-container.sh
```

`--onto` replays only the patch commits (everything after the *old* tag) on top of the new
one. Update the old tag in that command each time you move forward.

If it conflicts, the patch touches only two files — `fedimint-rocksdb/src/{lib,envs}.rs`.
Resolve, `git rebase --continue`, and re-run the build script.

Sanity-check after any rebase that upstream has not changed the `set_sync` call site out
from under the patch:

```sh
git grep -n 'set_sync' -- fedimint-rocksdb/src   # must show should_sync(), not `true`
```

## Version safety

After DKG there is no peer-to-peer version enforcement — `code_version` is frozen in the
persisted config and the runtime version is only reported, never checked. A patched binary
will not be rejected by your peers.

Do **not** use a patched binary during initial DKG: `code_version` is part of
`ServerConfigConsensus` and every guardian must hash identically.
