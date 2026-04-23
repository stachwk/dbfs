# DBFS Decisions, Follow-ups & Archive

This document records the small set of open follow-ups plus completed work, closed decisions, and regression notes for DBFS. It is not an active implementation backlog.

## Current Follow-ups

- Detect single-node vs read-only replica mode early and let runtime choose the appropriate lock strategy before mount.
- Keep `workers_read` and `workers_write` constrained to the cases where they really help: disjoint read gaps and segmented copy operations, not small contiguous fetches.
- Keep the long-term direction visible: userspace FUSE + PostgreSQL backend + a native Rust storage/hot-path engine, with Python staying as the orchestration layer until the native core is ready.
- Keep the Python-orchestrator / Rust-hot-path split as a standing direction, not as an active rewrite backlog.
- Treat the current copy profile comparison as a frozen baseline; continue the narrow Rust hot-path work in `libdbfs-2.so`, with planner, changed-run packing, persist padding, read assembly, startup query handling, and the first repository lookups/mutations moving to Rust while Python remains the fallback and orchestration layer.
- Keep Rust dedupe opt-in and off by default; benchmark notes show it can be slower than the Python fallback on repeated-copy workloads.
- Start consolidating the small Rust helper surface into shared planners where the same arithmetic is repeated across read and write paths, beginning with a common worker-count planner before adding broader write/read generalizations.
- Keep the truncate/fallocate resize planner in Rust and treat the actual mutation boundary as the next step; the current helper only plans logical resize, while Python still performs the SQL transaction and overlay cleanup.
- Treat namespace mutation cleanup (`unlink`/`rename`/`rmdir`, xattrs, cache invalidation, epoch bumps) as a future Rust repository/backend rewrite if it ever moves out of Python; do not split it into one-off FFI helpers.
- When schema changes actually hit a limit or compatibility failure, add a concrete migration file plus a regression test instead of widening the schema bootstrap path ad hoc.

## Extent Engine Direction

- Keep the logical filesystem model at 4 KiB blocks for now.
- Make Rust the owner of the extent and overlay engine.
- Move PostgreSQL away from storing thousands of 4 KiB blocks directly and toward dynamic extents.
- Move SQL/query execution from Python into Rust so the persistence path becomes Python -> Rust -> PostgreSQL, not Python -> PostgreSQL directly.
- Keep Python as the orchestration layer for FUSE callbacks, reconnect retries, ACL, journal, and runtime config.
- Capture the next architectural step explicitly:
  - `logical_block_size = 4k`
  - `persist model = extents`
  - `persist extent classes = 4k..4MiB`
  - `payload stores only used bytes`
  - `Rust returns PersistPlan`
  - `Python executes transaction`
- Next concrete step:
  - prove a minimal Rust repository/query boundary with one scalar PostgreSQL query path (`Python -> Rust -> PostgreSQL`) before moving broader SQL ownership,
  - write a small extent-engine proof of concept with a new `data_extents` table,
  - implement one simple write/read path for extents without full merge/split logic,
  - benchmark `4k`, `64k`, `1M`, `4M`, and `copy_file_range`,
  - only then migrate `truncate`, `copy`, `flush/release`, workers, and the old `data_blocks` layout.

## Target Architecture

### Warstwa 1 - Python zostaje jako orchestrator

Python should remain the orchestration and control plane for DBFS:

- `dbfs_bootstrap.py`
- `mkfs.dbfs.py`
- config and profile loading
- `dbfs_fuse.py` FUSE callbacks
- administrative logic
- schema migrations
- integration tests
- ACL / permissions / journal / runtime validation policy layers

Why this stays in Python:

- the project already has good modularity and test coverage around these areas
- the README and roadmap already point toward thinner `dbfs_fuse.py`, more delegation, and explicit userspace layering

### Warstwa 2 - Rust jako silnik storage hot-path

Move only the CPU/memory-heavy hot path into Rust:

- block engine
- read block assembly
- range slicing
- overlay plus `block_map` merging
- read buffer preparation
- write overlay engine
- `write_into_state()`
- `truncate_to_size()`
- dirty block management
- block payload assembly for flush
- copy engine
- `copy_file_range_into_state()`
- copy segmentation
- worker coordination for read/write copy paths
- segment ordering
- persist preparation
- list preparation for `(file_id, block_index, data)`
- block padding
- deleting blocks beyond EOF
- dirty range accounting

Why this matters:

- the benchmarks show that write path and finalization are the main bottleneck, not the project structure itself
- the highest-value Rust work is the part that reshapes data before SQL, not the SQL layer itself

## Archived Work

### Recent Architecture Cleanup

- [x] Split metadata cache payloads by purpose: attribute cache and directory-entry cache are now stored separately instead of sharing one mixed payload shape.
- [x] Fix the listing/getattr cache regression where `ls -al` on a directory could disagree with `ls file` because `readdir()` and `getattr()` interpreted the same cache key differently.
- [x] Remove runtime method rebinding from `dbfs_fuse.py` and replace it with explicit wrapper methods that delegate through `dbfs_repository.py`.
- [x] Unify lookup helpers through the repository layer so `get_file_id`, `get_dir_id`, `get_entry_kind_and_id`, `entry_exists`, and related helpers no longer depend on hidden `__init__` rebinding.
- [x] Move the `getattr()` / `readdir()` query layer into `dbfs_repository.py` so the main FUSE module no longer owns direct listing/attribute SQL for those hot paths.
- [x] Confirm the current refactor through `make test-all` plus an explicit regression check for create/list/remove consistency on a mounted DBFS instance.

### Performance Plan

- Current numeric baselines and profile outputs live in [`BENCHMARKS.md`](BENCHMARKS.md). Keep this section focused on decisions, accepted changes, and rejected experiments.

- [x] Extract CLI parsing and mount startup into `dbfs_bootstrap.py` so `dbfs_fuse.py` only carries the filesystem operation layer.
- [x] Extract inode/path identity helpers into `dbfs_identity.py` so path normalization, inode generation, and ownership defaults are shared in one place.
- [x] Extract PostgreSQL connection pooling and config query helpers into `dbfs_backend.py` so the filesystem layer stops owning raw pool lifecycle.
- [x] Extract xattr and ACL policy helpers into `dbfs_xattr_acl.py` so xattr normalization, ACL encoding, and ACL checks live outside the filesystem core.
- [x] Remove the old in-class POSIX ACL constants and helpers from `dbfs_fuse.py` so `dbfs_xattr_acl.py` is the single source of truth for xattr/ACL policy.
- [x] Move xattrs from path-based storage to inode-based storage in `dbfs_xattr_store.py` so rename no longer has to rewrite xattr keys.
- [x] Extract lock state and lock conflict handling into `dbfs_locking.py` so advisory lock policy no longer lives in the filesystem core.
- [x] Extract data block loading, write cache management, dirty tracking, and buffer persistence into `dbfs_storage.py` so the write path is no longer embedded in the filesystem core.
- [x] Add an optional PostgreSQL-side CRC cache for unchanged-block copy detection. The cache is populated lazily on demand and refreshed during block persistence, so repeated copy-heavy workloads can reuse stored CRCs instead of rereading full destination blocks every time.
- [x] Remove the in-class write-buffer persistence implementation from `dbfs_fuse.py` so `dbfs_storage.py` is the single source of truth for data block persistence.
- [x] Move the read path to block-range loading with a small block cache and read-ahead so `read()` no longer has to load whole files on every access.
- [x] Benchmark the current write path on mounted DBFS and record the baseline for large sequential writes. The live baseline is tracked in [`BENCHMARKS.md`](BENCHMARKS.md) and the profile entry points are `make test-throughput` and `make test-throughput-sync`.
- [x] Batch buffered writes in memory and persist them once per flush/release instead of rewriting the whole file repeatedly during a write burst.
- [x] Add block-delta persistence so unchanged blocks are not rewritten on every flush.
- [x] Profile the remaining hot paths (`getattr`, `readdir`, `persist_buffer`) and add only the indexes that move the benchmark. Added directory-parent indexes and confirmed the block-order index is used on `data_blocks`.
- [x] Capture a live throughput baseline for different write sizes on a mounted DBFS instance. The measured values are recorded in [`BENCHMARKS.md`](BENCHMARKS.md).
- [x] Add schema versioning so the schema can be repaired in a controlled way instead of relying only on `init` to recreate the database. Current version is `3`, exposed via `schema_version`, exported by `mkfs.dbfs.py status`, and checked by `make test-schema-upgrade` and `make test-schema-status`; `init` is now idempotent and non-destructive.
- [x] Add named runtime profiles for production-style tuning in `dbfs_config.ini`. Current profiles include `bulk_write` and `metadata_heavy`, selected with `DBFS_PROFILE`.
- [x] Add optional PostgreSQL TLS connection parameters (`sslmode`, `sslrootcert`, `sslcert`, `sslkey`) in `dbfs_pg_tls.py`, and move client cert/key generation to `mkfs.dbfs.py` for `init` and `upgrade`.
- [x] Add a regression test for the flush/release dirty gate so clean closes stay cheap and dirty data is persisted exactly once. Added `make test-flush-release-profile`.
- [x] Try skipping the tail-delete optimization for normal growth writes. Rejected: the change regressed small-write throughput, so it was reverted.
- [x] Why it was rejected: the added bookkeeping outweighed the saved `DELETE`; see the historical benchmark notes in [`BENCHMARKS.md`](BENCHMARKS.md) if you need the exact comparison.
- [x] Record a live profile of the current write path so the next performance step starts from measured data instead of guesswork. The current profile split between write, persist, flush, and finalization is kept in [`BENCHMARKS.md`](BENCHMARKS.md) and surfaced by `make test-flush-release-profile`.
- [x] Reject a second tail-delete shortcut based on in-memory persisted-size tracking. Rejected for the same reason as the first shortcut: it hurt small-write throughput.
- [x] Do not retry tail-delete shortcuts or in-memory persisted-size tracking as a default performance strategy. Any future attempt must start from a different hypothesis and be benchmarked before merge.
- [x] Reject the per-block copy/fast-path rewrite in `persist_buffer`. Reverted after it regressed both small and medium writes.
- [x] Reject the shrink-marker variant for skipping tail `DELETE` on growth writes. Reverted because it was still worse than the stable baseline.
- [x] Gate `write()` profiling so it only runs when `DBFS_PROFILE_IO=1`. This removed hot-path timing overhead.
- [x] Reject the memoryview-based `persist_buffer` copy path. Reverted after it regressed the write benchmark.
- [x] Cache `block_size` on the DBFS instance instead of querying config during write-path operations. This removed a hot-path DB lookup.
- [x] Remove the extra `bytes()` copy from `write()` and assign the incoming buffer directly into the in-memory cache.
- [x] Skip `persist_buffer()` work in `flush()` / `release()` / `fsync()` when the file is not dirty.
- [x] Combine the tail `DELETE` and file-size `UPDATE` into one SQL round-trip inside `persist_buffer()`.
- [x] Avoid path-based lock cleanup work in `release()` and use the already-known file handle as the resource key.
- [x] Set `execute_values(..., page_size=len(blocks))` inside `persist_buffer()` so large block batches stay in one SQL round-trip.
- [x] Replace the `DELETE; UPDATE` multi-statement block in `persist_buffer()` with a single CTE-based statement.
- [x] Stop sorting dirty block indexes before persisting.
- [x] Stop copying the whole write buffer into `bytes()` before persisting and use `bytearray` slices directly for block payloads.
- [x] Split very large `persist_buffer()` batches into smaller SQL chunks instead of building one massive `execute_values()` payload.
- [x] Add a large-write auto-flush threshold so very large dirty buffers can be persisted before close instead of concentrating the entire cost in `release()`.
- [x] Expose `synchronous_commit` as a separate runtime knob for PostgreSQL sessions. Keep the default at `on` and treat `off` as an explicit tuning choice unless a future workload benchmark proves otherwise. The current local comparison is recorded in [`BENCHMARKS.md`](BENCHMARKS.md).
- [x] Expose `persist_buffer_chunk_blocks` as a separate runtime knob for flush batching. Keep the default conservative and let profiles override it when larger `execute_values()` batches help. The current comparison is recorded in [`BENCHMARKS.md`](BENCHMARKS.md).
- [x] Use the current benchmark baseline in [`BENCHMARKS.md`](BENCHMARKS.md) to decide whether the next performance work should focus on fewer SQL round-trips for small writes or on additional batching around flush/release. Decision: the current baseline is good enough; stop further tuning here unless a regression appears or a new benchmark target is introduced.
- [x] Confirm the large-write chunking fix on a real 1 GiB sequential write. The `dd if=/dev/zero of=test bs=1M count=1024` scenario completed successfully on `/mnt/dbfs`, and the file was visible afterward at the expected size.
- [x] Record the interpretation of the 1 GiB `dd` timing correctly: the data path itself finished in about `12 s`, while the remaining wall time was spent draining `flush()` / `release()` / `persist_buffer()` work. Future throughput work should measure `write` and finalization separately instead of treating `dd` wall time as pure copy speed.
- [x] Keep the benchmark suite expanded with explicit coverage for large `copy_file_range()` transfers, large multi-block file writes, and remount durability so write-path tuning stays comparable across releases.
- [x] Compare DBFS atime behavior on a short wall-time benchmark. The current measured values live in [`BENCHMARKS.md`](BENCHMARKS.md).

### Finalized Performance Wins

These changes are already merged into the codebase and should be kept:

- `write()` profiling is opt-in via `DBFS_PROFILE_IO=1`, so normal hot-path writes do not pay timing overhead.
- `block_size` is cached on the DBFS instance instead of being queried from the database in the write path.
- `write()` no longer copies the incoming buffer through an extra `bytes()` conversion before writing into the cache.
- `flush()`, `release()`, and `fsync()` skip `persist_buffer()` work when the buffer is not dirty.
- `persist_buffer()` combines tail `DELETE` and file-size `UPDATE` into one SQL round-trip.
- `release()` uses the known file handle as the resource key for regular files instead of re-resolving the path.
- `persist_buffer()` splits very large block batches into smaller SQL chunks so large sequential writes do not exhaust the PostgreSQL client output buffer.
- Large sequential writes are now confirmed to work end-to-end on the mounted filesystem; the 1 GiB `dd` scenario completed successfully after chunking was added.
- Large dirty buffers can now auto-flush before close when they reach the configured threshold, which helps move finalization work out of `release()`.
- The read path now uses block-range loading with a small cache and read-ahead instead of loading whole files on every access.

### Must have

- [x] Fuller `xattr` family support:
  - `user.*`
  - `security.*`
  - `trusted.*`

#### Definition of done

- Each item in this section has:
  - an implementation in `dbfs_fuse.py` or a deliberate unsupported decision
  - at least one integration test or smoke test
  - passing coverage in `make test-all`

### Should have

- [x] Optional `mount --help`/README matrix with ready-made profiles:
  - `dbfs-relaxed`
  - `dbfs-linux-default`
  - `dbfs-selinux`

### Later

- [x] A short smoke profile for DBFS atime behavior in `relatime` mode, plus a separate one for `noatime`.
- [x] A simple benchmark for DBFS atime behavior that can compare `default`, `noatime`, and `nodiratime` runs on file reads and directory listings.
- [x] A target `make test-all-full` if symlinks/locks/journaling grow further.
- [x] Consider a separate test for `statfs` and `use_ino`.

## Already in place

- `getattr`, `readdir`, `open`, `read`, `write`, `truncate`, `rename`, `unlink`, `mkdir`, `rmdir`, `chmod`, `chown`, `utimens`, `statfs`
- `opendir`, `releasedir`, `fsyncdir`
- `destroy`
- `mknod` for FIFO and device nodes; `st_rdev` and `st_dev` are reported, and `open` for special nodes is still unsupported
- `flock`
- `fallocate`
- `copy_file_range`
- `ioctl` for `FIONREAD`
- `read_buf` / `write_buf`
- `poll` as a backend helper for regular files
- `lseek` as a backend helper for `SEEK_SET/SEEK_CUR/SEEK_END`
- `xattr` backend for `user.*`, `trusted.*`, `security.selinux`, and `system.posix_acl_*` with ACL enforcement and default inheritance
- `access()` smoke test for `R_OK`, `W_OK`, `X_OK`
- `access()` test for owner, primary group, and supplementary groups
- `access()` smoke test is part of `make test-all`
- mount suite covers end-to-end file and directory operations: `mkdir`, `rmdir`, `create`, `unlink`, `rename`, `read`, `write`, `truncate`, `chmod`, `chown`, `utimens`, `symlink`, `readlink`, `stat`, `statfs`, `df -Ph`, `df -Phi`, `access()`
- mount profile matrix is documented in README: `dbfs-relaxed`, `dbfs-linux-default`, `dbfs-selinux`
- smoke profiles for DBFS atime behavior in `noatime` and `relatime` modes are exposed as `make test-atime-noatime` and `make test-atime-relatime`
- `make test-atime-benchmark` prints a simple wall-time baseline for file and directory atime behavior so `default`, `noatime`, and `nodiratime` runs can be compared directly.
- access-date writes are deduplicated per open handle, so a single `read()` / `readdir()` sequence only touches the timestamp once and does not rewrite it continuously.
- write-side `mtime` / `ctime` persistence is checked through a regression that confirms multiple writes on the same open file only advance metadata when the dirty buffer is flushed, not on every intermediate write call.
- sequential read-ahead now has a regression that verifies a second adjacent `read()` on the same handle preloads additional blocks into the cache instead of only fetching the requested byte range.
- the read cache defaults to a larger LRU size now, and sequential access stretches the read-ahead window so adjacent scans can reuse prefetched blocks more often.
- a dedicated read-cache benchmark can compare `DBFS_READ_CACHE_BLOCKS=256` vs `1024` on sequential scans.
- metadata cache TTLs are configurable in `dbfs_config.ini` (`metadata_cache_ttl_seconds`, `statfs_cache_ttl_seconds`), and `getattr()` / `readdir()` / `statfs()` use short-lived caches with invalidation on mutating operations.
- metadata cache payloads are now split by type, so attribute and directory-entry state no longer share one cache payload layout.
- schema versioning is explicit: `mkfs.dbfs.py` writes `schema_version = 3`, `mkfs.dbfs.py status` exports the current version and migration manifest, `init` is idempotent and non-destructive, and `make test-schema-upgrade` / `make test-schema-status` verify that `upgrade` can repair missing schema state, restore the current version, and enforce the schema-admin secret for later `init` / `upgrade` / `clean` calls on an existing database.
- runtime profiles are explicit: `DBFS_PROFILE=bulk_write` and `DBFS_PROFILE=metadata_heavy` override the base `[dbfs]` tuning values from `dbfs_config.ini`.
- `make test-all-full` extends `make test-all` with workflow checks for files/directories/metadata/symlink, shell statfs/use_ino, mount workflow, atime smoke, and throughput
- `make test-tree-scale` benchmarks `getattr`/`readdir` on a larger seeded tree
- stable inode model based on durable `inode_seed` values for directories, files, and symlinks
- ownership inheritance for setgid parent directories, including `mkdir` and `rename` edge cases
- `bmap` as a logical mapping for regular files and hardlinks
- hot-path indexes: `hardlinks.id_file`, `directories.id_parent`, `files.id_directory`, `hardlinks.id_directory`, `symlinks.id_parent`, and `data_blocks(id_file, _order)`
- `st_blocks` heuristic for directories and small files
- `st_nlink` for directories and root counted only from subdirectories
- `poll` as a backend helper for regular files
- `default_permissions` as the default mount option
- `use_ino` as the default mount option
- explicit wrapper/delegation model from `dbfs_fuse.py` into `dbfs_repository.py`
- repository-owned query helpers for `getattr()` and `readdir()`
- SELinux options:
  - `--selinux auto|on|off`
  - default `off`
  - `auto` only when host-driven detection is desired
  - runtime SELinux is active only when `on` or `auto`
  - ACL activation uses `--acl on|off`
  - `context`
  - `fscontext`
  - `defcontext`
  - `rootcontext`
- atime / sync options:
  - `noatime`
  - `nodiratime`
  - `relatime`
  - `strictatime`
  - `lazytime`
  - `sync`
  - `dirsync`

## Documented Decisions

### SELinux

Status: intentionally closed as a non-goal for this repo.

- Decision: DBFS keeps SELinux as xattr-backed metadata plus runtime gating only; it does not attempt to implement a full mount label policy.
- Keep the existing coverage for:
  - mount with `DBFS_SELINUX=on`
  - mount with `DBFS_SELINUX=off`
  - mount with `DBFS_SELINUX=auto`
  - reading and writing `security.selinux`
- Document explicitly that full SELinux correctness depends on host policy and is not implied by xattr storage alone.

### Full FUSE / Linux Compatibility

Based on the `libfuse` documentation and Linux VFS behavior, this section records the current `ioctl` compatibility state:

#### Plan

1. Extend `ioctl` coverage beyond the current `FIONREAD` path if a real consumer appears in the mount suite or `pjdfstest`.
1. Add a dedicated smoke test for the consumer instead of the raw syscall wrapper, so the behavior is validated end-to-end through the mount.
1. Revisit whether any other libfuse hooks need explicit end-to-end coverage once `ioctl` has a real consumer.

Status: the mount suite now includes a real `ioctl/FIONREAD` consumer smoke test, so the first two items above are closed.
This section is now a documented compatibility note rather than active backlog.

#### Current status

- no known mount-smoke gap remains for `ioctl/FIONREAD`; keep the backend-only unit test as a low-level regression and the mount-suite smoke as the end-to-end check

### Repository / FUSE Boundary

Status: active design direction, but the main decisions below are already taken.

- Decision: `dbfs_fuse.py` should prefer explicit wrapper methods over runtime rebinding in `__init__`.
- Decision: lookup helpers and the `getattr()` / `readdir()` query layer should live in `dbfs_repository.py`, not directly in the main FUSE module.
- Decision: metadata cache payloads should stay split by type instead of sharing one generic cache payload across attrs and directory listings.
- Follow-up: when `dbfs_repository.py` grows enough, split it into lookup/query and mutation-oriented modules instead of moving SQL back into `dbfs_fuse.py`.

### Missing Filesystem Features

- No known open `file_metadata` gaps remain beyond the already-covered `change_date`/ctime tracking, `read`-driven `atime`, write/truncate/touch coverage, explicit `touch -a` / `touch -m` semantics, zero-length `write` handling, `truncate` no-op handling for unchanged sizes, and `utimens` no-op handling for unchanged timestamps on files and directories. Keep the regression tests in place.
- No known open permission gaps remain beyond the already-covered sticky-bit enforcement, owner/root checks, supplementary-group-aware `chown`, symlink metadata handling, special-bit clearing on ownership changes, unchanged-ownership `chown`/`chmod` no-op behavior on files and directories, and directory `setgid`/`setuid` inheritance semantics. Keep the regression tests in place.

### pjdfstest Observations

- DBFS keeps directory `setgid` bits on ownership changes to match Linux-like behavior observed in `pjdfstest`, while still clearing special bits on regular files.
- DBFS keeps `chown(-1, -1)` as an explicit no-op. The upstream `pjdfstest` cases note that POSIX allows timestamps to remain unchanged in that case, and DBFS keeps that behavior explicit and tested.
- DBFS keeps `unlink()` on directories as `EPERM`. The upstream `pjdfstest` coverage exercises this case, and DBFS keeps it covered by a regression test.
- If a future change touches `chmod`, `chown`, `rename`, or `utimens` semantics again, extend compatibility coverage with additional `pjdfstest` subsets.

### Operational

- `LICENSE` is already set to MIT.
- PostgreSQL TLS is optional: `sslmode=require` gives encryption, and `mkfs.dbfs.py --generate-client-tls-pair` can create a local client cert/key pair for certificate-auth setups during `init` or `upgrade`.
- DBFS expects transactional PostgreSQL connections with `autocommit` disabled; `read committed` is sufficient for the current lock and metadata flows.
- Detect single-node vs read-only replica mode early and let the runtime pick an appropriate lock strategy for each case; `postgres_lease` stays the default production backend for writable primary mounts.
- Keep `workers_read` and `workers_write` constrained to the cases where they really help: disjoint read gaps and segmented copy operations, not small contiguous fetches.
- The most likely long-term direction for DBFS is still userspace FUSE + PostgreSQL backend + a native Rust storage/hot-path engine, with Python kept as the orchestration layer until the native core is ready.

## Notes

- Storing `security.selinux` in xattr alone is not enough to make the filesystem fully SELinux-aware. It is the foundation, not the whole model.
- `mknod` creates FIFO and char-device metadata, but `open` for special nodes still needs separate semantics.
- `poll` is available as a backend helper for regular files; the native FUSE hook still depends on what `fusepy` exposes.
- `fallocate`, `flock`, `copy_file_range`, `ioctl`, `read_buf`, `write_buf`, `opendir`, `releasedir`, `fsyncdir`, `destroy`, `access`, `bmap`, `lseek`, `rename`, `mknod`, `st_blocks`, `st_nlink`, `ownership inheritance`, advisory `locks`, sticky-bit enforcement on `unlink`/`rmdir`, `chown` special-bit clearing, journal UID tracking, `ctime`/`change_date` metadata tracking, and the stable inode model are already in `Already in place`.
- `statfs` and `use_ino` have a dedicated shell smoke test: `make test-statfs-use-ino`.
- metadata cache and statfs cache are TTL-backed and configurable via `dbfs_config.ini`; keep cache invalidation on mutating operations in sync with the read-side cache helpers.
- schema upgrades are intentionally conservative for now: `init` is idempotent and non-destructive, `upgrade` repairs missing schema objects and restores `schema_version`, the schema-admin secret is required once a database already exists, and the repo still does not ship multi-step migration files for future schema changes.
- production profiles should remain documented and tested when tuning defaults change, because they are part of the supported runtime surface.
- the public roadmap lives in `ROADMAP.md`, and the current comparison baselines live in `BENCHMARKS.md`; keep both in sync with changes to CI or runtime tuning.
- PostgreSQL-backed advisory locking is the supported production path for both `flock` and `fcntl` range locks; crash-recovery/TTL-expiry coverage is in place, and the remaining work is mostly operational hardening plus any edge-case cleanup.
- For Linux VFS, the main priority now is to keep metadata, permission checks, `statfs`, and the repository/FUSE boundary sane and consistent.
