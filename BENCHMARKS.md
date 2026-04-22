# DBFS Benchmarks

This file records the current comparison baselines for the main performance-sensitive paths.

## Current Status

- The benchmark suite is now tied to documented runtime profiles and CI-visible regression targets.
- Throughput, finalization, read-cache, and atime numbers are treated as baselines, not fixed promises.
- `make test-throughput` and `make test-flush-release-profile` are the current write-path and finalization entry points.
- Additional write-oriented baselines now cover large `copy_file_range()` transfers, large multi-block file writes, and remount durability checks.
- `test-tree-scale` now seeds a unique root per run and cleans it up afterward, so profile comparisons can be rerun on the same seed without duplicate-key conflicts.
- When a tuning change matters, the repository should record the before/after numbers here and in `TODO.md`.
- DBFS assumes transactional PostgreSQL connections with `autocommit` disabled; the practical operating floor is PostgreSQL 9.5+, `read committed`, and `max_connections` above `pool_max_connections + 2`.
- The next write-path comparison should separate `write` without `fsync`, `write` with `fsync`, and a larger `THROUGHPUT_BLOCK_SIZE` batch so the dominant bottleneck becomes explicit.
- `persist_buffer_chunk_blocks` is now a separate runtime knob for flush batching; larger batches can reduce SQL round-trips on dirty-write finalization.
- `synchronous_commit` is now a separate runtime knob; the latest local comparison was mixed across block sizes, so it is exposed for tuning rather than forced as the default.
- PostgreSQL session normalization to UTC is now initialized once per physical pooled connection; the measured steady-state overhead is effectively the pool acquire/release plus a cheap `rollback()`.

## Current Baseline Snapshot

### Write Path Throughput

Observed on a mounted DBFS instance:

- `4 KiB` burst writes: roughly `0.03 MiB/s`
- `1 MiB` write: roughly `4.53 MiB/s`
- `4 MiB` write: roughly `9.87 MiB/s`
- `8 MiB` write: roughly `9.06 MiB/s`
- `16 MiB` write: roughly `7.83 MiB/s`

### Recent Throughput Run

Observed on the current default throughput target (`make test-throughput`):

- `4M x8`
  - `33554432 bytes in 1.522s (21.02 MiB/s)`
- `8M x4`
  - `33554432 bytes in 1.486s (21.54 MiB/s)`
- `16M x2`
  - `33554432 bytes in 1.588s (20.15 MiB/s)`

### Finalization Profile

Observed on the current mounted DBFS instance with `DBFS_PROFILE_IO=1`:

- `persist_buffer_chunk_blocks=128`
  - `write_seconds=0.001252`
  - `persist_seconds=0.004660`
  - `flush_seconds=0.004696`
  - `finalization_seconds=0.009356`
- `persist_buffer_chunk_blocks=512`
  - `write_seconds=0.001751`
  - `persist_seconds=0.004242`
  - `flush_seconds=0.004312`
  - `finalization_seconds=0.008554`
- `release()` cleanup after `persist_buffer()`
  - `write_seconds=0.000913`
  - `persist_seconds=0.005033`
  - `flush_seconds=0.005079`
  - `finalization_seconds=0.010112`
- truncate-only flush/release on a large file
  - `persist_seconds=0.003040`
  - `flush_seconds=0.003073`
  - `finalization_seconds=0.006112`

The larger chunk setting shaved a bit off the finalization path on this run, so `bulk_write` now uses the larger batch size.
The write side itself is now effectively negligible in this profile; the remaining work is concentrated in `persist_buffer()` and `flush()`.
The latest small win came from switching block upserts inside `persist_buffer()` to PostgreSQL `execute_values()`, making the batch size configurable, avoiding an extra copy when building block payloads for flush, and caching dirty-byte accounting so `maybe_flush_dirty_write_buffer()` does not rescan every dirty block on each write.
Truncate-only finalization now short-circuits block packing when no dirty blocks remain, which keeps the large-file truncate path from paying extra Python-side work before the necessary tail delete.

## Throughput

The write path has also been measured on a large sequential write where chunked persistence prevented PostgreSQL client buffer exhaustion.

Recent comparison on the current runtime profile:

- `THROUGHPUT_BLOCK_SIZE=4M THROUGHPUT_COUNT=8`
  - `33554432 bytes in 6.217s (5.15 MiB/s)`
- `THROUGHPUT_BLOCK_SIZE=4M THROUGHPUT_COUNT=8 THROUGHPUT_SYNC=1`
  - `33554432 bytes in 6.476s (4.94 MiB/s)`
- `THROUGHPUT_BLOCK_SIZE=8M THROUGHPUT_COUNT=4`
  - `33554432 bytes in 6.388s (5.01 MiB/s)`

Current read:
- `write` without `fsync` is still the fastest of the three.
- `write` with `fsync` is the clearest durable-write penalty.
- a larger `THROUGHPUT_BLOCK_SIZE` did not beat the current `4M` baseline on this run, so the bottleneck is not just block granularity.

### Synchronous Commit

Observed on the current flush/release profile:

- `DBFS_SYNCHRONOUS_COMMIT=on`
  - `write_seconds=0.000605`
  - `persist_seconds=0.007334`
  - `flush_seconds=0.007374`
  - `finalization_seconds=0.014708`
- `DBFS_SYNCHRONOUS_COMMIT=off`
  - `write_seconds=0.000870`
  - `persist_seconds=0.005471`
  - `flush_seconds=0.005533`
  - `finalization_seconds=0.011004`

On this local Docker/PostgreSQL run, `off` improved the flush/release path, while the overall throughput comparisons below still remain workload-sensitive, so it is kept as an explicit tuning knob rather than a forced default.

#### Throughput Comparison

Observed on the current throughput profile:

- `4M x8`
  - `DBFS_SYNCHRONOUS_COMMIT=off` -> `33554432 bytes in 6.217s (5.15 MiB/s)`
  - `DBFS_SYNCHRONOUS_COMMIT=on` -> `33554432 bytes in 6.287s (5.09 MiB/s)`
- `8M x4`
  - `DBFS_SYNCHRONOUS_COMMIT=off` -> `33554432 bytes in 6.388s (5.01 MiB/s)`
- `16M x2`
  - `DBFS_SYNCHRONOUS_COMMIT=off` -> `33554432 bytes in 6.414s (4.99 MiB/s)`
  - `DBFS_SYNCHRONOUS_COMMIT=on` -> `33554432 bytes in 6.484s (4.94 MiB/s)`

The effect is workload-sensitive: `off` helped some batch sizes and slightly hurt another, so the knob remains explicit rather than being forced globally.

`copy_skip_unchanged_blocks` should follow the same rule: keep it off for ordinary ingest and one-shot copies, and only enable it for rsync-like workloads or repeated copy-heavy syncs where destination blocks are often already identical. The extra destination reads can easily outweigh the saved writes if the file contents are usually changing anyway.

Worker parallelism is still block-oriented, so `block_size` changes when a workload crosses the thresholds for `workers_read` or `workers_write`. It does not translate directly into "N bytes per thread"; it only changes how many blocks a given transfer is split into before the worker thresholds are applied.

### Bulk Write Profile Comparison

Observed on the current `bulk_write` profile after restoring a stronger read-side:

- large sequential copy
  - `bytes=67108864`
  - `elapsed_s=2.491982`
  - `throughput_mib_s=25.68`
- large multi-block file write
  - `bytes=67108864`
  - `elapsed_s=2.229123`
  - `throughput_mib_s=28.71`
  - `write_seconds=0.072068`
  - `persist_seconds=2.110270`
  - `flush_seconds=2.112674`
  - `finalization_seconds=4.222943`
- flush/release profile
  - `write_seconds=0.001076`
  - `persist_seconds=0.006235`
  - `flush_seconds=0.006303`
  - `finalization_seconds=0.012537`

The write-path optimization that avoids loading brand-new blocks from PostgreSQL before writing them made the `bulk_write` profile much stronger on copy-heavy ingest and large multi-block writes.
The profile is still workload-specific, but it now clearly favors the intended ingest/copy path while keeping finalization cost bounded.

### Copy Dedupe / Repeated Copy

Observed on a repeated copy where the destination already contained the same block content:

- `copy_skip_unchanged_blocks=off`
  - `bytes=67108864`
  - `elapsed_s=8.557302`
  - `throughput_mib_s=7.48`
  - `write_seconds=0.000000`
  - `persist_seconds=1.753822`
  - `flush_seconds=0.000000`
  - `finalization_seconds=1.753822`
- `copy_skip_unchanged_blocks=on`
  - `bytes=67108864`
  - `elapsed_s=56.208670`
  - `throughput_mib_s=1.14`
  - `write_seconds=0.000000`
  - `persist_seconds=0.000000`
  - `flush_seconds=0.000000`
  - `finalization_seconds=0.000000`

This run shows that the dedupe path is only worth enabling for cases where avoiding rewritten destination blocks matters more than the extra comparison cost. For identical destination copies on this host, the comparison overhead is much higher than a normal replay of the write path, so the knob should stay off by default and be reserved for repeated sync-style workloads with a clear skip win.

### PostgreSQL Session Cost

Measured on a pooled DBFS backend:

- first pooled connection initialization:
  - `first_ms=1.0561`
- steady state after warmup:
  - `steady_mean_ms=0.2841`
  - `steady_p95_ms=0.4627`

Interpretation:

- the UTC `SET TIME ZONE` cost is paid once per physical connection
- after warmup, the remaining overhead is sub-millisecond per acquire and still small compared with filesystem-level I/O

## Read Cache

Sequential read-cache comparison:

- `DBFS_READ_CACHE_BLOCKS=256` -> `elapsed_ms=14379`
- `DBFS_READ_CACHE_BLOCKS=1024` -> `elapsed_ms=3244`

The larger cache is the current default and the tests keep the regression covered.

## Tree Scale / Metadata Heavy

Comparison on the same `20 x 20` seeded tree:

- default profile
  - `dirs=20`
  - `files_per_dir=20`
  - `ls_ms=621.00`
  - `find_ms=9478.38`
- `metadata_heavy`
  - `dirs=20`
  - `files_per_dir=20`
  - `ls_ms=401.25`
  - `find_ms=8581.42`

`metadata_heavy` is noticeably better for `ls` on this tree and slightly better for `find`, which matches its goal: reduce metadata churn on tree-walking workloads without pushing the write side.

## Atime Behavior

Short wall-time benchmark on file reads and directory listings:

- file reads:
  - `default=1742 ms`
  - `noatime=453 ms`
  - `nodiratime=1744 ms`
- directory listings:
  - `default=70412 ms`
  - `noatime=66684 ms`
  - `nodiratime=65972 ms`

The benchmark is useful as a smoke baseline, not as a strong microbenchmark for exact atime savings.

## Large Copy

Large `copy_file_range()` benchmark on the current runtime profile:

- `bytes=67108864`
  - `elapsed_s=2.491982`
  - `throughput_mib_s=25.68`

This is the current baseline for large backend copy operations.

## Large Multi-Block Files

Large multi-block file write benchmark on the current runtime profile:

- `bytes=67108864`
  - `elapsed_s=2.229123`
  - `throughput_mib_s=28.71`
  - `write_seconds=0.072068`
  - `persist_seconds=2.110270`
  - `flush_seconds=2.112674`
  - `finalization_seconds=4.222943`

This baseline tracks a large file write split across many blocks so the write/persist split stays visible.

## Remount Durability

Remount durability smoke benchmark on the current runtime profile:

- `bytes=24576`
  - `elapsed_s=1.087950`

This is a durability baseline, not a throughput target. The goal is to keep the remount/reopen path explicit and data-safe.
