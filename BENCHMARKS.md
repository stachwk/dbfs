# DBFS Benchmarks

This file records the current comparison baselines for the main performance-sensitive paths.

## Current Status

- The benchmark suite is now tied to documented runtime profiles and CI-visible regression targets.
- Throughput, finalization, read-cache, and atime numbers are treated as baselines, not fixed promises.
- `make test-throughput` and `make test-flush-release-profile` are the current write-path and finalization entry points.
- When a tuning change matters, the repository should record the before/after numbers here and in `TODO.md`.
- DBFS assumes transactional PostgreSQL connections with `autocommit` disabled; the practical operating floor is PostgreSQL 9.5+, `read committed`, and `max_connections` above `pool_max_connections + 2`.
- The next write-path comparison should separate `write` without `fsync`, `write` with `fsync`, and a larger `THROUGHPUT_BLOCK_SIZE` batch so the dominant bottleneck becomes explicit.
- `persist_buffer_chunk_blocks` is now a separate runtime knob for flush batching; larger batches can reduce SQL round-trips on dirty-write finalization.
- `synchronous_commit` is now a separate runtime knob; the latest local comparison was mixed across block sizes, so it is exposed for tuning rather than forced as the default.
- PostgreSQL session normalization to UTC is now initialized once per physical pooled connection; the measured steady-state overhead is effectively the pool acquire/release plus a cheap `rollback()`.

## Current Baseline Snapshot

### Write Path Throughput

Observed on a mounted DBFS instance:

- `4 KiB` burst writes: roughly `0.47 MB/s`
- `1 MiB` write: roughly `1.77 MB/s`
- `4 MiB` write: roughly `2.0 MB/s`
- `8 MiB` write: roughly `2.4 MB/s`
- `16 MiB` write: roughly `2.7 MB/s`

### Finalization Profile

Observed on the current mounted DBFS instance with `DBFS_PROFILE_IO=1`:

- `persist_buffer_chunk_blocks=128`
  - `write_seconds=0.002033`
  - `persist_seconds=0.004535`
  - `flush_seconds=0.004594`
  - `finalization_seconds=0.009129`
- `persist_buffer_chunk_blocks=512`
  - `write_seconds=0.001751`
  - `persist_seconds=0.004242`
  - `flush_seconds=0.004312`
  - `finalization_seconds=0.008554`

The larger chunk setting shaved a bit off the finalization path on this run, so `bulk_write` now uses the larger batch size.
The write side itself is now effectively negligible in this profile; the remaining work is concentrated in `persist_buffer()` and `flush()`.
The latest small win came from switching block upserts inside `persist_buffer()` to PostgreSQL `execute_values()` and from making the batch size configurable, which shaved a bit off the total finalization path without changing write semantics.

## Throughput

The write path has also been measured on a large sequential write where chunked persistence prevented PostgreSQL client buffer exhaustion.

Recent comparison on the current runtime profile:

- `THROUGHPUT_BLOCK_SIZE=4M THROUGHPUT_COUNT=8`
  - `33554432 bytes in 12.910s (2.48 MiB/s)`
- `THROUGHPUT_BLOCK_SIZE=4M THROUGHPUT_COUNT=8 THROUGHPUT_SYNC=1`
  - `33554432 bytes in 15.187s (2.11 MiB/s)`
- `THROUGHPUT_BLOCK_SIZE=8M THROUGHPUT_COUNT=4`
  - `33554432 bytes in 14.467s (2.21 MiB/s)`

Current read:
- `write` without `fsync` is still the fastest of the three.
- `write` with `fsync` is the clearest durable-write penalty.
- a larger `THROUGHPUT_BLOCK_SIZE` did not beat the current `4M` baseline on this run, so the bottleneck is not just block granularity.

### Synchronous Commit

Observed on the current flush/release profile:

- `DBFS_SYNCHRONOUS_COMMIT=on`
  - `write_seconds=0.001639`
  - `persist_seconds=0.004723`
  - `flush_seconds=0.004774`
  - `finalization_seconds=0.009497`
- `DBFS_SYNCHRONOUS_COMMIT=off`
  - `write_seconds=0.001668`
  - `persist_seconds=0.006771`
  - `flush_seconds=0.006817`
  - `finalization_seconds=0.013588`

On this local Docker/PostgreSQL run, `off` did not consistently improve the finalization path, so it is currently treated as an explicit tuning knob rather than a better default.

#### Throughput Comparison

Observed on the current throughput profile:

- `4M x8`
  - `DBFS_SYNCHRONOUS_COMMIT=off` -> `33554432 bytes in 10.704s (2.99 MiB/s)`
  - `DBFS_SYNCHRONOUS_COMMIT=on` -> `33554432 bytes in 12.389s (2.58 MiB/s)`
- `8M x4`
  - `DBFS_SYNCHRONOUS_COMMIT=off` -> `33554432 bytes in 12.019s (2.66 MiB/s)`
  - `DBFS_SYNCHRONOUS_COMMIT=on` -> `33554432 bytes in 12.223s (2.62 MiB/s)`
- `16M x2`
  - `DBFS_SYNCHRONOUS_COMMIT=off` -> `33554432 bytes in 11.849s (2.70 MiB/s)`
  - `DBFS_SYNCHRONOUS_COMMIT=on` -> `33554432 bytes in 11.696s (2.74 MiB/s)`

The effect is workload-sensitive: `off` helped some batch sizes and slightly hurt another, so the knob remains explicit rather than being forced globally.

### PostgreSQL Session Cost

Measured on a pooled DBFS backend:

- first pooled connection initialization:
  - `first_ms=0.8013`
- steady state after warmup:
  - `steady_mean_ms=0.0029`
  - `steady_p95_ms=0.0032`

Interpretation:

- the UTC `SET TIME ZONE` cost is paid once per physical connection
- after warmup, the remaining overhead is negligible compared with filesystem-level I/O

## Read Cache

Sequential read-cache comparison:

- `DBFS_READ_CACHE_BLOCKS=256` -> `elapsed_ms=14379`
- `DBFS_READ_CACHE_BLOCKS=1024` -> `elapsed_ms=7563`

The larger cache is the current default and the tests keep the regression covered.

## Atime Behavior

Short wall-time benchmark on file reads and directory listings:

- file reads:
  - `default=429 ms`
  - `noatime=414 ms`
  - `nodiratime=543 ms`
- directory listings:
  - `default=29489 ms`
  - `noatime=30130 ms`
  - `nodiratime=29949 ms`

The benchmark is useful as a smoke baseline, not as a strong microbenchmark for exact atime savings.
