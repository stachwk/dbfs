# DBFS Benchmarks

This file records the current comparison baselines for the main performance-sensitive paths.

## Current Status

- The benchmark suite is now tied to documented runtime profiles and CI-visible regression targets.
- Throughput, finalization, read-cache, and atime numbers are treated as baselines, not fixed promises.
- `make test-throughput` and `make test-flush-release-profile` are the current write-path and finalization entry points.
- When a tuning change matters, the repository should record the before/after numbers here and in `TODO.md`.
- DBFS assumes transactional PostgreSQL connections with `autocommit` disabled; the practical operating floor is PostgreSQL 9.5+, `read committed`, and `max_connections` above `pool_max_connections + 2`.
- The next write-path comparison should separate `write` without `fsync`, `write` with `fsync`, and a larger `THROUGHPUT_BLOCK_SIZE` batch so the dominant bottleneck becomes explicit.

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

- `write_seconds=0.000035`
- `persist_seconds=0.005019`
- `flush_seconds=0.005066`
- `finalization_seconds=0.010085`

The write side itself is now effectively negligible in this profile; the remaining work is concentrated in `persist_buffer()` and `flush()`.

## Throughput

The write path has also been measured on a large sequential write where chunked persistence prevented PostgreSQL client buffer exhaustion.

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
