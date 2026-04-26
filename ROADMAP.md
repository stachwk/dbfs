# DBFS Roadmap

## Current Status

- The project has a working PostgreSQL-backed FUSE core with integration tests and documented runtime profiles.
- CI runs a curated regression matrix plus a byte-compile job for the current codebase.
- Benchmark baselines are tracked in [`BENCHMARKS.md`](BENCHMARKS.md), while [`TODO.md`](TODO.md) records decisions, accepted changes, and regression notes.
- SELinux mount-label policy is a deliberate non-goal; xattr-backed metadata and runtime gating are the supported path.
- Schema init/upgrade/clean are non-destructive by default on existing databases and are protected by a stored schema-admin secret; the behavior is covered by `make test-schema-upgrade`, while `mkfs.dbfs status` and `make test-schema-status` export the current schema state and migration manifest.
- The FUSE runtime is now Rust-backed: mount entrypoints, namespace mutations, lookup helpers, and the `getattr()` / `readdir()` query layer live in Rust rather than in Python.
- Metadata cache state is now split by payload type (attribute cache vs directory-entry cache), which removes the old mixed-payload ambiguity and keeps `getattr()` / `readdir()` behavior easier to reason about.
- PostgreSQL-backed advisory locking is the supported production path for both `flock` and `fcntl` range locks; the remaining work is operational hardening and edge-case cleanup, not maintaining a parallel in-memory backend.
- The long-term architecture direction is now explicit: Rust owns the runtime and hot-path engine, while non-runtime docs and test harnesses remain in Python only where needed.

## Completed Foundation

- PostgreSQL-backed FUSE filesystem core
- block-range reads with cache and read-ahead
- buffered writes with dirty tracking and chunked persistence
- xattr / ACL support
- advisory locking
- runtime tunables in `dbfs_config.ini`
- safe schema init/repair behavior
- CI workflow for core regression targets
- Rust-backed repository-layer delegation from the mount frontend
- query-layer extraction for `getattr()` and `readdir()`
- split metadata cache model for attrs vs directory entries

## Near Term

- keep CI green on the backend regression suite
- keep the benchmark baselines in [`BENCHMARKS.md`](BENCHMARKS.md) and the decision notes in [`TODO.md`](TODO.md) current
- compare write without `fsync`, write with `fsync`, and a larger `THROUGHPUT_BLOCK_SIZE` batch so the main throughput limiter is obvious
- keep the `THROUGHPUT_SYNC=1` comparison updated, since it is the main durability-vs-throughput reference and currently remains workload-sensitive rather than globally faster
- treat the current `bulk_write` vs `metadata_heavy` large-copy comparison as a baseline and continue the Rust POC on the write/copy hot path, with the planner, changed-copy dedupe, changed-run packing, persist padding, and read assembly enabled by default while Python stays as the fallback, instead of more Python tuning
- keep the PostgreSQL operating floor explicit: PostgreSQL 9.5+, transactional connections with `autocommit` disabled, `read committed` isolation, and `max_connections` comfortably above `pool_max_connections`
- keep the schema-admin secret flow explicit: the first `init` / `upgrade` on a database can bootstrap the secret, and later schema-tool actions require it
- keep the schema status export (`mkfs.dbfs status`) aligned with the migration manifest and the current schema version
- extend schema migrations to explicit multi-step migration files when the schema changes again
- keep the documented runtime profiles aligned with practical workloads
- keep the Rust-runtime / non-runtime-doc split visible in README and in the architecture notes as the project evolves
- keep the current Rust module boundaries explicit and avoid reintroducing legacy Python runtime layers

## Medium Term

- tighten mount-level smoke coverage where it adds real signal
- add more production-style profiles only when there is a measurable workload fit
- keep backup / restore behavior aligned with PostgreSQL operational practices
- further reduce direct SQL ownership in the remaining non-runtime documentation / harness layer only where it still exists

## Non-Goals for Now

- full SELinux mount-label policy
- special device node execution semantics beyond the stored metadata
- replacing PostgreSQL backup/restore with a custom DBFS-specific backup system
