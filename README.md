# DBFS

[![CI](https://github.com/stachwk/dbfs/actions/workflows/ci.yml/badge.svg)](https://github.com/stachwk/dbfs/actions/workflows/ci.yml) [Roadmap](ROADMAP.md) [Benchmarks](BENCHMARKS.md)

DBFS is a PostgreSQL-backed filesystem exposed through FUSE. It is designed to behave like a practical Linux filesystem, with predictable metadata, working directory semantics, advisory locking, ACL-aware access checks, and a test suite that exercises the hot paths end to end.

The project focuses on:

- stable filesystem metadata
- sensible Linux/VFS compatibility
- explicit runtime controls for SELinux, ACL, and atime policy
- integration tests that validate the real mount behavior, not just backend helpers

## Current Status

- Core FUSE operations are implemented and covered by integration tests.
- `make test-all` passes, and `make test-all-full` is available for wider coverage.
- Reads use block-range loading with a small read cache and read-ahead instead of loading whole files on every access.
- Lookup and namespace resolution have been split into `dbfs_namespace.py`, while the repository logic now lives under `mod/repository/` as a wrapper plus `lookup.py`, `attrs_listing.py`, `create.py`, `delete.py`, and `mutations.py`.
- The main FUSE module no longer owns direct path/ID resolution, namespace CRUD, or the query layer for `getattr()` / `readdir()`; those flows delegate through explicit repository wrappers.
- Metadata/query helpers and short-TTL caches have been split into `dbfs_metadata.py`, journal append logic lives in `dbfs_journal.py`, permission/ownership policy lives in `dbfs_permissions.py`, and mount/runtime validation lives in `dbfs_runtime_validation.py`.
- Metadata caching is now explicitly split between attribute cache and directory-entry cache instead of using one shared payload shape for both.
- SELinux is xattr-backed with runtime gating; full mount-label policy is intentionally out of scope.
- PostgreSQL TLS is optional and config-driven; DBFS can also generate a local client cert/key pair when requested.
- Transient PostgreSQL disconnects in the read/write hot path are retried once with state preserved in the client process, so in-flight dirty write state and read caches can survive a reconnect attempt.
- PostgreSQL-backed leases are the production lock path for both `flock` and `fcntl` range locks, with TTL and heartbeat. `make test-locking` remains the lock-semantics suite, while `make test-pg-lock-manager` covers the production PostgreSQL-backed backend, including a multi-client same-file write regression that proves two DBFS clients do not trample each other's lock-protected writes.
- Schema changes live under `migrations/` with sequential versions, an explicit `mkfs.dbfs.py status` export, and an upgrade path from older schema states.
- The current DBFS version is defined in `dbfs_version.py`, and both `dbfs_bootstrap.py --version` and `mkfs.dbfs.py --version` should print the same value.
- Performance work is merged, and the current benchmark baselines are recorded in `BENCHMARKS.md`.
- The local Docker Compose stack preloads `pg_stat_statements`, so query analysis and runtime profiling can use persistent PostgreSQL statistics.
- `TODO.md` serves as a decisions-and-notes log rather than an active implementation backlog.

## CI Coverage

The GitHub Actions workflow runs a small compile job plus a curated test matrix:

| Job | What it does |
| --- | --- |
| `compile` | Byte-compiles the core modules and the current integration-test entry points. |
| `workflow runtime` | Forces JavaScript actions onto Node 24 ahead of the GitHub default switch. |
| `test-runtime-config` | Verifies runtime config parsing and resolved tuning values. |
| `test-runtime-validation` | Verifies invalid runtime tuning values fail fast. |
| `test-runtime-profile` | Verifies named runtime profiles. |
| `test-schema-upgrade` | Verifies non-destructive schema init, version repair, and schema-admin secret protection. |
| `test-schema-status` | Verifies schema status export and the documented migration manifest. |
| `test-postgresql-requirements` | Verifies the minimum PostgreSQL version and connection capacity. |
| `test-metadata-cache` | Verifies the short-TTL metadata and `statfs` cache behavior. |
| `test-pg-lock-manager` | Verifies the PostgreSQL-backed lock backend and TTL / heartbeat behavior. |
| `test-read-ahead-sequence` | Verifies sequential read-ahead behavior. |
| `test-block-read` | Verifies block-range reads instead of whole-file reads. |
| `test-flush-release-profile` | Verifies flush/release profiling behavior. |

## Known Limits

- Full SELinux mount-label policy is intentionally out of scope; DBFS keeps SELinux as xattr-backed metadata plus runtime gating.
- `ioctl` support is intentionally limited to `FIONREAD` for now.
- Special device node metadata is stored, but direct special-node execution semantics are still not a general-purpose focus.
- `make test-all` is the main regression target; mount-heavy workflows are covered, but CI is still focused on a curated subset that is stable in automation.
- Schema upgrades are currently conservative: `init` is idempotent and non-destructive, `upgrade` repairs missing schema state and restores the current version, but the repo does not yet ship a long chain of migration files.
- DBFS normalizes timestamps through a UTC PostgreSQL session and UTC-aware conversions on the Python side so local timezone differences do not shift metadata. The UTC session setup is initialized once per physical pooled connection, not on every filesystem operation, and the database creation defaults are not relied on.
- Recovery is limited to retrying transient disconnects on the read/write hot path; DBFS keeps in-memory dirty state and caches across reconnects, but it does not yet implement full replay of arbitrary in-flight SQL work.

License: MIT

## Requirements

- Python 3
- `fusepy` (`pip install fusepy`)
- `psycopg2` or `psycopg2-binary`
- PostgreSQL
- FUSE support on the host
- `openssl` if you want DBFS to auto-generate a PostgreSQL TLS client cert/key pair

## Pip Packaging

DBFS can be installed into a virtual environment with pip:

```bash
make venv
make pip-install-editable
```

That installs the project scripts into the active venv:

- `dbfs-bootstrap`
- `mkfs.dbfs`
- `mount.dbfs`

The source tree still keeps the direct-run scripts `dbfs_bootstrap.py` and `mkfs.dbfs.py`; the pip package installs the shorter command names above. If you want a non-editable install, use `make pip-install`. Editable installs are available via `make pip-install-editable` if your venv can see `setuptools`. The package metadata lives in `setup.py`.
The installed `mount.dbfs` wrapper prefers `.venv/bin/dbfs-bootstrap` from the current project directory, then `dbfs-bootstrap` from `PATH`. If neither is available, it exits with a clear setup hint instead of guessing a Python interpreter.

Example:

```bash
DBFS_PROFILE=bulk_write mount.dbfs /mnt/dbfs
dbfs-bootstrap --profile bulk_write -f /mnt/dbfs
mount.dbfs /mnt/dbfs -o profile=bulk_write
```

PostgreSQL requirements for the current feature set:

- PostgreSQL 9.5 or newer
- `max_connections` should be comfortably above `pool_max_connections`; as a practical minimum, keep at least two extra server connections available for admin and concurrent DBFS clients
- no special lock-manager parameters are required; the default `read committed` transaction isolation is sufficient
- DBFS expects transactional PostgreSQL connections with `autocommit` disabled
- DBFS initializes the UTC session state once per pooled physical connection and keeps the steady-state return path to a cheap `rollback()`
- `sslmode=require` is enough for encrypted connections, and `verify-full` is appropriate if you also want certificate verification

| Requirement | Value |
| --- | --- |
| PostgreSQL version | `9.5+` |
| Transaction mode | `autocommit = off` |
| Isolation level | `read committed` |
| `max_connections` | `pool_max_connections + 2` or higher |
| TLS | `sslmode=require` for encryption, `verify-full` for certificate verification |

## Example `dbfs_config.ini`

This is a minimal starting point:

```ini
[database]
host = 127.0.0.1
port = 5432
dbname = dbfsdbname
user = dbfsuser
password = cichosza

[dbfs]
pool_max_connections = 10
synchronous_commit = on
write_flush_threshold_bytes = 67108864
read_cache_blocks = 1024
read_ahead_blocks = 4
sequential_read_ahead_blocks = 8
small_file_read_threshold_blocks = 8
workers_read = 4
workers_read_min_blocks = 8
workers_write = 4
workers_write_min_blocks = 8
metadata_cache_ttl_seconds = 1
statfs_cache_ttl_seconds = 2

[dbfs.profile.bulk_write]
write_flush_threshold_bytes = 268435456
read_cache_blocks = 512
read_ahead_blocks = 2
sequential_read_ahead_blocks = 4
small_file_read_threshold_blocks = 4
workers_read = 4
workers_read_min_blocks = 8
workers_write = 8
workers_write_min_blocks = 8
metadata_cache_ttl_seconds = 2
statfs_cache_ttl_seconds = 2

[dbfs.profile.metadata_heavy]
write_flush_threshold_bytes = 67108864
read_cache_blocks = 1024
read_ahead_blocks = 4
sequential_read_ahead_blocks = 8
small_file_read_threshold_blocks = 8
workers_read = 4
workers_read_min_blocks = 8
workers_write = 4
workers_write_min_blocks = 8
metadata_cache_ttl_seconds = 5
statfs_cache_ttl_seconds = 5

[dbfs.profile.pg_locking]
lock_backend = postgres_lease
lock_lease_ttl_seconds = 30
lock_heartbeat_interval_seconds = 10
lock_poll_interval_seconds = 0.05
```

## First Run

If this is your first time using DBFS, follow these steps:

1. Install the dependencies listed above.
1. Prepare PostgreSQL and make sure the database user and password in `dbfs_config.ini` are correct.
1. Choose where DBFS should read configuration from:
   - `/etc/dbfs/dbfs_config.ini`
   - or the local `./dbfs_config.ini`
1. Create the schema:

   ```bash
   python3 mkfs.dbfs.py init --schema-admin-password YOUR_SECRET
   ```

1. Mount the filesystem:

   ```bash
   python3 dbfs_bootstrap.py -f /path/to/mountpoint
   ```

1. Put a file into the mount, read it back, and confirm the data survives a remount.
1. When you are done, unmount it:

   ```bash
   fusermount3 -u /path/to/mountpoint
   ```

## Minimal Startup

If you only want the shortest path from zero to a mounted filesystem, run:

```bash
make up
make init
make mount
```

If you want to use the user-level config file instead of `/etc/dbfs/dbfs_config.ini`, use:

```bash
make install-config-user
make mount-user
```

## Quick Start

1. Configure `/etc/dbfs/dbfs_config.ini` or local `dbfs_config.ini`.
1. Optionally run `make install-config` to copy `dbfs_config.ini` to `/etc/dbfs/dbfs_config.ini`.
1. For local development you can run `make install-config-user` to install `dbfs_config.ini` to `~/.config/dbfs/dbfs_config.ini` without `sudo`.
1. Use `make config-show` to see which config file is resolved and `make mount-user` to force the user-level `~/.config/dbfs/dbfs_config.ini`.
1. Initialize the schema:

   ```bash
   python3 mkfs.dbfs.py init --schema-admin-password YOUR_SECRET
   ```

   If you want DBFS to generate a local PostgreSQL TLS client pair during schema setup, use:

   ```bash
   python3 mkfs.dbfs.py init --schema-admin-password YOUR_SECRET --generate-client-tls-pair 1
   ```

   The same switch also works with `upgrade`:

   ```bash
   python3 mkfs.dbfs.py upgrade --schema-admin-password YOUR_SECRET --generate-client-tls-pair 1
   ```

1. Mount the filesystem:

   ```bash
   python3 dbfs_bootstrap.py -f /path/to/mountpoint
   ```

## Supported Parameters

DBFS is controlled by a mix of CLI flags, environment variables, and config file values.

### Main DBFS Runtime

| Parameter | Type | Default | Effect |
| --- | --- | --- | --- |
| `-f`, `--mountpoint` | CLI | required | Mount point for the FUSE filesystem. |
| `--role auto|primary|replica` | CLI / `DBFS_ROLE` | `auto` | Controls read-only replica behavior and role autodetection. |
| `--selinux auto|on|off` | CLI / `DBFS_SELINUX` | `off` | Enables or disables `security.selinux` handling. |
| `--acl on|off` | CLI / `DBFS_ACL` | `off` | Enables or disables POSIX ACL enforcement. |
| `--default-permissions` / `--no-default-permissions` | CLI / `DBFS_DEFAULT_PERMISSIONS` | on | Controls whether kernel/default permission checks are enabled. |
| `--atime-policy default|noatime|nodiratime|relatime|strictatime` | CLI / `DBFS_ATIME_POLICY` | `default` | Selects the internal DBFS atime behavior. |
| `--lazytime` | CLI / `DBFS_LAZYTIME` | off | Enables the `lazytime` mount option. |
| `--sync` | CLI / `DBFS_SYNC` | off | Enables the `sync` mount option. |
| `--dirsync` | CLI / `DBFS_DIRSYNC` | off | Enables the `dirsync` mount option. |
| `DBFS_ALLOW_OTHER=1` | Environment | off | Enables `allow_other` if FUSE allows it. |
| `DBFS_DEBUG=1` | Environment | off | Enables debug mount mode by default. |
| `DBFS_LOG_LEVEL=DEBUG|INFO|...` | Environment | `INFO` | Controls logging verbosity. |
| `DBFS_CONFIG` | Environment | auto-resolved | Forces a specific config file path. |
| `DBFS_SELINUX_CONTEXT` | Environment | unset | Sets the SELinux mount `context=` option. |
| `DBFS_SELINUX_FSCONTEXT` | Environment | unset | Sets the SELinux mount `fscontext=` option. |
| `DBFS_SELINUX_DEFCONTEXT` | Environment | unset | Sets the SELinux mount `defcontext=` option. |
| `DBFS_SELINUX_ROOTCONTEXT` | Environment | unset | Sets the SELinux mount `rootcontext=` option. |
| `DBFS_DEFAULT_PERMISSIONS` | Environment | `1` | Controls whether default permissions are passed to FUSE. |
| `DBFS_ENTRY_TIMEOUT_SECONDS` | Environment | `0` | Controls FUSE directory-entry cache TTL. |
| `DBFS_ATTR_TIMEOUT_SECONDS` | Environment | `0` | Controls FUSE attribute cache TTL. |
| `DBFS_NEGATIVE_TIMEOUT_SECONDS` | Environment | `0` | Controls FUSE negative-entry cache TTL. |
| `DBFS_SYNCHRONOUS_COMMIT` | Environment | `on` | Controls PostgreSQL `synchronous_commit` per connection. |
| `DBFS_PERSIST_BUFFER_CHUNK_BLOCKS` | Environment | `128` | Controls how many dirty blocks DBFS batches per `persist_buffer()` SQL call. |
| `DBFS_PG_SSLMODE`, `DBFS_PG_SSLROOTCERT`, `DBFS_PG_SSLCERT`, `DBFS_PG_SSLKEY` | Environment | unset | Overrides PostgreSQL TLS connection parameters. |

### Configuration File

`dbfs_config.ini` is expected to contain a `[database]` section with PostgreSQL connection parameters:

- `host`
- `port`
- `dbname`
- `user`
- `password`
- `sslmode` for encrypted PostgreSQL connections, for example `require` or `verify-full`
- `sslrootcert` for the CA certificate used to verify the server
- `sslcert` and `sslkey` for optional client certificate authentication

It may also include a `[dbfs]` section with:

- `pool_max_connections`
- `write_flush_threshold_bytes`
- `read_cache_blocks`
- `read_ahead_blocks`
- `sequential_read_ahead_blocks`
- `small_file_read_threshold_blocks`
- `workers_read`
- `workers_read_min_blocks`
- `workers_write`
- `workers_write_min_blocks`
- `persist_buffer_chunk_blocks`
- `copy_skip_unchanged_blocks`
- `copy_skip_unchanged_blocks_min_blocks`
- `metadata_cache_ttl_seconds`
- `statfs_cache_ttl_seconds`
- `synchronous_commit`

### Schema Tool

`mkfs.dbfs.py` supports:

`init` is idempotent and does not drop `public`; `upgrade` recreates missing DBFS objects and restores `schema_version`; `clean` is the only destructive schema-tool action, and it becomes a no-op once the DBFS `public` schema is already gone. The schema tool uses a single explicit source for the schema-admin password: `--schema-admin-password`. If the password is missing, `init`, `upgrade`, and `clean` fail fast instead of prompting or generating a secret implicitly. `mkfs.dbfs.py status` reports whether the schema-admin secret is present and whether DBFS is ready without revealing the secret itself.

| Parameter | Type | Default | Effect |
| --- | --- | --- | --- |
| `init` | action | required | Create or repair the DBFS schema without dropping unrelated objects. |
| `upgrade` | action | required | Recreate missing DBFS objects and restore `schema_version` to the current code version. |
| `clean` | action | required | Drop DBFS objects created by the schema tool. |
| `status` | action | required | Export readiness, schema version, and migration-manifest status. |
| `--block-size N` | CLI | `4096` | Sets the default block size used when initializing the schema. |
| `--schema-admin-password PASS` | CLI | required | Schema-tool secret stored in the database and required for `init`, `upgrade`, and `clean`. |
| `--generate-client-tls-pair 1` | CLI | off | Generate a local PostgreSQL TLS client cert/key pair during `init` or `upgrade`. Use `0` to disable explicitly. |
| `--tls-material-dir PATH` | CLI | `.dbfs/tls` | Controls where generated PostgreSQL TLS material is stored. |
| `--tls-common-name NAME` | CLI | `dbfs` | Sets the common name used for generated TLS material. |
| `--tls-cert-days N` | CLI | `365` | Sets the certificate lifetime for generated TLS material. |

## Docker Lab

For a local PostgreSQL backend:

```bash
make up
make init
make smoke
make mount
# in another shell:
make unmount

# one-shot demo:
make demo

# integration test:
make test-integration

# role autodetect:
make test-role-autodetect

# full local check:
make test-all

# extended full local check:
make test-all-full
```

The project keeps the more specific smoke targets separate so you can rerun only the area you care about:

- `make test-files`
- `make test-block-read`
- `make test-directories`
- `make test-metadata`
- `make test-symlink`
- `make test-destroy`
- `make test-locking`
- `make test-permissions`
- `make test-hardlink`
- `make test-fallocate`
- `make test-copy-file-range`
- `make test-ioctl`
- `make test-mknod`
- `make test-bufio`
- `make test-lseek`
- `make test-poll`
- `make test-utimens-noop`
- `make test-timestamp-touch-once`
- `make test-read-ahead-sequence`
- `make test-read-cache-benchmark`
- `make test-runtime-config`
- `make test-runtime-validation`
- `make test-mkfs-pg-tls`
- `make test-metadata-cache`
- `make test-runtime-profile`
- `make test-schema-upgrade`
- `make test-schema-status`
- `make test-access-groups`
- `make test-inode-model`
- `make test-ownership-inheritance`
- `make test-bmap`
- `make test-statfs-use-ino`
- `make test-atime-noatime`
- `make test-atime-relatime`
- `make test-pool-connections`
- `make test-mount-suite`
- `make test-all-full`

## Mount Helper

If you want DBFS to behave like a `mount.dbfs` helper, install the wrapper script into a directory on your `PATH`:

```bash
sudo install -m 755 mount.dbfs /usr/local/sbin/mount.dbfs
```

You can do the same with:

```bash
make install-mount-helper
```

After that you can mount DBFS with:

```bash
mount.dbfs /mnt/dbfs
```

You can also pass DBFS-specific options through `-o`, for example:

```bash
mount.dbfs /mnt/dbfs -o role=auto,selinux=off,acl=off,default_permissions
```

If you need a custom config file, set `DBFS_CONFIG` before calling the helper:

```bash
DBFS_CONFIG=/path/to/dbfs_config.ini mount.dbfs /mnt/dbfs
```

What the tests cover:

- `make test-files` checks create/write/truncate/rename/unlink.
- `make test-directories` checks mkdir/rmdir/rename/stat/ls on directory trees and verifies that `unlink()` on a directory fails with `EPERM`.
- `make test-metadata` checks stat, chmod, chown, read, write, touch, truncate, access behavior, stable `st_dev` reporting, and `ctime`/`mtime`/`atime` updates on metadata changes, including explicit `touch -a` and `touch -m` semantics and `truncate` no-op handling for unchanged sizes.
- `make test-write-noop` checks that a zero-length `write()` is a no-op and does not advance `ctime`, `mtime`, or file size.
- `make test-symlink` checks `ln -s`, `readlink`, `cat` through the symlink, `mv` on the symlink itself, and the orphaned-symlink case after the target is removed. The test also shows the broken link with `ls -al` on the symlink path itself.
- `make test-destroy` checks that `destroy()` flushes pending buffers and leaves data durable for a new DBFS instance.
- `make test-journal` checks that the journal records the main mutating operations in order and stores the current OS user id.
- `make test-locking` checks lock semantics and ownership behavior, including range conflicts, shared-lock coexistence, and unlock cleanup.
- `make test-pg-lock-manager` checks the PostgreSQL-backed production lock backend with TTL and heartbeat, including the multi-client same-file write regression.
- `make test-permissions` checks sticky-bit enforcement on `unlink`/`rmdir`, rejects `chmod` on symlinks, allows root-only `chown` on symlinks, enforces owner/root checks plus supplementary-group-aware `chown`, keeps `chown(-1, -1)` as a no-op, treats `chown` with unchanged ownership as a no-op on both files and directories, treats `chmod` with an unchanged mode as a no-op on both files and directories, clears `setuid`/`setgid` on ownership changes for regular files, and preserves directory `setgid` while still clearing directory `setuid` when ownership changes.
- `make test-utimens-noop` checks that `utimens` with unchanged timestamps is a no-op and does not advance `ctime` on both regular files and directories.
- `pjdfstest` compatibility notes: DBFS keeps `unlink()` on directories as `EPERM`, preserves directory `setgid` bits on ownership changes, and treats `utimens` and ownership-change edge cases according to the Linux/POSIX behavior observed in the test suite.
- `make test-hardlink` checks hardlink creation, rename, and link-count behavior through the DBFS backend.
- `make test-fallocate` checks preallocation and zero-filled growth through the DBFS backend.
- `make test-copy-file-range` checks data copying with offsets through the DBFS backend.
- `make test-ioctl` checks `FIONREAD` ioctl support through the DBFS backend.
- `make test-mknod` checks FIFO and char-device creation plus `stat` type/rdev reporting through the DBFS backend. Open on special nodes is still unsupported.
- `make test-bufio` checks the backend `read_buf`/`write_buf` helpers and keeps their semantics aligned with `read`/`write`.
- `make test-lseek` checks the backend seek helper for `SEEK_SET`, `SEEK_CUR`, and `SEEK_END`.
- `make test-poll` checks the backend poll helper for readable/writable readiness on regular files.
- `make test-access-groups` checks `access()` for owner, primary group, and supplementary groups against backend state.
- `make test-inode-model` checks that `st_ino` survives rename and a full DBFS restart for directories, files, hardlinks, and symlinks.
- `make test-ownership-inheritance` checks that `chmod`/`chown` on a parent directory with setgid causes new children to inherit the parent gid, and that `rename` preserves source metadata while `mkdir` propagates setgid to new subdirectories.
- `make test-rename-root-conflict` checks file-over-file replacement, empty-dir replacement, cross-parent moves, and root-path edge cases for `rename`.
- `make test-bmap` checks the logical block mapping helper for regular files and hardlinks. It is not a physical disk map, just the most stable mapping available in this PostgreSQL-backed filesystem.
- `make test-statfs-use-ino` checks, through a small shell smoke, that mount-visible inode values match the backend and that `statvfs()` reports the same filesystem figures as the backend `statfs()` helper.
- `make test-mount-root-permissions` checks a fresh mount root plus directory chmod/chown/write behavior on a newly mounted filesystem.
- `make test-atime-noatime` checks DBFS atime behavior in `noatime` mode and confirms that reads do not advance atime.
- `make test-atime-relatime` checks DBFS atime behavior in `relatime` mode and confirms that a stale atime advances on read.
- `make test-atime-benchmark` prints a short wall-time baseline for DBFS atime behavior on file reads and directory listings so you can compare `default`, `noatime`, and `nodiratime` runs without paying for a very long smoke loop.
- `make test-pool-connections` checks that DBFS starts its PostgreSQL pool with the configured connection limit.
- `make test-mount-suite` is the main Python launcher-backed mount smoke suite; it covers files, directories, metadata, access modes, symlinks, `ioctl/FIONREAD`, file `read`-driven `atime`, feature-off runtime checks for ACL/SELinux, SELinux-on coverage when enabled, `df`, and replica read-only behavior in one run.
- `make test-throughput` runs a small `dd if=/dev/zero` benchmark on a mounted DBFS instance and prints elapsed time plus MiB/s.
- `make test-throughput-sync` is the ready-made fsync variant.
- `make test-large-copy-benchmark` measures a large `copy_file_range()` transfer through the backend and prints elapsed time plus MiB/s.
- `make test-large-file-multiblock-benchmark` measures a large multi-block file write and prints write/persist/flush split times.
- `make test-remount-durability-benchmark` checks that data survives a stop/remount/reopen cycle and prints the round-trip time.
- `make test-tree-scale` benchmarks `getattr` and `readdir` on a larger seeded tree and reports `ls`/`find` timings.
- `make test-flush-release-profile` checks that clean `flush()` / `release()` calls stay cheap and that a dirty flush persists exactly once.
- `make test-write-flush-threshold` checks that a low write-flush threshold can push dirty data before close and that the buffer is no longer left dirty after the write.
- `make test-all-full` extends `make test-all` with the standalone files/directories/metadata/symlink workflow checks, the shell `statfs/use_ino` smoke, the mount workflow smoke, both atime smoke profiles, and the throughput benchmarks.

`make test-all` includes the xattr/SELinux/trusted/ACL check and the consolidated mount suite.
Replica mounts can be forced with `--role replica`. Default `--role auto` detects replicas via `pg_is_in_recovery()` and mounts them read-only.

The current comparison baselines for throughput, large copy, large multi-block files, remount durability, read cache, and atime behavior live in [BENCHMARKS.md](BENCHMARKS.md).

## Runtime Options

If you need `allow_other`, run the mount with `DBFS_ALLOW_OTHER=1`, but only if your `/etc/fuse.conf` permits it.
`/etc/dbfs/dbfs_config.ini` can also include a `[dbfs]` section with `pool_max_connections = N` to control how many PostgreSQL connections the filesystem pool may open. The same section can also set storage and read-tuning defaults such as `write_flush_threshold_bytes`, `read_cache_blocks`, `read_ahead_blocks`, `sequential_read_ahead_blocks`, `small_file_read_threshold_blocks`, `metadata_cache_ttl_seconds`, and `statfs_cache_ttl_seconds`. If that file does not exist, DBFS falls back to `dbfs_config.ini` in the project root.
The same section may also set threaded read/write knobs such as `workers_read`, `workers_read_min_blocks`, `workers_write`, and `workers_write_min_blocks`, plus `persist_buffer_chunk_blocks` for larger or smaller `execute_values()` batches during flush. `workers_read` is only used when a read misses split into multiple disjoint block ranges, and `workers_write` is only used for copy operations that can be split into multiple source segments. `block_size` still matters here because the worker heuristics operate in blocks, not in raw bytes, so a smaller or larger block size can change when parallelism becomes worthwhile without directly turning "4 KiB" into "one thread". For rsync-like or repeated copy workloads, `copy_skip_unchanged_blocks` can compare destination blocks and skip unchanged ranges during `copy_file_range()`; keep it off by default unless you are copying into files that often already contain the same data. It can also set `synchronous_commit` to control PostgreSQL session durability per connection; valid values are `on`, `off`, `local`, `remote_write`, and `remote_apply`.
If you want a production-style preset, set `DBFS_PROFILE=bulk_write`, `DBFS_PROFILE=metadata_heavy`, or `DBFS_PROFILE=pg_locking` before mount. The selected profile overrides the base `[dbfs]` values from `dbfs_config.ini`.
You can also pass the profile explicitly as `--profile bulk_write` to `dbfs_bootstrap.py` / `dbfs-bootstrap`, or as `-o profile=bulk_write` to `mount.dbfs`.
The same `DBFS_PROFILE` variable works with `make mount`, `make mount-user`, and `make demo`.
SELinux xattr support is controlled with `--selinux auto|on|off` or `DBFS_SELINUX=auto|on|off`.
The default is `off`. Use `on` to force it or `auto` if you want host-driven detection.
POSIX ACL support is controlled with `--acl on|off` or `DBFS_ACL=on|off`.
The default is `off`.
At mount start DBFS logs the effective runtime profile, schema version, PostgreSQL TLS settings, PostgreSQL session durability (`synchronous_commit`), storage tuning, mount options, and lock backend so you can verify the active configuration without guessing which defaults were applied.
`DBFS_WRITE_FLUSH_THRESHOLD_BYTES` controls how much dirty data may accumulate before DBFS auto-persists a large write buffer during `write()`, `truncate()`, `fallocate()`, or `copy_file_range()`. The default is `67108864` bytes.
`metadata_cache_ttl_seconds` controls the short TTL cache for `getattr()` and `readdir()` metadata lookups. The default is `1` second.
`statfs_cache_ttl_seconds` controls the short TTL cache for `statfs()`. The default is `2` seconds.
`DBFS_METADATA_CACHE_TTL_SECONDS` and `DBFS_STATFS_CACHE_TTL_SECONDS` override the matching `dbfs_config.ini` values if you want to tune those caches per environment.
`DBFS_PROFILE` selects a named runtime profile from `dbfs_config.ini`, such as `bulk_write` or `metadata_heavy`.
`DBFS_ATIME_POLICY` is an internal DBFS behavior selector, not a raw FUSE mount option. It controls when DBFS updates `atime` in its own read path; `noatime`, `nodiratime`, `relatime`, and `strictatime` are handled inside DBFS instead of being forwarded to `fusepy`.
To avoid continuously rewriting the same timestamp row during a single open/read or open/readdir sequence, DBFS touches `access_date` only once per handle and then suppresses duplicate touches until the handle is released.
The same principle applies to write-side timestamp persistence: repeated writes on the same open file update `mtime`/`ctime` only when the dirty buffer is persisted, not on every intermediate write call.
Read path caching now defaults to a larger block LRU, and sequential reads increase read-ahead automatically so adjacent reads can reuse prefetched blocks instead of repeatedly hitting PostgreSQL.

## Backup and Restore

DBFS backup and restore are PostgreSQL backup and restore operations.

1. Use `pg_dump` / `pg_dumpall` or your standard PostgreSQL backup tooling.
1. Restore into a PostgreSQL instance that matches the DBFS schema version.
1. Run `make test-schema-upgrade` after restore if you want a quick schema init/upgrade safety check.
1. Keep the database dump and the `dbfs_config.ini` profile you used together so the restore lands on the same tuning baseline.

Mount-time visibility options:

- `--default-permissions` is enabled by default; disable with `--no-default-permissions` if you want FUSE-only checks.
- DBFS atime behavior can be selected with `--atime-policy default|noatime|nodiratime|relatime|strictatime`.
- `noatime` disables atime updates for both file reads and directory listings; `nodiratime` disables directory atime updates but keeps file atime updates enabled.
- `--lazytime`, `--sync`, and `--dirsync` are also available as mount options.
- SELinux mount labels can be passed through `DBFS_SELINUX_CONTEXT`, `DBFS_SELINUX_FSCONTEXT`, `DBFS_SELINUX_DEFCONTEXT`, and `DBFS_SELINUX_ROOTCONTEXT`.
- Set `DBFS_LOG_LEVEL=DEBUG` when you want full traceback-style diagnostics; the default is `INFO` so expected `ENODATA` cases stay quiet.
- `--acl on` is required if you want ACLs enforced during runtime; otherwise ACL xattrs stay inactive.
- `--selinux on` or `--selinux auto` is required if you want `security.selinux` active during runtime; otherwise SELinux xattrs stay inactive.
- `make test-mount-suite` contains both SELinux-off and SELinux-on smoke coverage; the SELinux-on case is skipped automatically unless the mount starts with `DBFS_SELINUX=on|auto`.
- DBFS stores SELinux labels as xattrs and gates them at runtime; it does not implement a full mount label policy on its own.
- That SELinux model is intentional: the repo treats full mount-label policy as out of scope and relies on host policy plus xattr storage for behavior checks.
- `mknod` creation and `stat` metadata for FIFO and char devices are supported; `st_rdev` and `st_dev` are reported, and special-node `open` is still unsupported.
- `system.posix_acl_*` is supported for access ACLs and default ACL inheritance; the backend stores, propagates, and enforces ACLs.
- `poll` is available as a backend helper for regular files; native FUSE hook support still depends on `fusepy`.

## Troubleshooting

- Start with `mkfs.dbfs.py status` to see whether the schema-admin secret is present and whether DBFS is ready.
- If `mkfs.dbfs.py init` fails, verify that PostgreSQL is running and the credentials in `dbfs_config.ini` match the server.
- If mounting fails with `DBFS schema is not initialized`, run `make init` first; for schema-tool operations, always pass `--schema-admin-password`.
- If mounting fails with `DBFS schema version mismatch`, run `mkfs.dbfs.py upgrade` with the schema-admin secret so the database schema matches the code version.
- On successful mount startup, DBFS logs `DBFS schema version=<db> expected=<code>` so you can confirm compatibility before using the mount.
- If mounting fails with `ENOTCONN` or a connection error, run `make smoke` first to confirm DB connectivity.
- If `fusermount3` is missing, try `fusermount` or install the FUSE userspace tools for your distribution.
- If `allow_other` is ignored, check `/etc/fuse.conf` and make sure `user_allow_other` is enabled.
- If ACL or SELinux features appear inactive, confirm that the mount was started with `--acl on` or `--selinux on|auto`.

## Recommended Mount Profiles

| Profile | Intended use | Key options |
| --- | --- | --- |
| `dbfs-relaxed` | Simple local dev and smoke runs | `--no-default-permissions`, `DBFS_ACL=off`, `DBFS_SELINUX=off`, `--atime-policy default` |
| `dbfs-linux-default` | Closest to a typical Linux mount | `--default-permissions`, `DBFS_ACL=off`, `DBFS_SELINUX=off`, `--atime-policy relatime` |
| `dbfs-selinux` | SELinux-aware environments | `--default-permissions`, `DBFS_ACL=on`, `DBFS_SELINUX=auto` or `on`, `DBFS_SELINUX_CONTEXT` as needed |

## Recommended Workloads

| Runtime profile | Good fit | Why |
| --- | --- | --- |
| `dbfs-relaxed` | Local development, smoke runs, and quick manual checks | Minimal policy friction and the loosest mount semantics. |
| `dbfs-linux-default` | Mixed workloads that should feel close to a normal Linux mount | Balanced defaults for ACL-off, SELinux-off, and relatime-style behavior. |
| `bulk_write` | Large sequential ingest, `copy_file_range()`, throughput runs, remount durability checks | Larger write flush batches and more aggressive write-side tuning. |
| `metadata_heavy` | `ls`, `find`, `stat`, browsing deep trees, many small metadata-only operations | Longer metadata cache TTL and more conservative write pressure. |
| `pg_locking` | Multi-client coordination and lock regression tests | Lock backend tuning only, with a shorter poll interval for lease checks. |

## Anti-Patterns

- Do not use `bulk_write` for metadata storms or tiny-file interactive browsing; it is tuned for throughput, not low-latency namespace churn.
- Do not use `metadata_heavy` for large sequential ingest or `copy_file_range()` workloads; it is intentionally conservative on the write side.
- Do not use `dbfs-relaxed` for multi-user or production-like mounts that need Linux-like permission semantics.
- Do not treat `synchronous_commit=off` as a default durability setting; use it only when the workload accepts the trade-off and the benchmark says it is worthwhile.
- Do not expect the `pg_locking` profile to improve write throughput by itself; it is about coordination semantics, not data-path speed.

## Target Architecture

DBFS is intentionally staying as a Python-orchestrated FUSE frontend for now:

- Python owns bootstrap, mkfs, config/profile loading, FUSE callbacks, admin logic, schema migrations, integration tests, and policy layers such as ACL/permissions/journal/runtime validation.
- Rust is the likely long-term hot-path engine for block assembly, overlay writes, copy segmentation, and persist-preparation work if and when the project moves that code out of Python.
- The goal is a thinner `dbfs_fuse.py`, more delegation into dedicated modules, and a future native storage core only where benchmarks justify it.
