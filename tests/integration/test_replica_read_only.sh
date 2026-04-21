#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-replica-ro
trap dbfs_test_cleanup EXIT

dbfs_test_init_schema

POSTGRES_DB="${POSTGRES_DB}" POSTGRES_USER="${POSTGRES_USER}" POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" "${VENV_PYTHON}" - <<PY
import os
from dbfs_fuse import DBFS, load_dsn_from_config

root = "${ROOT}"
dsn, db_config = load_dsn_from_config(root)
fs = DBFS(dsn, db_config)
dir_path = "/replica_seed"
file_path = f"{dir_path}/seed.txt"
fs.mkdir(dir_path, 0o755)
fh = fs.create(file_path, 0o644)
fs.write(file_path, b"seed-data", 0, fh)
fs.flush(file_path, fh)
fs.release(file_path, fh)
PY

DBFS_ROLE=replica dbfs_test_start_mount "${MOUNTPOINT}"

cat "${MOUNTPOINT}/replica_seed/seed.txt" >/tmp/dbfs-replica-ro.read
grep -q 'seed-data' /tmp/dbfs-replica-ro.read

if touch "${MOUNTPOINT}/new.txt" 2>/tmp/dbfs-replica-ro.touch.err; then
  echo "Expected touch to fail on replica mount"
  exit 1
fi
grep -Eq 'Read-only file system|Operation not permitted' /tmp/dbfs-replica-ro.touch.err

if mkdir "${MOUNTPOINT}/newdir" 2>/tmp/dbfs-replica-ro.mkdir.err; then
  echo "Expected mkdir to fail on replica mount"
  exit 1
fi
grep -Eq 'Read-only file system|Operation not permitted' /tmp/dbfs-replica-ro.mkdir.err

echo "OK replica/read-only"
