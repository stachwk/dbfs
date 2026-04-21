#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-statfs-use-ino

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

suffix="$(python3 - <<'PY'
import uuid
print(uuid.uuid4().hex[:8])
PY
)"
dir_path="${MOUNTPOINT}/statfs-${suffix}"
file_path="${dir_path}/payload.txt"
backend_dump="$(mktemp /tmp/dbfs-statfs-use-ino.backend.XXXXXX)"

cleanup() {
  rm -f "${backend_dump}"
  dbfs_test_cleanup
}
trap cleanup EXIT

mkdir -p "${dir_path}"
printf '%s\n' "statfs use_ino" > "${file_path}"

export dir_path
ROOT="${ROOT}" VENV_PYTHON="${VENV_PYTHON}" DBFS_SELINUX="${DBFS_SELINUX}" DBFS_ACL="${DBFS_ACL}" DBFS_DEFAULT_PERMISSIONS="${DBFS_DEFAULT_PERMISSIONS}" DBFS_ATIME_POLICY="${DBFS_ATIME_POLICY}" DBFS_LAZYTIME="${DBFS_LAZYTIME}" DBFS_SYNC="${DBFS_SYNC}" DBFS_DIRSYNC="${DBFS_DIRSYNC}" "${VENV_PYTHON}" - <<'PY' > "${backend_dump}"
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(os.environ["ROOT"])
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config

dsn, db_config = load_dsn_from_config(ROOT)
fs = DBFS(dsn, db_config)
try:
    suffix = os.path.basename(os.environ["dir_path"])
    file_rel = f"/{suffix}/payload.txt"
    dir_rel = f"/{suffix}"
    file_stat = fs.getattr(file_rel)
    dir_stat = fs.getattr(dir_rel)
    statfs = fs.statfs("/")
    print(f"backend_file_ino={file_stat['st_ino']}")
    print(f"backend_dir_ino={dir_stat['st_ino']}")
    print(f"backend_bsize={statfs['f_bsize']}")
    print(f"backend_frsize={statfs['f_frsize']}")
    print(f"backend_blocks={statfs['f_blocks']}")
    print(f"backend_bfree={statfs['f_bfree']}")
    print(f"backend_bavail={statfs['f_bavail']}")
    print(f"backend_files={statfs['f_files']}")
    print(f"backend_ffree={statfs['f_ffree']}")
finally:
    fs.connection_pool.closeall()
PY

set -a
source "${backend_dump}"
set +a

mount_file_ino="$(stat -c '%i' "${file_path}")"
mount_dir_ino="$(stat -c '%i' "${dir_path}")"
mount_statfs="$(stat -f -c 'bsize=%S frsize=%s blocks=%b bfree=%f bavail=%a files=%c ffree=%d' "${MOUNTPOINT}")"
mount_df="$(df -Ph "${MOUNTPOINT}")"

if [[ "${mount_file_ino}" != "${backend_file_ino}" ]]; then
  echo "file inode mismatch: mount=${mount_file_ino} backend=${backend_file_ino}"
  exit 1
fi

if [[ "${mount_dir_ino}" != "${backend_dir_ino}" ]]; then
  echo "directory inode mismatch: mount=${mount_dir_ino} backend=${backend_dir_ino}"
  exit 1
fi

if [[ "${mount_statfs}" != "bsize=${backend_bsize} frsize=${backend_frsize} blocks=${backend_blocks} bfree=${backend_bfree} bavail=${backend_bavail} files=${backend_files} ffree=${backend_ffree}" ]]; then
  echo "statfs mismatch: ${mount_statfs}"
  echo "expected: bsize=${backend_bsize} frsize=${backend_frsize} blocks=${backend_blocks} bfree=${backend_bfree} bavail=${backend_bavail} files=${backend_files} ffree=${backend_ffree}"
  exit 1
fi

if ! grep -Fq -- "${MOUNTPOINT}" <<<"${mount_df}"; then
  echo "df output does not mention mountpoint"
  exit 1
fi

echo "OK statfs/use_ino"
