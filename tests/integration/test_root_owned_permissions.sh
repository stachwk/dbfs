#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"

tmpdir="$(mktemp -d /tmp/dbfs-root-owned.XXXXXX)"
trap 'set +e; if [[ -n "${DBFS_PID:-}" ]] && kill -0 "${DBFS_PID}" >/dev/null 2>&1; then kill "${DBFS_PID}" >/dev/null 2>&1 || true; wait "${DBFS_PID}" >/dev/null 2>&1 || true; fi; if [[ -n "${MOUNTPOINT:-}" ]]; then fusermount3 -u "${MOUNTPOINT}" 2>/dev/null || fusermount -u "${MOUNTPOINT}" 2>/dev/null || umount "${MOUNTPOINT}" 2>/dev/null || true; fi; rm -rf "${tmpdir}"' EXIT

if ! grep -Eq '^[[:space:]]*user_allow_other[[:space:]]*$' /etc/fuse.conf 2>/dev/null; then
  echo "SKIP root-owned permissions (user_allow_other is disabled in /etc/fuse.conf)"
  exit 0
fi

local_dir="$(mktemp -d /home/wojtek/dbfs-root-owned-local.XXXXXX)"
local_file="${local_dir}/root-owned.txt"

mkdir -p "${local_dir}"
sudo -n install -m 0644 /dev/null "${local_file}"
local_stat="$(stat -c '%u:%g|%a' "${local_file}")"
rm -f "${local_file}"
rmdir "${local_dir}"

MOUNTPOINT="${tmpdir}/dbfs-root-owned"
mkdir -p "${MOUNTPOINT}"
POSTGRES_DB="${POSTGRES_DB}" POSTGRES_USER="${POSTGRES_USER}" POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  sudo -n env DBFS_USE_FUSE_CONTEXT=1 /usr/local/sbin/mount.dbfs "${MOUNTPOINT}" \
  -o role=auto,selinux=off,acl=off,default_permissions,allow_other,profile=default \
  >/tmp/dbfs-root-owned.mount.log 2>&1 &
DBFS_PID=$!

for _ in $(seq 1 60); do
  if mountpoint -q "${MOUNTPOINT}"; then
    break
  fi
  if ! kill -0 "${DBFS_PID}" >/dev/null 2>&1; then
    cat /tmp/dbfs-root-owned.mount.log
    echo "DBFS root mount failed"
    exit 1
  fi
  sleep 1
done

if ! mountpoint -q "${MOUNTPOINT}"; then
  cat /tmp/dbfs-root-owned.mount.log
  echo "DBFS root mount did not become ready"
  exit 1
fi

dbfs_dir="${MOUNTPOINT}/root-owned"
mkdir -p "${dbfs_dir}"
dbfs_file="${dbfs_dir}/root-owned.txt"
sudo -n install -m 0644 /dev/null "${dbfs_file}"
dbfs_stat="$(stat -c '%u:%g|%a' "${dbfs_file}")"
rm -f "${dbfs_file}"
rmdir "${dbfs_dir}"

if [[ "${local_stat}" != "${dbfs_stat}" ]]; then
  echo "local root-owned stat: ${local_stat}"
  echo "dbfs root-owned stat:  ${dbfs_stat}"
  exit 1
fi

echo "OK root-owned-permissions ${local_stat}"
