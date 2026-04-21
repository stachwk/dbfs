#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-dirs
trap dbfs_test_cleanup EXIT

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

suffix="$(date +%s%N)"
dir="${MOUNTPOINT}/alpha-${suffix}"

mkdir -p "${dir}"
dbfs_stat "${dir}" '%n|%F|%h|%a|%u|%g' >/tmp/dbfs-dirs.alpha.stat
dbfs_ls "${MOUNTPOINT}" /tmp/dbfs-dirs.ls-root
dbfs_ls "${dir}" /tmp/dbfs-dirs.ls-alpha
dbfs_find_sorted "${MOUNTPOINT}" 1 /tmp/dbfs-dirs.find

root_nlink="$(dbfs_stat "${MOUNTPOINT}" '%h')"
dir_nlink="$(dbfs_stat "${dir}" '%h')"
root_blocks="$(dbfs_stat "${MOUNTPOINT}" '%b')"
dir_blocks="$(dbfs_stat "${dir}" '%b')"
dbfs_assert_ge "${root_nlink}" 3 "root hard links"
dbfs_assert_ge "${dir_nlink}" 2 "directory hard links"
dbfs_assert_ge "${root_blocks}" 1 "root blocks"
dbfs_assert_ge "${dir_blocks}" 1 "directory blocks"

if unlink "${dir}" 2>/tmp/dbfs-dirs.unlink.err; then
    echo "expected unlink on directory to fail"
    exit 1
fi
if ! grep -q "Is a directory\|Permission denied\|Operation not permitted" /tmp/dbfs-dirs.unlink.err; then
    echo "unexpected unlink error for directory"
    cat /tmp/dbfs-dirs.unlink.err
    exit 1
fi

rmdir "${dir}"

echo "OK directories/mkdir/rmdir/stat/ls/unlink-dir"
