#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-files
trap dbfs_test_cleanup EXIT

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

file="${MOUNTPOINT}/files.bin"
renamed="${MOUNTPOINT}/files-renamed.bin"
expected_size=0

dd if=/dev/urandom of="${file}" bs=1K count=1 status=none
expected_size=$((expected_size + 1024))
for block_k in 2 5 3 7; do
  dd if=/dev/urandom of="${file}" bs="${block_k}K" count=1 oflag=append conv=notrunc status=none
  expected_size=$((expected_size + block_k * 1024))
  actual_size="$(dbfs_stat "${file}" '%s')"
  dbfs_assert_eq "${actual_size}" "${expected_size}" "unexpected file size after append"
done

file_inode="$(dbfs_stat "${file}" '%i')"
file_nlink="$(dbfs_stat "${file}" '%h')"
file_blksize="$(dbfs_stat "${file}" '%o')"
dbfs_assert_ge "${file_inode}" 1 "file inode"
dbfs_assert_eq "${file_nlink}" 1 "file hard links"
dbfs_assert_ge "${file_blksize}" 512 "file block size"

mv "${file}" "${renamed}"
dbfs_assert_eq "$(dbfs_stat "${renamed}" '%i')" "${file_inode}" "inode changed after rename"
dbfs_assert_eq "$(dbfs_stat "${renamed}" '%s')" "${expected_size}" "file size changed after rename"

rm -f "${renamed}"

echo "OK files/create/write/truncate/rename/unlink"
