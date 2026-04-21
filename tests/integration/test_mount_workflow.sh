#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-mount-workflow
trap dbfs_test_cleanup EXIT

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

file="${MOUNTPOINT}/grow.bin"
dir="${MOUNTPOINT}/alpha"
subdir_a="${dir}/beta"
subdir_b="${dir}/gamma"
rename_file="${MOUNTPOINT}/grow-renamed.bin"
rename_dir="${dir}/beta-renamed"
expected_size=0

dd if=/dev/urandom of="${file}" bs=1K count=1 status=none
expected_size=$((expected_size + 1024))
file_inode="$(stat -c '%i' "${file}")"
file_blksize="$(stat -c '%o' "${file}")"
file_nlink="$(stat -c '%h' "${file}")"
[[ "${file_inode}" -gt 0 ]]
[[ "${file_blksize}" -ge 512 ]]
[[ "${file_nlink}" -eq 1 ]]

for block_k in 2 5 3 7; do
  dd if=/dev/urandom of="${file}" bs="${block_k}K" count=1 oflag=append conv=notrunc status=none
  expected_size=$((expected_size + block_k * 1024))
  actual_size="$(stat -c '%s' "${file}")"
  [[ "${actual_size}" -eq "${expected_size}" ]]
done

mkdir -p "${subdir_a}" "${subdir_b}" "${MOUNTPOINT}/delta"
root_nlink="$(stat -c '%h' "${MOUNTPOINT}")"
dir_nlink="$(stat -c '%h' "${dir}")"
[[ "${root_nlink}" -ge 3 ]]
[[ "${dir_nlink}" -ge 4 ]]
dbfs_ls "${MOUNTPOINT}" /tmp/dbfs-mount-workflow.ls-root
dbfs_ls "${dir}" /tmp/dbfs-mount-workflow.ls-alpha
stat -c '%n|%F|%a|%u|%g|%s' "${file}" "${dir}" "${subdir_a}" "${subdir_b}" "${MOUNTPOINT}/delta" >/tmp/dbfs-mount-workflow.stat

mv "${file}" "${rename_file}"
mv "${subdir_a}" "${rename_dir}"
dbfs_find_sorted "${MOUNTPOINT}" 2 /tmp/dbfs-mount-workflow.find
renamed_inode="$(stat -c '%i' "${rename_file}")"
renamed_blksize="$(stat -c '%o' "${rename_file}")"
renamed_nlink="$(stat -c '%h' "${rename_file}")"
[[ "${renamed_inode}" -eq "${file_inode}" ]]
[[ "${renamed_blksize}" -eq "${file_blksize}" ]]
[[ "${renamed_nlink}" -eq 1 ]]

chmod 640 "${rename_file}"
chmod 750 "${dir}"

current_uid="$(id -u)"
current_gid="$(id -g)"
chown "${current_uid}:${current_gid}" "${rename_file}"
chown "${current_uid}:${current_gid}" "${dir}"

test -r "${rename_file}"
test -w "${rename_file}"
test -x "${dir}"

chmod 000 "${rename_file}"
if cat "${rename_file}" >/dev/null 2>&1; then
  echo "Expected access denied after chmod 000"
  exit 1
fi
chmod 640 "${rename_file}"

stat -c '%n|%F|%a|%u|%g|%s' "${rename_file}" "${dir}" "${rename_dir}" >/tmp/dbfs-mount-workflow.after

dbfs_assert_contains /tmp/dbfs-mount-workflow.find 'grow-renamed.bin'
dbfs_assert_contains /tmp/dbfs-mount-workflow.after "${rename_file}|regular file|640|${current_uid}|${current_gid}|18432"
dbfs_assert_contains /tmp/dbfs-mount-workflow.after "${dir}|directory|750|${current_uid}|${current_gid}|0"

rm -f "${rename_file}"
rmdir "${rename_dir}"
rmdir "${subdir_b}"
rmdir "${dir}"
rmdir "${MOUNTPOINT}/delta"

echo "OK mount/dd/stat/ls/chown/chmod/rename"
