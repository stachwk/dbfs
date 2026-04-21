#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-mount-root-permissions
trap dbfs_test_cleanup EXIT

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

root_stat="$(stat -c '%F|%a|%u|%g' "${MOUNTPOINT}")"
current_uid="$(id -u)"
current_gid="$(id -g)"

dbfs_assert_contains_text "${root_stat}" "directory"

dir="${MOUNTPOINT}/root-perms"
mkdir "${dir}"
chmod 750 "${dir}"
chown "${current_uid}:${current_gid}" "${dir}"

dir_stat="$(stat -c '%n|%F|%a|%u|%g|%s' "${dir}")"
dbfs_assert_contains_text "${dir_stat}" "${dir}|directory|750|${current_uid}|${current_gid}|0"

test -r "${dir}"
test -x "${dir}"
test -w "${dir}"

payload="${dir}/nested.txt"
printf 'root-permissions\n' >"${payload}"
test -f "${payload}"

dbfs_ls "${MOUNTPOINT}" /tmp/dbfs-mount-root-permissions.ls
dbfs_assert_contains /tmp/dbfs-mount-root-permissions.ls 'root-perms'

rm -f "${payload}"
rmdir "${dir}"

echo "OK mount/root-permissions"
