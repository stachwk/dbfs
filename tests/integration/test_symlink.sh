#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-symlink
trap dbfs_test_cleanup EXIT

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

suffix="$(date +%s%N)"
payload="${MOUNTPOINT}/payload-${suffix}.txt"
link_path="${MOUNTPOINT}/payload-link-${suffix}"
link_renamed="${MOUNTPOINT}/payload-link-renamed-${suffix}"
orphaned_link="${MOUNTPOINT}/payload-orphaned-${suffix}"
payload_value="symlink smoke payload"

printf '%s\n' "${payload_value}" >"${payload}"
ln -s "${payload}" "${link_path}"

[[ -L "${link_path}" ]]
dbfs_assert_eq "$(readlink "${link_path}")" "${payload}" "symlink target mismatch"
dbfs_assert_eq "$(cat "${link_path}")" "${payload_value}" "symlink payload mismatch"

mv "${link_path}" "${link_renamed}"
[[ -L "${link_renamed}" ]]
dbfs_assert_eq "$(readlink "${link_renamed}")" "${payload}" "renamed symlink target mismatch"
dbfs_assert_eq "$(cat "${link_renamed}")" "${payload_value}" "renamed symlink payload mismatch"

rm -f "${link_renamed}" "${payload}"

printf '%s\n' "${payload_value}" >"${payload}"
ln -s "${payload}" "${orphaned_link}"
rm -f "${payload}"

[[ -L "${orphaned_link}" ]]
[[ ! -e "${orphaned_link}" ]]
dbfs_assert_eq "$(readlink "${orphaned_link}")" "${payload}" "orphaned symlink target mismatch"
dbfs_assert_eq "$(cat "${orphaned_link}" 2>/dev/null || true)" "" "orphaned symlink should not resolve to content"
ls_orphaned_output="$(ls -al "${orphaned_link}")"
grep -F "${orphaned_link} -> ${payload}" <<<"${ls_orphaned_output}" >/dev/null

echo "OK symlink/readlink"
