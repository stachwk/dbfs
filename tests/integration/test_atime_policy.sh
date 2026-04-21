#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-atime
trap dbfs_test_cleanup EXIT

policy="${DBFS_ATIME_POLICY:-default}"
dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

file="${MOUNTPOINT}/atime-${policy}.txt"
printf '%s\n' "atime smoke" > "${file}"

before="$(dbfs_stat "${file}" '%X')"

case "${policy}" in
  noatime)
    cat "${file}" >/dev/null
    after="$(dbfs_stat "${file}" '%X')"
    dbfs_assert_eq "${after}" "${before}" "atime changed under noatime"
    ;;
  relatime)
    touch -a -d '2 days ago' "${file}"
    before="$(dbfs_stat "${file}" '%X')"
    cat "${file}" >/dev/null
    after="$(dbfs_stat "${file}" '%X')"
    if (( after <= before )); then
      echo "expected atime to advance under relatime: before=${before} after=${after}"
      exit 1
    fi
    ;;
  *)
    echo "unsupported ATIME policy: ${policy}"
    exit 1
    ;;
esac

echo "OK atime/${policy}"
