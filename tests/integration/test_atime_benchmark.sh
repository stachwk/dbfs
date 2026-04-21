#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"

dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-atime-bench
trap dbfs_test_cleanup EXIT

policy="${DBFS_ATIME_POLICY:-default}"
kind="${ATIME_BENCH_KIND:-file}"
iterations="${ATIME_BENCH_ITERATIONS:-50}"
entries="${ATIME_BENCH_ENTRIES:-64}"

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

start_ns="$(date +%s%N)"

case "${kind}" in
  file)
    file="${MOUNTPOINT}/bench.txt"
    printf '%s\n' "atime benchmark" > "${file}"
    for _ in $(seq 1 "${iterations}"); do
      cat "${file}" >/dev/null
    done
    ;;
  dir)
    dir="${MOUNTPOINT}/bench-dir"
    mkdir -p "${dir}"
    for i in $(seq 1 "${entries}"); do
      printf '%s\n' "${i}" > "${dir}/entry-${i}.txt"
    done
    for _ in $(seq 1 "${iterations}"); do
      ls -1 "${dir}" >/dev/null
    done
    ;;
  *)
    echo "unsupported ATIME bench kind: ${kind}"
    exit 1
    ;;
esac

end_ns="$(date +%s%N)"
elapsed_ns=$((end_ns - start_ns))
elapsed_ms=$((elapsed_ns / 1000000))
echo "OK atime-benchmark/${kind}/${policy} elapsed_ms=${elapsed_ms} iterations=${iterations}"
