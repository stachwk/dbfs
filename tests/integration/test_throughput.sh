#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-throughput
trap dbfs_test_cleanup EXIT

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

file="${MOUNTPOINT}/throughput.bin"
block_size="${THROUGHPUT_BLOCK_SIZE:-1M}"
count="${THROUGHPUT_COUNT:-1}"
sync_mode="${THROUGHPUT_SYNC:-0}"

block_size_to_bytes() {
  local value="$1"
  case "${value}" in
    *K|*k) echo $(( ${value%[Kk]} * 1024 )) ;;
    *M|*m) echo $(( ${value%[Mm]} * 1024 * 1024 )) ;;
    *G|*g) echo $(( ${value%[Gg]} * 1024 * 1024 * 1024 )) ;;
    *) echo "${value}" ;;
  esac
}

block_bytes="$(block_size_to_bytes "${block_size}")"
expected_size=$((count * block_bytes))

start_ns="$(date +%s%N)"
if [[ "${sync_mode}" =~ ^(0|false|False|no|off)$ ]]; then
  dd if=/dev/zero of="${file}" bs="${block_size}" count="${count}" status=none
else
  dd if=/dev/zero of="${file}" bs="${block_size}" count="${count}" conv=fsync status=none
fi
end_ns="$(date +%s%N)"

actual_size=""
for _ in $(seq 1 50); do
  actual_size="$(dbfs_stat "${file}" '%s' 2>/dev/null || echo 0)"
  if [[ "${actual_size}" == "${expected_size}" ]]; then
    break
  fi
  sleep 0.1
done
dbfs_assert_eq "${actual_size}" "${expected_size}" "throughput file size"

elapsed_ns=$((end_ns - start_ns))
if (( elapsed_ns <= 0 )); then
  echo "Invalid elapsed time"
  exit 1
fi

elapsed_s="$(awk "BEGIN { printf \"%.3f\", ${elapsed_ns} / 1000000000 }")"
throughput_mb_s="$(awk "BEGIN { printf \"%.2f\", ${expected_size} / 1024 / 1024 / (${elapsed_ns} / 1000000000) }")"

echo "OK throughput/write ${expected_size} bytes in ${elapsed_s}s (${throughput_mb_s} MiB/s)"
if [[ "${sync_mode}" =~ ^(0|false|False|no|off)$ ]]; then
  echo "Tip: set THROUGHPUT_SYNC=1 to force fsync-backed throughput measurement."
fi
