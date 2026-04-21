#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-meta
trap dbfs_test_cleanup EXIT

dbfs_test_init_schema
dbfs_test_start_mount "${MOUNTPOINT}"

file="${MOUNTPOINT}/meta.txt"
dir="${MOUNTPOINT}/meta-dir"

mkdir -p "${dir}"
printf 'metadata\n' >"${file}"

chmod 640 "${file}"
chmod 750 "${dir}"

current_uid="$(id -u)"
current_gid="$(id -g)"
chown "${current_uid}:${current_gid}" "${file}"
chown "${current_uid}:${current_gid}" "${dir}"

test -r "${file}"
test -w "${file}"
test -x "${dir}"

file_stat="$(dbfs_stat "${file}" '%n|%F|%i|%h|%o|%a|%u|%g|%s')"
dir_stat="$(dbfs_stat "${dir}" '%n|%F|%i|%h|%o|%a|%u|%g|%s')"
file_ctime_before="$(dbfs_stat "${file}" '%Z')"
file_mtime_before="$(dbfs_stat "${file}" '%Y')"
file_size_before="$(dbfs_stat "${file}" '%s')"
file_dev="$(dbfs_stat "${file}" '%d')"
dir_dev="$(dbfs_stat "${dir}" '%d')"
dbfs_assert_contains_text "${file_stat}" "${file}|regular file"
dbfs_assert_contains_text "${dir_stat}" "${dir}|directory"
dbfs_assert_contains_text "${file_stat}" "|640|${current_uid}|${current_gid}|9"
dbfs_assert_contains_text "${dir_stat}" "|750|${current_uid}|${current_gid}|0"
if [ "${file_dev}" != "${dir_dev}" ]; then
  echo "Expected file and directory to report the same st_dev"
  exit 1
fi
if [ "${file_dev}" = "0" ]; then
  echo "Expected st_dev to be non-zero"
  exit 1
fi

touch -a -d '2 days ago' "${file}"
file_atime_stale="$(dbfs_stat "${file}" '%X')"
cat "${file}" >/dev/null
file_atime_after_read="$(dbfs_stat "${file}" '%X')"
if [ "${file_atime_after_read}" -le "${file_atime_stale}" ]; then
  echo "Expected atime to move forward after read"
  exit 1
fi

file_atime_before_touch_m="$(dbfs_stat "${file}" '%X')"
file_mtime_before_touch_m="$(dbfs_stat "${file}" '%Y')"
file_ctime_before_touch_m="$(dbfs_stat "${file}" '%Z')"
touch -m "${file}"
file_atime_after_touch_m="$(dbfs_stat "${file}" '%X')"
file_mtime_after_touch_m="$(dbfs_stat "${file}" '%Y')"
file_ctime_after_touch_m="$(dbfs_stat "${file}" '%Z')"
if [ "${file_atime_after_touch_m}" != "${file_atime_before_touch_m}" ]; then
  echo "Expected touch -m to leave atime unchanged"
  exit 1
fi
if [ "${file_mtime_after_touch_m}" -lt "${file_mtime_before_touch_m}" ]; then
  echo "Expected touch -m to move mtime forward"
  exit 1
fi
if [ "${file_ctime_after_touch_m}" -lt "${file_ctime_before_touch_m}" ]; then
  echo "Expected touch -m to move ctime forward"
  exit 1
fi

file_atime_before_touch_a="$(dbfs_stat "${file}" '%X')"
file_mtime_before_touch_a="$(dbfs_stat "${file}" '%Y')"
file_ctime_before_touch_a="$(dbfs_stat "${file}" '%Z')"
touch -a "${file}"
file_atime_after_touch_a="$(dbfs_stat "${file}" '%X')"
file_mtime_after_touch_a="$(dbfs_stat "${file}" '%Y')"
file_ctime_after_touch_a="$(dbfs_stat "${file}" '%Z')"
if [ "${file_atime_after_touch_a}" -lt "${file_atime_before_touch_a}" ]; then
  echo "Expected touch -a to move atime forward"
  exit 1
fi
if [ "${file_mtime_after_touch_a}" != "${file_mtime_before_touch_a}" ]; then
  echo "Expected touch -a to leave mtime unchanged"
  exit 1
fi
if [ "${file_ctime_after_touch_a}" -lt "${file_ctime_before_touch_a}" ]; then
  echo "Expected touch -a to move ctime forward"
  exit 1
fi

chmod 000 "${file}"
file_ctime_after="$(dbfs_stat "${file}" '%Z')"
if [ "${file_ctime_after}" -lt "${file_ctime_before}" ]; then
  echo "Expected ctime to move forward after chmod"
  exit 1
fi
if cat "${file}" >/dev/null 2>&1; then
  echo "Expected access denied after chmod 000"
  exit 1
fi

chmod 640 "${file}"
printf 'more-data\n' >>"${file}"
file_size_after_write="$(dbfs_stat "${file}" '%s')"
file_mtime_after_write="$(dbfs_stat "${file}" '%Y')"
file_ctime_after_write="$(dbfs_stat "${file}" '%Z')"
if [ "${file_size_after_write}" -le "${file_size_before}" ]; then
  echo "Expected file size to grow after write"
  exit 1
fi
if [ "${file_mtime_after_write}" -lt "${file_mtime_before}" ]; then
  echo "Expected mtime to move forward after write"
  exit 1
fi
if [ "${file_ctime_after_write}" -lt "${file_ctime_after}" ]; then
  echo "Expected ctime to move forward after write"
  exit 1
fi

truncate -s "${file_size_after_write}" "${file}"
file_size_after_truncate_same="$(dbfs_stat "${file}" '%s')"
file_mtime_after_truncate_same="$(dbfs_stat "${file}" '%Y')"
file_ctime_after_truncate_same="$(dbfs_stat "${file}" '%Z')"
if [ "${file_size_after_truncate_same}" != "${file_size_after_write}" ]; then
  echo "Expected truncate to the same size to keep file size unchanged"
  exit 1
fi
if [ "${file_mtime_after_truncate_same}" != "${file_mtime_after_write}" ]; then
  echo "Expected truncate to the same size to leave mtime unchanged"
  exit 1
fi
if [ "${file_ctime_after_truncate_same}" != "${file_ctime_after_write}" ]; then
  echo "Expected truncate to the same size to leave ctime unchanged"
  exit 1
fi

touch "${file}"
file_mtime_after_touch="$(dbfs_stat "${file}" '%Y')"
file_ctime_after_touch="$(dbfs_stat "${file}" '%Z')"
if [ "${file_mtime_after_touch}" -lt "${file_mtime_after_write}" ]; then
  echo "Expected mtime to move forward after touch"
  exit 1
fi
if [ "${file_ctime_after_touch}" -lt "${file_ctime_after_write}" ]; then
  echo "Expected ctime to move forward after touch"
  exit 1
fi

truncate -s 0 "${file}"
file_size_after_truncate="$(dbfs_stat "${file}" '%s')"
file_mtime_after_truncate="$(dbfs_stat "${file}" '%Y')"
file_ctime_after_truncate="$(dbfs_stat "${file}" '%Z')"
if [ "${file_size_after_truncate}" != "0" ]; then
  echo "Expected truncate to shrink the file to zero"
  exit 1
fi

echo "OK metadata/stat/access"
