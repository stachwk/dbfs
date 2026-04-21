#!/usr/bin/env bash

dbfs_test_setup() {
  local root_dir="$1"
  ROOT="${root_dir}"
  VENV_PYTHON="${VENV_PYTHON:-${ROOT}/.venv/bin/python}"
  POSTGRES_DB="${POSTGRES_DB:-dbfsdbname}"
  POSTGRES_USER="${POSTGRES_USER:-dbfsuser}"
  POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-cichosza}"
  if [[ -z "${DBFS_SCHEMA_ADMIN_PASSWORD:-}" ]]; then
    DBFS_SCHEMA_ADMIN_PASSWORD="$("${VENV_PYTHON}" -c 'import secrets; print(secrets.token_urlsafe(24))')"
  fi
  DBFS_SELINUX="${DBFS_SELINUX:-off}"
  DBFS_ACL="${DBFS_ACL:-off}"
  DBFS_DEFAULT_PERMISSIONS="${DBFS_DEFAULT_PERMISSIONS:-1}"
  DBFS_ATIME_POLICY="${DBFS_ATIME_POLICY:-default}"
  DBFS_ROLE="${DBFS_ROLE:-auto}"
  DBFS_LAZYTIME="${DBFS_LAZYTIME:-0}"
  DBFS_SYNC="${DBFS_SYNC:-0}"
  DBFS_DIRSYNC="${DBFS_DIRSYNC:-0}"
  DBFS_SELINUX_CONTEXT="${DBFS_SELINUX_CONTEXT:-}"
  DBFS_SELINUX_FSCONTEXT="${DBFS_SELINUX_FSCONTEXT:-}"
  DBFS_SELINUX_DEFCONTEXT="${DBFS_SELINUX_DEFCONTEXT:-}"
  DBFS_SELINUX_ROOTCONTEXT="${DBFS_SELINUX_ROOTCONTEXT:-}"
}

dbfs_test_make_mountpoint() {
  local prefix="$1"
  MOUNTPOINT="$(mktemp -d "${prefix}.XXXXXX")"
  LOG_FILE="$(mktemp "${prefix}.XXXXXX.log")"
  DBFS_PID=""
}

dbfs_test_cleanup() {
  set +e
  if mountpoint -q "${MOUNTPOINT}"; then
    if command -v fusermount3 >/dev/null 2>&1; then
      fusermount3 -u "${MOUNTPOINT}"
    elif command -v fusermount >/dev/null 2>&1; then
      fusermount -u "${MOUNTPOINT}"
    else
      umount "${MOUNTPOINT}"
    fi
  fi
  if [[ -n "${DBFS_PID}" ]] && kill -0 "${DBFS_PID}" >/dev/null 2>&1; then
    kill "${DBFS_PID}" >/dev/null 2>&1 || true
    wait "${DBFS_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${DBFS_PROFILE_IO:-0}" =~ ^(1|true|True|yes|on)$ ]]; then
    echo "DBFS I/O profile summary:"
    grep -E "DBFS I/O profile:" "${LOG_FILE}" || tail -n 20 "${LOG_FILE}" || true
  fi
  rm -rf "${MOUNTPOINT}" "${LOG_FILE}"
}

dbfs_test_init_schema() {
  local status_output
  status_output="$(
    POSTGRES_DB="${POSTGRES_DB}" POSTGRES_USER="${POSTGRES_USER}" POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
      "${VENV_PYTHON}" "${ROOT}/mkfs.dbfs.py" status 2>/dev/null || true
  )"
  if grep -Fq "DBFS ready: yes" <<<"${status_output}"; then
    return 0
  fi
  POSTGRES_DB="${POSTGRES_DB}" POSTGRES_USER="${POSTGRES_USER}" POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" "${VENV_PYTHON}" "${ROOT}/mkfs.dbfs.py" init --schema-admin-password "${DBFS_SCHEMA_ADMIN_PASSWORD}"
}

dbfs_test_build_args() {
  DBFS_ARGS=(--role "${DBFS_ROLE}" --selinux "${DBFS_SELINUX}" --acl "${DBFS_ACL}" --atime-policy "${DBFS_ATIME_POLICY}")

  if [[ "${DBFS_DEFAULT_PERMISSIONS}" =~ ^(0|false|False|no)$ ]]; then
    DBFS_ARGS+=(--no-default-permissions)
  else
    DBFS_ARGS+=(--default-permissions)
  fi

  if [[ "${DBFS_LAZYTIME}" =~ ^(0|false|False|no|off)$ ]]; then :; else DBFS_ARGS+=(--lazytime); fi
  if [[ "${DBFS_SYNC}" =~ ^(0|false|False|no|off)$ ]]; then :; else DBFS_ARGS+=(--sync); fi
  if [[ "${DBFS_DIRSYNC}" =~ ^(0|false|False|no|off)$ ]]; then :; else DBFS_ARGS+=(--dirsync); fi
}

dbfs_test_start_mount() {
  local mountpoint="$1"
  dbfs_test_build_args
  mkdir -p "${mountpoint}"
  POSTGRES_DB="${POSTGRES_DB}" POSTGRES_USER="${POSTGRES_USER}" POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" "${VENV_PYTHON}" "${ROOT}/dbfs_fuse.py" "${DBFS_ARGS[@]}" -f "${mountpoint}" >"${LOG_FILE}" 2>&1 &
  DBFS_PID=$!

  for _ in $(seq 1 60); do
    if mountpoint -q "${mountpoint}"; then
      return 0
    fi
    if ! kill -0 "${DBFS_PID}" >/dev/null 2>&1; then
      cat "${LOG_FILE}"
      return 1
    fi
    sleep 1
  done

  if ! mountpoint -q "${mountpoint}"; then
    cat "${LOG_FILE}"
    echo "DBFS mount did not become ready"
    return 1
  fi
}

dbfs_assert_eq() {
  local actual="$1"
  local expected="$2"
  local message="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "${message}: expected=${expected} actual=${actual}"
    return 1
  fi
}

dbfs_assert_ge() {
  local actual="$1"
  local expected="$2"
  local message="$3"
  if (( actual < expected )); then
    echo "${message}: expected>=${expected} actual=${actual}"
    return 1
  fi
}

dbfs_assert_contains() {
  local file_path="$1"
  local needle="$2"
  if ! grep -Fq -- "${needle}" "${file_path}"; then
    echo "missing '${needle}' in ${file_path}"
    return 1
  fi
}

dbfs_assert_contains_text() {
  local text="$1"
  local needle="$2"
  if ! grep -Fq -- "${needle}" <<<"${text}"; then
    echo "missing '${needle}' in provided text"
    return 1
  fi
}

dbfs_stat() {
  local path="$1"
  local fmt="$2"
  stat -c "${fmt}" "${path}"
}

dbfs_ls() {
  local path="$1"
  local output="$2"
  ls -la "${path}" >"${output}"
}

dbfs_find_sorted() {
  local path="$1"
  local maxdepth="$2"
  local output="$3"
  find "${path}" -maxdepth "${maxdepth}" -print | sort >"${output}"
}
