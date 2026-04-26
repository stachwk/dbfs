#!/usr/bin/env bash

dbfs_test_setup() {
  local root_dir="$1"
  ROOT="${root_dir}"
  POSTGRES_DB="${POSTGRES_DB:-dbfsdbname}"
  POSTGRES_USER="${POSTGRES_USER:-dbfsuser}"
  POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-cichosza}"
  if [[ -z "${DBFS_SCHEMA_ADMIN_PASSWORD:-}" ]]; then
    DBFS_SCHEMA_ADMIN_PASSWORD="dbfs-$(tr -dc 'A-Za-z0-9_' </dev/urandom | head -c 24)"
  fi
  DBFS_CONFIG="${DBFS_CONFIG:-${ROOT}/dbfs_config.ini}"
  DBFS_SELINUX="${DBFS_SELINUX:-off}"
  DBFS_ACL="${DBFS_ACL:-off}"
  DBFS_DEFAULT_PERMISSIONS="${DBFS_DEFAULT_PERMISSIONS:-1}"
  DBFS_ATIME_POLICY="${DBFS_ATIME_POLICY:-default}"
  DBFS_ROLE="${DBFS_ROLE:-auto}"
  DBFS_LAZYTIME="${DBFS_LAZYTIME:-0}"
  DBFS_SYNC="${DBFS_SYNC:-0}"
  DBFS_DIRSYNC="${DBFS_DIRSYNC:-0}"
  DBFS_SYNCHRONOUS_COMMIT="${DBFS_SYNCHRONOUS_COMMIT:-on}"
  DBFS_SELINUX_CONTEXT="${DBFS_SELINUX_CONTEXT:-}"
  DBFS_SELINUX_FSCONTEXT="${DBFS_SELINUX_FSCONTEXT:-}"
  DBFS_SELINUX_DEFCONTEXT="${DBFS_SELINUX_DEFCONTEXT:-}"
  DBFS_SELINUX_ROOTCONTEXT="${DBFS_SELINUX_ROOTCONTEXT:-}"
  if [[ -n "${DBFS_BOOTSTRAP_BIN:-}" ]]; then
    :
  elif [[ -x "${ROOT}/rust_mkfs/target/debug/dbfs-bootstrap" ]]; then
    DBFS_BOOTSTRAP_BIN="${ROOT}/rust_mkfs/target/debug/dbfs-bootstrap"
  elif [[ -x "${ROOT}/rust_mkfs/target/release/dbfs-bootstrap" ]]; then
    DBFS_BOOTSTRAP_BIN="${ROOT}/rust_mkfs/target/release/dbfs-bootstrap"
  else
    DBFS_BOOTSTRAP_BIN="/usr/local/bin/dbfs-bootstrap"
  fi
  if [[ -n "${DBFS_MKFS_BIN:-}" ]]; then
    :
  elif [[ -x "${ROOT}/rust_mkfs/target/debug/dbfs-rust-mkfs" ]]; then
    DBFS_MKFS_BIN="${ROOT}/rust_mkfs/target/debug/dbfs-rust-mkfs"
  elif [[ -x "${ROOT}/rust_mkfs/target/release/dbfs-rust-mkfs" ]]; then
    DBFS_MKFS_BIN="${ROOT}/rust_mkfs/target/release/dbfs-rust-mkfs"
  else
    DBFS_MKFS_BIN="/usr/local/bin/dbfs-rust-mkfs"
  fi
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
      DBFS_CONFIG="${DBFS_CONFIG}" "${DBFS_MKFS_BIN}" status 2>/dev/null || true
  )"
  if grep -Fq "DBFS ready: yes" <<<"${status_output}"; then
    return 0
  fi
  POSTGRES_DB="${POSTGRES_DB}" POSTGRES_USER="${POSTGRES_USER}" POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" DBFS_CONFIG="${DBFS_CONFIG}" "${DBFS_MKFS_BIN}" init --schema-admin-password "${DBFS_SCHEMA_ADMIN_PASSWORD}"
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
  POSTGRES_DB="${POSTGRES_DB}" POSTGRES_USER="${POSTGRES_USER}" POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" DBFS_CONFIG="${DBFS_CONFIG}" DBFS_BOOTSTRAP_BIN="${DBFS_BOOTSTRAP_BIN}" DBFS_USE_RUST_FUSE=1 DBFS_USE_FUSE_CONTEXT=1 DBFS_SELINUX_CONTEXT="${DBFS_SELINUX_CONTEXT}" DBFS_SELINUX_FSCONTEXT="${DBFS_SELINUX_FSCONTEXT}" DBFS_SELINUX_DEFCONTEXT="${DBFS_SELINUX_DEFCONTEXT}" DBFS_SELINUX_ROOTCONTEXT="${DBFS_SELINUX_ROOTCONTEXT}" "${DBFS_BOOTSTRAP_BIN}" "${DBFS_ARGS[@]}" -f "${mountpoint}" >"${LOG_FILE}" 2>&1 &
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
