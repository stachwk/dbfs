#!/usr/bin/env bash
set -euo pipefail

tmpdir="$(mktemp -d /tmp/dbfs-mount-wrapper.XXXXXX)"
trap 'rm -rf "${tmpdir}"' EXIT

mkdir -p "${tmpdir}/bin"
cat >"${tmpdir}/bin/dbfs-bootstrap" <<'EOF'
#!/usr/bin/env bash
printf 'DBFS_CONFIG=%s\n' "${DBFS_CONFIG:-unset}"
printf 'DBFS_ALLOW_OTHER=%s\n' "${DBFS_ALLOW_OTHER:-unset}"
printf 'DBFS_PROFILE=%s\n' "${DBFS_PROFILE:-unset}"
printf 'ARGS=%s\n' "$*"
EOF
chmod +x "${tmpdir}/bin/dbfs-bootstrap"

printf '[database]\n' >"${tmpdir}/dbfs_config.ini"
mkdir -p "${tmpdir}/mnt"

(
  cd "${tmpdir}"
  DBFS_BOOTSTRAP_BIN="${tmpdir}/bin/dbfs-bootstrap" /media/wojtek/virtdata/home/wojtek/git/dbfs/mount.dbfs "${tmpdir}/mnt" -o role=auto,allow_other,profile=bulk_write,selinux=off,acl=off,default_permissions
) >"${tmpdir}/output.txt"

grep -Fq "DBFS_CONFIG=${tmpdir}/dbfs_config.ini" "${tmpdir}/output.txt"
grep -Fq "DBFS_ALLOW_OTHER=1" "${tmpdir}/output.txt"
grep -Fq "DBFS_PROFILE=bulk_write" "${tmpdir}/output.txt"
grep -Fq "ARGS=-f ${tmpdir}/mnt --profile bulk_write" "${tmpdir}/output.txt"

echo "OK mount-wrapper-options"
