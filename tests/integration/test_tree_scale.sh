#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/integration/dbfs_testlib.sh"
dbfs_test_setup "${ROOT}"
dbfs_test_make_mountpoint /tmp/dbfs-tree-scale
tree_scale_root_name="scale_${$}"

dbfs_tree_scale_cleanup_db() {
  "${VENV_PYTHON}" - "${POSTGRES_DB}" "${POSTGRES_USER}" "${POSTGRES_PASSWORD}" "${tree_scale_root_name}" <<'PY'
import os
import sys

import psycopg2

db_name, db_user, db_password, root_name = sys.argv[1:]
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=os.environ.get("POSTGRES_PORT", "5432"),
)

with conn, conn.cursor() as cur:
    cur.execute(
        """
        WITH RECURSIVE subtree AS (
            SELECT id_directory
            FROM directories
            WHERE id_parent IS NULL AND name = %s
            UNION ALL
            SELECT d.id_directory
            FROM directories d
            JOIN subtree s ON d.id_parent = s.id_directory
        ),
        file_ids AS (
            SELECT id_file FROM files WHERE id_directory IN (SELECT id_directory FROM subtree)
        )
        DELETE FROM data_blocks WHERE id_file IN (SELECT id_file FROM file_ids)
        """
        ,
        (root_name,),
    )
    cur.execute(
        """
        WITH RECURSIVE subtree AS (
            SELECT id_directory
            FROM directories
            WHERE id_parent IS NULL AND name = %s
            UNION ALL
            SELECT d.id_directory
            FROM directories d
            JOIN subtree s ON d.id_parent = s.id_directory
        )
        DELETE FROM files WHERE id_directory IN (SELECT id_directory FROM subtree)
        """,
        (root_name,),
    )
    cur.execute(
        """
        WITH RECURSIVE subtree AS (
            SELECT id_directory
            FROM directories
            WHERE id_parent IS NULL AND name = %s
            UNION ALL
            SELECT d.id_directory
            FROM directories d
            JOIN subtree s ON d.id_parent = s.id_directory
        )
        DELETE FROM directories WHERE id_directory IN (SELECT id_directory FROM subtree)
        """,
        (root_name,),
    )

conn.close()
PY
}

dbfs_tree_scale_cleanup() {
  set +e
  dbfs_test_cleanup
  dbfs_tree_scale_cleanup_db
}

trap dbfs_tree_scale_cleanup EXIT

dbfs_test_init_schema

dir_count="${TREE_SCALE_DIRS:-60}"
files_per_dir="${TREE_SCALE_FILES:-100}"

"${VENV_PYTHON}" - "${POSTGRES_DB}" "${POSTGRES_USER}" "${POSTGRES_PASSWORD}" "${dir_count}" "${files_per_dir}" "${tree_scale_root_name}" <<'PY'
import os
import sys
import psycopg2
from psycopg2.extras import execute_values

db_name, db_user, db_password, dir_count, files_per_dir, root_name = sys.argv[1:]
dir_count = int(dir_count)
files_per_dir = int(files_per_dir)

conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=os.environ.get("POSTGRES_PORT", "5432"),
)

uid = os.getuid() if hasattr(os, "getuid") else 0
gid = os.getgid() if hasattr(os, "getgid") else 0

with conn, conn.cursor() as cur:
    cur.execute(
        """
        INSERT INTO directories (id_parent, name, mode, uid, gid, inode_seed, modification_date, access_date, change_date, creation_date)
        VALUES (%s, %s, '755', %s, %s, %s, NOW(), NOW(), NOW(), NOW())
        RETURNING id_directory
        """,
        (None, root_name, uid, gid, f"{root_name}:root"),
    )
    scale_id = cur.fetchone()[0]

    for dir_idx in range(dir_count):
        dir_name = f"d{dir_idx:03d}"
        inode_seed = f"{root_name}:dir:{dir_idx:03d}"
        cur.execute(
            """
            INSERT INTO directories (id_parent, name, mode, uid, gid, inode_seed, modification_date, access_date, change_date, creation_date)
            VALUES (%s, %s, '755', %s, %s, %s, NOW(), NOW(), NOW(), NOW())
            RETURNING id_directory
            """,
            (scale_id, dir_name, uid, gid, inode_seed),
        )
        dir_id = cur.fetchone()[0]

        rows = []
        for file_idx in range(files_per_dir):
            rows.append(
                (
                    dir_id,
                    f"f{file_idx:03d}.txt",
                    0,
                    "644",
                    uid,
                    gid,
                    f"{root_name}:file:{dir_idx:03d}:{file_idx:03d}",
                )
            )

        execute_values(
            cur,
            """
            INSERT INTO files (id_directory, name, size, mode, uid, gid, inode_seed, modification_date, access_date, change_date, creation_date)
            VALUES %s
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW(), NOW())",
        )

conn.close()
PY

dbfs_test_start_mount "${MOUNTPOINT}"

tree_root="${MOUNTPOINT}/${tree_scale_root_name}"
sample_dir="${tree_root}/d000"
sample_file="${sample_dir}/f000.txt"
measure_find="/tmp/dbfs-tree-scale.find"
measure_ls="/tmp/dbfs-tree-scale.ls"

expected_files=$((dir_count * files_per_dir))
expected_dirs=$((dir_count + 1))

actual_files="$(find "${tree_root}" -type f | wc -l | tr -d ' ')"
actual_dirs="$(find "${tree_root}" -type d | wc -l | tr -d ' ')"
dbfs_assert_eq "${actual_files}" "${expected_files}" "tree file count"
dbfs_assert_eq "${actual_dirs}" "${expected_dirs}" "tree directory count"

start_ns="$(date +%s%N)"
ls -la "${tree_root}" >"${measure_ls}"
end_ns="$(date +%s%N)"
ls_elapsed_ns=$((end_ns - start_ns))

start_ns="$(date +%s%N)"
find "${tree_root}" -print >"${measure_find}"
end_ns="$(date +%s%N)"
find_elapsed_ns=$((end_ns - start_ns))

sample_dir_inode="$(stat -c '%i' "${sample_dir}")"
sample_file_inode="$(stat -c '%i' "${sample_file}")"
sample_dir_nlink="$(stat -c '%h' "${sample_dir}")"
sample_file_size="$(stat -c '%s' "${sample_file}")"

dbfs_assert_ge "${sample_dir_inode}" 1 "sample dir inode"
dbfs_assert_ge "${sample_file_inode}" 1 "sample file inode"
dbfs_assert_ge "${sample_dir_nlink}" 2 "sample dir hard links"
dbfs_assert_eq "${sample_file_size}" 0 "sample file size"
dbfs_assert_contains "${measure_ls}" "d000"
dbfs_assert_contains "${measure_find}" "${sample_file}"

ls_elapsed_ms="$(awk "BEGIN { printf \"%.2f\", ${ls_elapsed_ns} / 1000000 }")"
find_elapsed_ms="$(awk "BEGIN { printf \"%.2f\", ${find_elapsed_ns} / 1000000 }")"

echo "OK tree-scale/readdir-getattr dirs=${dir_count} files_per_dir=${files_per_dir} ls_ms=${ls_elapsed_ms} find_ms=${find_elapsed_ms}"
