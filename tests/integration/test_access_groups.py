#!/usr/bin/env python3

import os
import sys
import uuid
import errno
import unittest
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dbfs_fuse import DBFS, load_dsn_from_config


class AccessGroupTests(unittest.TestCase):
    def test_access_owner_group_and_supplementary_groups(self):
        dsn, db_config = load_dsn_from_config(ROOT)
        fs = DBFS(dsn, db_config)

        suffix = uuid.uuid4().hex[:8]
        dir_path = f"/access_groups_{suffix}"
        file_path = f"{dir_path}/payload.txt"
        payload = b"access-groups\n"

        owner_uid = 123456
        owner_gid = 234567
        unrelated_uid = 345678
        unrelated_gid = 456789

        try:
            fs.mkdir(dir_path, 0o755)
            fh = fs.create(file_path, 0o640)
            fs.write(file_path, payload, 0, fh)
            fs.flush(file_path, fh)
            fs.release(file_path, fh)

            with fs.db_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE files SET uid = %s, gid = %s WHERE id_file = %s",
                    (owner_uid, owner_gid, fs.get_file_id(file_path)),
                )
                conn.commit()

            with patch.object(fs, "current_uid_gid", return_value=(owner_uid, unrelated_gid)), patch(
                "dbfs_fuse.os.getgroups", return_value=[]
            ):
                self.assertEqual(fs.access(file_path, os.R_OK), 0)
                self.assertEqual(fs.access(file_path, os.W_OK), 0)
                with self.assertRaises(OSError) as ctx:
                    fs.access(file_path, os.X_OK)
                self.assertEqual(ctx.exception.errno, errno.EACCES)

            with patch.object(fs, "current_uid_gid", return_value=(unrelated_uid, unrelated_gid)), patch(
                "dbfs_fuse.os.getgroups", return_value=[owner_gid]
            ):
                self.assertEqual(fs.access(file_path, os.R_OK), 0)
                with self.assertRaises(OSError) as ctx:
                    fs.access(file_path, os.W_OK)
                self.assertEqual(ctx.exception.errno, errno.EACCES)

            with patch.object(fs, "current_uid_gid", return_value=(unrelated_uid, unrelated_gid)), patch(
                "dbfs_fuse.os.getgroups", return_value=[]
            ):
                with self.assertRaises(OSError) as ctx:
                    fs.access(file_path, os.R_OK)
                self.assertEqual(ctx.exception.errno, errno.EACCES)

            print("OK access/groups")
        finally:
            try:
                fs.unlink(file_path)
            except Exception:
                pass
            try:
                fs.rmdir(dir_path)
            except Exception:
                pass
            try:
                fs.connection_pool.closeall()
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
