#!/usr/bin/env python3

from __future__ import annotations

import errno
import array
import fcntl
import os
import time
import termios
import subprocess
import tempfile
import shutil
import unittest
import uuid
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_fuse import DBFS, load_dsn_from_config
from tests.integration.dbfs_mount import DBFSMount


class DBFSMountSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.launcher = DBFSMount(str(ROOT))
        cls.launcher.init_schema()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="/tmp/dbfs-suite.")
        self.mountpoint = Path(self.temp_dir.name)
        self.launcher.start(str(self.mountpoint))

    def tearDown(self):
        self.launcher.stop()
        self.temp_dir.cleanup()

    def test_files(self):
        suffix = uuid.uuid4().hex[:8]
        file_path = self.mountpoint / f"files-{suffix}.bin"
        renamed = self.mountpoint / f"files-{suffix}-renamed.bin"
        expected_size = 0

        file_path.write_bytes(os.urandom(1024))
        expected_size += 1024
        listing = subprocess.check_output(["ls", "-al", str(file_path)], text=True)
        self.assertIn(file_path.name, listing)
        for block_k in (2, 5, 3, 7):
            with file_path.open("ab") as fh:
                fh.write(os.urandom(block_k * 1024))
            expected_size += block_k * 1024
            self.assertEqual(file_path.stat().st_size, expected_size)

        file_inode = file_path.stat().st_ino
        self.assertGreater(file_inode, 0)
        self.assertEqual(file_path.stat().st_nlink, 1)
        self.assertGreaterEqual(file_path.stat().st_blksize, 512)
        self.assertGreaterEqual(file_path.stat().st_blocks, 1)

        subprocess.run(["mv", str(file_path), str(renamed)], check=True)
        self.assertEqual(renamed.stat().st_ino, file_inode)
        self.assertEqual(renamed.stat().st_size, expected_size)
        subprocess.run(["rm", "-f", str(renamed)], check=True)

    def test_directories(self):
        suffix = uuid.uuid4().hex[:8]
        dir_path = self.mountpoint / f"alpha-{suffix}"
        sub_a = dir_path / "beta"
        sub_b = dir_path / "gamma"
        sub_c = self.mountpoint / f"delta-{suffix}"

        sub_a.mkdir(parents=True)
        sub_b.mkdir(parents=True)
        sub_c.mkdir(parents=True)

        self.assertEqual(dir_path.stat().st_nlink, 4)
        self.assertGreaterEqual(self.mountpoint.stat().st_nlink, 3)
        self.assertGreaterEqual(dir_path.stat().st_blocks, 1)

        beta_renamed = dir_path / "beta-renamed"
        subprocess.run(["mv", str(sub_a), str(beta_renamed)], check=True)
        time.sleep(0.2)
        self.assertTrue(beta_renamed.exists())
        self.assertFalse(sub_a.exists())

        subprocess.run(["rmdir", str(beta_renamed)], check=True)
        subprocess.run(["rmdir", str(sub_b)], check=True)
        subprocess.run(["rmdir", str(dir_path)], check=True)
        subprocess.run(["rmdir", str(sub_c)], check=True)

    def test_metadata(self):
        suffix = uuid.uuid4().hex[:8]
        file_path = self.mountpoint / f"meta-{suffix}.txt"
        dir_path = self.mountpoint / f"meta-dir-{suffix}"
        dir_path.mkdir()
        file_path.write_text("metadata\n", encoding="utf-8")

        file_path.chmod(0o640)
        dir_path.chmod(0o750)

        current_uid = os.getuid()
        current_gid = os.getgid()
        os.chown(file_path, current_uid, current_gid)
        os.chown(dir_path, current_uid, current_gid)

        self.assertTrue(os.access(file_path, os.R_OK))
        self.assertTrue(os.access(file_path, os.W_OK))
        self.assertTrue(os.access(dir_path, os.X_OK))

        file_stat = file_path.stat()
        dir_stat = dir_path.stat()
        self.assertEqual(oct(file_stat.st_mode & 0o777), "0o640")
        self.assertEqual(oct(dir_stat.st_mode & 0o777), "0o750")
        self.assertEqual(file_stat.st_uid, current_uid)
        self.assertEqual(file_stat.st_gid, current_gid)
        self.assertEqual(dir_stat.st_uid, current_uid)
        self.assertEqual(dir_stat.st_gid, current_gid)
        self.assertEqual(file_stat.st_dev, dir_stat.st_dev)
        self.assertGreater(file_stat.st_dev, 0)
        self.assertGreaterEqual(file_stat.st_blocks, 1)
        self.assertGreaterEqual(dir_stat.st_blocks, 1)
        self.assertEqual(file_stat.st_size, 9)
        self.assertEqual(dir_stat.st_size, 0)
        stale_atime = file_stat.st_atime
        file_path.read_text(encoding="utf-8")
        refreshed_atime = file_path.stat().st_atime
        self.assertGreaterEqual(refreshed_atime, stale_atime)

        file_path.chmod(0o000)
        with self.assertRaises(PermissionError):
            file_path.read_text(encoding="utf-8")

    def test_access_modes(self):
        suffix = uuid.uuid4().hex[:8]
        file_path = self.mountpoint / f"access-{suffix}.txt"
        dir_path = self.mountpoint / f"access-dir-{suffix}"
        try:
            file_path.write_text("access\n", encoding="utf-8")
            dir_path.mkdir()

            file_path.chmod(0o640)
            dir_path.chmod(0o750)

            self.assertTrue(os.access(file_path, os.R_OK))
            self.assertTrue(os.access(file_path, os.W_OK))
            self.assertFalse(os.access(file_path, os.X_OK))
            self.assertTrue(os.access(dir_path, os.R_OK))
            self.assertTrue(os.access(dir_path, os.W_OK))
            self.assertTrue(os.access(dir_path, os.X_OK))

            file_path.chmod(0o000)
            dir_path.chmod(0o000)
            self.assertFalse(os.access(file_path, os.R_OK))
            self.assertFalse(os.access(file_path, os.W_OK))
            self.assertFalse(os.access(file_path, os.X_OK))
            self.assertFalse(os.access(dir_path, os.R_OK))
            self.assertFalse(os.access(dir_path, os.W_OK))
            self.assertFalse(os.access(dir_path, os.X_OK))
        finally:
            try:
                file_path.chmod(0o640)
            except Exception:
                pass
            try:
                dir_path.chmod(0o750)
            except Exception:
                pass
            try:
                file_path.unlink()
            except Exception:
                pass
            try:
                dir_path.rmdir()
            except Exception:
                pass

    def test_ioctl_fionread(self):
        suffix = uuid.uuid4().hex[:8]
        file_path = self.mountpoint / f"ioctl-{suffix}.txt"
        payload = b"ioctl payload\n"
        file_path.write_bytes(payload)

        fd = os.open(file_path, os.O_RDONLY)
        try:
            buf = array.array("i", [0])
            fcntl.ioctl(fd, termios.FIONREAD, buf, True)
            self.assertEqual(buf[0], len(payload))
        finally:
            os.close(fd)

    def test_runtime_features_off(self):
        suffix = uuid.uuid4().hex[:8]
        file_path = self.mountpoint / f"runtime-off-{suffix}.txt"
        file_path.write_text("runtime-off\n", encoding="utf-8")

        acl_blob = b"\x02\x00\x00\x00"
        for name, value in (
            ("security.selinux", b"system_u:object_r:tmp_t:s0"),
            ("system.posix_acl_access", acl_blob),
        ):
            with self.assertRaises(OSError) as ctx:
                os.setxattr(file_path, name, value)
            self.assertIn(ctx.exception.errno, {errno.EOPNOTSUPP, errno.ENOTSUP, errno.EPERM})

        self.assertNotIn("security.selinux", os.listxattr(file_path))
        self.assertNotIn("system.posix_acl_access", os.listxattr(file_path))

    def test_selinux_runtime_feature_on(self):
        if self.launcher.selinux not in {"on", "auto"}:
            self.skipTest("SELinux runtime feature is disabled for this mount")

        suffix = uuid.uuid4().hex[:8]
        file_path = self.mountpoint / f"selinux-on-{suffix}.txt"
        file_path.write_text("selinux-on\n", encoding="utf-8")

        selinux_value = b"system_u:object_r:tmp_t:s0"
        try:
            os.setxattr(file_path, "security.selinux", selinux_value)
        except OSError as exc:
            if exc.errno in {errno.EPERM, errno.EOPNOTSUPP, errno.ENOTSUP}:
                self.skipTest("SELinux xattr is not enabled on this host")
            raise
        self.assertEqual(os.getxattr(file_path, "security.selinux"), selinux_value)
        self.assertIn("security.selinux", os.listxattr(file_path))

    def test_symlink(self):
        suffix = uuid.uuid4().hex[:8]
        payload = self.mountpoint / f"payload-{suffix}.txt"
        link_path = self.mountpoint / f"payload-link-{suffix}"
        renamed = self.mountpoint / f"payload-link-renamed-{suffix}"
        orphaned = self.mountpoint / f"payload-orphaned-{suffix}"
        payload_value = "symlink smoke payload\n"
        payload.write_text(payload_value, encoding="utf-8")

        subprocess.run(["ln", "-s", str(payload), str(link_path)], check=True)
        self.assertTrue(link_path.is_symlink())
        self.assertEqual(os.readlink(link_path), str(payload))
        self.assertEqual(link_path.read_text(encoding="utf-8"), payload_value)

        subprocess.run(["mv", str(link_path), str(renamed)], check=True)
        self.assertTrue(renamed.is_symlink())
        self.assertEqual(os.readlink(renamed), str(payload))
        self.assertEqual(renamed.read_text(encoding="utf-8"), payload_value)

        subprocess.run(["ln", "-s", str(payload), str(orphaned)], check=True)
        subprocess.run(["rm", "-f", str(payload)], check=True)
        self.assertTrue(orphaned.is_symlink())
        self.assertFalse(orphaned.exists())
        self.assertEqual(os.readlink(orphaned), str(payload))
        ls_output = subprocess.check_output(["ls", "-al", str(orphaned)], text=True)
        self.assertIn(f"{orphaned} -> {payload}", ls_output)

        subprocess.run(["rm", "-f", str(renamed)], check=True)
        subprocess.run(["rm", "-f", str(orphaned)], check=True)

    def test_df(self):
        ph = subprocess.check_output(["df", "-Ph", str(self.mountpoint)], text=True)
        phi = subprocess.check_output(["df", "-Phi", str(self.mountpoint)], text=True)
        self.assertIn(str(self.mountpoint), ph)
        self.assertIn(str(self.mountpoint), phi)

    def test_replica_read_only(self):
        replica_launcher = DBFSMount(str(ROOT), role="replica")
        replica_mountpoint = Path(tempfile.mkdtemp(prefix="dbfs-replica-suite.", dir="/tmp"))
        dsn = None
        db_config = None
        seed_dir = None
        seed_file = None
        try:
            dsn, db_config = load_dsn_from_config(ROOT)
            fs = DBFS(dsn, db_config)
            suffix = uuid.uuid4().hex[:8]
            seed_dir = f"/replica_seed_{suffix}"
            seed_file = f"{seed_dir}/seed.txt"
            fs.mkdir(seed_dir, 0o755)
            fh = fs.create(seed_file, 0o644)
            fs.write(seed_file, b"seed-data", 0, fh)
            fs.flush(seed_file, fh)
            fs.release(seed_file, fh)
            fs.connection_pool.closeall()
            replica_launcher.start(str(replica_mountpoint))
            replica_output = subprocess.check_output(
                ["cat", str(replica_mountpoint / seed_dir.lstrip("/") / "seed.txt")],
                text=True,
            )
            self.assertEqual(replica_output, "seed-data")

            with self.assertRaises(OSError) as touch_exc:
                (replica_mountpoint / "new.txt").write_text("x", encoding="utf-8")
            self.assertIn(touch_exc.exception.errno, {errno.EROFS, errno.EPERM})

            with self.assertRaises(OSError) as mkdir_exc:
                (replica_mountpoint / "newdir").mkdir()
            self.assertIn(mkdir_exc.exception.errno, {errno.EROFS, errno.EPERM})
        finally:
            replica_launcher.stop()
            shutil.rmtree(replica_mountpoint, ignore_errors=True)
            if dsn is not None and db_config is not None and seed_dir is not None and seed_file is not None:
                try:
                    fs = DBFS(dsn, db_config)
                    fs.unlink(seed_file)
                    fs.rmdir(seed_dir)
                    fs.connection_pool.closeall()
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
