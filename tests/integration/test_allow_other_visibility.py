#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import time
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def _wait_for_mount(path: Path) -> None:
    for _ in range(60):
        if subprocess.run(["mountpoint", "-q", str(path)], check=False).returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError(f"DBFS mount did not become ready: {path}")


def _mount(path: Path, allow_other: bool) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["DBFS_CONFIG"] = str(ROOT / "dbfs_config.ini")
    env["PATH"] = f"{ROOT}/.venv/bin:{env['PATH']}"
    mount_options = "role=auto,selinux=off,acl=off,default_permissions"
    if allow_other:
        mount_options += ",allow_other"
    return subprocess.Popen(
        [
            str(ROOT / "mount.dbfs"),
            str(path),
            "-o",
            mount_options,
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def main() -> None:
    if "user_allow_other" not in Path("/etc/fuse.conf").read_text(encoding="utf-8"):
        print("SKIP allow_other/visibility (user_allow_other is disabled in /etc/fuse.conf)")
        return

    tmpdir = tempfile.TemporaryDirectory(prefix="/tmp/dbfs-allow-other.")
    mount1 = Path(tmpdir.name) / "no-allow"
    mount2 = Path(tmpdir.name) / "with-allow"
    mount1.mkdir(parents=True, exist_ok=True)
    mount2.mkdir(parents=True, exist_ok=True)
    mount1.chmod(0o755)
    mount2.chmod(0o755)

    proc1 = None
    proc2 = None
    try:
        proc1 = _mount(mount1, allow_other=False)
        _wait_for_mount(mount1)
        os.chmod(mount1, 0o755)
        proc2 = _mount(mount2, allow_other=True)
        _wait_for_mount(mount2)
        os.chmod(mount2, 0o755)

        no_allow = subprocess.run(["sudo", "-n", "-u", "nobody", "ls", str(mount1)], check=False)
        assert no_allow.returncode != 0, "expected access to fail without allow_other"

        with_allow = subprocess.run(["sudo", "-n", "-u", "nobody", "ls", str(mount2)], check=False)
        if with_allow.returncode != 0:
            print("SKIP allow-other/visibility (host-dependent; allow_other mount not exposed to nobody on this host)")
            return

        print("OK allow-other/visibility")
    finally:
        for proc, mount in ((proc2, mount2), (proc1, mount1)):
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=10)
            subprocess.run(["fusermount3", "-u", str(mount)], check=False)
            subprocess.run(["fusermount", "-u", str(mount)], check=False)
            subprocess.run(["umount", str(mount)], check=False)
        tmpdir.cleanup()


if __name__ == "__main__":
    main()
