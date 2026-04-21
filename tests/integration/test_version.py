#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_version import DBFS_VERSION_LABEL


def main() -> None:
    bootstrap = subprocess.run(
        [sys.executable, str(ROOT / "dbfs_bootstrap.py"), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    mkfs = subprocess.run(
        [sys.executable, str(ROOT / "mkfs.dbfs.py"), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert bootstrap == DBFS_VERSION_LABEL, bootstrap
    assert mkfs == DBFS_VERSION_LABEL, mkfs
    print(f"OK version {DBFS_VERSION_LABEL}")


if __name__ == "__main__":
    main()
