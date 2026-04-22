from __future__ import annotations

import sys
from pathlib import Path
from threading import RLock
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dbfs_storage import StorageSupport


def main() -> None:
    owner = SimpleNamespace(_read_sequence_guard=RLock(), _read_sequence_state={})
    storage = StorageSupport(owner)
    assert storage._load_rust_hotpath_lib() is not None, "expected built Rust hot-path library"

    assert storage._record_read_sequence(7, 0, 4096) == (False, 0)
    assert owner._read_sequence_state[7] == {"last_end": 4096, "streak": 0}

    assert storage._record_read_sequence(7, 4096, 8192) == (True, 1)
    assert owner._read_sequence_state[7] == {"last_end": 8192, "streak": 1}

    assert storage._record_read_sequence(7, 8192, 12288) == (True, 2)
    assert owner._read_sequence_state[7] == {"last_end": 12288, "streak": 2}

    assert storage._record_read_sequence(7, 16384, 20480) == (False, 0)
    assert owner._read_sequence_state[7] == {"last_end": 20480, "streak": 0}

    print("OK rust-hotpath-read-sequence")


if __name__ == "__main__":
    main()
