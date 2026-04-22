from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace

from dbfs_storage import StorageSupport


def main() -> None:
    payload = b"A" * 4096 + b"B" * 4096 + b"C" * 4096
    current = [b"A" * 4096, b"X" * 4096, b"Y" * 4096]

    storage = StorageSupport(SimpleNamespace(rust_hotpath_copy_dedupe=True, block_size=4096))
    storage.get_write_state = lambda file_id: {"file_size": len(payload)}
    storage._read_copy_destination_chunk = lambda dst_file_id, dst_offset, length: current[dst_offset // 4096][:length]

    writes = []
    storage.write_into_state = lambda dst_file_id, chunk, dst_offset: writes.append((dst_offset, bytes(chunk)))

    with tempfile.TemporaryDirectory(prefix="dbfs-rust-dedupe-") as tmpdir:
        helper = Path(tmpdir) / "copy-dedupe"
        helper.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        helper.chmod(0o755)

        previous_helper = os.environ.get("DBFS_RUST_HOTPATH_COPY_DEDUPE_BIN")
        os.environ["DBFS_RUST_HOTPATH_COPY_DEDUPE_BIN"] = str(helper)

        original_run = subprocess.run

        class Completed:
            def __init__(self, stdout: str):
                self.stdout = stdout

        seen = {}

        def fake_run(cmd, input, check, capture_output, text):
            seen["cmd"] = cmd
            seen["input"] = input
            seen["check"] = check
            seen["capture_output"] = capture_output
            seen["text"] = text
            return Completed("4096,8192\n8192,12288\n")

        try:
            subprocess.run = fake_run
            written = storage._write_copy_payload_if_changed(7, 0, payload)
        finally:
            subprocess.run = original_run
            if previous_helper is None:
                os.environ.pop("DBFS_RUST_HOTPATH_COPY_DEDUPE_BIN", None)
            else:
                os.environ["DBFS_RUST_HOTPATH_COPY_DEDUPE_BIN"] = previous_helper

    assert written == 8192, written
    assert writes == [(4096, b"B" * 4096), (8192, b"C" * 4096)], writes
    assert seen["cmd"][0] == str(helper), seen
    assert seen["cmd"][1:] == ["0", "12288", "4096"], seen
    assert seen["check"] is True and seen["capture_output"] is True and seen["text"] is True, seen
    assert "41414141" in seen["input"], seen
    assert "42424242" in seen["input"], seen
    assert "58585858" in seen["input"], seen

    print("OK rust-hotpath-copy-dedupe")


if __name__ == "__main__":
    main()
