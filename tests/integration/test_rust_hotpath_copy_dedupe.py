from __future__ import annotations

from types import SimpleNamespace

from dbfs_storage import StorageSupport


def main() -> None:
    payload = b"A" * 4096 + b"B" * 4096 + b"C" * 4096
    current = [b"A" * 4096, b"X" * 4096, b"Y" * 4096]

    storage = StorageSupport(
        SimpleNamespace(
            rust_hotpath_copy_dedupe=True,
            copy_dedupe_enabled=True,
            copy_dedupe_min_blocks=1,
            block_size=4096,
        )
    )
    assert storage._load_rust_hotpath_lib() is not None, "expected built Rust hot-path library"

    storage.get_write_state = lambda file_id: {"file_size": len(payload)}
    storage._read_copy_destination_chunk = lambda dst_file_id, dst_offset, length: current[dst_offset // 4096][:length]

    ffi_writes = []
    storage.write_into_state = lambda dst_file_id, chunk, dst_offset: ffi_writes.append((dst_offset, bytes(chunk)))

    written = storage._write_copy_payload_if_changed(7, 0, payload)

    assert written == 8192, written
    assert ffi_writes == [(4096, b"B" * 4096 + b"C" * 4096)], ffi_writes

    storage = StorageSupport(
        SimpleNamespace(
            rust_hotpath_copy_dedupe=False,
            copy_dedupe_enabled=True,
            copy_dedupe_min_blocks=1,
            block_size=4096,
        )
    )
    storage._load_rust_hotpath_lib = lambda: None
    storage.get_write_state = lambda file_id: {"file_size": len(payload)}
    storage._read_copy_destination_chunk = lambda dst_file_id, dst_offset, length: current[dst_offset // 4096][:length]
    fallback_writes = []
    storage.write_into_state = lambda dst_file_id, chunk, dst_offset: fallback_writes.append((dst_offset, bytes(chunk)))
    fallback_written = storage._write_copy_payload_if_changed(7, 0, payload)

    assert fallback_written == 8192, fallback_written
    assert fallback_writes in [
        [(4096, b"B" * 4096), (8192, b"C" * 4096)],
        [(4096, b"B" * 4096 + b"C" * 4096)],
    ], fallback_writes

    print("OK rust-hotpath-copy-dedupe")


if __name__ == "__main__":
    main()
