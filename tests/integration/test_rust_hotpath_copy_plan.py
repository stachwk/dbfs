from __future__ import annotations

import ctypes
from types import SimpleNamespace

from dbfs_storage import DbfsCopySegment, StorageSupport


def main() -> None:
    support = StorageSupport(SimpleNamespace(rust_hotpath_copy_plan=True))
    lib = support._load_rust_hotpath_lib()
    assert lib is not None, "expected built Rust hot-path library"

    cases = [
        (0, 0, 0, 4096, 4),
        (3, 5, 1, 4096, 4),
        (10, 20, 8193, 4096, 4),
        (123, 456, 16384, 4096, 3),
        (7, 9, 32768, 8192, 2),
    ]

    for case in cases:
        src, dst, length, block_size, workers = case
        out_ptr = ctypes.POINTER(DbfsCopySegment)()
        out_len = ctypes.c_size_t()
        rc = lib.dbfs_copy_plan(
            src,
            dst,
            length,
            block_size,
            workers,
            ctypes.byref(out_ptr),
            ctypes.byref(out_len),
        )
        assert rc == 0, case
        try:
            segments = [
                (int(out_ptr[i].src), int(out_ptr[i].dst), int(out_ptr[i].len))
                for i in range(out_len.value)
            ]
        finally:
            lib.dbfs_free_copy_segments(out_ptr, out_len)

        if length == 0:
            assert segments == [], (case, segments)
            continue

        assert segments, (case, segments)
        assert segments[0][0] == src and segments[0][1] == dst, (case, segments)
        assert sum(segment_len for _, _, segment_len in segments) == length, (case, segments)
        for index in range(1, len(segments)):
            prev_src, prev_dst, prev_len = segments[index - 1]
            src_i, dst_i, _ = segments[index]
            assert src_i == prev_src + prev_len, (case, segments)
            assert dst_i == prev_dst + prev_len, (case, segments)

    print("OK rust-hotpath-copy-plan")


if __name__ == "__main__":
    main()
