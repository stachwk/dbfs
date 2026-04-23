from __future__ import annotations

from types import SimpleNamespace

from dbfs_storage import StorageSupport


def main() -> None:
    support = StorageSupport(SimpleNamespace(rust_hotpath_copy_plan=True))
    assert support._load_rust_hotpath_lib() is not None, "expected built Rust hot-path library"

    cases = [
        (0, 0, 0, 4096, 4),
        (3, 5, 1, 4096, 4),
        (10, 20, 8193, 4096, 4),
        (123, 456, 16384, 4096, 3),
        (7, 9, 32768, 8192, 2),
    ]

    for case in cases:
        python_segments = support.python_to_rust_hotpath_copy_segments(*case)
        runtime_segments = support._copy_segments(*case)
        assert runtime_segments == python_segments, (
            f"copy plan mismatch for {case}: "
            f"python={python_segments!r} runtime={runtime_segments!r}"
        )

    print("OK rust-hotpath-copy-plan")


if __name__ == "__main__":
    main()
