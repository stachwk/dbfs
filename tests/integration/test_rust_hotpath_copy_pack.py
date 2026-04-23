from __future__ import annotations

from types import SimpleNamespace

from dbfs_storage import StorageSupport


def python_pack_changed_ranges(
    off_out: int,
    total_len: int,
    block_size: int,
    changed_mask: list[bool],
):
    block_size = max(1, int(block_size))
    ranges = []
    run_start = None
    copy_end = off_out + total_len

    for block_index, changed in enumerate(changed_mask):
        block_start = off_out + block_index * block_size
        if changed:
            if run_start is None:
                run_start = block_start
            continue

        if run_start is not None:
            ranges.append((run_start, block_start))
            run_start = None

    if run_start is not None:
        ranges.append((run_start, copy_end))

    return ranges


def main() -> None:
    storage = StorageSupport(SimpleNamespace(rust_hotpath_copy_pack=True))
    assert storage._load_rust_hotpath_lib() is not None, "expected built Rust hot-path library"

    cases = [
        (100, 7 * 4096, 4096, [True, True, False, True, False, False, True]),
        (0, 0, 1, []),
        (7, 1024, 8192, [True]),
        (12, 5 * 512, 512, [False, True, True, True, False]),
    ]

    for case in cases:
        python_ranges = python_pack_changed_ranges(*case)
        runtime_ranges = storage._pack_changed_copy_ranges(*case)
        assert runtime_ranges == python_ranges, (
            f"pack mismatch for {case}: "
            f"python={python_ranges!r} runtime={runtime_ranges!r}"
        )

    print("OK rust-hotpath-copy-pack")


if __name__ == "__main__":
    main()
