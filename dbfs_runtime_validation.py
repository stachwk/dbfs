from __future__ import annotations


RUNTIME_NUMERIC_SPECS = {
    "pool_max_connections": (int, 1, 10),
    "write_flush_threshold_bytes": (int, 1, 64 * 1024 * 1024),
    "read_cache_blocks": (int, 1, 1024),
    "read_ahead_blocks": (int, 0, 4),
    "sequential_read_ahead_blocks": (int, 0, 8),
    "small_file_read_threshold_blocks": (int, 0, 8),
    "workers_read": (int, 1, 4),
    "workers_read_min_blocks": (int, 1, 8),
    "workers_write": (int, 1, 4),
    "workers_write_min_blocks": (int, 1, 8),
    "persist_buffer_chunk_blocks": (int, 1, 128),
    "copy_skip_unchanged_blocks_max_blocks": (int, 0, 0),
    "metadata_cache_ttl_seconds": (float, 0.0, 1.0),
    "statfs_cache_ttl_seconds": (float, 0.0, 2.0),
    "lock_lease_ttl_seconds": (int, 1, 30),
    "lock_heartbeat_interval_seconds": (int, 1, 10),
    "lock_poll_interval_seconds": (float, 1e-09, 0.1),
}

RUNTIME_ENUM_SPECS = {
    "synchronous_commit": {
        "allowed": {"on", "off", "local", "remote_write", "remote_apply"},
        "default": "on",
    },
}

RUNTIME_BOOL_SPECS = {
    "copy_skip_unchanged_blocks": False,
    "copy_skip_unchanged_blocks_crc_table": False,
    "rust_hotpath_copy_plan": True,
    "rust_hotpath_copy_dedupe": True,
    "rust_hotpath_copy_pack": True,
    "rust_hotpath_persist_pad": True,
    "rust_hotpath_read_assemble": True,
}


def _coerce_typed_value(name, raw_value, caster, minimum, default):
    if raw_value is None or raw_value == "":
        return default
    try:
        value = caster(raw_value)
    except Exception as exc:
        raise ValueError(f"{name} must be a valid {caster.__name__}") from exc
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _coerce_bool_value(name, raw_value, default):
    if raw_value is None or raw_value == "":
        return default
    if isinstance(raw_value, bool):
        return raw_value
    value = str(raw_value).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def validate_runtime_config(runtime_config):
    validated = dict(runtime_config or {})
    for name, (caster, minimum, default) in RUNTIME_NUMERIC_SPECS.items():
        validated[name] = _coerce_typed_value(name, validated.get(name), caster, minimum, default)
    for name, spec in RUNTIME_ENUM_SPECS.items():
        raw_value = validated.get(name)
        if raw_value is None or raw_value == "":
            validated[name] = spec["default"]
            continue
        value = str(raw_value).strip().lower()
        if value not in spec["allowed"]:
            allowed = ", ".join(sorted(spec["allowed"]))
            raise ValueError(f"{name} must be one of: {allowed}")
        validated[name] = value
    for name, default in RUNTIME_BOOL_SPECS.items():
        validated[name] = _coerce_bool_value(name, validated.get(name), default)
    return validated
