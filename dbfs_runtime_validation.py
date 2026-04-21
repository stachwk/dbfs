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
    "metadata_cache_ttl_seconds": (float, 0.0, 1.0),
    "statfs_cache_ttl_seconds": (float, 0.0, 2.0),
    "lock_lease_ttl_seconds": (int, 1, 30),
    "lock_heartbeat_interval_seconds": (int, 1, 10),
    "lock_poll_interval_seconds": (float, 1e-09, 0.1),
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


def validate_runtime_config(runtime_config):
    validated = dict(runtime_config or {})
    for name, (caster, minimum, default) in RUNTIME_NUMERIC_SPECS.items():
        validated[name] = _coerce_typed_value(name, validated.get(name), caster, minimum, default)
    return validated
