from __future__ import annotations

from datetime import datetime, timezone


UTC = timezone.utc


def utc_now():
    return datetime.now(UTC)


def utc_now_naive():
    return utc_now().replace(tzinfo=None)


def epoch_to_utc_datetime(value):
    return datetime.fromtimestamp(float(value), tz=UTC)


def db_timestamp_to_epoch(value):
    if value is None:
        return 0.0
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    if isinstance(value, datetime):
        return value.timestamp()
    return float(value)
