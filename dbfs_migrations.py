from __future__ import annotations

from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
MIGRATION_DESCRIPTIONS = {
    1: "Base schema and initial DBFS tables",
    2: "Schema admin secret table",
    3: "Schema version tracking table",
    4: "Copy block CRC cache table",
    5: "Data objects for copy-on-write and dedupe",
}


def migration_files():
    return sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9][0-9]_*.sql"))


def migration_version_from_path(path: Path) -> int:
    prefix = path.name.split("_", 1)[0]
    return int(prefix)


def latest_migration_version() -> int:
    files = migration_files()
    if not files:
        return 0
    return max(migration_version_from_path(path) for path in files)


def migration_sql(version: int) -> str:
    path = MIGRATIONS_DIR / f"{version:04d}_"
    candidates = sorted(MIGRATIONS_DIR.glob(f"{version:04d}_*.sql"))
    if not candidates:
        raise FileNotFoundError(f"Missing migration file for version {version}")
    return candidates[0].read_text(encoding="utf-8")


def migration_exists(version: int) -> bool:
    return any(path.name.startswith(f"{version:04d}_") for path in migration_files())


def migration_description(version: int) -> str:
    return MIGRATION_DESCRIPTIONS.get(version, f"Migration {version:04d}")


def migration_manifest():
    manifest = []
    for path in migration_files():
        version = migration_version_from_path(path)
        manifest.append(
            {
                "version": version,
                "filename": path.name,
                "description": migration_description(version),
            }
        )
    return manifest
