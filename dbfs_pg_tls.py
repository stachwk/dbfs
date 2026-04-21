from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any, Mapping


def _config_value(mapping: Mapping[str, Any], key: str, default: str = "") -> str:
    value = mapping.get(key, default)
    if value is None:
        return default
    return str(value).strip()


def _resolve_path(value: str | os.PathLike[str], config_dir: Path | None) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute() and config_dir is not None:
        return (config_dir / path).resolve()
    return path.resolve() if path.exists() else path


def generate_client_tls_pair(material_dir: str | os.PathLike[str], common_name: str = "dbfs", days: int = 365) -> tuple[Path, Path]:
    material_path = Path(material_dir).expanduser()
    material_path.mkdir(parents=True, exist_ok=True)
    try:
        material_path.chmod(0o700)
    except Exception:
        pass

    cert_path = material_path / "client.crt"
    key_path = material_path / "client.key"
    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    cmd = [
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-sha256",
        "-nodes",
        "-days",
        str(max(1, int(days))),
        "-subj",
        f"/CN={common_name}",
        "-keyout",
        str(key_path),
        "-out",
        str(cert_path),
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as exc:
        raise RuntimeError("openssl is required to generate a PostgreSQL TLS client pair") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="replace") if exc.stderr else ""
        raise RuntimeError(f"Failed to generate PostgreSQL TLS client pair: {stderr.strip() or exc}") from exc

    try:
        key_path.chmod(0o600)
    except Exception:
        pass
    try:
        cert_path.chmod(0o644)
    except Exception:
        pass
    return cert_path, key_path


def resolve_pg_connection_params(db_config: Mapping[str, Any], *, config_dir: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    config_dir_path = Path(config_dir).expanduser().resolve() if config_dir is not None else None
    connection_params: dict[str, Any] = {
        "host": _config_value(db_config, "host", "127.0.0.1"),
        "port": _config_value(db_config, "port", "5432"),
        "dbname": _config_value(db_config, "dbname", "dbfsdbname"),
        "user": _config_value(db_config, "user", "dbfsuser"),
        "password": _config_value(db_config, "password", ""),
    }

    sslmode = os.environ.get("DBFS_PG_SSLMODE", _config_value(db_config, "sslmode", "disable")).strip() or "disable"
    sslrootcert = os.environ.get("DBFS_PG_SSLROOTCERT", _config_value(db_config, "sslrootcert", ""))
    sslcert = os.environ.get("DBFS_PG_SSLCERT", _config_value(db_config, "sslcert", ""))
    sslkey = os.environ.get("DBFS_PG_SSLKEY", _config_value(db_config, "sslkey", ""))

    if sslmode and sslmode != "disable":
        connection_params["sslmode"] = sslmode
    if sslrootcert:
        connection_params["sslrootcert"] = str(_resolve_path(sslrootcert, config_dir_path))
    if sslcert:
        connection_params["sslcert"] = str(_resolve_path(sslcert, config_dir_path))
    if sslkey:
        connection_params["sslkey"] = str(_resolve_path(sslkey, config_dir_path))

    return connection_params
