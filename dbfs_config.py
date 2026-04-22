from __future__ import annotations

import configparser
import os
from pathlib import Path


SYSTEM_CONFIG_PATH = Path("/etc/dbfs/dbfs_config.ini")
USER_CONFIG_PATH = Path.home() / ".config" / "dbfs" / "dbfs_config.ini"
LOCAL_CONFIG_NAMES = ("dbfs_config.ini",)
ENV_CONFIG_VAR = "DBFS_CONFIG"


def resolve_config_path(file_path: str | os.PathLike[str] | None = None, base_dir: str | os.PathLike[str] | None = None) -> Path:
    candidates: list[Path] = []

    env_path = os.environ.get(ENV_CONFIG_VAR)
    if env_path:
        candidates.append(Path(env_path).expanduser())

    if file_path is not None:
        explicit_path = Path(file_path).expanduser()
        if explicit_path.is_file():
            return explicit_path
        if explicit_path.is_dir():
            base_dir = explicit_path
        elif base_dir is None and explicit_path.parent != Path("."):
            base_dir = explicit_path.parent

    candidates.append(SYSTEM_CONFIG_PATH)
    candidates.append(USER_CONFIG_PATH)

    search_root = Path(base_dir).expanduser().resolve() if base_dir is not None else Path.cwd().resolve()
    for name in LOCAL_CONFIG_NAMES:
        candidates.append(search_root / name)

    if file_path is not None:
        file_name = Path(file_path).name
        if file_name not in LOCAL_CONFIG_NAMES:
            candidates.append(search_root / file_name)

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    raise FileNotFoundError(
        "DBFS configuration file not found. Expected "
        f"{SYSTEM_CONFIG_PATH} or {search_root / 'dbfs_config.ini'}."
    )


def load_config_parser(file_path: str | os.PathLike[str] | None = None, base_dir: str | os.PathLike[str] | None = None) -> tuple[configparser.ConfigParser, Path]:
    config_path = resolve_config_path(file_path=file_path, base_dir=base_dir)
    config = configparser.ConfigParser()
    read_files = config.read(config_path)
    if not read_files:
        raise FileNotFoundError(f"Unable to read DBFS configuration: {config_path}")
    return config, config_path
