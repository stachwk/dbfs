from __future__ import annotations

import os
import pwd
import stat
import uuid
import zlib
from typing import Callable, Mapping, Any


def _use_fuse_context() -> bool:
    return os.environ.get("DBFS_USE_FUSE_CONTEXT", "0").strip().lower() not in {"", "0", "false", "no", "off"}


def _fuse_context_uid_gid() -> tuple[int, int] | None:
    if not _use_fuse_context():
        return None
    try:
        from fuse import fuse_get_context
    except Exception:
        return None
    try:
        uid, gid, pid = fuse_get_context()
    except Exception:
        return None
    try:
        os.kill(int(pid), 0)
    except Exception:
        return None
    return int(uid), int(gid)


def current_uid_gid(prefer_fuse_context: bool = False) -> tuple[int, int]:
    if prefer_fuse_context:
        fuse_ctx = _fuse_context_uid_gid()
        if fuse_ctx is not None:
            return fuse_ctx
    uid = os.getuid() if hasattr(os, "getuid") else 0
    gid = os.getgid() if hasattr(os, "getgid") else 0
    return uid, gid


def current_group_ids(prefer_fuse_context: bool = False) -> set[int]:
    if prefer_fuse_context:
        fuse_ctx = _fuse_context_uid_gid()
        if fuse_ctx is not None:
            uid, gid = fuse_ctx
            try:
                user_name = pwd.getpwuid(uid).pw_name
                group_ids = set(os.getgrouplist(user_name, gid))
                group_ids.add(gid)
                return group_ids
            except Exception:
                return {gid}
    _, gid = current_uid_gid()
    group_ids = {gid}
    if hasattr(os, "getgroups"):
        try:
            group_ids.update(os.getgroups())
        except Exception:
            pass
    return group_ids


def ctime_column(table_name: str) -> str:
    return "change_date"


def normalize_path(path: str | os.PathLike[str]) -> str:
    if path is None:
        return "/"
    if isinstance(path, bytes):
        path = path.decode("utf-8")
    path = str(path)
    if path == "":
        return "/"
    if not path.startswith("/"):
        path = "/" + path
    normalized = os.path.normpath(path)
    if normalized == ".":
        return "/"
    return normalized


def creation_uid_gid(
    parent_path: str | os.PathLike[str],
    get_attrs: Callable[[str], Mapping[str, Any]],
    normalize_path_fn: Callable[[str | os.PathLike[str]], str] = normalize_path,
) -> tuple[int, int]:
    uid, gid = current_uid_gid()
    parent_path = normalize_path_fn(parent_path)
    if parent_path != "/":
        try:
            parent_attrs = get_attrs(parent_path)
        except Exception:
            return uid, gid
        if parent_attrs["st_mode"] & stat.S_ISGID:
            gid = parent_attrs["st_gid"]
    return uid, gid


def inherited_directory_mode(
    parent_path: str | os.PathLike[str],
    mode: int,
    get_attrs: Callable[[str], Mapping[str, Any]],
    normalize_path_fn: Callable[[str | os.PathLike[str]], str] = normalize_path,
) -> int:
    parent_path = normalize_path_fn(parent_path)
    try:
        parent_attrs = get_attrs(parent_path) if parent_path != "/" else {"st_mode": stat.S_IFDIR | 0o755}
    except Exception:
        return mode
    if parent_attrs["st_mode"] & stat.S_ISGID:
        return mode | stat.S_ISGID
    return mode


def compute_device_id(db_config: Mapping[str, Any]) -> int:
    device_seed = "|".join(
        [
            str(db_config.get("host", "")),
            str(db_config.get("port", "")),
            str(db_config.get("dbname", "")),
            str(db_config.get("user", "")),
        ]
    ).encode("utf-8")
    device_id = zlib.crc32(device_seed) & 0xFFFFFFFF
    return device_id or 1


def generate_inode_seed() -> str:
    return uuid.uuid4().hex


def logical_inode(obj_type: str, entry_id: int | str) -> int:
    if obj_type == "file":
        return 1_000_000 + int(entry_id)
    if obj_type == "dir":
        return 2_000_000 + int(entry_id)
    if obj_type == "symlink":
        return 3_000_000 + int(entry_id)
    if obj_type == "hardlink":
        return 1_000_000 + int(entry_id)
    return int(entry_id)


def stable_inode(obj_type: str, inode_seed: str | None, entry_id: int | str) -> int:
    if inode_seed:
        payload = f"{obj_type}:{inode_seed}".encode("utf-8")
        inode = zlib.crc32(payload) & 0xFFFFFFFF
        return inode or logical_inode(obj_type, entry_id)
    return logical_inode(obj_type, entry_id)
