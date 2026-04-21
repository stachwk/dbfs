from __future__ import annotations

import argparse
import logging
import os

from fuse import FUSE

from dbfs_config import resolve_config_path
from dbfs_backend import load_dbfs_runtime_config, load_dsn_from_config
from dbfs_schema import SCHEMA_VERSION
from dbfs_fuse import DBFS, configure_logging
from dbfs_version import DBFS_VERSION_LABEL


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mount a FUSE filesystem from PostgreSQL.")
    parser.add_argument(
        "--debug",
        action="store_true",
        default=os.environ.get("DBFS_DEBUG", "0") not in {"0", "false", "False", "no", "off"},
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("DBFS_LOG_LEVEL"),
        help="Set an explicit log level such as INFO or DEBUG.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=DBFS_VERSION_LABEL,
    )
    parser.add_argument("-f", "--mountpoint", required=True, help="FUSE filesystem mount point.")
    parser.add_argument(
        "--default-permissions",
        dest="default_permissions",
        action="store_true",
        default=os.environ.get("DBFS_DEFAULT_PERMISSIONS", "1") not in {"0", "false", "False", "no"},
        help="Enable kernel-side permission checks.",
    )
    parser.add_argument(
        "--no-default-permissions",
        dest="default_permissions",
        action="store_false",
        help="Disable kernel-side permission checks.",
    )
    parser.add_argument(
        "--atime-policy",
        choices=("default", "noatime", "nodiratime", "relatime", "strictatime"),
        default=os.environ.get("DBFS_ATIME_POLICY", "default"),
        help="Select DBFS atime behavior.",
    )
    parser.add_argument(
        "--lazytime",
        action="store_true",
        default=os.environ.get("DBFS_LAZYTIME", "0") not in {"0", "false", "False", "no", "off"},
        help="Enable lazytime mount option.",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        default=os.environ.get("DBFS_SYNC", "0") not in {"0", "false", "False", "no", "off"},
        help="Enable sync mount option.",
    )
    parser.add_argument(
        "--dirsync",
        action="store_true",
        default=os.environ.get("DBFS_DIRSYNC", "0") not in {"0", "false", "False", "no", "off"},
        help="Enable dirsync mount option.",
    )
    parser.add_argument(
        "--selinux",
        choices=("auto", "on", "off"),
        default=os.environ.get("DBFS_SELINUX", "off"),
        help="Enable SELinux xattr support automatically, always, or never.",
    )
    parser.add_argument(
        "--acl",
        choices=("on", "off"),
        default=os.environ.get("DBFS_ACL", "off"),
        help="Enable or disable POSIX ACL support.",
    )
    parser.add_argument(
        "--role",
        choices=("auto", "primary", "replica"),
        default=os.environ.get("DBFS_ROLE", "auto"),
        help="Select FS role; auto detects replica via pg_is_in_recovery().",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    log_level_name = args.log_level or ("DEBUG" if args.debug else None)
    configure_logging(log_level_name)

    config_path = resolve_config_path(base_dir=os.path.dirname(os.path.abspath(__file__)))
    logging.info("Using DBFS config file: %s", config_path)
    logging.info("DBFS version=%s", DBFS_VERSION_LABEL)
    logging.debug("Resolved mountpoint argument: %s", os.path.abspath(args.mountpoint))
    dsn, db_config = load_dsn_from_config(config_path)
    runtime_config = load_dbfs_runtime_config(config_path)
    logging.debug("Creating FUSE instance")
    mount_kwargs = {"foreground": True}
    fs = DBFS(
        dsn,
        db_config,
        runtime_config=runtime_config,
        selinux_mode=args.selinux,
        acl_mode=args.acl,
        role=args.role,
        pool_max_connections=int(runtime_config.get("pool_max_connections", 10)) if runtime_config else 10,
    )
    mount_kwargs = fs.apply_mount_options(mount_kwargs, args)
    logging.info("SELinux xattr mode=%s enabled=%s", fs.selinux_mode, fs.selinux_enabled)
    logging.info("ACL support mode=%s enabled=%s", fs.acl_mode, fs.acl_enabled)
    logging.info("DBFS role=%s requested_role=%s read_only=%s", fs.role, fs.requested_role, fs.read_only)
    logging.info("DBFS runtime profile=%s", fs.runtime_config_get("profile", "default"))
    logging.info("DBFS schema version=%s expected=%s", fs.backend.schema_version(), SCHEMA_VERSION)
    logging.info(
        "DBFS PostgreSQL TLS sslmode=%s sslrootcert=%s sslcert=%s sslkey=%s",
        dsn.get("sslmode", "disable") if isinstance(dsn, dict) else "disable",
        dsn.get("sslrootcert", "") if isinstance(dsn, dict) else "",
        dsn.get("sslcert", "") if isinstance(dsn, dict) else "",
        dsn.get("sslkey", "") if isinstance(dsn, dict) else "",
    )
    logging.info("DBFS PostgreSQL synchronous_commit=%s", fs.synchronous_commit)
    logging.info(
        "DBFS storage tuning: write_flush_threshold=%s bytes read_cache_blocks=%s read_ahead_blocks=%s sequential_read_ahead_blocks=%s small_file_threshold_blocks=%s workers_read=%s workers_read_min_blocks=%s workers_write=%s workers_write_min_blocks=%s persist_buffer_chunk_blocks=%s metadata_cache_ttl=%ss statfs_cache_ttl=%ss",
        fs.write_flush_threshold_bytes,
        fs.read_cache_max_blocks,
        fs.read_ahead_blocks,
        fs.sequential_read_ahead_blocks,
        fs.small_file_read_threshold_blocks,
        fs.workers_read,
        fs.workers_read_min_blocks,
        fs.workers_write,
        fs.workers_write_min_blocks,
        fs.persist_buffer_chunk_blocks,
        fs.metadata_cache_ttl_seconds,
        fs.statfs_cache_ttl_seconds,
    )
    logging.info(
        "DBFS mount options: use_ino=%s ro=%s default_permissions=%s allow_other=%s entry_timeout=%ss attr_timeout=%ss negative_timeout=%ss lazytime=%s sync=%s dirsync=%s atime_policy=%s",
        mount_kwargs.get("use_ino"),
        mount_kwargs.get("ro"),
        mount_kwargs.get("default_permissions", False),
        mount_kwargs.get("allow_other", False),
        mount_kwargs.get("entry_timeout"),
        mount_kwargs.get("attr_timeout"),
        mount_kwargs.get("negative_timeout"),
        mount_kwargs.get("lazytime", False),
        mount_kwargs.get("sync", False),
        mount_kwargs.get("dirsync", False),
        fs.atime_policy,
    )
    logging.info(
        "DBFS lock backend=%s lease_ttl=%ss heartbeat=%ss poll=%ss",
        fs.lock_backend,
        fs.lock_lease_ttl_seconds,
        fs.lock_heartbeat_interval_seconds,
        fs.lock_poll_interval_seconds,
    )
    logging.info("Schema mode=new ctime_column=change_date for files/directories/symlinks/hardlinks")
    FUSE(fs, args.mountpoint, **mount_kwargs)
    logging.debug("DBFS mounted")


if __name__ == "__main__":
    main()
