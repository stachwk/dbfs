from __future__ import annotations

from pathlib import Path

from setuptools import find_packages, setup

from dbfs_version import DBFS_VERSION


ROOT = Path(__file__).resolve().parent
README = (ROOT / "README.md").read_text(encoding="utf-8")


PY_MODULES = [
    "dbfs_backend",
    "dbfs_bootstrap",
    "dbfs_config",
    "dbfs_fuse",
    "dbfs_identity",
    "dbfs_journal",
    "dbfs_locking",
    "dbfs_metadata",
    "dbfs_migrations",
    "dbfs_mkfs",
    "dbfs_namespace",
    "dbfs_permissions",
    "dbfs_pg_lock_manager",
    "dbfs_pg_tls",
    "dbfs_repository",
    "dbfs_runtime_validation",
    "dbfs_schema",
    "dbfs_storage",
    "dbfs_time",
    "dbfs_version",
    "dbfs_xattr_acl",
    "dbfs_xattr_store",
]


setup(
    name="dbfs",
    version=DBFS_VERSION,
    description="DBFS FUSE filesystem backed by PostgreSQL",
    long_description=README,
    long_description_content_type="text/markdown",
    python_requires=">=3.11",
    py_modules=PY_MODULES,
    packages=find_packages(include=["mod", "mod.*"]),
    include_package_data=True,
    install_requires=[
        "fusepy",
        "psycopg2-binary",
    ],
    entry_points={
        "console_scripts": [
            "dbfs-bootstrap=dbfs_bootstrap:main",
            "mkfs.dbfs=dbfs_mkfs:main",
        ],
    },
    scripts=["mount.dbfs"],
)
