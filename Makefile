PYTHON ?= python3
VENV_DIR ?= .venv
VENV_PYTHON := $(VENV_DIR)/bin/python
VENV_PIP := $(VENV_DIR)/bin/pip
SYSTEM_SITE_PACKAGES := $(shell $(PYTHON) -c 'import site; print(":".join(site.getsitepackages()))')
COMPOSE ?= docker compose
COMPOSE_FILE ?= docker-compose.yml
DBFS_CONFIG_SOURCE ?= dbfs_config.ini
DBFS_CONFIG_DEST ?= /etc/dbfs/dbfs_config.ini
POSTGRES_DB ?= dbfsdbname
POSTGRES_USER ?= dbfsuser
POSTGRES_PASSWORD ?= cichosza
POSTGRES_PORT ?= 5432
MOUNTPOINT ?= /tmp/dbfs-mount
DBFS_SELINUX ?= auto
DBFS_DEFAULT_PERMISSIONS ?= 1
DBFS_ATIME_POLICY ?= default
DBFS_ROLE ?= auto
DBFS_PROFILE ?=
DBFS_LOG_LEVEL ?= INFO
DBFS_ACL ?= off
ifndef DBFS_SCHEMA_ADMIN_PASSWORD
DBFS_SCHEMA_ADMIN_PASSWORD := $(shell $(PYTHON) -c 'import secrets; print("dbfs-" + secrets.token_urlsafe(24))')
endif
export DBFS_SCHEMA_ADMIN_PASSWORD
DBFS_SELINUX_CONTEXT ?=
DBFS_SELINUX_FSCONTEXT ?=
DBFS_SELINUX_DEFCONTEXT ?=
DBFS_SELINUX_ROOTCONTEXT ?=
DBFS_LAZYTIME ?= 0
DBFS_SYNC ?= 0
DBFS_DIRSYNC ?= 0
MOUNT_HELPER_DEST ?= /usr/local/sbin/mount.dbfs

.PHONY: help venv deps up down restart logs wait init reset smoke mount mount-user demo unmount db-shell install-config install-config-user install-mount-helper install-root-scripts install-on-root pip-build pip-install pip-install-editable config-show test-integration test-xattr test-df test-locking test-pg-lock-manager test-permissions test-journal test-destroy test-dirhooks test-hardlink test-fallocate test-copy-file-range test-copy-skip-unchanged-blocks-benchmark test-worker-thresholds-block-size test-ioctl test-mknod test-bufio test-lseek test-poll test-access-groups test-inode-model test-ownership-inheritance test-rename-root-conflict test-bmap test-statfs-use-ino test-mount-workflow test-mount-root-permissions test-mount-wrapper-options test-files test-directories test-metadata test-symlink test-pool-connections test-mount-suite test-atime-noatime test-atime-relatime test-atime-benchmark test-timestamp-touch-once test-read-ahead-sequence test-read-cache-benchmark test-workers-read-parallel test-workers-write-parallel-copy test-runtime-config test-runtime-validation test-metadata-cache test-mkfs-pg-tls test-runtime-profile test-schema-upgrade test-schema-status test-postgresql-requirements test-throughput test-throughput-sync test-large-copy-benchmark test-large-file-multiblock-benchmark test-remount-durability-benchmark test-tree-scale test-flush-release-profile test-truncate-release-profile test-persist-buffer-chunking test-write-flush-threshold test-utimens-noop test-write-noop test-multi-open-unique-handles test-version test-block-read test-connection-recovery test-all test-all-full clean

help:
	@printf '%s\n' \
		'Targets:' \
		'  make venv       - create .venv and install fusepy + psycopg2-binary' \
		'  make deps       - refresh dependencies in the existing .venv' \
		'  make up         - start local PostgreSQL in Docker' \
		'  make down       - stop local PostgreSQL' \
		'  make restart    - restart local PostgreSQL' \
		'  make logs       - show local PostgreSQL logs' \
		'  make wait       - wait until PostgreSQL is ready' \
		'  make init       - create the DBFS schema in local PostgreSQL with --schema-admin-password' \
		'  make reset      - down -v / up / wait / init for a clean start' \
		'  make install-config - install dbfs_config.ini to /etc/dbfs/dbfs_config.ini' \
		'  make install-config-user - install dbfs_config.ini to $$HOME/.config/dbfs/dbfs_config.ini without sudo' \
		'  make install-mount-helper - install mount.dbfs to $(MOUNT_HELPER_DEST)' \
		'  make install-root-scripts - install dbfs-bootstrap and mkfs.dbfs to /usr/local/bin' \
		'  make install-on-root - install system config, pip package, and mount helper' \
		'  make pip-build - build a pip wheel into dist/' \
		'  make pip-install - install the package into the active venv' \
		'  make pip-install-editable - install the package in editable mode' \
		'  make config-show - show which file DBFS uses for configuration' \
		'  make smoke      - quick database connectivity test' \
		'  make mount      - mount DBFS at $(MOUNTPOINT)' \
		'  make mount-user - mount DBFS with the local ./dbfs_config.ini' \
		'  make demo       - up/init and then mount DBFS at $(MOUNTPOINT)' \
		'  make unmount    - unmount DBFS from $(MOUNTPOINT)' \
		'  make test-integration - run mkdir/create/write/read tests against local PostgreSQL' \
		'  make test-role-autodetect - verify primary/replica autodetection on startup' \
		'  make test-xattr - run xattr/SELinux backend tests' \
		'  make test-df   - verify df -Ph and df -Phi on a mounted DBFS' \
		'  make test-locking - verify in-memory DBFS advisory locks' \
		'  make test-pg-lock-manager - verify PostgreSQL-backed flock and range leases' \
		'  make test-permissions - verify sticky bit and chown permission semantics' \
		'  make test-journal - verify journal entries for mutating operations' \
		'  make test-destroy - verify the destroy cleanup hook' \
		'  make test-dirhooks - verify opendir/releasedir/fsyncdir on a directory' \
		'  make test-hardlink - verify hardlinks through the DBFS backend' \
		'  make test-fallocate - verify fallocate through the DBFS backend' \
		'  make test-copy-file-range - verify copy_file_range through the DBFS backend' \
		'  make test-copy-skip-unchanged-blocks-benchmark - benchmark repeated copy with unchanged-block dedupe' \
		'  make test-worker-thresholds-block-size - verify worker thresholds against block-sized transfers' \
		'  make test-ioctl - verify ioctl/FIONREAD through the DBFS backend' \
		'  make test-mknod - verify FIFO mknod through the DBFS backend' \
		'  make test-bufio - verify read_buf/write_buf through the DBFS backend' \
		'  make test-lseek - verify backend lseek through the DBFS backend' \
		'  make test-poll - verify backend poll through the DBFS backend' \
		'  make test-utimens-noop - verify utimens same-timestamp no-op behavior' \
		'  make test-write-noop - verify zero-length write no-op behavior' \
		'  make test-multi-open-unique-handles - verify independent fh values for concurrent opens' \
		'  make test-version - verify the published DBFS version string' \
		'  make test-access-groups - verify access() for owner, primary group, and supplementary groups' \
		'  make test-inode-model - verify a stable inode model after FS restart' \
		'  make test-ownership-inheritance - verify gid inheritance after parent chmod/chown' \
		'  make test-rename-root-conflict - verify rename replace semantics and edge cases' \
		'  make test-bmap - verify logical bmap for regular files' \
		'  make test-statfs-use-ino - verify statfs and use_ino behavior on a mount' \
		'  make test-atime-noatime - smoke test for DBFS atime behavior (noatime)' \
		'  make test-atime-relatime - smoke test for DBFS atime behavior (relatime)' \
		'  make test-atime-benchmark - benchmark DBFS atime behavior (file and directory reads)' \
		'  make test-timestamp-touch-once - regression for one-touch-at-a-time timestamp writes' \
		'  make test-read-ahead-sequence - regression for sequential read-ahead cache behavior' \
		'  make test-read-cache-benchmark - benchmark DBFS block cache size under sequential reads' \
		'  make test-workers-read-parallel - verify workers_read only parallelize disjoint read gaps' \
		'  make test-workers-write-parallel-copy - verify small copy stays sequential and large copy threads' \
		'  make test-runtime-config - verify dbfs_config.ini runtime tuning values' \
		'  make test-runtime-validation - verify runtime config rejects invalid values' \
		'  make test-metadata-cache - verify short TTL metadata/statfs caching' \
		'  make test-mkfs-pg-tls - verify PostgreSQL TLS generation on mkfs init/upgrade' \
		'  make test-postgresql-requirements - verify minimum PostgreSQL version and connection capacity' \
		'  make test-runtime-profile - verify named DBFS runtime profiles' \
		'  make test-schema-upgrade - verify schema version reporting for upgrade flow' \
		'  make test-replica-ro - verify that replica mounts the FS read-only' \
		'  make test-files - files: create/write/truncate/rename/unlink' \
		'  make test-block-read - range reads, block cache, and read-ahead' \
		'  make test-directories - directories: mkdir/rmdir/rename/stat/ls' \
		'  make test-metadata - metadata: stat/chmod/chown/access' \
		'  make test-mount-workflow - mount + dd + stat + ls + rename + chown + chmod + access' \
		'  make test-mount-root-permissions - fresh mount + directory chmod/chown/write smoke' \
		'  make test-mount-wrapper-options - verify mount.dbfs wrapper option parsing' \
		'  make test-symlink - mount + ln -s + readlink + rename symlink + orphaned symlink ls on the symlink path' \
		'  make test-throughput - benchmark DBFS writes with dd if=/dev/zero' \
		'  make test-throughput-sync - benchmark DBFS writes with conv=fsync' \
		'  make test-large-copy-benchmark - benchmark large copy_file_range transfers' \
		'  make test-large-file-multiblock-benchmark - benchmark large multi-block file writes' \
		'  make test-remount-durability-benchmark - benchmark data survival across remounts' \
		'  make test-tree-scale - benchmark getattr/readdir on a larger tree' \
		'  make test-flush-release-profile - verify clean flush/release and dirty flush regression handling' \
		'  make test-truncate-release-profile - benchmark truncate-only flush/release on large files' \
		'  make test-all-full - full integration suite + atime + throughput' \
		'  make test-pool-connections - verify ThreadedConnectionPool configuration' \
		'  make test-mount-suite - shared Python mount smoke runner' \
		'  make test-all   - smoke + full integration suite' \
		'  make db-shell   - open psql on local PostgreSQL' \
		'  make clean      - remove .venv'

$(VENV_PYTHON):
	$(PYTHON) -m venv $(VENV_DIR)

venv: $(VENV_PYTHON)
	$(VENV_PYTHON) -m ensurepip --upgrade
	$(VENV_PIP) install fusepy psycopg2-binary

deps: $(VENV_PYTHON)
	$(VENV_PYTHON) -m ensurepip --upgrade
	$(VENV_PIP) install fusepy psycopg2-binary

up:
	COMPOSE_PROJECT_NAME=dbfs POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) POSTGRES_PORT=$(POSTGRES_PORT) \
	$(COMPOSE) -f $(COMPOSE_FILE) up -d postgres
	@$(MAKE) wait

down:
	COMPOSE_PROJECT_NAME=dbfs POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) POSTGRES_PORT=$(POSTGRES_PORT) \
	$(COMPOSE) -f $(COMPOSE_FILE) down

restart: down up

logs:
	COMPOSE_PROJECT_NAME=dbfs POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) POSTGRES_PORT=$(POSTGRES_PORT) \
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f postgres

wait:
	@set -eu; \
	echo "Waiting for PostgreSQL in Docker and on 127.0.0.1:$(POSTGRES_PORT)..."; \
	for i in $$(seq 1 60); do \
		if $(COMPOSE) -f $(COMPOSE_FILE) exec -T postgres pg_isready -U $(POSTGRES_USER) -d $(POSTGRES_DB) >/dev/null 2>&1; then \
			echo "PostgreSQL ready."; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "PostgreSQL did not start within the expected time."; \
	exit 1

init: venv up
	@set -eu; \
	status_output="$$(POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) mkfs.dbfs.py status 2>/dev/null || true)"; \
	if printf '%s\n' "$$status_output" | grep -Fq 'DBFS ready: yes'; then \
		echo 'DBFS schema already initialized; skipping init.'; \
	else \
		POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) mkfs.dbfs.py init --schema-admin-password "$(DBFS_SCHEMA_ADMIN_PASSWORD)"; \
	fi

reset: venv
	$(COMPOSE) -f $(COMPOSE_FILE) down -v
	$(MAKE) up
	sleep 2
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) mkfs.dbfs.py init --schema-admin-password "$(DBFS_SCHEMA_ADMIN_PASSWORD)"

install-config:
	@printf '%s\n' "Installing $(DBFS_CONFIG_SOURCE) -> $(DBFS_CONFIG_DEST)"
	sudo install -D -m 0644 $(DBFS_CONFIG_SOURCE) $(DBFS_CONFIG_DEST)

install-config-user:
	@printf '%s\n' "Installing $(DBFS_CONFIG_SOURCE) -> $$HOME/.config/dbfs/dbfs_config.ini"
	install -D -m 0644 $(DBFS_CONFIG_SOURCE) $$HOME/.config/dbfs/dbfs_config.ini

install-mount-helper:
	@printf '%s\n' "Installing mount.dbfs -> $(MOUNT_HELPER_DEST)"
	sudo install -D -m 0755 mount.dbfs $(MOUNT_HELPER_DEST)

install-root-scripts: pip-install
	@printf '%s\n' "Installing dbfs-bootstrap and mkfs.dbfs -> /usr/local/bin"
	sudo install -D -m 0755 "$(VENV_DIR)/bin/dbfs-bootstrap" /usr/local/bin/dbfs-bootstrap
	sudo install -D -m 0755 "$(VENV_DIR)/bin/mkfs.dbfs" /usr/local/bin/mkfs.dbfs

install-on-root: install-config install-root-scripts install-mount-helper
	@printf '%s\n' "DBFS installed for root-style use: config, pip package, and mount helper"

pip-build:
	PYTHONPATH=$(SYSTEM_SITE_PACKAGES) $(VENV_PYTHON) setup.py bdist_wheel -d dist

pip-install:
	PYTHONPATH=$(SYSTEM_SITE_PACKAGES) $(VENV_PYTHON) -m pip install --no-build-isolation --no-use-pep517 --no-deps .

pip-install-editable:
	PYTHONPATH=$(SYSTEM_SITE_PACKAGES) $(VENV_PYTHON) -m pip install --no-build-isolation --no-use-pep517 --no-deps -e .

config-show:
	$(VENV_PYTHON) -c "from dbfs_config import resolve_config_path; print(resolve_config_path(base_dir='.'))"

smoke: venv up
	@set -eu; \
	for attempt in 1 2 3 4 5; do \
		if POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) -c "from dbfs_config import load_config_parser; import psycopg2; c,_=load_config_parser(base_dir='.'); db=dict(c['database']); conn=psycopg2.connect(**db); cur=conn.cursor(); cur.execute('SELECT 1'); print(cur.fetchone()[0]); cur.close(); conn.close()"; then \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	exit 1

mount: venv up
	mkdir -p $(MOUNTPOINT)
	@printf '%s\n' "Using DBFS config file: /etc/dbfs/dbfs_config.ini (fallback: ./dbfs_config.ini)"
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_ROLE=$(DBFS_ROLE) DBFS_PROFILE=$(DBFS_PROFILE) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_LOG_LEVEL=$(DBFS_LOG_LEVEL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) DBFS_SELINUX_CONTEXT=$(DBFS_SELINUX_CONTEXT) DBFS_SELINUX_FSCONTEXT=$(DBFS_SELINUX_FSCONTEXT) DBFS_SELINUX_DEFCONTEXT=$(DBFS_SELINUX_DEFCONTEXT) DBFS_SELINUX_ROOTCONTEXT=$(DBFS_SELINUX_ROOTCONTEXT) $(VENV_PYTHON) dbfs_bootstrap.py --role $(DBFS_ROLE) $(if $(strip $(DBFS_PROFILE)),--profile $(DBFS_PROFILE)) --selinux $(DBFS_SELINUX) --acl $(DBFS_ACL) --atime-policy $(DBFS_ATIME_POLICY) $(if $(filter 0 false False no,$(DBFS_DEFAULT_PERMISSIONS)),--no-default-permissions,--default-permissions) -f $(MOUNTPOINT)

mount-user: venv up
	mkdir -p $(MOUNTPOINT)
	@printf '%s\n' "Using DBFS config file: $$HOME/.config/dbfs/dbfs_config.ini (fallback: ./dbfs_config.ini)"
	DBFS_CONFIG=$$HOME/.config/dbfs/dbfs_config.ini POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_ROLE=$(DBFS_ROLE) DBFS_PROFILE=$(DBFS_PROFILE) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_LOG_LEVEL=$(DBFS_LOG_LEVEL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) DBFS_SELINUX_CONTEXT=$(DBFS_SELINUX_CONTEXT) DBFS_SELINUX_FSCONTEXT=$(DBFS_SELINUX_FSCONTEXT) DBFS_SELINUX_DEFCONTEXT=$(DBFS_SELINUX_DEFCONTEXT) DBFS_SELINUX_ROOTCONTEXT=$(DBFS_SELINUX_ROOTCONTEXT) $(VENV_PYTHON) dbfs_bootstrap.py --role $(DBFS_ROLE) $(if $(strip $(DBFS_PROFILE)),--profile $(DBFS_PROFILE)) --selinux $(DBFS_SELINUX) --acl $(DBFS_ACL) --atime-policy $(DBFS_ATIME_POLICY) $(if $(filter 0 false False no,$(DBFS_DEFAULT_PERMISSIONS)),--no-default-permissions,--default-permissions) -f $(MOUNTPOINT)

demo: init
	mkdir -p $(MOUNTPOINT)
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_ROLE=$(DBFS_ROLE) DBFS_PROFILE=$(DBFS_PROFILE) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_LOG_LEVEL=$(DBFS_LOG_LEVEL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) DBFS_SELINUX_CONTEXT=$(DBFS_SELINUX_CONTEXT) DBFS_SELINUX_FSCONTEXT=$(DBFS_SELINUX_FSCONTEXT) DBFS_SELINUX_DEFCONTEXT=$(DBFS_SELINUX_DEFCONTEXT) DBFS_SELINUX_ROOTCONTEXT=$(DBFS_SELINUX_ROOTCONTEXT) $(VENV_PYTHON) dbfs_bootstrap.py --role $(DBFS_ROLE) $(if $(strip $(DBFS_PROFILE)),--profile $(DBFS_PROFILE)) --selinux $(DBFS_SELINUX) --acl $(DBFS_ACL) --atime-policy $(DBFS_ATIME_POLICY) $(if $(filter 0 false False no,$(DBFS_DEFAULT_PERMISSIONS)),--no-default-permissions,--default-permissions) -f $(MOUNTPOINT)

unmount:
	@set -eu; \
	if command -v fusermount3 >/dev/null 2>&1; then \
		fusermount3 -u $(MOUNTPOINT); \
	elif command -v fusermount >/dev/null 2>&1; then \
		fusermount -u $(MOUNTPOINT); \
	else \
		umount $(MOUNTPOINT); \
	fi

test-integration: reset test-flush-release-profile test-persist-buffer-chunking test-write-flush-threshold test-utimens-noop test-write-noop test-multi-open-unique-handles test-workers-read-parallel test-workers-write-parallel-copy test-worker-thresholds-block-size test-version test-timestamp-touch-once test-read-ahead-sequence test-read-cache-benchmark test-runtime-config test-runtime-validation test-metadata-cache test-mkfs-pg-tls test-runtime-profile test-schema-upgrade test-schema-status test-postgresql-requirements test-block-read test-pg-lock-manager test-mount-root-permissions test-mount-wrapper-options test-connection-recovery
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_mkdir_create_write_read.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_mkdir_parent_missing.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_truncate_rename.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_chmod_rmdir.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_rename_root_conflict.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_destroy.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_dirhooks.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_hardlink.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_fallocate.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_copy_file_range.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_ioctl.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_mknod.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_bufio.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_lseek.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_poll.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_access_groups.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_inode_model.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_ownership_inheritance.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_permissions.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_bmap.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_role_autodetect.py
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_xattr.py

test-role-autodetect: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_role_autodetect.py

test-xattr: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_xattr.py

test-locking: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_locking.py

test-pg-lock-manager: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_pg_lock_manager.py

test-permissions: up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_permissions.py

test-journal: up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_journal.py

test-destroy: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_destroy.py

test-dirhooks: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_dirhooks.py

test-hardlink: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_hardlink.py

test-fallocate: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_fallocate.py

test-copy-file-range: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_copy_file_range.py

test-copy-skip-unchanged-blocks-benchmark: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_copy_skip_unchanged_blocks_benchmark.py

test-worker-thresholds-block-size: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_worker_thresholds_block_size.py

test-ioctl: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_ioctl.py

test-mknod: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_mknod.py

test-bufio: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_bufio.py

test-lseek: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_lseek.py

test-poll: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_poll.py

test-access-groups: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_access_groups.py

test-inode-model: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_inode_model.py

test-ownership-inheritance: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_ownership_inheritance.py

test-rename-root-conflict: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_rename_root_conflict.py

test-bmap: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_bmap.py

test-statfs-use-ino: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SCHEMA_ADMIN_PASSWORD=$(DBFS_SCHEMA_ADMIN_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_statfs_use_ino.sh

test-atime-noatime: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SCHEMA_ADMIN_PASSWORD=$(DBFS_SCHEMA_ADMIN_PASSWORD) DBFS_ATIME_POLICY=noatime DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_atime_policy.sh

test-atime-relatime: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SCHEMA_ADMIN_PASSWORD=$(DBFS_SCHEMA_ADMIN_PASSWORD) DBFS_ATIME_POLICY=relatime DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_atime_policy.sh

test-atime-benchmark: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SCHEMA_ADMIN_PASSWORD=$(DBFS_SCHEMA_ADMIN_PASSWORD) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) ATIME_BENCH_KIND=file bash tests/integration/test_atime_benchmark.sh
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SCHEMA_ADMIN_PASSWORD=$(DBFS_SCHEMA_ADMIN_PASSWORD) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) ATIME_BENCH_KIND=dir bash tests/integration/test_atime_benchmark.sh

test-timestamp-touch-once: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_timestamp_touch_once.py

test-read-ahead-sequence: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_read_ahead_sequence.py

test-read-cache-benchmark: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_read_cache_benchmark.py

test-workers-read-parallel: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_workers_read_parallel.py

test-workers-write-parallel-copy: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_workers_write_parallel_copy.py

test-runtime-config: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_runtime_config.py

test-runtime-validation:
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_runtime_validation.py

test-metadata-cache: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_metadata_cache.py

test-mkfs-pg-tls: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_mkfs_pg_tls.py

test-postgresql-requirements: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_postgresql_requirements.py

test-runtime-profile: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_runtime_profile.py

test-schema-upgrade: up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_schema_upgrade.py

test-schema-status: up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_schema_status.py

test-df: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_df.sh

test-replica-ro: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_ROLE=replica DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_replica_read_only.sh

test-mount-workflow: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) DBFS_SELINUX_CONTEXT=$(DBFS_SELINUX_CONTEXT) DBFS_SELINUX_FSCONTEXT=$(DBFS_SELINUX_FSCONTEXT) DBFS_SELINUX_DEFCONTEXT=$(DBFS_SELINUX_DEFCONTEXT) DBFS_SELINUX_ROOTCONTEXT=$(DBFS_SELINUX_ROOTCONTEXT) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_mount_workflow.sh

test-mount-root-permissions: reset
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SCHEMA_ADMIN_PASSWORD=$(DBFS_SCHEMA_ADMIN_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_mount_root_permissions.sh

test-mount-wrapper-options:
	bash tests/integration/test_mount_wrapper_options.sh

test-files: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_files.sh

test-block-read: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_block_read.py

test-directories: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_directories.sh

test-metadata: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_metadata.sh

test-symlink: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) bash tests/integration/test_symlink.sh

test-throughput: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) THROUGHPUT_BLOCK_SIZE=$(THROUGHPUT_BLOCK_SIZE) THROUGHPUT_COUNT=$(THROUGHPUT_COUNT) THROUGHPUT_SYNC=$(THROUGHPUT_SYNC) bash tests/integration/test_throughput.sh

test-throughput-sync: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) THROUGHPUT_BLOCK_SIZE=$(THROUGHPUT_BLOCK_SIZE) THROUGHPUT_COUNT=$(THROUGHPUT_COUNT) THROUGHPUT_SYNC=1 bash tests/integration/test_throughput.sh

test-large-copy-benchmark: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_large_copy_benchmark.py

test-large-file-multiblock-benchmark: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_large_file_multiblock_benchmark.py

test-remount-durability-benchmark: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_remount_durability_benchmark.py

test-tree-scale: venv up
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) VENV_PYTHON=$(VENV_PYTHON) TREE_SCALE_DIRS=$(TREE_SCALE_DIRS) TREE_SCALE_FILES=$(TREE_SCALE_FILES) bash tests/integration/test_tree_scale.sh

test-flush-release-profile: reset
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_flush_release_profile.py

test-truncate-release-profile: reset
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_truncate_release_profile.py

test-persist-buffer-chunking: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_persist_buffer_chunking.py

test-write-flush-threshold: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_write_flush_threshold.py

test-utimens-noop: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_utimens_noop.py

test-write-noop: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_write_noop.py

test-multi-open-unique-handles: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_multi_open_unique_handles.py

test-version:
	$(VENV_PYTHON) tests/integration/test_version.py

test-connection-recovery: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_connection_recovery.py

test-pool-connections: init
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) $(VENV_PYTHON) tests/integration/test_pool_connections.py

test-mount-suite: venv
	$(MAKE) reset
	POSTGRES_DB=$(POSTGRES_DB) POSTGRES_USER=$(POSTGRES_USER) POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) VENV_PYTHON=$(VENV_PYTHON) DBFS_SELINUX=$(DBFS_SELINUX) DBFS_ACL=$(DBFS_ACL) DBFS_DEFAULT_PERMISSIONS=$(DBFS_DEFAULT_PERMISSIONS) DBFS_ATIME_POLICY=$(DBFS_ATIME_POLICY) DBFS_ROLE=$(DBFS_ROLE) DBFS_LAZYTIME=$(DBFS_LAZYTIME) DBFS_SYNC=$(DBFS_SYNC) DBFS_DIRSYNC=$(DBFS_DIRSYNC) DBFS_SELINUX_CONTEXT=$(DBFS_SELINUX_CONTEXT) DBFS_SELINUX_FSCONTEXT=$(DBFS_SELINUX_FSCONTEXT) DBFS_SELINUX_DEFCONTEXT=$(DBFS_SELINUX_DEFCONTEXT) DBFS_SELINUX_ROOTCONTEXT=$(DBFS_SELINUX_ROOTCONTEXT) $(VENV_PYTHON) tests/integration/test_mount_suite.py

test-all: smoke test-integration test-mount-suite test-xattr test-locking test-permissions test-journal test-destroy test-dirhooks test-hardlink test-fallocate test-copy-file-range test-ioctl test-mknod test-bufio test-lseek test-poll test-access-groups test-inode-model test-ownership-inheritance test-rename-root-conflict test-bmap test-pool-connections test-timestamp-touch-once test-read-ahead-sequence test-read-cache-benchmark test-workers-read-parallel test-workers-write-parallel-copy test-worker-thresholds-block-size test-runtime-config test-runtime-validation test-metadata-cache test-mkfs-pg-tls test-runtime-profile test-schema-upgrade test-postgresql-requirements test-connection-recovery test-multi-open-unique-handles

test-all-full: test-all test-files test-directories test-metadata test-symlink test-mount-workflow test-statfs-use-ino test-atime-noatime test-atime-relatime test-throughput test-throughput-sync

db-shell:
	$(COMPOSE) -f $(COMPOSE_FILE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

clean:
	rm -rf $(VENV_DIR)
