CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS directories (
    id_directory SERIAL PRIMARY KEY,
    id_parent INTEGER REFERENCES directories(id_directory),
    name VARCHAR(255) NOT NULL,
    mode VARCHAR(6) NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    inode_seed TEXT NOT NULL,
    modification_date TIMESTAMP NOT NULL,
    access_date TIMESTAMP NOT NULL,
    change_date TIMESTAMP NOT NULL,
    creation_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id_file SERIAL PRIMARY KEY,
    id_directory INTEGER REFERENCES directories(id_directory),
    name VARCHAR(255) NOT NULL,
    size BIGINT NOT NULL,
    mode VARCHAR(6) NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    inode_seed TEXT NOT NULL,
    modification_date TIMESTAMP NOT NULL,
    access_date TIMESTAMP NOT NULL,
    change_date TIMESTAMP NOT NULL,
    creation_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS special_files (
    id_file INTEGER PRIMARY KEY REFERENCES files(id_file) ON DELETE CASCADE,
    file_type VARCHAR(10) NOT NULL,
    rdev_major INTEGER NOT NULL DEFAULT 0,
    rdev_minor INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS hardlinks (
    id_hardlink SERIAL PRIMARY KEY,
    id_file INTEGER NOT NULL REFERENCES files(id_file) ON DELETE CASCADE,
    id_directory INTEGER REFERENCES directories(id_directory),
    name VARCHAR(255) NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    modification_date TIMESTAMP NOT NULL,
    access_date TIMESTAMP NOT NULL,
    change_date TIMESTAMP NOT NULL,
    creation_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS symlinks (
    id_symlink SERIAL PRIMARY KEY,
    id_parent INTEGER REFERENCES directories(id_directory),
    name VARCHAR(255) NOT NULL,
    target TEXT NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    inode_seed TEXT NOT NULL,
    modification_date TIMESTAMP NOT NULL,
    access_date TIMESTAMP NOT NULL,
    change_date TIMESTAMP NOT NULL,
    creation_date TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS data_blocks (
    id_block SERIAL PRIMARY KEY,
    id_file INTEGER NOT NULL REFERENCES files(id_file),
    _order INTEGER NOT NULL,
    data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id_user SERIAL PRIMARY KEY,
    user_name VARCHAR(255) UNIQUE NOT NULL,
    password TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS permissions (
    id_permission SERIAL PRIMARY KEY,
    id_user INTEGER NOT NULL REFERENCES users(id_user),
    id_directory INTEGER REFERENCES directories(id_directory),
    id_file INTEGER REFERENCES files(id_file),
    permission_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS journal (
    id_entry SERIAL PRIMARY KEY,
    id_user INTEGER,
    id_directory INTEGER REFERENCES directories(id_directory) ON DELETE SET NULL,
    id_file INTEGER REFERENCES files(id_file) ON DELETE SET NULL,
    action TEXT NOT NULL,
    date_time TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS file_metadata (
    id_metadata SERIAL PRIMARY KEY,
    id_file INTEGER NOT NULL REFERENCES files(id_file),
    key VARCHAR(255) NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS locks (
    id_lock SERIAL PRIMARY KEY,
    id_file INTEGER NOT NULL REFERENCES files(id_file),
    flags INTEGER NOT NULL,
    lock_time TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS config (
    key VARCHAR(50) PRIMARY KEY,
    value BIGINT
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_admin (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    password_hash TEXT NOT NULL,
    password_salt TEXT NOT NULL,
    password_iterations INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS xattrs (
    id SERIAL PRIMARY KEY,
    owner_kind VARCHAR(20) NOT NULL,
    owner_id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    value BYTEA NOT NULL,
    UNIQUE(owner_kind, owner_id, name)
);

CREATE TABLE IF NOT EXISTS lock_leases (
    id_lock SERIAL PRIMARY KEY,
    resource_kind VARCHAR(20) NOT NULL,
    resource_id BIGINT NOT NULL,
    owner_key BIGINT NOT NULL,
    lease_kind VARCHAR(20) NOT NULL,
    lock_type INTEGER NOT NULL,
    lease_expires_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(resource_kind, resource_id, owner_key, lease_kind)
);

CREATE TABLE IF NOT EXISTS lock_range_leases (
    id_lock SERIAL PRIMARY KEY,
    resource_kind VARCHAR(20) NOT NULL,
    resource_id BIGINT NOT NULL,
    owner_key BIGINT NOT NULL,
    lock_type INTEGER NOT NULL,
    range_start BIGINT NOT NULL,
    range_end BIGINT NULL,
    lease_expires_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO directories (id_parent, name, mode, uid, gid, inode_seed, modification_date, access_date, change_date, creation_date)
SELECT NULL, '/', '755', 0, 0, 'root', NOW(), NOW(), NOW(), NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM directories WHERE id_parent IS NULL AND name = '/'
);

INSERT INTO directories (id_parent, name, mode, uid, gid, inode_seed, modification_date, access_date, change_date, creation_date)
SELECT NULL, '.Trash-1000', '755', 0, 0, 'pseudo:.Trash-1000', NOW(), NOW(), NOW(), NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM directories WHERE id_parent IS NULL AND name = '.Trash-1000'
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_directories_root_name ON directories (name) WHERE id_parent IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_directories_parent_name ON directories (id_parent, name) WHERE id_parent IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_files_root_name ON files (name) WHERE id_directory IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_files_parent_name ON files (id_directory, name) WHERE id_directory IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_hardlinks_root_name ON hardlinks (name) WHERE id_directory IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_hardlinks_parent_name ON hardlinks (id_directory, name) WHERE id_directory IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_symlinks_root_name ON symlinks (name) WHERE id_parent IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uniq_symlinks_parent_name ON symlinks (id_parent, name) WHERE id_parent IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_blocks_file_order ON data_blocks (id_file, _order);
CREATE INDEX IF NOT EXISTS idx_directories_parent_id ON directories (id_parent);
CREATE INDEX IF NOT EXISTS idx_files_directory_id ON files (id_directory);
CREATE INDEX IF NOT EXISTS idx_hardlinks_directory_id ON hardlinks (id_directory);
CREATE INDEX IF NOT EXISTS idx_symlinks_parent_id ON symlinks (id_parent);
CREATE INDEX IF NOT EXISTS idx_hardlinks_file_id ON hardlinks (id_file);
CREATE INDEX IF NOT EXISTS idx_xattrs_owner ON xattrs (owner_kind, owner_id);
CREATE INDEX IF NOT EXISTS idx_lock_leases_resource ON lock_leases (resource_kind, resource_id, lease_kind);
CREATE INDEX IF NOT EXISTS idx_lock_leases_expires ON lock_leases (lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_lock_range_leases_resource ON lock_range_leases (resource_kind, resource_id);
CREATE INDEX IF NOT EXISTS idx_lock_range_leases_expires ON lock_range_leases (lease_expires_at);
