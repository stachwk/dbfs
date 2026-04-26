use std::env;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use dbfs_rust_hotpath::pg::DbRepo;

fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|err| err.into_inner())
}

fn conninfo() -> String {
    let dbname = env::var("POSTGRES_DB").unwrap_or_else(|_| "dbfsdbname".to_string());
    let user = env::var("POSTGRES_USER").unwrap_or_else(|_| "dbfsuser".to_string());
    let password = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "cichosza".to_string());
    let host = env::var("POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
    format!(
        "host={host} port={port} dbname={dbname} user={user} password={password} connect_timeout=5"
    )
}

fn repo() -> DbRepo {
    DbRepo::new(&conninfo()).expect("failed to connect to PostgreSQL")
}

fn resource_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos() as u64
}

#[test]
fn flock_leases_conflict_and_release() {
    let _guard = test_guard();
    let repo = repo();
    repo.exec(
        r#"
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
        CREATE INDEX IF NOT EXISTS idx_lock_leases_resource
            ON lock_leases (resource_kind, resource_id, lease_kind);
        CREATE INDEX IF NOT EXISTS idx_lock_leases_expires
            ON lock_leases (lease_expires_at);
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
        CREATE INDEX IF NOT EXISTS idx_lock_range_leases_resource
            ON lock_range_leases (resource_kind, resource_id);
        CREATE INDEX IF NOT EXISTS idx_lock_range_leases_expires
            ON lock_range_leases (lease_expires_at);
        "#
    )
    .expect("create lock schema");
    let rid = resource_id();
    let owner_a = rid.saturating_add(1);
    let owner_b = rid.saturating_add(2);

    assert!(
        repo.acquire_flock_lease("file", rid, owner_a, 2, 2, 1)
            .expect("acquire owner_a")
    );
    assert!(
        !repo.acquire_flock_lease("file", rid, owner_b, 2, 2, 1)
            .expect("acquire owner_b conflict")
    );
    repo.heartbeat_lock_lease("file", rid, owner_a, 2)
        .expect("heartbeat");
    repo.release_flock_lease("file", rid, owner_a)
        .expect("release owner_a");
    assert!(
        repo.acquire_flock_lease("file", rid, owner_b, 2, 2, 1)
            .expect("acquire owner_b after release")
    );
    repo.release_flock_lease("file", rid, owner_b)
        .expect("release owner_b");
}

#[test]
fn range_leases_roundtrip_and_cleanup() {
    let _guard = test_guard();
    let repo = repo();
    repo.exec(
        r#"
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
        CREATE INDEX IF NOT EXISTS idx_lock_leases_resource
            ON lock_leases (resource_kind, resource_id, lease_kind);
        CREATE INDEX IF NOT EXISTS idx_lock_leases_expires
            ON lock_leases (lease_expires_at);
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
        CREATE INDEX IF NOT EXISTS idx_lock_range_leases_resource
            ON lock_range_leases (resource_kind, resource_id);
        CREATE INDEX IF NOT EXISTS idx_lock_range_leases_expires
            ON lock_range_leases (lease_expires_at);
        "#
    )
    .expect("create lock schema");
    let rid = resource_id();

    let payload = "1001\t1\t0\t5\n1002\t2\t5\t";
    repo.persist_lock_range_state_blob("file", rid, 2, payload)
        .expect("persist range state");
    let loaded = repo
        .load_lock_range_state_blob("file", rid)
        .expect("load range state");
    assert_eq!(loaded, payload.as_bytes());

    repo.delete_range_leases("file", rid, None)
        .expect("delete range leases");
    let after_prune = repo
        .load_lock_range_state_blob("file", rid)
        .expect("load after prune");
    assert!(after_prune.is_empty(), "{after_prune:?}");
}
