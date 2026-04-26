use std::env;
use std::sync::{Mutex, OnceLock};

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

#[test]
fn query_error_does_not_poison_followup_queries() {
    let _guard = test_guard();
    let repo = DbRepo::new(&conninfo()).expect("failed to connect to PostgreSQL");

    let healthy = repo.query_scalar_text("SELECT 1").expect("warmup query");
    assert_eq!(healthy.trim(), "1");

    assert!(repo
        .query_scalar_text("SELECT * FROM dbfs_rust_connection_recovery_missing_table")
        .is_err());

    let recovered = repo.query_scalar_text("SELECT 1").expect("recovery query");
    assert_eq!(recovered.trim(), "1");

    let snapshot = repo.startup_snapshot().expect("startup snapshot after recovery");
    assert!(snapshot.schema_is_initialized);
}
