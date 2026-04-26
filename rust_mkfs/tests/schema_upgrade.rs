#[path = "../src/pg.rs"]
mod pg;

use pg::DbConn;
use std::env;
use std::process::Command;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());
const SCHEMA_VERSION: u64 = 5;

fn conninfo_from_env() -> String {
    let host = env::var("POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
    let dbname = env::var("POSTGRES_DB").unwrap_or_else(|_| "dbfsdbname".to_string());
    let user = env::var("POSTGRES_USER").unwrap_or_else(|_| "dbfsuser".to_string());
    let password = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "cichosza".to_string());
    format!(
        "host='{}' port='{}' dbname='{}' user='{}' password='{}'",
        host, port, dbname, user, password
    )
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

fn run_mkfs(action: &str, extra_args: &[&str], envs: &[(&str, String)]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dbfs-rust-mkfs"));
    command.arg(action);
    for arg in extra_args {
        command.arg(arg);
    }
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().expect("failed to run dbfs-rust-mkfs")
}

fn assert_upgrade_message(output: &str) {
    if output.contains(&format!("Schema upgraded to version {}.", SCHEMA_VERSION))
        || output.contains(&format!("Schema already at version {}.", SCHEMA_VERSION))
    {
        return;
    }
    panic!("{output}");
}

fn assert_password_source(output: &str, source: &str) {
    let expected = format!("Schema admin password source: {} (no prompt needed)", source);
    assert!(output.contains(&expected), "{output}");
}

fn table_exists(conn: &DbConn, table_name: &str) -> Result<bool, String> {
    conn.query_exists(&format!(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{}')",
        table_name.replace('\'', "''")
    ))
}

#[test]
fn schema_upgrade_non_destructive_password_protected() {
    let _guard = ENV_LOCK.lock().unwrap();
    let conninfo = conninfo_from_env();
    let conn = DbConn::connect(&conninfo).expect("connect");

    conn.exec("DROP SCHEMA IF EXISTS public CASCADE").expect("drop schema");
    conn.exec("CREATE SCHEMA public").expect("create schema");

    let guard_table = unique_name("schema_upgrade_guard");
    conn.exec(&format!(
        "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, note TEXT NOT NULL)",
        DbConn::quote_identifier(&guard_table)
    ))
    .expect("create guard table");
    conn.exec(&format!(
        "INSERT INTO {} (id, note) VALUES (1, 'guard') ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
        DbConn::quote_identifier(&guard_table)
    ))
    .expect("seed guard table");

    let envs = vec![
        ("POSTGRES_DB", env::var("POSTGRES_DB").unwrap_or_else(|_| "dbfsdbname".to_string())),
        ("POSTGRES_USER", env::var("POSTGRES_USER").unwrap_or_else(|_| "dbfsuser".to_string())),
        ("POSTGRES_PASSWORD", env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "cichosza".to_string())),
    ];
    let schema_password = env::var("DBFS_SCHEMA_ADMIN_PASSWORD")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("dbfs-{}", unique_name("schema").replace('-', "")));

    let init_without_secret = run_mkfs("init", &[], &envs);
    assert_ne!(init_without_secret.status.code(), Some(0));
    let init_without_output = format!(
        "{}{}",
        String::from_utf8_lossy(&init_without_secret.stdout),
        String::from_utf8_lossy(&init_without_secret.stderr)
    );
    assert!(
        init_without_output.contains("Schema admin password is required for init; pass --schema-admin-password."),
        "{init_without_output}"
    );

    let init_with_secret = run_mkfs("init", &["--schema-admin-password", &schema_password], &envs);
    assert!(init_with_secret.status.success(), "{}", String::from_utf8_lossy(&init_with_secret.stdout));
    assert_password_source(&String::from_utf8_lossy(&init_with_secret.stdout), "cli");
    assert!(
        String::from_utf8_lossy(&init_with_secret.stdout).contains("Initialization completed successfully."),
        "{}",
        String::from_utf8_lossy(&init_with_secret.stdout)
    );

    assert!(table_exists(&conn, &guard_table).expect("table_exists"));
    let guard_note = conn
        .query_scalar_text(&format!(
            "SELECT note FROM {} WHERE id = 1",
            DbConn::quote_identifier(&guard_table)
        ))
        .expect("select guard");
    assert_eq!(guard_note.as_deref(), Some("guard"));

    let version = conn
        .query_scalar_u64("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
        .expect("version")
        .expect("schema version");
    assert_eq!(version, SCHEMA_VERSION);

    let admin_count = conn
        .query_scalar_u64("SELECT COUNT(*) FROM schema_admin WHERE id = 1")
        .expect("admin count")
        .expect("admin count row");
    assert_eq!(admin_count, 1);

    let upgrade_wrong = run_mkfs(
        "upgrade",
        &["--schema-admin-password", "wrong-password"],
        &envs,
    );
    assert_ne!(upgrade_wrong.status.code(), Some(0));
    let upgrade_wrong_output = format!(
        "{}{}",
        String::from_utf8_lossy(&upgrade_wrong.stdout),
        String::from_utf8_lossy(&upgrade_wrong.stderr)
    );
    assert!(
        upgrade_wrong_output.contains("does not match the schema-admin secret currently stored in the DBFS database"),
        "{upgrade_wrong_output}"
    );

    conn.exec("DELETE FROM schema_version").expect("delete schema_version");

    let upgrade_result = run_mkfs("upgrade", &["--schema-admin-password", &schema_password], &envs);
    assert!(upgrade_result.status.success(), "{}", String::from_utf8_lossy(&upgrade_result.stdout));
    assert_password_source(&String::from_utf8_lossy(&upgrade_result.stdout), "cli");
    assert_upgrade_message(&String::from_utf8_lossy(&upgrade_result.stdout));

    assert!(table_exists(&conn, &guard_table).expect("table_exists"));
    let guard_note = conn
        .query_scalar_text(&format!(
            "SELECT note FROM {} WHERE id = 1",
            DbConn::quote_identifier(&guard_table)
        ))
        .expect("select guard");
    assert_eq!(guard_note.as_deref(), Some("guard"));

    let version = conn
        .query_scalar_u64("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
        .expect("version")
        .expect("schema version");
    assert_eq!(version, SCHEMA_VERSION);

    let clean_missing_secret = run_mkfs("clean", &[], &envs);
    assert_ne!(clean_missing_secret.status.code(), Some(0));
    let clean_missing_output = format!(
        "{}{}",
        String::from_utf8_lossy(&clean_missing_secret.stdout),
        String::from_utf8_lossy(&clean_missing_secret.stderr)
    );
    assert!(
        clean_missing_output.contains("Schema admin password is required for clean; pass --schema-admin-password."),
        "{clean_missing_output}"
    );

    let clean_result = run_mkfs("clean", &["--schema-admin-password", &schema_password], &envs);
    assert!(clean_result.status.success(), "{}", String::from_utf8_lossy(&clean_result.stdout));
    assert_password_source(&String::from_utf8_lossy(&clean_result.stdout), "cli");
    assert!(
        String::from_utf8_lossy(&clean_result.stdout).contains("Cleanup completed."),
        "{}",
        String::from_utf8_lossy(&clean_result.stdout)
    );

    let clean_again = run_mkfs("clean", &["--schema-admin-password", &schema_password], &envs);
    assert!(clean_again.status.success(), "{}", String::from_utf8_lossy(&clean_again.stdout));
    assert!(
        String::from_utf8_lossy(&clean_again.stdout).contains("Cleanup completed."),
        "{}",
        String::from_utf8_lossy(&clean_again.stdout)
    );

    let init_after_clean = run_mkfs("init", &["--schema-admin-password", &schema_password], &envs);
    assert!(init_after_clean.status.success(), "{}", String::from_utf8_lossy(&init_after_clean.stdout));
    assert_password_source(&String::from_utf8_lossy(&init_after_clean.stdout), "cli");
    assert!(
        String::from_utf8_lossy(&init_after_clean.stdout).contains("Initialization completed successfully."),
        "{}",
        String::from_utf8_lossy(&init_after_clean.stdout)
    );

    let upgrade_after_clean = run_mkfs("upgrade", &["--schema-admin-password", &schema_password], &envs);
    assert!(upgrade_after_clean.status.success(), "{}", String::from_utf8_lossy(&upgrade_after_clean.stdout));
    assert_password_source(&String::from_utf8_lossy(&upgrade_after_clean.stdout), "cli");
    assert_upgrade_message(&String::from_utf8_lossy(&upgrade_after_clean.stdout));

    conn.exec(&format!(
        "UPDATE {} SET version = {}",
        DbConn::quote_identifier("schema_version"),
        SCHEMA_VERSION - 1
    ))
    .expect("downgrade schema version");

    let upgrade_result = run_mkfs("upgrade", &["--schema-admin-password", &schema_password], &envs);
    assert!(upgrade_result.status.success(), "{}", String::from_utf8_lossy(&upgrade_result.stdout));
    assert_password_source(&String::from_utf8_lossy(&upgrade_result.stdout), "cli");
    assert_upgrade_message(&String::from_utf8_lossy(&upgrade_result.stdout));

    conn.exec("DELETE FROM schema_admin").expect("delete schema_admin");
    conn.exec("DELETE FROM lock_range_leases").expect("delete lock_range_leases");
    conn.exec("UPDATE schema_version SET version = 1").expect("downgrade to v1");

    let upgrade_result = run_mkfs("upgrade", &["--schema-admin-password", &schema_password], &envs);
    assert!(upgrade_result.status.success(), "{}", String::from_utf8_lossy(&upgrade_result.stdout));
    assert_password_source(&String::from_utf8_lossy(&upgrade_result.stdout), "cli");
    assert!(
        String::from_utf8_lossy(&upgrade_result.stdout).contains(&format!("Schema upgraded to version {}.", SCHEMA_VERSION)),
        "{}",
        String::from_utf8_lossy(&upgrade_result.stdout)
    );

    assert!(table_exists(&conn, "schema_admin").expect("schema_admin exists"));
    assert!(table_exists(&conn, "lock_range_leases").expect("lock_range_leases exists"));
    let version = conn
        .query_scalar_u64("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
        .expect("version")
        .expect("schema version");
    assert_eq!(version, SCHEMA_VERSION);

    conn.exec(&format!(
        "DROP TABLE IF EXISTS {}",
        DbConn::quote_identifier(&guard_table)
    ))
    .expect("drop guard table");

    println!("OK schema-upgrade/non-destructive/password-protected");
}
