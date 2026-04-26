use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "../src/config.rs"]
mod config;
#[path = "../src/pg_config.rs"]
mod pg_config;
#[path = "../src/version.rs"]
mod version;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|err| err.into_inner())
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.push(format!("dbfs-{prefix}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn write_config(dir: &Path) {
    let config_path = dir.join("dbfs_config.ini");
    let contents = r#"
[database]
host = 127.0.0.1
port = 5432
dbname = dbfsdbname
user = dbfsuser
password = cichosza
sslmode = require
sslrootcert = ca.crt
sslcert = client.crt
sslkey = client.key

[dbfs]
profile = bulk_write
write_flush_threshold_bytes = 64MiB
read_cache_blocks = 1024
read_ahead_blocks = 4
sequential_read_ahead_blocks = 8
workers_read = 4
workers_write = 4

[dbfs.profile.bulk_write]
read_cache_blocks = 512
workers_write = 8
"#;
    fs::write(config_path, contents).unwrap();
}

#[test]
fn version_matches_bootstrap_and_mkfs() {
    let _guard = env_guard();
    let bootstrap = Command::new(env!("CARGO_BIN_EXE_dbfs-bootstrap"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(bootstrap.status.success());
    let bootstrap_version = String::from_utf8(bootstrap.stdout).unwrap();
    assert!(bootstrap_version.contains(version::DBFS_VERSION_LABEL));

    let mkfs = Command::new(env!("CARGO_BIN_EXE_dbfs-rust-mkfs"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(mkfs.status.success());
    let mkfs_version = String::from_utf8(mkfs.stdout).unwrap();
    assert!(mkfs_version.contains(version::DBFS_VERSION_LABEL));
}

#[test]
fn resolve_path_and_runtime_config_and_connection_params() {
    let _guard = env_guard();
    let temp_dir = unique_temp_dir("config");
    write_config(&temp_dir);
    let config_path = temp_dir.join("dbfs_config.ini");
    let _old_config = env::var_os("DBFS_CONFIG");
    env::set_var("DBFS_CONFIG", &config_path);

    let resolve = Command::new(env!("CARGO_BIN_EXE_dbfs-config"))
        .arg("resolve-path")
        .output()
        .unwrap();
    assert!(resolve.status.success());
    assert_eq!(
        String::from_utf8(resolve.stdout).unwrap().trim(),
        config_path.display().to_string()
    );

    let connection = Command::new(env!("CARGO_BIN_EXE_dbfs-config"))
        .arg("connection-params")
        .output()
        .unwrap();
    assert!(connection.status.success());
    let params: serde_json::Value = serde_json::from_slice(&connection.stdout).unwrap();
    assert_eq!(params["host"], "127.0.0.1");
    assert_eq!(params["port"], "5432");
    assert_eq!(params["dbname"], "dbfsdbname");
    assert_eq!(params["user"], "dbfsuser");
    assert_eq!(params["password"], "cichosza");
    assert_eq!(params["sslmode"], "require");
    assert_eq!(params["sslrootcert"], temp_dir.join("ca.crt").display().to_string());
    assert_eq!(params["sslcert"], temp_dir.join("client.crt").display().to_string());
    assert_eq!(params["sslkey"], temp_dir.join("client.key").display().to_string());

    let runtime = Command::new(env!("CARGO_BIN_EXE_dbfs-config"))
        .env("DBFS_PROFILE", "bulk_write")
        .arg("runtime-config")
        .output()
        .unwrap();
    assert!(runtime.status.success());
    let runtime: serde_json::Value = serde_json::from_slice(&runtime.stdout).unwrap();
    assert_eq!(runtime["profile"], "bulk_write");
    assert_eq!(runtime["read_cache_blocks"], "512");
    assert_eq!(runtime["workers_write"], "8");
    assert_eq!(runtime["write_flush_threshold_bytes"], "64MiB");

    match _old_config {
        Some(value) => env::set_var("DBFS_CONFIG", value),
        None => env::remove_var("DBFS_CONFIG"),
    }
}
