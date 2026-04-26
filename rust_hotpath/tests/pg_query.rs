use dbfs_rust_hotpath::pg::DbRepo;
use std::env;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static ENV_LOCK: Mutex<()> = Mutex::new(());

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

#[test]
fn live_pg_query_helpers_work() {
    let _guard = ENV_LOCK.lock().unwrap();
    let repo = DbRepo::new(&conninfo_from_env()).expect("repo");

    let scalar = repo.query_scalar_text("SELECT 1").expect("query_scalar_text");
    assert_eq!(scalar.trim(), "1");

    let snapshot = repo.startup_snapshot().expect("startup_snapshot");
    assert!(snapshot.block_size.unwrap_or(0) > 0);
    assert!(!snapshot.is_in_recovery);
    assert!(snapshot.schema_version.unwrap_or(0) > 0);
    assert!(snapshot.schema_is_initialized);

    assert!(repo.is_in_recovery().is_ok());
    assert!(repo.schema_is_initialized().unwrap());
    assert!(repo.schema_version().unwrap().is_some());
    assert!(repo.query_config_value("block_size").unwrap().is_some());

    let root = repo.resolve_path("/").expect("resolve root");
    assert_eq!(root.kind.as_deref(), Some("dir"));
    assert_eq!(root.parent_id, None);
    assert!(root.entry_id.is_some());

    let dirname = unique_name("rust_pg_dir");
    let dir_id = repo
        .create_directory(None, &dirname, 0o755, 1000, 1000, &unique_name("dir_seed"))
        .expect("create directory");
    assert_eq!(repo.get_dir_id(&format!("/{dirname}")).unwrap(), Some(dir_id));
    assert_eq!(repo.resolve_path(&format!("/{dirname}")).unwrap().kind.as_deref(), Some("dir"));

    let symlink_name = unique_name("rust_pg_link");
    let symlink_id = repo
        .create_symlink(Some(dir_id), &symlink_name, "/target", 1000, 1000, &unique_name("symlink_seed"))
        .expect("create symlink");
    assert_eq!(repo.get_symlink_id(&format!("/{dirname}/{symlink_name}")).unwrap(), Some(symlink_id));
    assert_eq!(repo.load_symlink_target(symlink_id).unwrap().as_deref(), Some("/target"));

    let file_name = unique_name("rust_pg_file");
    let file_id = repo
        .create_file(Some(dir_id), &file_name, 0o644, 1000, 1000, &unique_name("file_seed"))
        .expect("create file");
    assert_eq!(repo.get_file_id(&format!("/{dirname}/{file_name}")).unwrap(), Some(file_id));

    let hardlink_name = unique_name("rust_pg_hardlink");
    let hardlink_id = repo
        .create_hardlink(file_id, Some(dir_id), &hardlink_name, 1000, 1000)
        .expect("create hardlink");
    assert_eq!(
        repo.get_hardlink_id(&format!("/{dirname}/{hardlink_name}")).unwrap(),
        Some(hardlink_id)
    );
    assert_eq!(repo.get_hardlink_file_id(hardlink_id).unwrap(), Some(file_id));
    assert!(repo.count_file_links(file_id).unwrap() >= 2);

    assert!(repo.count_files().unwrap() > 0);
    assert!(repo.count_directories().unwrap() > 0);
    let _ = repo.total_data_size().unwrap();
}
