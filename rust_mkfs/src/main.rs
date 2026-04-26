mod config;
mod version;
mod runtime;
mod pg;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use clap::{Parser, ValueEnum};
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use config::{load_config_parser, resolve_config_path};
use pg::DbConn;

use version::DBFS_VERSION_LABEL;
const SCHEMA_VERSION: u64 = 5;
const MIGRATION_FILES: [&str; 5] = [
    "0001_base.sql",
    "0002_schema_admin.sql",
    "0003_schema_version_sql.sql",
    "0004_copy_block_crc.sql",
    "0005_data_objects.sql",
];

const MIGRATION_DESCRIPTIONS: [&str; 5] = [
    "Base schema and initial DBFS tables",
    "Schema admin secret table",
    "Schema version tracking table",
    "Copy block CRC cache table",
    "Data objects for copy-on-write and dedupe",
];

#[derive(Copy, Clone, Eq, PartialEq, ValueEnum)]
enum Action {
    Init,
    Upgrade,
    Clean,
    Status,
}

#[derive(Parser)]
#[command(name = "dbfs-mkfs", version = DBFS_VERSION_LABEL, about = "Manage the DBFS schema.")]
struct Cli {
    #[arg(value_enum)]
    action: Action,
    #[arg(long, default_value_t = 4096)]
    block_size: u64,
    #[arg(long)]
    schema_admin_password: Option<String>,
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "1",
        default_value = "false",
        value_parser = parse_truthy_arg
    )]
    generate_client_tls_pair: bool,
    #[arg(long, default_value = ".dbfs/tls")]
    tls_material_dir: String,
    #[arg(long, default_value = "dbfs")]
    tls_common_name: String,
    #[arg(long, default_value_t = 365)]
    tls_cert_days: i64,
}

fn parse_truthy_arg(value: &str) -> Result<bool, String> {
    let normalized = value.trim().to_lowercase();
    match normalized.as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err("expected 0/1, true/false, yes/no, or on/off".to_string()),
    }
}

fn format_schema_admin_source_message(source: &str) -> String {
    format!("Schema admin password source: {} (no prompt needed)", source)
}

fn schema_admin_secret_required_message(action_name: &str) -> String {
    format!("Schema admin password is required for {}; pass --schema-admin-password.", action_name)
}

fn current_uid_gid() -> (u32, u32) {
    #[cfg(unix)]
    {
        unsafe { (libc::getuid() as u32, libc::getgid() as u32) }
    }
    #[cfg(not(unix))]
    {
        (0, 0)
    }
}

fn get_value(map: &HashMap<String, String>, key: &str, default: &str) -> String {
    map.get(key).cloned().unwrap_or_else(|| default.to_string())
}

fn resolve_path(value: &str, config_dir: &Path) -> PathBuf {
    let path = expand_user(Path::new(value));
    if path.is_absolute() {
        path
    } else {
        config_dir.join(path)
    }
}

fn expand_user(path: &Path) -> PathBuf {
    let raw = path.to_string_lossy();
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(raw.as_ref())
}

fn resolve_pg_connection_params(db_config: &HashMap<String, String>, config_dir: &Path) -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert("host".to_string(), get_value(db_config, "host", "127.0.0.1"));
    params.insert("port".to_string(), get_value(db_config, "port", "5432"));
    params.insert("dbname".to_string(), get_value(db_config, "dbname", "dbfsdbname"));
    params.insert("user".to_string(), get_value(db_config, "user", "dbfsuser"));
    params.insert("password".to_string(), get_value(db_config, "password", ""));

    let sslmode = env::var("DBFS_PG_SSLMODE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| get_value(db_config, "sslmode", "disable"));
    if !sslmode.is_empty() && sslmode != "disable" {
        params.insert("sslmode".to_string(), sslmode);
    }

    let sslrootcert = env::var("DBFS_PG_SSLROOTCERT").ok().unwrap_or_else(|| get_value(db_config, "sslrootcert", ""));
    if !sslrootcert.trim().is_empty() {
        params.insert("sslrootcert".to_string(), resolve_path(&sslrootcert, config_dir).display().to_string());
    }
    let sslcert = env::var("DBFS_PG_SSLCERT").ok().unwrap_or_else(|| get_value(db_config, "sslcert", ""));
    if !sslcert.trim().is_empty() {
        params.insert("sslcert".to_string(), resolve_path(&sslcert, config_dir).display().to_string());
    }
    let sslkey = env::var("DBFS_PG_SSLKEY").ok().unwrap_or_else(|| get_value(db_config, "sslkey", ""));
    if !sslkey.trim().is_empty() {
        params.insert("sslkey".to_string(), resolve_path(&sslkey, config_dir).display().to_string());
    }

    params
}

fn make_conninfo(params: &HashMap<String, String>) -> String {
    let mut parts = Vec::new();
    for key in ["host", "port", "dbname", "user", "password", "sslmode", "sslrootcert", "sslcert", "sslkey"] {
        if let Some(value) = params.get(key) {
            if value.is_empty() {
                continue;
            }
            let escaped = value.replace('\'', "''");
            parts.push(format!("{}='{}'", key, escaped));
        }
    }
    parts.join(" ")
}

fn load_schema_admin_password(cli: &Cli) -> (Option<String>, Option<String>) {
    if let Some(password) = &cli.schema_admin_password {
        (Some(password.clone()), Some("cli".to_string()))
    } else {
        (None, None)
    }
}

fn generate_client_tls_pair(material_dir: &Path, common_name: &str, days: i64) -> Result<(PathBuf, PathBuf), String> {
    let material_dir = expand_user(material_dir);
    fs::create_dir_all(&material_dir)
        .map_err(|e| format!("Unable to create TLS material directory {}: {}", material_dir.display(), e))?;
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(&material_dir, fs::Permissions::from_mode(0o700));
    }

    let cert_path = material_dir.join("client.crt");
    let key_path = material_dir.join("client.key");
    if cert_path.exists() && key_path.exists() {
        return Ok((cert_path, key_path));
    }

    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-days",
            &days.max(1).to_string(),
            "-subj",
            &format!("/CN={}", common_name),
            "-keyout",
            key_path.to_string_lossy().as_ref(),
            "-out",
            cert_path.to_string_lossy().as_ref(),
        ])
        .status()
        .map_err(|_| "openssl is required to generate a PostgreSQL TLS client pair".to_string())?;

    if !status.success() {
        return Err("Failed to generate PostgreSQL TLS client pair".to_string());
    }
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600));
        let _ = fs::set_permissions(&cert_path, fs::Permissions::from_mode(0o644));
    }
    Ok((cert_path, key_path))
}

fn derive_schema_admin_secret(password: &str, salt: Option<&[u8]>, iterations: u32) -> (String, String, u32) {
    let mut salt_bytes = [0u8; 16];
    if let Some(source) = salt {
        let copy_len = source.len().min(salt_bytes.len());
        salt_bytes[..copy_len].copy_from_slice(&source[..copy_len]);
    } else {
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut salt_bytes);
    }
    let mut output = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt_bytes, iterations, &mut output);
    (
        BASE64_STANDARD.encode(salt_bytes),
        BASE64_STANDARD.encode(output),
        iterations,
    )
}

fn verify_schema_admin_secret(password: &str, salt_b64: &str, hash_b64: &str, iterations: u32) -> bool {
    let salt = BASE64_STANDARD.decode(salt_b64.as_bytes()).unwrap_or_default();
    let (_, derived_hash, _) = derive_schema_admin_secret(password, Some(&salt), iterations);
    derived_hash == hash_b64
}

fn migration_sql(version: u64) -> &'static str {
    match version {
        1 => include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../migrations/0001_base.sql")),
        2 => include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../migrations/0002_schema_admin.sql")),
        3 => include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../migrations/0003_schema_version_sql.sql")),
        4 => include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../migrations/0004_copy_block_crc.sql")),
        5 => include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../migrations/0005_data_objects.sql")),
        _ => "",
    }
}

fn migration_description(version: u64) -> &'static str {
    match version {
        1 => MIGRATION_DESCRIPTIONS[0],
        2 => MIGRATION_DESCRIPTIONS[1],
        3 => MIGRATION_DESCRIPTIONS[2],
        4 => MIGRATION_DESCRIPTIONS[3],
        5 => MIGRATION_DESCRIPTIONS[4],
        _ => "Migration",
    }
}

fn migration_filename(version: u64) -> &'static str {
    match version {
        1 => MIGRATION_FILES[0],
        2 => MIGRATION_FILES[1],
        3 => MIGRATION_FILES[2],
        4 => MIGRATION_FILES[3],
        5 => MIGRATION_FILES[4],
        _ => "unknown.sql",
    }
}

fn latest_migration_version() -> u64 {
    SCHEMA_VERSION
}

fn migration_exists(version: u64) -> bool {
    (1..=latest_migration_version()).contains(&version)
}

fn migration_manifest() -> Vec<(u64, &'static str, &'static str)> {
    (1..=latest_migration_version())
        .map(|version| (version, migration_filename(version), migration_description(version)))
        .collect()
}

fn apply_migration(conn: &DbConn, version: u64) -> Result<(), String> {
    let sql = migration_sql(version);
    if sql.is_empty() {
        return Err(format!("Missing migration file for version {}", version));
    }
    conn.exec(sql)
}

fn apply_migrations_up_to(conn: &DbConn, target_version: u64) -> Result<(), String> {
    for version in 1..=target_version {
        if !migration_exists(version) {
            return Err(format!("Missing migration file for version {}", version));
        }
        apply_migration(conn, version)?;
    }
    Ok(())
}

fn read_schema_version(conn: &DbConn) -> Result<Option<u64>, String> {
    if !conn.query_exists("SELECT to_regclass('schema_version') IS NOT NULL")? {
        return Ok(None);
    }
    conn.query_scalar_u64("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
}

fn public_schema_exists(conn: &DbConn) -> Result<bool, String> {
    conn.query_exists("SELECT to_regnamespace('public') IS NOT NULL")
}

fn schema_admin_secret_exists(conn: &DbConn) -> Result<bool, String> {
    if !conn.query_exists("SELECT to_regclass('schema_admin') IS NOT NULL")? {
        return Ok(false);
    }
    conn.query_exists("SELECT EXISTS (SELECT 1 FROM schema_admin WHERE id = 1)")
}

fn write_schema_version(conn: &DbConn, version: u64) -> Result<(), String> {
    conn.exec("DELETE FROM schema_version")?;
    conn.exec(&format!(
        "INSERT INTO schema_version (version, applied_at) VALUES ({}, NOW())",
        version
    ))
}

fn ensure_schema_admin_secret(conn: &DbConn, password: Option<&str>) -> Result<bool, String> {
    conn.exec(
        "CREATE TABLE IF NOT EXISTS schema_admin (\
            id INTEGER PRIMARY KEY CHECK (id = 1),\
            password_hash TEXT NOT NULL,\
            password_salt TEXT NOT NULL,\
            password_iterations INTEGER NOT NULL,\
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),\
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()\
        )",
    )?;

    if let Some(row) = conn.query_scalar_text(
        "SELECT password_hash || E'\\n' || password_salt || E'\\n' || password_iterations::text FROM schema_admin WHERE id = 1",
    )? {
        let mut parts = row.splitn(3, '\n');
        let hash = parts.next().unwrap_or_default();
        let salt = parts.next().unwrap_or_default();
        let iterations = parts.next().unwrap_or("0").parse::<u32>().unwrap_or(0);
        let Some(password) = password else {
            return Err("Schema admin password is required for this existing database; pass --schema-admin-password.".to_string());
        };
        if !verify_schema_admin_secret(password, salt, hash, iterations) {
            return Err("Schema admin password does not match the schema-admin secret currently stored in the DBFS database. This usually means you are using a secret from a different bootstrap; rerun init to generate a new secret or provide the current one.".to_string());
        }
        return Ok(false);
    }

    let Some(password) = password else {
        return Err("Schema admin password is required for the first DBFS bootstrap; pass --schema-admin-password.".to_string());
    };
    let (salt_b64, hash_b64, iterations) = derive_schema_admin_secret(password, None, 200_000);
    let salt_sql = DbConn::quote_literal(&salt_b64);
    let hash_sql = DbConn::quote_literal(&hash_b64);
    conn.exec(&format!(
        "INSERT INTO schema_admin (id, password_hash, password_salt, password_iterations, created_at, updated_at) \
         VALUES (1, {}, {}, {}, NOW(), NOW()) \
         ON CONFLICT (id) DO UPDATE SET \
            password_hash = EXCLUDED.password_hash, \
            password_salt = EXCLUDED.password_salt, \
            password_iterations = EXCLUDED.password_iterations, \
            updated_at = NOW()",
        hash_sql, salt_sql, iterations
    ))?;
    Ok(true)
}

fn run_sql_commands(conn: &DbConn, sql_commands: &str) -> Result<(), String> {
    conn.exec(sql_commands)
}

fn run_sql_commands_user(conn: &DbConn, sql_commands: &str, db_user: &str) -> Result<(), String> {
    let sql = sql_commands.replace("{}", &DbConn::quote_identifier(db_user));
    conn.exec(&sql)
}

fn main() {
    let cli = Cli::parse();
    if cli.block_size % 1024 != 0 {
        eprintln!("block_size must be a multiple of 1024");
        std::process::exit(1);
    }

    let config_path = match resolve_config_path(None) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };
    let (config, config_dir) = match load_config_parser(Some(&config_path)) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };
    let db_section = match config.section("database") {
        Some(section) => section.clone(),
        None => {
            eprintln!("Missing [database] section in DBFS configuration");
            std::process::exit(1);
        }
    };
    let db_config = resolve_pg_connection_params(&db_section, &config_dir);
    let conninfo = make_conninfo(&db_config);
    let conn = match DbConn::connect(&conninfo) {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };

    let (schema_admin_password, schema_admin_source) = load_schema_admin_password(&cli);
    let uid_gid = current_uid_gid();

    match cli.action {
        Action::Init => {
            if schema_admin_password.is_none() {
                eprintln!("{}", schema_admin_secret_required_message("init"));
                std::process::exit(1);
            }
            if cli.generate_client_tls_pair {
                let tls_dir = expand_user(Path::new(&cli.tls_material_dir));
                if let Err(err) = generate_client_tls_pair(&tls_dir, &cli.tls_common_name, cli.tls_cert_days) {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
            println!("{}", format_schema_admin_source_message(schema_admin_source.as_deref().unwrap_or("cli")));
            if let Err(err) = apply_migrations_up_to(&conn, latest_migration_version()) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            if let Err(err) = ensure_schema_admin_secret(&conn, schema_admin_password.as_deref()) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            if let Err(err) = conn.exec("CREATE TABLE IF NOT EXISTS config (key VARCHAR(50) PRIMARY KEY, value BIGINT)") {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            if let Err(err) = conn.exec(&format!(
                "INSERT INTO config (key, value) VALUES ('max_fs_size_bytes', {}) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                10_u64 * 1024 * 1024 * 1024
            )) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            if let Err(err) = conn.exec(&format!(
                "INSERT INTO config (key, value) VALUES ('block_size', {}) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                cli.block_size
            )) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            if let Err(err) = write_schema_version(&conn, SCHEMA_VERSION) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            let (uid, gid) = uid_gid;
            if let Err(err) = conn.exec(&format!(
                "UPDATE directories SET uid = {}, gid = {} WHERE name IN ('/', '.Trash-1000') AND id_parent IS NULL",
                uid, gid
            )) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            let db_user = get_value(&db_section, "user", "dbfsuser");
            if let Err(err) = run_sql_commands_user(&conn, "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {}", &db_user) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            println!("Initialization completed successfully.");
        }
        Action::Upgrade => {
            if schema_admin_password.is_none() {
                eprintln!("{}", schema_admin_secret_required_message("upgrade"));
                std::process::exit(1);
            }
            println!("{}", format_schema_admin_source_message(schema_admin_source.as_deref().unwrap_or("cli")));
            let current_version = match read_schema_version(&conn) {
                Ok(version) => version,
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            };
            if let Some(version) = current_version {
                if version > SCHEMA_VERSION {
                    eprintln!("Unsupported schema version {}; expected {}.", version, SCHEMA_VERSION);
                    std::process::exit(1);
                }
            }
            let start_version = current_version.unwrap_or(0);
            for version in (start_version + 1)..=SCHEMA_VERSION {
                if !migration_exists(version) {
                    eprintln!("Missing migration file for version {}", version);
                    std::process::exit(1);
                }
                if let Err(err) = apply_migration(&conn, version) {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
            if let Err(err) = ensure_schema_admin_secret(&conn, schema_admin_password.as_deref()) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            if let Err(err) = write_schema_version(&conn, SCHEMA_VERSION) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            if current_version == Some(SCHEMA_VERSION) {
                println!("Schema already at version {}.", SCHEMA_VERSION);
            } else {
                println!("Schema upgraded to version {}.", SCHEMA_VERSION);
            }
        }
        Action::Clean => {
            let exists = match public_schema_exists(&conn) {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            };
            if !exists {
                println!("Cleanup completed.");
                return;
            }
            if schema_admin_password.is_none() {
                eprintln!("{}", schema_admin_secret_required_message("clean"));
                std::process::exit(1);
            }
            println!("{}", format_schema_admin_source_message(schema_admin_source.as_deref().unwrap_or("cli")));
            if let Err(err) = ensure_schema_admin_secret(&conn, schema_admin_password.as_deref()) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            let db_user = get_value(&db_section, "user", "dbfsuser");
            if let Err(err) = run_sql_commands_user(&conn, "REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM {}", &db_user) {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            if let Err(err) = run_sql_commands(&conn, "DROP SCHEMA IF EXISTS public CASCADE;") {
                eprintln!("{}", err);
                std::process::exit(1);
            }
            println!("Cleanup completed.");
        }
        Action::Status => {
            let current_version = match read_schema_version(&conn) {
                Ok(version) => version,
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            };
            let latest_version = latest_migration_version();
            let pending_versions: Vec<u64> = ((current_version.unwrap_or(0) + 1)..=latest_version).collect();
            let manifest = migration_manifest();
            let secret_present = match schema_admin_secret_exists(&conn) {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            };
            let ready = current_version == Some(latest_version) && secret_present;
            println!("Schema version: {}", current_version.map(|v| v.to_string()).unwrap_or_else(|| "none".to_string()));
            println!("Latest migration version: {}", latest_version);
            println!("Schema admin secret: {}", if secret_present { "present" } else { "missing" });
            println!("DBFS ready: {}", if ready { "yes" } else { "no" });
            if pending_versions.is_empty() {
                println!("Pending migrations: none");
            } else {
                let joined = pending_versions
                    .iter()
                    .map(|version| format!("{:04}", version))
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("Pending migrations: {}", joined);
            }
            println!("Migration path:");
            for (version, filename, description) in manifest {
                println!("  - {:04}: {} :: {}", version, filename, description);
            }
        }
    }
}
