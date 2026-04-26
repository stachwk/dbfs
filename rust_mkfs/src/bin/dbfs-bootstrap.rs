#[path = "../config.rs"]
mod config;
#[path = "../pg_config.rs"]
mod pg_config;
#[path = "../version.rs"]
mod version;

use clap::Parser;
use config::{load_config_parser, resolve_config_path};
use pg_config::{make_conninfo, resolve_pg_connection_params};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Parser)]
#[command(name = "dbfs-bootstrap", version = version::DBFS_VERSION_LABEL, about = "Mount DBFS through the Rust FUSE frontend.")]
struct Cli {
    #[arg(short = 'f', long = "mountpoint")]
    mountpoint: String,
    #[arg(long, default_value = "auto")]
    role: String,
    #[arg(long)]
    profile: Option<String>,
    #[arg(long, default_value = "off")]
    selinux: String,
    #[arg(long, default_value = "off")]
    acl: String,
    #[arg(long, default_value = "default")]
    atime_policy: String,
    #[arg(long, default_value_t = true)]
    default_permissions: bool,
    #[arg(long, default_value_t = false)]
    lazytime: bool,
    #[arg(long, default_value_t = false)]
    sync: bool,
    #[arg(long, default_value_t = false)]
    dirsync: bool,
    #[arg(long, default_value_t = false)]
    debug: bool,
    #[arg(long)]
    log_level: Option<String>,
}

fn configure_env(cli: &Cli) {
    let log_level = cli
        .log_level
        .clone()
        .or_else(|| if cli.debug { Some("DEBUG".to_string()) } else { None })
        .unwrap_or_else(|| "INFO".to_string());
    env::set_var("DBFS_LOG_LEVEL", log_level);
    env::set_var("DBFS_USE_FUSE_CONTEXT", "1");
    if let Some(profile) = &cli.profile {
        env::set_var("DBFS_PROFILE", profile);
    }
    env::set_var("DBFS_ROLE", &cli.role);
    env::set_var("DBFS_SELINUX", &cli.selinux);
    env::set_var("DBFS_ACL", &cli.acl);
    env::set_var("DBFS_ATIME_POLICY", &cli.atime_policy);
    env::set_var("DBFS_DEFAULT_PERMISSIONS", if cli.default_permissions { "1" } else { "0" });
    env::set_var("DBFS_LAZYTIME", if cli.lazytime { "1" } else { "0" });
    env::set_var("DBFS_SYNC", if cli.sync { "1" } else { "0" });
    env::set_var("DBFS_DIRSYNC", if cli.dirsync { "1" } else { "0" });
    env::set_var("DBFS_USE_RUST_FUSE", "1");
}

fn validate_mountpoint(mountpoint: &Path) -> Result<(), String> {
    if !mountpoint.exists() {
        return Err(format!("Mountpoint {} does not exist. Create an empty directory first.", mountpoint.display()));
    }
    if !mountpoint.is_dir() {
        return Err(format!("Mountpoint {} is not a directory.", mountpoint.display()));
    }
    let mut entries = Vec::new();
    for entry in fs::read_dir(mountpoint).map_err(|e| format!("Cannot inspect mountpoint {}: {}", mountpoint.display(), e))? {
        let entry = entry.map_err(|e| format!("Cannot inspect mountpoint {}: {}", mountpoint.display(), e))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name != "." && name != ".." {
            entries.push(name);
        }
    }
    if !entries.is_empty() {
        let preview = entries.iter().take(5).cloned().collect::<Vec<_>>().join(", ");
        let suffix = if entries.len() <= 5 {
            String::new()
        } else {
            format!(" (+{} more)", entries.len() - 5)
        };
        return Err(format!(
            "Mountpoint {} is not empty ({} entries: {}{}). Please use an empty directory.",
            mountpoint.display(),
            entries.len(),
            preview,
            suffix
        ));
    }
    Ok(())
}

fn rust_fuse_binary() -> Option<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap_or_else(|| Path::new("."));
    let candidates = [
        root.join("rust_fuse/target/debug/dbfs-rust-fuse"),
        root.join("rust_fuse/target/release/dbfs-rust-fuse"),
        PathBuf::from("/usr/local/bin/dbfs-rust-fuse"),
    ];
    candidates.into_iter().find(|candidate| candidate.is_file())
}

fn main() {
    let cli = Cli::parse();
    let rust_fuse = match rust_fuse_binary() {
        Some(path) => path,
        None => {
            eprintln!("Rust FUSE binary is unavailable; build rust_fuse/target/debug/dbfs-rust-fuse first.");
            std::process::exit(1);
        }
    };
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
    let params = resolve_pg_connection_params(&db_section, &config_dir);
    let conninfo = make_conninfo(&params);
    configure_env(&cli);
    env::set_var("DBFS_DSN_CONNINFO", conninfo);
    let mountpoint = PathBuf::from(&cli.mountpoint);
    if let Err(err) = validate_mountpoint(&mountpoint) {
        eprintln!("{}", err);
        std::process::exit(1);
    }
    let readonly_env = env::var("DBFS_RUST_FUSE_READONLY").unwrap_or_default();
    let readonly = cli.role == "replica" || !matches!(readonly_env.trim().to_lowercase().as_str(), "" | "0" | "false" | "no" | "off");
    let mut command = Command::new(&rust_fuse);
    command.arg("-f").arg(&cli.mountpoint);
    if readonly {
        command.arg("--readonly");
    }
    let status = command.status();
    match status {
        Ok(status) if status.success() => std::process::exit(0),
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(err) => {
            eprintln!("Failed to launch Rust FUSE frontend: {}", err);
            std::process::exit(1);
        }
    }
}
