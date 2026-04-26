mod fs;

use clap::Parser;
use fs::DbfsFuse;
use fuser::{mount2, MountOption};
use rust_hotpath::pg::DbRepo;
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short = 'f', long = "mountpoint")]
    mountpoint: PathBuf,
    #[arg(long = "readonly", default_value_t = false)]
    readonly: bool,
}

fn env_flag(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => !matches!(value.trim().to_ascii_lowercase().as_str(), "" | "0" | "false" | "no" | "off"),
        Err(_) => default,
    }
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name).ok().and_then(|value| {
        let value = value.trim().to_string();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    })
}

fn env_u64(name: &str) -> u64 {
    env_value(name)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

fn mount_options(readonly: bool) -> Vec<MountOption> {
    let mut options = vec![
        MountOption::FSName("dbfs".to_string()),
        MountOption::AutoUnmount,
    ];
    if env_flag("DBFS_DEFAULT_PERMISSIONS", true) {
        options.push(MountOption::DefaultPermissions);
    }
    if env_flag("DBFS_ALLOW_OTHER", false) {
        options.push(MountOption::AllowOther);
    }
    if env_flag("DBFS_LAZYTIME", false) {
        options.push(MountOption::CUSTOM("lazytime".to_string()));
    }
    if env_flag("DBFS_SYNC", false) {
        options.push(MountOption::Sync);
    }
    if env_flag("DBFS_DIRSYNC", false) {
        options.push(MountOption::DirSync);
    }
    if readonly {
        options.push(MountOption::RO);
    }
    if let Some(value) = env_value("DBFS_ENTRY_TIMEOUT_SECONDS") {
        options.push(MountOption::CUSTOM(format!("entry_timeout={value}")));
    }
    if let Some(value) = env_value("DBFS_ATTR_TIMEOUT_SECONDS") {
        options.push(MountOption::CUSTOM(format!("attr_timeout={value}")));
    }
    if let Some(value) = env_value("DBFS_NEGATIVE_TIMEOUT_SECONDS") {
        options.push(MountOption::CUSTOM(format!("negative_timeout={value}")));
    }
    options
}

fn main() {
    let args = Args::parse();
    let conninfo = std::env::var("DBFS_DSN_CONNINFO")
        .expect("DBFS_DSN_CONNINFO must be set when launching dbfs-rust-fuse");
    let repo = DbRepo::new(&conninfo).unwrap_or_else(|err| {
        eprintln!("dbfs-rust-fuse: failed to open PostgreSQL repo: {err}");
        std::process::exit(1);
    });
    let snapshot = repo.startup_snapshot().unwrap_or_else(|err| {
        eprintln!("dbfs-rust-fuse: failed to read startup snapshot: {err}");
        std::process::exit(1);
    });
    let block_size = snapshot.block_size.unwrap_or(4096) as u64;
    let write_flush_threshold_bytes = env_u64("DBFS_WRITE_FLUSH_THRESHOLD_BYTES");
    let read_only = args.readonly || snapshot.is_in_recovery;
    let selinux_enabled = env_flag("DBFS_SELINUX", false);
    let acl_enabled = env_flag("DBFS_ACL", false);
    let fs = DbfsFuse::new(
        repo,
        block_size,
        write_flush_threshold_bytes,
        read_only,
        selinux_enabled,
        acl_enabled,
    );
    let options = mount_options(read_only);
    if let Err(err) = mount2(fs, &args.mountpoint, &options) {
        eprintln!("dbfs-rust-fuse: mount failed: {err}");
        std::process::exit(1);
    }
}
