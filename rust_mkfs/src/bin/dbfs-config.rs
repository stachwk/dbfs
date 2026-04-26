#[path = "../config.rs"]
mod config;
#[path = "../pg_config.rs"]
mod pg_config;
#[path = "../version.rs"]
mod version;

use clap::{Parser, Subcommand};
use config::{load_config_parser, resolve_config_path};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Parser)]
#[command(name = "dbfs-config", about = "Resolve DBFS config and TLS helpers.")]
struct Cli {
    #[arg(long)]
    config_path: Option<PathBuf>,
    #[arg(long)]
    base_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Subcommand)]
enum CommandKind {
    ResolvePath,
    ConnectionParams,
    RuntimeConfig,
    Version,
    GenerateTls {
        #[arg(long, default_value = ".dbfs/tls")]
        material_dir: PathBuf,
        #[arg(long, default_value = "dbfs")]
        common_name: String,
        #[arg(long, default_value_t = 365)]
        days: i64,
    },
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

fn main() {
    let cli = Cli::parse();
    let config_path = match resolve_config_path(cli.config_path.as_deref()) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };
    let (config, config_path) = match load_config_parser(Some(&config_path)) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };
    match cli.command {
        CommandKind::ResolvePath => {
            println!("{}", config_path.display());
        }
        CommandKind::ConnectionParams => {
            let db_section = match config.section("database") {
                Some(section) => section.clone(),
                None => {
                    eprintln!("Missing [database] section in DBFS configuration");
                    std::process::exit(1);
                }
            };
            let params = pg_config::resolve_pg_connection_params(&db_section, &config_path.parent().unwrap_or(Path::new(".")));
            let mut map = serde_json::Map::new();
            for (key, value) in params {
                map.insert(key, serde_json::Value::String(value));
            }
            println!("{}", serde_json::Value::Object(map));
        }
        CommandKind::RuntimeConfig => {
            let mut runtime = if let Some(section) = config.section("dbfs") {
                section.clone()
            } else {
                HashMap::new()
            };
            let profile_name = env::var("DBFS_PROFILE").ok().or_else(|| runtime.get("profile").cloned());
            if let Some(profile_name) = profile_name {
                for section_name in [format!("dbfs.profile.{}", profile_name), format!("dbfs.profile:{}", profile_name)] {
                    if let Some(section) = config.section(&section_name) {
                        runtime.extend(section.clone());
                        runtime.insert("profile".to_string(), profile_name.clone());
                        break;
                    }
                }
            }
            let mut map = serde_json::Map::new();
            for (key, value) in runtime {
                map.insert(key, serde_json::Value::String(value));
            }
            println!("{}", serde_json::Value::Object(map));
        }
        CommandKind::Version => {
            println!("{}", version::DBFS_VERSION_LABEL);
        }
        CommandKind::GenerateTls {
            material_dir,
            common_name,
            days,
        } => {
            match generate_client_tls_pair(&material_dir, &common_name, days) {
                Ok((cert_path, key_path)) => {
                    println!(
                        "{}",
                        json!({"cert_path": cert_path.display().to_string(), "key_path": key_path.display().to_string()})
                    );
                }
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
        }
    }
}
