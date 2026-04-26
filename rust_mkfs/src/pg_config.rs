use std::collections::HashMap;
use std::env;
use std::path::Path;

fn get_value(map: &HashMap<String, String>, key: &str, default: &str) -> String {
    map.get(key).cloned().unwrap_or_else(|| default.to_string())
}

fn resolve_path(value: &str, config_dir: &Path) -> String {
    let path = if let Some(rest) = value.strip_prefix("~/") {
        if let Some(home) = env::var_os("HOME") {
            Path::new(&home).join(rest)
        } else {
            Path::new(value).to_path_buf()
        }
    } else {
        Path::new(value).to_path_buf()
    };
    if path.is_absolute() {
        path.display().to_string()
    } else {
        config_dir.join(path).display().to_string()
    }
}

pub fn resolve_pg_connection_params(db_config: &HashMap<String, String>, config_dir: &Path) -> HashMap<String, String> {
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
        params.insert("sslrootcert".to_string(), resolve_path(&sslrootcert, config_dir));
    }
    let sslcert = env::var("DBFS_PG_SSLCERT").ok().unwrap_or_else(|| get_value(db_config, "sslcert", ""));
    if !sslcert.trim().is_empty() {
        params.insert("sslcert".to_string(), resolve_path(&sslcert, config_dir));
    }
    let sslkey = env::var("DBFS_PG_SSLKEY").ok().unwrap_or_else(|| get_value(db_config, "sslkey", ""));
    if !sslkey.trim().is_empty() {
        params.insert("sslkey".to_string(), resolve_path(&sslkey, config_dir));
    }

    params
}

#[allow(dead_code)]
pub fn make_conninfo(params: &HashMap<String, String>) -> String {
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
