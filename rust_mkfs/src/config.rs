use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const SYSTEM_CONFIG_PATH: &str = "/etc/dbfs/dbfs_config.ini";
const USER_CONFIG_PATH: &str = ".config/dbfs/dbfs_config.ini";
const LOCAL_CONFIG_NAME: &str = "dbfs_config.ini";
const ENV_CONFIG_VAR: &str = "DBFS_CONFIG";

#[derive(Debug, Clone)]
pub struct IniConfig {
    pub sections: HashMap<String, HashMap<String, String>>,
}

impl IniConfig {
    pub fn section(&self, name: &str) -> Option<&HashMap<String, String>> {
        self.sections.get(&name.to_lowercase())
    }
}

fn strip_inline_comment(value: &str) -> &str {
    let mut end = value.len();
    for marker in ['#', ';'] {
        if let Some(idx) = value.find(marker) {
            end = end.min(idx);
        }
    }
    &value[..end]
}

pub fn resolve_config_path(file_path: Option<&Path>) -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Some(env_path) = env::var_os(ENV_CONFIG_VAR) {
        candidates.push(PathBuf::from(env_path).expanduser());
    }

    let mut base_dir: Option<PathBuf> = None;
    if let Some(path) = file_path {
        let expanded = path.expanduser();
        if expanded.is_file() {
            return Ok(expanded);
        }
        if expanded.is_dir() {
            base_dir = Some(expanded);
        } else if expanded.parent() != Some(Path::new(".")) {
            base_dir = expanded.parent().map(|p| p.to_path_buf());
        }
    }

    candidates.push(PathBuf::from(SYSTEM_CONFIG_PATH));
    if let Some(home) = env::var_os("HOME") {
        candidates.push(PathBuf::from(home).join(USER_CONFIG_PATH));
    }

    let search_root = base_dir
        .or_else(|| env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    candidates.push(search_root.join(LOCAL_CONFIG_NAME));

    if let Some(path) = file_path {
        let file_name = path.file_name().map(|name| name.to_owned());
        if let Some(file_name) = file_name {
            if file_name.to_string_lossy() != LOCAL_CONFIG_NAME {
                candidates.push(search_root.join(file_name));
            }
        }
    }

    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(format!(
        "DBFS configuration file not found. Expected {} or {}/{}.",
        SYSTEM_CONFIG_PATH,
        search_root.display(),
        LOCAL_CONFIG_NAME
    ))
}

trait ExpandUser {
    fn expanduser(self) -> PathBuf;
}

impl ExpandUser for PathBuf {
    fn expanduser(self) -> PathBuf {
        let s = self.to_string_lossy().to_string();
        if let Some(rest) = s.strip_prefix("~/") {
            if let Some(home) = env::var_os("HOME") {
                return PathBuf::from(home).join(rest);
            }
        }
        PathBuf::from(s)
    }
}

impl ExpandUser for &Path {
    fn expanduser(self) -> PathBuf {
        PathBuf::from(self.to_string_lossy().to_string()).expanduser()
    }
}

pub fn load_config_parser(file_path: Option<&Path>) -> Result<(IniConfig, PathBuf), String> {
    let config_path = resolve_config_path(file_path)?;
    let contents = fs::read_to_string(&config_path)
        .map_err(|e| format!("Unable to read DBFS configuration: {}: {}", config_path.display(), e))?;

    let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut current_section = String::new();

    for raw_line in contents.lines() {
        let line = strip_inline_comment(raw_line).trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            current_section = line[1..line.len() - 1].trim().to_lowercase();
            sections.entry(current_section.clone()).or_default();
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let section = sections.entry(current_section.clone()).or_default();
            section.insert(key.trim().to_lowercase(), value.trim().to_string());
        }
    }

    Ok((IniConfig { sections }, config_path))
}
