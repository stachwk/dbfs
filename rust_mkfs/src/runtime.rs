use std::collections::HashMap;
use std::path::Path;

#[cfg(unix)]
use libc::statvfs;

pub fn parse_bool(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("invalid boolean value: {other}")),
    }
}

#[allow(dead_code)]
pub fn parse_size_bytes(value: &str) -> Result<u64, String> {
    let text = value.trim();
    if text.is_empty() {
        return Err("size value is empty".to_string());
    }
    let lower = text.to_ascii_lowercase();
    let (number_text, multiplier) = if let Some(stripped) = lower.strip_suffix("kib") {
        (stripped, 1024u64)
    } else if let Some(stripped) = lower.strip_suffix("mib") {
        (stripped, 1024u64.pow(2))
    } else if let Some(stripped) = lower.strip_suffix("gib") {
        (stripped, 1024u64.pow(3))
    } else if let Some(stripped) = lower.strip_suffix("tib") {
        (stripped, 1024u64.pow(4))
    } else if let Some(stripped) = lower.strip_suffix("kb") {
        (stripped, 1000u64)
    } else if let Some(stripped) = lower.strip_suffix("mb") {
        (stripped, 1000u64.pow(2))
    } else if let Some(stripped) = lower.strip_suffix("gb") {
        (stripped, 1000u64.pow(3))
    } else if let Some(stripped) = lower.strip_suffix("tb") {
        (stripped, 1000u64.pow(4))
    } else if let Some(stripped) = lower.strip_suffix('b') {
        (stripped, 1u64)
    } else {
        (lower.as_str(), 1u64)
    };

    let number = number_text.trim().parse::<u64>().map_err(|_| format!("invalid size value: {value}"))?;
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size value overflows u64: {value}"))
}

#[allow(dead_code)]
#[cfg(unix)] 
pub fn statvfs_total_bytes(path: &Path) -> Result<u64, String> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| format!("path contains NUL byte: {}", path.display()))?;
    let mut stats = MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { statvfs(c_path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return Err(format!("statvfs failed for {}", path.display()));
    }
    let stats = unsafe { stats.assume_init() };
    Ok((stats.f_frsize as u64).saturating_mul(stats.f_blocks as u64))
}

#[cfg(not(unix))]
pub fn statvfs_total_bytes(_path: &Path) -> Result<u64, String> {
    Err("statvfs is unavailable on this platform".to_string())
}

#[allow(dead_code)]
pub fn resolve_max_fs_size_bytes(
    max_fs_size_bytes: Option<&str>,
    pg_visible_path: Option<&Path>,
    default_max_fs_size_bytes: u64,
) -> Result<u64, String> {
    let requested = match max_fs_size_bytes {
        Some(value) if !value.trim().is_empty() => parse_size_bytes(value)?,
        _ => default_max_fs_size_bytes,
    };
    if let Some(path) = pg_visible_path {
        let visible_total = statvfs_total_bytes(path)?;
        Ok(requested.min(visible_total))
    } else {
        Ok(requested)
    }
}

#[allow(dead_code)]
pub fn validate_runtime_tuning(runtime: &HashMap<String, String>) -> Result<(), String> {
    fn validate_positive_u64(runtime: &HashMap<String, String>, key: &str) -> Result<(), String> {
        if let Some(value) = runtime.get(key) {
            let parsed = value
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("invalid {}: {}", key, value))?;
            if parsed == 0 {
                return Err(format!("{} must be greater than zero", key));
            }
        }
        Ok(())
    }

    fn validate_positive_seconds(runtime: &HashMap<String, String>, key: &str) -> Result<(), String> {
        if let Some(value) = runtime.get(key) {
            let parsed = value
                .trim()
                .parse::<f64>()
                .map_err(|_| format!("invalid {}: {}", key, value))?;
            if !parsed.is_finite() || parsed <= 0.0 {
                return Err(format!("{} must be greater than zero", key));
            }
        }
        Ok(())
    }

    fn validate_bool(runtime: &HashMap<String, String>, key: &str) -> Result<(), String> {
        if let Some(value) = runtime.get(key) {
            parse_bool(value).map(|_| ())
                .map_err(|err| format!("{}: {}", key, err))?;
        }
        Ok(())
    }

    validate_positive_seconds(runtime, "lock_poll_interval_seconds")?;
    validate_positive_u64(runtime, "read_cache_blocks")?;
    validate_positive_u64(runtime, "workers_read")?;
    validate_positive_u64(runtime, "workers_write")?;
    validate_positive_u64(runtime, "persist_buffer_chunk_blocks")?;
    validate_positive_u64(runtime, "copy_dedupe_min_blocks")?;
    if let Some(value) = runtime.get("copy_dedupe_max_blocks") {
        value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("invalid copy_dedupe_max_blocks: {}", value))?;
    }

    validate_bool(runtime, "copy_dedupe_enabled")?;
    validate_bool(runtime, "copy_dedupe_crc_table")?;
    validate_bool(runtime, "rust_hotpath_copy_pack")?;
    validate_bool(runtime, "rust_hotpath_copy_plan")?;
    validate_bool(runtime, "rust_hotpath_copy_dedupe")?;
    validate_bool(runtime, "rust_hotpath_persist_pad")?;
    validate_bool(runtime, "rust_hotpath_read_assemble")?;

    if let Some(value) = runtime.get("synchronous_commit") {
        match value.trim().to_ascii_lowercase().as_str() {
            "on" | "off" | "local" | "remote_write" | "remote_apply" | "true" | "false" => {}
            other => return Err(format!("invalid synchronous_commit: {other}")),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_bool, parse_size_bytes, resolve_max_fs_size_bytes, validate_runtime_tuning};
    use std::collections::HashMap;
    use std::path::Path;

    #[test]
    fn parses_size_bytes() {
        assert_eq!(parse_size_bytes("50GiB").unwrap(), 50 * 1024u64.pow(3));
        assert_eq!(parse_size_bytes("1TiB").unwrap(), 1024u64.pow(4));
        assert_eq!(parse_size_bytes("10GiB").unwrap(), 10 * 1024u64.pow(3));
        assert_eq!(parse_size_bytes("4096").unwrap(), 4096);
        assert_eq!(parse_size_bytes("2gib").unwrap(), 2 * 1024u64.pow(3));
    }

    #[test]
    fn parses_bool_values() {
        assert!(parse_bool("1").unwrap());
        assert!(parse_bool("on").unwrap());
        assert!(!parse_bool("0").unwrap());
        assert!(parse_bool("off").is_ok());
        assert!(parse_bool("maybe").is_err());
    }

    #[test]
    fn validates_runtime_tuning_values() {
        let mut runtime = HashMap::new();
        runtime.insert("lock_poll_interval_seconds".to_string(), "0.05".to_string());
        runtime.insert("read_cache_blocks".to_string(), "1024".to_string());
        runtime.insert("workers_read".to_string(), "4".to_string());
        runtime.insert("workers_write".to_string(), "4".to_string());
        runtime.insert("persist_buffer_chunk_blocks".to_string(), "128".to_string());
        runtime.insert("copy_dedupe_enabled".to_string(), "off".to_string());
        runtime.insert("copy_dedupe_min_blocks".to_string(), "16".to_string());
        runtime.insert("copy_dedupe_max_blocks".to_string(), "32".to_string());
        runtime.insert("copy_dedupe_crc_table".to_string(), "off".to_string());
        runtime.insert("rust_hotpath_copy_pack".to_string(), "on".to_string());
        runtime.insert("rust_hotpath_copy_plan".to_string(), "on".to_string());
        runtime.insert("rust_hotpath_copy_dedupe".to_string(), "off".to_string());
        runtime.insert("rust_hotpath_persist_pad".to_string(), "on".to_string());
        runtime.insert("rust_hotpath_read_assemble".to_string(), "on".to_string());
        runtime.insert("synchronous_commit".to_string(), "on".to_string());
        validate_runtime_tuning(&runtime).unwrap();

        runtime.insert("workers_read".to_string(), "0".to_string());
        assert!(validate_runtime_tuning(&runtime).is_err());
    }

    #[test]
    fn clamps_size_to_visible_fs() {
        let temp_dir = std::env::temp_dir();
        let result = resolve_max_fs_size_bytes(Some("1TiB"), Some(Path::new(&temp_dir)), 4096);
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }
}
