use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::io::Write;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rust_hotpath::pg::DbRepo;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rust_fuse lives inside the repo root")
        .to_path_buf()
}

fn config_path() -> PathBuf {
    repo_root().join("dbfs_config.ini")
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}

fn create_workspace(name: &str) -> Result<(PathBuf, PathBuf, PathBuf), String> {
    let base = env::temp_dir().join(format!("dbfs-rust-fuse-{}-{name}", unique_suffix()));
    let mountpoint = base.join("mount");
    let log_path = base.join("mount.log");
    fs::create_dir_all(&mountpoint).map_err(|err| err.to_string())?;
    Ok((base, mountpoint, log_path))
}

fn parse_database_section(config_text: &str) -> Result<Vec<(String, String)>, String> {
    let mut in_database = false;
    let mut pairs = Vec::new();

    for raw_line in config_text.lines() {
        let line = raw_line.split(['#', ';']).next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            in_database = line[1..line.len() - 1].trim().eq_ignore_ascii_case("database");
            continue;
        }
        if !in_database {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            pairs.push((key.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }

    if pairs.is_empty() {
        return Err("missing [database] section in dbfs_config.ini".to_string());
    }
    Ok(pairs)
}

fn conninfo_from_config() -> Result<String, String> {
    let config = fs::read_to_string(config_path())
        .map_err(|err| format!("failed to read dbfs_config.ini: {err}"))?;
    let mut host = "127.0.0.1".to_string();
    let mut port = "5432".to_string();
    let mut dbname = "dbfsdbname".to_string();
    let mut user = "dbfsuser".to_string();
    let mut password = "cichosza".to_string();
    let mut sslmode = None::<String>;
    let mut sslrootcert = None::<String>;
    let mut sslcert = None::<String>;
    let mut sslkey = None::<String>;

    for (key, value) in parse_database_section(&config)? {
        match key.as_str() {
            "host" => host = value,
            "port" => port = value,
            "dbname" => dbname = value,
            "user" => user = value,
            "password" => password = value,
            "sslmode" if !value.eq_ignore_ascii_case("disable") => sslmode = Some(value),
            "sslrootcert" if !value.is_empty() => sslrootcert = Some(value),
            "sslcert" if !value.is_empty() => sslcert = Some(value),
            "sslkey" if !value.is_empty() => sslkey = Some(value),
            _ => {}
        }
    }

    let mut parts = vec![
        format!("host='{}'", host.replace('\'', "''")),
        format!("port='{}'", port.replace('\'', "''")),
        format!("dbname='{}'", dbname.replace('\'', "''")),
        format!("user='{}'", user.replace('\'', "''")),
        format!("password='{}'", password.replace('\'', "''")),
    ];
    if let Some(value) = sslmode {
        parts.push(format!("sslmode='{}'", value.replace('\'', "''")));
    }
    if let Some(value) = sslrootcert {
        parts.push(format!("sslrootcert='{}'", value.replace('\'', "''")));
    }
    if let Some(value) = sslcert {
        parts.push(format!("sslcert='{}'", value.replace('\'', "''")));
    }
    if let Some(value) = sslkey {
        parts.push(format!("sslkey='{}'", value.replace('\'', "''")));
    }
    Ok(parts.join(" "))
}

fn block_size_from_config() -> Result<usize, String> {
    let repo = DbRepo::new(&conninfo_from_config()?)?;
    let snapshot = repo.startup_snapshot()?;
    Ok(snapshot.block_size.unwrap_or(4096) as usize)
}

fn ensure_schema_initialized() -> Result<(), String> {
    let root = repo_root();
    let config = config_path();
    let schema_password = env::var("DBFS_SCHEMA_ADMIN_PASSWORD")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("dbfs-{}", unique_suffix().replace('-', "")));

    let status = Command::new("cargo")
        .arg("run")
        .arg("--offline")
        .arg("--manifest-path")
        .arg(root.join("rust_mkfs/Cargo.toml"))
        .arg("--quiet")
        .arg("--bin")
        .arg("dbfs-rust-mkfs")
        .arg("--")
        .arg("status")
        .env("DBFS_CONFIG", &config)
        .env("POSTGRES_DB", env::var("POSTGRES_DB").unwrap_or_else(|_| "dbfsdbname".to_string()))
        .env("POSTGRES_USER", env::var("POSTGRES_USER").unwrap_or_else(|_| "dbfsuser".to_string()))
        .env("POSTGRES_PASSWORD", env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "cichosza".to_string()))
        .output()
        .map_err(|err| format!("failed to check schema status: {err}"))?;

    if status.status.success() {
        let stdout = String::from_utf8_lossy(&status.stdout);
        if stdout.contains("DBFS ready: yes") {
            return Ok(());
        }
    }

    let output = Command::new("cargo")
        .arg("run")
        .arg("--offline")
        .arg("--manifest-path")
        .arg(root.join("rust_mkfs/Cargo.toml"))
        .arg("--quiet")
        .arg("--bin")
        .arg("dbfs-rust-mkfs")
        .arg("--")
        .arg("init")
        .arg("--schema-admin-password")
        .arg(&schema_password)
        .env("DBFS_CONFIG", &config)
        .env("POSTGRES_DB", env::var("POSTGRES_DB").unwrap_or_else(|_| "dbfsdbname".to_string()))
        .env("POSTGRES_USER", env::var("POSTGRES_USER").unwrap_or_else(|_| "dbfsuser".to_string()))
        .env("POSTGRES_PASSWORD", env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "cichosza".to_string()))
        .output()
        .map_err(|err| format!("failed to initialize schema: {err}"))?;

    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "dbfs-rust-mkfs init failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn bootstrap_binary() -> PathBuf {
    let root = repo_root();
    if let Ok(path) = env::var("DBFS_BOOTSTRAP_BIN") {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return candidate;
        }
    }
    for candidate in [
        root.join("rust_mkfs/target/debug/dbfs-bootstrap"),
        root.join("rust_mkfs/target/release/dbfs-bootstrap"),
        PathBuf::from("/usr/local/bin/dbfs-bootstrap"),
    ] {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("dbfs-bootstrap binary not found; build rust_mkfs first");
}

fn mountpoint_ready(path: &Path) -> bool {
    Command::new("mountpoint")
        .arg("-q")
        .arg(path)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn try_unmount(path: &Path) {
    for program in ["fusermount3", "fusermount", "umount"] {
        let _ = Command::new(program).arg("-u").arg(path).status();
        if !mountpoint_ready(path) {
            break;
        }
    }
}

struct MountedFs {
    workspace: PathBuf,
    mountpoint: PathBuf,
    child: Child,
}

impl MountedFs {
    fn start(name: &str) -> Result<Self, String> {
        ensure_schema_initialized()?;
        Self::start_without_init(name)
    }

    fn start_without_init(name: &str) -> Result<Self, String> {
        let (workspace, mountpoint, log_path) = create_workspace(name)?;
        let log_file = fs::File::create(&log_path).map_err(|err| err.to_string())?;
        let bootstrap = bootstrap_binary();
        let conninfo = conninfo_from_config()?;
        let mut command = Command::new(bootstrap);
        command
            .current_dir(repo_root())
            .arg("-f")
            .arg(&mountpoint)
            .env("DBFS_CONFIG", config_path())
            .env("DBFS_DSN_CONNINFO", conninfo)
            .env("DBFS_USE_RUST_FUSE", "1")
            .env("DBFS_USE_FUSE_CONTEXT", "1")
            .env("DBFS_SELINUX", "off")
            .env("DBFS_ACL", "off")
            .env("DBFS_DEFAULT_PERMISSIONS", "1")
            .env("DBFS_ATIME_POLICY", "default")
            .stdout(Stdio::from(log_file.try_clone().map_err(|err| err.to_string())?))
            .stderr(Stdio::from(log_file));
        let mut child = command.spawn().map_err(|err| err.to_string())?;

        for _ in 0..60 {
            if mountpoint_ready(&mountpoint) {
                return Ok(Self {
                    workspace,
                    mountpoint,
                    child,
                });
            }
            if let Some(status) = child.try_wait().map_err(|err| err.to_string())? {
                return Err(format!(
                    "dbfs-bootstrap exited too early with status {status:?}\n{}",
                    fs::read_to_string(&log_path).unwrap_or_default()
                ));
            }
            sleep(Duration::from_secs(1));
        }

        Err(format!(
            "mountpoint did not become ready\n{}",
            fs::read_to_string(&log_path).unwrap_or_default()
        ))
    }
}

impl Drop for MountedFs {
    fn drop(&mut self) {
        try_unmount(&self.mountpoint);
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.workspace);
    }
}

fn metadata_times(path: &Path) -> Result<(i64, i64, i64, i64), String> {
    let meta = fs::metadata(path).map_err(|err| err.to_string())?;
    Ok((meta.mtime(), meta.mtime_nsec(), meta.ctime(), meta.ctime_nsec()))
}

#[test]
fn write_noop() -> Result<(), String> {
    let mounted = MountedFs::start("write-noop")?;
    let file_path = mounted.mountpoint.join("write-noop.txt");
    let payload = b"payload\n";

    fs::write(&file_path, payload).map_err(|err| err.to_string())?;
    let before = metadata_times(&file_path)?;
    OpenOptions::new()
        .write(true)
        .open(&file_path)
        .map_err(|err| err.to_string())?
        .write_all(payload)
        .map_err(|err| err.to_string())?;
    let after = metadata_times(&file_path)?;

    let size = fs::metadata(&file_path).map_err(|err| err.to_string())?.len();
    if size != payload.len() as u64 {
        return Err(format!("expected size {}, got {}", payload.len(), size));
    }
    if before != after {
        return Err(format!("write noop changed metadata: before={before:?} after={after:?}"));
    }
    Ok(())
}

#[test]
fn unlink_after_write() -> Result<(), String> {
    let mounted = MountedFs::start("unlink-after-write")?;
    let dir_path = mounted.mountpoint.join("unlink-after-write");
    let file_path = dir_path.join("payload.bin");

    fs::create_dir(&dir_path).map_err(|err| err.to_string())?;
    fs::write(&file_path, b"payload").map_err(|err| err.to_string())?;
    fs::remove_file(&file_path).map_err(|err| err.to_string())?;

    if file_path.exists() {
        return Err("file still exists after unlink".to_string());
    }
    Ok(())
}

#[test]
fn multi_open_unique_handles() -> Result<(), String> {
    let mounted = MountedFs::start("multi-open-unique-handles")?;
    let suffix = unique_suffix();
    let dir_path = mounted.mountpoint.join(format!("multi_open_unique_handles_{suffix}"));
    let file_path = dir_path.join("payload.bin");

    fs::create_dir(&dir_path)
        .map_err(|err| format!("create_dir failed: {err}"))?;
    fs::write(&file_path, b"")
        .map_err(|err| format!("create empty file failed: {err}"))?;

    let mut fh_plain = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .map_err(|err| format!("open fh_plain failed: {err}"))?;
    let fh_probe = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .map_err(|err| format!("open fh_probe failed: {err}"))?;

    if fh_plain.as_raw_fd() == fh_probe.as_raw_fd() {
        return Err("handles should be independent".to_string());
    }

    fh_plain
        .write_all(b"AA")
        .map_err(|err| format!("write fh_plain failed: {err}"))?;
    fh_plain
        .flush()
        .map_err(|err| format!("flush fh_plain before append failed: {err}"))?;
    drop(fh_probe);
    drop(fh_plain);

    let mut fh_append = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .map_err(|err| format!("reopen fh_append failed: {err}"))?;
    fh_append
        .seek(SeekFrom::Start(2))
        .map_err(|err| format!("append seek failed: {err}"))?;
    fh_append
        .write_all(b"BB")
        .map_err(|err| format!("write fh_append failed: {err}"))?;
    fh_append.flush().map_err(|err| format!("flush fh_append failed: {err}"))?;
    drop(fh_append);

    let mut data = Vec::new();
    fs::File::open(&file_path)
        .map_err(|err| format!("reopen failed: {err}"))?
        .read_to_end(&mut data)
        .map_err(|err| format!("read back failed: {err}"))?;
    if data != b"AABB" {
        return Err(format!("unexpected data after concurrent opens: {:?}", data));
    }
    Ok(())
}

#[test]
fn mkdir_parent_missing() -> Result<(), String> {
    let mounted = MountedFs::start_without_init("mkdir-parent-missing")?;
    let suffix = unique_suffix();
    let missing_parent = mounted.mountpoint.join(format!("missing-parent-{suffix}"));
    let nested_dir = missing_parent.join("child");

    let err = fs::create_dir(&nested_dir).expect_err("mkdir unexpectedly created missing parents");
    if err.kind() != std::io::ErrorKind::NotFound {
        return Err(format!("expected ENOENT/NotFound, got {err}"));
    }

    if missing_parent.exists() {
        return Err("missing parent should not have been created".to_string());
    }

    Ok(())
}

#[test]
fn truncate_rename() -> Result<(), String> {
    let mounted = MountedFs::start_without_init("truncate-rename")?;
    let suffix = unique_suffix();
    let dir_path = mounted.mountpoint.join(format!("truncate_{suffix}"));
    let file_path = dir_path.join("data.txt");
    let renamed_path = dir_path.join("data-renamed.txt");
    let payload = b"abcdef123456";

    fs::create_dir(&dir_path).map_err(|err| err.to_string())?;
    fs::write(&file_path, payload).map_err(|err| err.to_string())?;
    fs::rename(&file_path, &renamed_path).map_err(|err| err.to_string())?;

    if fs::read(&renamed_path).map_err(|err| err.to_string())? != payload {
        return Err("rename/read mismatch".to_string());
    }

    let fh = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&renamed_path)
        .map_err(|err| err.to_string())?;
    fh.set_len(4).map_err(|err| err.to_string())?;
    drop(fh);

    if fs::read(&renamed_path).map_err(|err| err.to_string())? != &payload[..4] {
        return Err("truncate/read mismatch".to_string());
    }

    if file_path.exists() {
        return Err("old path still opens after rename".to_string());
    }

    Ok(())
}

#[test]
fn block_read_range() -> Result<(), String> {
    let mounted = MountedFs::start_without_init("block-read")?;
    let dir_path = mounted.mountpoint.join("block-read");
    let file_path = dir_path.join("payload.bin");
    let block_size = block_size_from_config()?;
    let payload_size = (block_size * 3) + 321;
    let mut pattern = Vec::with_capacity(payload_size);
    while pattern.len() < payload_size {
        pattern.extend_from_slice(b"0123456789abcdef");
    }
    pattern.truncate(payload_size);

    fs::create_dir(&dir_path).map_err(|err| err.to_string())?;
    fs::write(&file_path, &pattern).map_err(|err| err.to_string())?;

    let mut fh = OpenOptions::new()
        .read(true)
        .open(&file_path)
        .map_err(|err| err.to_string())?;

    let offset = block_size - 7;
    let size = block_size + 33;
    fh.seek(SeekFrom::Start(offset as u64))
        .map_err(|err| err.to_string())?;
    let mut chunk = vec![0_u8; size];
    let read = fh.read(&mut chunk).map_err(|err| err.to_string())?;
    chunk.truncate(read);
    if chunk != pattern[offset..offset + read] {
        return Err("partial read mismatch".to_string());
    }

    let tail_offset = pattern.len().saturating_sub(17);
    fh.seek(SeekFrom::Start(tail_offset as u64))
        .map_err(|err| err.to_string())?;
    let mut tail = Vec::new();
    fh.read_to_end(&mut tail).map_err(|err| err.to_string())?;
    if tail != pattern[tail_offset..] {
        return Err("tail read mismatch".to_string());
    }

    Ok(())
}
