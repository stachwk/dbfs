use std::env;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;

use rust_hotpath::crc32_bytes;
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
        Self::start_with_env(name, &[])
    }

    fn start_with_env(name: &str, extra_env: &[(&str, String)]) -> Result<Self, String> {
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
        for (key, value) in extra_env {
            command.env(key, value);
        }
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

        Err("mountpoint did not become ready".to_string())
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

fn db_repo() -> Result<DbRepo, String> {
    DbRepo::new(&conninfo_from_config()?)
}

fn logical_mount_path(mountpoint: &Path, path: &Path) -> Result<String, String> {
    let relative = path
        .strip_prefix(mountpoint)
        .map_err(|_| format!("path {} is not under mountpoint {}", path.display(), mountpoint.display()))?;
    let mut logical = String::from("/");
    logical.push_str(&relative.to_string_lossy().replace('\\', "/"));
    Ok(logical.trim_end_matches('/').to_string())
}

fn resolve_file_id(repo: &DbRepo, mountpoint: &Path, path: &Path) -> Result<u64, String> {
    let logical_path = logical_mount_path(mountpoint, path)?;
    let resolved = repo.resolve_path(&logical_path)?;
    match (resolved.kind.as_deref(), resolved.entry_id) {
        (Some("hardlink"), Some(hardlink_id)) => repo.get_hardlink_file_id(hardlink_id)?.ok_or_else(|| "missing file id".to_string()),
        (Some("file"), Some(file_id)) => Ok(file_id),
        other => Err(format!("unexpected resolved path: {:?}", other)),
    }
}

fn query_u64(repo: &DbRepo, sql: &str) -> Result<u64, String> {
    let value = repo.query_scalar_text(sql)?;
    value
        .trim()
        .parse::<u64>()
        .map_err(|err| format!("failed to parse integer result {value:?}: {err}"))
}

fn data_object_id_for_file(repo: &DbRepo, file_id: u64) -> Result<u64, String> {
    query_u64(repo, &format!("SELECT data_object_id FROM files WHERE id_file = {file_id}"))
}

fn copy_block_crc_count(repo: &DbRepo, object_id: u64) -> Result<u64, String> {
    query_u64(
        repo,
        &format!("SELECT COUNT(*) FROM copy_block_crc WHERE data_object_id = {object_id}"),
    )
}

fn copy_block_crc_rows(repo: &DbRepo, object_id: u64) -> Result<Vec<u32>, String> {
    let text = repo.query_scalar_text(&format!(
        "SELECT COALESCE(string_agg(crc32::text, ',' ORDER BY _order), '') FROM copy_block_crc WHERE data_object_id = {object_id}"
    ))?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    text
        .trim()
        .split(',')
        .map(|value| {
            value
                .trim()
                .parse::<u32>()
                .map_err(|err| format!("failed to parse crc32 value {value:?}: {err}"))
        })
        .collect()
}

fn stat_times(path: &Path) -> Result<(i64, i64, i64, i64, i64, i64), String> {
    let meta = fs::metadata(path).map_err(|err| err.to_string())?;
    Ok((
        meta.atime(),
        meta.atime_nsec(),
        meta.mtime(),
        meta.mtime_nsec(),
        meta.ctime(),
        meta.ctime_nsec(),
    ))
}

fn utimens(path: &Path, atime_sec: i64, atime_nsec: i64, mtime_sec: i64, mtime_nsec: i64) -> Result<(), String> {
    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|err| err.to_string())?;
    let times = [
        libc::timespec {
            tv_sec: atime_sec as libc::time_t,
            tv_nsec: atime_nsec as libc::c_long,
        },
        libc::timespec {
            tv_sec: mtime_sec as libc::time_t,
            tv_nsec: mtime_nsec as libc::c_long,
        },
    ];
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().to_string())
    }
}

#[test]
fn flush_release_profile() -> Result<(), String> {
    let mounted = MountedFs::start("flush-release-profile")?;
    let repo = db_repo()?;
    let clean_dir = mounted.mountpoint.join("flush-clean");
    let clean_file = clean_dir.join("empty.txt");
    let dirty_dir = mounted.mountpoint.join("flush-dirty");
    let dirty_file = dirty_dir.join("payload.txt");

    fs::create_dir(&clean_dir).map_err(|err| err.to_string())?;
    fs::write(&clean_file, b"").map_err(|err| err.to_string())?;
    fs::create_dir(&dirty_dir).map_err(|err| err.to_string())?;
    fs::write(&dirty_file, b"flush-release-profile").map_err(|err| err.to_string())?;

    let clean_id = resolve_file_id(&repo, &mounted.mountpoint, &clean_file)?;
    let dirty_id = resolve_file_id(&repo, &mounted.mountpoint, &dirty_file)?;

    if repo.file_size(clean_id)?.unwrap_or(0) != 0 {
        return Err("clean file should remain empty".to_string());
    }
    if repo.count_file_blocks(clean_id)? != 0 {
        return Err("clean file should not persist data blocks".to_string());
    }

    let dirty_size = repo.file_size(dirty_id)?.unwrap_or(0);
    if dirty_size != b"flush-release-profile".len() as u64 {
        return Err(format!("dirty file size mismatch: {dirty_size}"));
    }
    if repo.count_file_blocks(dirty_id)? == 0 {
        return Err("dirty file should persist at least one block".to_string());
    }
    Ok(())
}

#[test]
fn truncate_release_profile() -> Result<(), String> {
    let mounted = MountedFs::start("truncate-release-profile")?;
    let repo = db_repo()?;
    let dir_path = mounted.mountpoint.join("truncate-release");
    let file_path = dir_path.join("payload.txt");
    let payload = b"abcdefghabcdefgh";

    fs::create_dir(&dir_path).map_err(|err| err.to_string())?;
    fs::write(&file_path, payload).map_err(|err| err.to_string())?;
    fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .map_err(|err| err.to_string())?
        .set_len(0)
        .map_err(|err| err.to_string())?;

    let file_id = resolve_file_id(&repo, &mounted.mountpoint, &file_path)?;
    if repo.file_size(file_id)?.unwrap_or(1) != 0 {
        return Err("truncate should shrink file to zero".to_string());
    }
    if repo.count_file_blocks(file_id)? != 0 {
        return Err("truncate should remove file blocks".to_string());
    }
    Ok(())
}

#[test]
fn persist_buffer_chunking() -> Result<(), String> {
    let mounted = MountedFs::start("persist-buffer-chunking")?;
    let suffix = unique_suffix();
    let dir_path = mounted.mountpoint.join(format!("persist-chunk-{suffix}"));
    let file_path = dir_path.join("big.bin");
    let payload = vec![0u8; 5 * 1024 * 1024];

    fs::create_dir(&dir_path).map_err(|err| err.to_string())?;
    fs::write(&file_path, &payload).map_err(|err| format!("write failed: {err}"))?;

    let meta = fs::metadata(&file_path).map_err(|err| format!("metadata failed: {err}"))?;
    if meta.len() != payload.len() as u64 {
        return Err(format!("unexpected file size: {} != {}", meta.len(), payload.len()));
    }

    let mut file = fs::File::open(&file_path).map_err(|err| format!("open failed: {err}"))?;
    let mut data = Vec::with_capacity(payload.len());
    let mut buf = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buf).map_err(|err| format!("read failed: {err}"))?;
        if read == 0 {
            break;
        }
        data.extend_from_slice(&buf[..read]);
    }
    let first = data.get(..4).ok_or_else(|| "missing first bytes".to_string())?;
    if first != &[0, 0, 0, 0] {
        return Err(format!("unexpected first bytes: {:?}", first));
    }
    let last = data.get(data.len().saturating_sub(4)..).ok_or_else(|| "missing last bytes".to_string())?;
    if last != &[0, 0, 0, 0] {
        return Err(format!("unexpected last bytes: {:?}", last));
    }

    Ok(())
}

#[test]
fn write_flush_threshold() -> Result<(), String> {
    let mounted = MountedFs::start_with_env(
        "write-flush-threshold",
        &[("DBFS_WRITE_FLUSH_THRESHOLD_BYTES", "1".to_string())],
    )?;
    let repo = db_repo()?;
    let dir_path = mounted.mountpoint.join("flush-threshold");
    let file_path = dir_path.join("payload.bin");
    let payload = b"ab";

    fs::create_dir(&dir_path).map_err(|err| err.to_string())?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&file_path)
        .map_err(|err| err.to_string())?;
    file.write_all(payload).map_err(|err| err.to_string())?;

    let file_id = resolve_file_id(&repo, &mounted.mountpoint, &file_path)?;
    if repo.count_file_blocks(file_id)? == 0 {
        return Err("write buffer should auto-flush after threshold".to_string());
    }
    if repo.file_size(file_id)?.unwrap_or(0) != payload.len() as u64 {
        return Err("file size should reflect auto-flushed payload".to_string());
    }
    file.seek(SeekFrom::Start(0)).map_err(|err| err.to_string())?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).map_err(|err| err.to_string())?;
    if data != payload {
        return Err(format!("unexpected payload after auto-flush: {:?}", data));
    }
    drop(file);
    Ok(())
}

#[test]
fn copy_block_crc_table() -> Result<(), String> {
    let mounted = MountedFs::start("copy-block-crc-table")?;
    let repo = db_repo()?;
    let suffix = unique_suffix();
    let dir_path = mounted.mountpoint.join(format!("copy-crc-{suffix}"));
    let src_path = dir_path.join("src.bin");
    let dst_path = dir_path.join("dst.bin");

    let snapshot = repo.startup_snapshot()?;
    let block_size = snapshot.block_size.unwrap_or(4096) as usize;
    let payload = {
        let mut data = Vec::new();
        while data.len() < block_size * 4 {
            data.extend_from_slice(b"dbfs-copy-crc-");
        }
        data.truncate(block_size * 4);
        data
    };

    fs::create_dir(&dir_path).map_err(|err| err.to_string())?;
    fs::write(&src_path, &payload).map_err(|err| err.to_string())?;
    fs::write(&dst_path, &payload).map_err(|err| err.to_string())?;

    let dst_file_id = resolve_file_id(&repo, &mounted.mountpoint, &dst_path)?;
    let dst_object_id = data_object_id_for_file(&repo, dst_file_id)?;
    repo.exec(&format!(
        "DELETE FROM copy_block_crc WHERE data_object_id = {dst_object_id}"
    ))?;
    if copy_block_crc_count(&repo, dst_object_id)? != 0 {
        return Err("expected empty CRC table before first copy".to_string());
    }

    let dst_file_id = resolve_file_id(&repo, &mounted.mountpoint, &dst_path)?;
    let dst_object_id = data_object_id_for_file(&repo, dst_file_id)?;
    let expected_full_blocks = payload.len() / block_size;
    let expected_source_crcs: Vec<u32> = payload
        .chunks(block_size)
        .take(expected_full_blocks)
        .map(crc32_bytes)
        .collect();
    let rows: Vec<rust_hotpath::pg::PersistBlockRow> = payload
        .chunks(block_size)
        .enumerate()
        .map(|(index, chunk)| rust_hotpath::pg::PersistBlockRow {
            block_index: index as u64,
            data: chunk,
            used_len: chunk.len() as u64,
        })
        .collect();
    repo.persist_copy_block_crc_rows(dst_file_id, block_size as u64, &rows)
        .map_err(|err| err.to_string())?;
    let after_first_copy = copy_block_crc_count(&repo, dst_object_id)?;
    if after_first_copy != expected_full_blocks as u64 {
        return Err(format!(
            "unexpected CRC row count after first copy: got {after_first_copy}, expected {}",
            expected_full_blocks
        ));
    }
    let actual_first_rows = copy_block_crc_rows(&repo, dst_object_id)?;
    if actual_first_rows != expected_source_crcs {
        return Err(format!(
            "CRC rows after first copy do not match source: got {:?}, expected {:?}",
            actual_first_rows, expected_source_crcs
        ));
    }

    let mut mutated_payload = payload.clone();
    mutated_payload[0] ^= 0xFF;
    {
        let mut fh = fs::OpenOptions::new()
            .write(true)
            .open(&dst_path)
            .map_err(|err| err.to_string())?;
        fh.seek(SeekFrom::Start(0)).map_err(|err| err.to_string())?;
        fh.write_all(&mutated_payload[..block_size])
            .map_err(|err| err.to_string())?;
    }
    let dst_file_id = resolve_file_id(&repo, &mounted.mountpoint, &dst_path)?;
    let dst_object_id = data_object_id_for_file(&repo, dst_file_id)?;
    let updated_crc = copy_block_crc_rows(&repo, dst_object_id)?
        .into_iter()
        .next()
        .ok_or_else(|| "missing updated CRC row".to_string())?;
    let mutated_crc = crc32_bytes(&mutated_payload[..block_size]);
    if updated_crc != mutated_crc {
        return Err("updated CRC did not match mutated block".to_string());
    }

    let victim_path = dir_path.join("victim.bin");
    let move_src_path = dir_path.join("move-src.bin");
    let victim_seed_payload = {
        let mut data = Vec::new();
        while data.len() < block_size * 2 {
            data.extend_from_slice(b"seed-");
        }
        data.truncate(block_size * 2);
        data
    };
    let victim_payload = {
        let mut data = Vec::new();
        while data.len() < block_size * 2 {
            data.extend_from_slice(b"victim-");
        }
        data.truncate(block_size * 2);
        data
    };

    fs::write(&victim_path, &victim_seed_payload).map_err(|err| err.to_string())?;
    let victim_file_id = resolve_file_id(&repo, &mounted.mountpoint, &victim_path)?;
    let victim_object_id = data_object_id_for_file(&repo, victim_file_id)?;
    if copy_block_crc_count(&repo, victim_object_id)? != 2 {
        return Err("victim should have 2 CRC rows".to_string());
    }

    fs::write(&move_src_path, &victim_payload).map_err(|err| err.to_string())?;
    let move_src_file_id = resolve_file_id(&repo, &mounted.mountpoint, &move_src_path)?;
    let move_src_object_id = data_object_id_for_file(&repo, move_src_file_id)?;
    if copy_block_crc_count(&repo, move_src_object_id)? != 2 {
        return Err("move-src should have 2 CRC rows".to_string());
    }

    fs::rename(&move_src_path, &victim_path).map_err(|err| err.to_string())?;
    if copy_block_crc_count(&repo, victim_object_id)? != 0 {
        return Err("renamed victim object should be unreferenced".to_string());
    }
    let replaced_victim_file_id = resolve_file_id(&repo, &mounted.mountpoint, &victim_path)?;
    let replaced_victim_object_id = data_object_id_for_file(&repo, replaced_victim_file_id)?;
    if copy_block_crc_count(&repo, replaced_victim_object_id)? != 2 {
        return Err("replaced victim should have 2 CRC rows".to_string());
    }

    {
        let mut fh = fs::OpenOptions::new()
            .write(true)
            .open(&dst_path)
            .map_err(|err| err.to_string())?;
        fh.seek(SeekFrom::Start(0)).map_err(|err| err.to_string())?;
        fh.write_all(&payload).map_err(|err| err.to_string())?;
    }

    let dst_file_id = resolve_file_id(&repo, &mounted.mountpoint, &dst_path)?;
    let dst_object_id = data_object_id_for_file(&repo, dst_file_id)?;
    let after_second_copy = copy_block_crc_count(&repo, dst_object_id)?;
    if after_second_copy != expected_full_blocks as u64 {
        return Err(format!(
            "unexpected CRC row count after second copy: got {after_second_copy}, expected {}",
            expected_full_blocks
        ));
    }
    let actual_second_rows = copy_block_crc_rows(&repo, dst_object_id)?;
    if actual_second_rows != expected_source_crcs {
        return Err(format!(
            "CRC rows after second copy do not match source: got {:?}, expected {:?}",
            actual_second_rows, expected_source_crcs
        ));
    }

    let mut read_back = Vec::new();
    fs::File::open(&dst_path)
        .map_err(|err| err.to_string())?
        .read_to_end(&mut read_back)
        .map_err(|err| err.to_string())?;
    if read_back != payload {
        return Err("copy block crc payload mismatch".to_string());
    }

    let dst_file_id = resolve_file_id(&repo, &mounted.mountpoint, &dst_path)?;
    let dst_object_id = data_object_id_for_file(&repo, dst_file_id)?;
    fs::remove_file(&dst_path).map_err(|err| err.to_string())?;
    if copy_block_crc_count(&repo, dst_object_id)? != 0 {
        return Err("CRC rows should be removed after unlink".to_string());
    }

    Ok(())
}

#[test]
fn utimens_noop() -> Result<(), String> {
    let mounted = MountedFs::start("utimens-noop")?;
    let suffix = unique_suffix();
    let file_path = mounted.mountpoint.join(format!("utimens-{suffix}.txt"));
    let dir_path = mounted.mountpoint.join(format!("utimens-{suffix}"));

    fs::write(&file_path, b"utimens\n").map_err(|err| err.to_string())?;
    let (before_atime, before_atime_nsec, before_mtime, before_mtime_nsec, before_ctime, before_ctime_nsec) =
        stat_times(&file_path)?;
    utimens(&file_path, before_atime, before_atime_nsec, before_mtime, before_mtime_nsec)?;
    let (same_atime, same_atime_nsec, same_mtime, same_mtime_nsec, same_ctime, same_ctime_nsec) =
        stat_times(&file_path)?;
    if (same_atime, same_atime_nsec) != (before_atime, before_atime_nsec) {
        return Err("file atime changed on no-op utimens".to_string());
    }
    if (same_mtime, same_mtime_nsec) != (before_mtime, before_mtime_nsec) {
        return Err("file mtime changed on no-op utimens".to_string());
    }
    if (same_ctime, same_ctime_nsec) != (before_ctime, before_ctime_nsec) {
        return Err("file ctime changed on no-op utimens".to_string());
    }

    utimens(
        &file_path,
        before_atime + 10,
        before_atime_nsec,
        before_mtime + 10,
        before_mtime_nsec,
    )?;
    let (new_atime, new_atime_nsec, new_mtime, new_mtime_nsec, new_ctime, new_ctime_nsec) =
        stat_times(&file_path)?;
    if (new_atime, new_atime_nsec) != (before_atime + 10, before_atime_nsec) {
        return Err("file atime did not update".to_string());
    }
    if (new_mtime, new_mtime_nsec) != (before_mtime + 10, before_mtime_nsec) {
        return Err("file mtime did not update".to_string());
    }
    if (new_ctime, new_ctime_nsec) < (before_ctime, before_ctime_nsec) {
        return Err("file ctime went backwards".to_string());
    }

    fs::create_dir(&dir_path).map_err(|err| err.to_string())?;
    let (dir_atime, dir_atime_nsec, dir_mtime, dir_mtime_nsec, dir_ctime, dir_ctime_nsec) =
        stat_times(&dir_path)?;
    utimens(&dir_path, dir_atime, dir_atime_nsec, dir_mtime, dir_mtime_nsec)?;
    let (same_dir_atime, same_dir_atime_nsec, same_dir_mtime, same_dir_mtime_nsec, same_dir_ctime, same_dir_ctime_nsec) =
        stat_times(&dir_path)?;
    if (same_dir_atime, same_dir_atime_nsec) != (dir_atime, dir_atime_nsec) {
        return Err("dir atime changed on no-op utimens".to_string());
    }
    if (same_dir_mtime, same_dir_mtime_nsec) != (dir_mtime, dir_mtime_nsec) {
        return Err("dir mtime changed on no-op utimens".to_string());
    }
    if (same_dir_ctime, same_dir_ctime_nsec) != (dir_ctime, dir_ctime_nsec) {
        return Err("dir ctime changed on no-op utimens".to_string());
    }

    utimens(
        &dir_path,
        dir_atime + 10,
        dir_atime_nsec,
        dir_mtime + 10,
        dir_mtime_nsec,
    )?;
    let (new_dir_atime, new_dir_atime_nsec, new_dir_mtime, new_dir_mtime_nsec, new_dir_ctime, new_dir_ctime_nsec) =
        stat_times(&dir_path)?;
    if (new_dir_atime, new_dir_atime_nsec) != (dir_atime + 10, dir_atime_nsec) {
        return Err("dir atime did not update".to_string());
    }
    if (new_dir_mtime, new_dir_mtime_nsec) != (dir_mtime + 10, dir_mtime_nsec) {
        return Err("dir mtime did not update".to_string());
    }
    if (new_dir_ctime, new_dir_ctime_nsec) < (dir_ctime, dir_ctime_nsec) {
        return Err("dir ctime went backwards".to_string());
    }

    Ok(())
}
