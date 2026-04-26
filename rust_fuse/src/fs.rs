use chrono::{DateTime, NaiveDateTime, Utc};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyBmap, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyLock, ReplyOpen, ReplyPoll,
    ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow,
};
use fuser::consts::{FUSE_FLOCK_LOCKS, FUSE_POSIX_LOCKS};
use libc::{EIO, ENOENT, ENOTEMPTY, ENOTTY, POLLIN, POLLOUT};
use rust_hotpath::{crc32_bytes, pg::DbRepo, pg::PersistBlockRow};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::path::Path;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread::sleep;

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;

#[derive(Debug, Clone)]
struct ParsedAttrs {
    file_attr: FileAttr,
}

#[derive(Debug, Clone)]
struct WriteState {
    file_id: u64,
    file_size: u64,
    truncate_pending: bool,
    buffered_bytes: u64,
    blocks: BTreeMap<u64, Vec<u8>>,
}

#[derive(Debug, Clone)]
struct PosixLockRecord {
    owner: u64,
    typ: i32,
    start: u64,
    end: Option<u64>,
    pid: u32,
}

#[derive(Debug, Clone)]
struct PosixAclEntry {
    tag: u16,
    perm: u16,
    id: i32,
}

pub struct DbfsFuse {
    pub repo: DbRepo,
    pub block_size: u64,
    pub write_flush_threshold_bytes: u64,
    pub read_only: bool,
    pub selinux_enabled: bool,
    pub acl_enabled: bool,
    inode_to_path: RwLock<HashMap<u64, String>>,
    path_to_inode: RwLock<HashMap<String, u64>>,
    fh_to_path: Mutex<HashMap<u64, String>>,
    fh_to_file_id: Mutex<HashMap<u64, u64>>,
    fh_to_flags: Mutex<HashMap<u64, i32>>,
    write_states: Mutex<HashMap<u64, WriteState>>,
    posix_locks: Mutex<HashMap<String, Vec<PosixLockRecord>>>,
    next_fh: Mutex<u64>,
}

impl DbfsFuse {
    pub fn new(
        repo: DbRepo,
        block_size: u64,
        write_flush_threshold_bytes: u64,
        read_only: bool,
        selinux_enabled: bool,
        acl_enabled: bool,
    ) -> Self {
        let mut inode_to_path = HashMap::new();
        let mut path_to_inode = HashMap::new();
        inode_to_path.insert(ROOT_INO, "/".to_string());
        path_to_inode.insert("/".to_string(), ROOT_INO);
        Self {
            repo,
            block_size: block_size.max(1),
            write_flush_threshold_bytes,
            read_only,
            selinux_enabled,
            acl_enabled,
            inode_to_path: RwLock::new(inode_to_path),
            path_to_inode: RwLock::new(path_to_inode),
            fh_to_path: Mutex::new(HashMap::new()),
            fh_to_file_id: Mutex::new(HashMap::new()),
            fh_to_flags: Mutex::new(HashMap::new()),
            write_states: Mutex::new(HashMap::new()),
            posix_locks: Mutex::new(HashMap::new()),
            next_fh: Mutex::new(1),
        }
    }

    fn normalize_path(path: &str) -> String {
        let mut value = path.trim().to_string();
        if value.is_empty() {
            return "/".to_string();
        }
        if !value.starts_with('/') {
            value.insert(0, '/');
        }
        if value.len() > 1 && value.ends_with('/') {
            while value.len() > 1 && value.ends_with('/') {
                value.pop();
            }
        }
        if value.is_empty() {
            "/".to_string()
        } else {
            value
        }
    }

    fn join_path(parent: &str, name: &OsStr) -> String {
        let name = name.to_string_lossy();
        if parent == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.trim_end_matches('/'), name)
        }
    }

    fn logical_inode(&self, obj_type: &str, entry_id: u64) -> u64 {
        match obj_type {
            "file" | "hardlink" => 1_000_000 + entry_id,
            "dir" => 2_000_000 + entry_id,
            "symlink" => 3_000_000 + entry_id,
            _ => entry_id,
        }
    }

    fn stable_inode(&self, obj_type: &str, inode_seed: &str, entry_id: u64) -> u64 {
        if inode_seed.is_empty() {
            return self.logical_inode(obj_type, entry_id);
        }
        let payload = format!("{obj_type}:{inode_seed}");
        let inode = u64::from(crc32_bytes(payload.as_bytes()));
        if inode == 0 {
            self.logical_inode(obj_type, entry_id)
        } else {
            inode
        }
    }

    fn parse_time(value: &str) -> SystemTime {
        let value = value.trim();
        if value.is_empty() {
            return UNIX_EPOCH;
        }
        let normalized = value.replace('T', " ");
        let naive_formats = [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
        ];
        for fmt in naive_formats {
            if let Ok(naive) = NaiveDateTime::parse_from_str(&normalized, fmt) {
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
                return SystemTime::UNIX_EPOCH + Duration::from_secs(dt.timestamp().max(0) as u64) + Duration::from_nanos(dt.timestamp_subsec_nanos() as u64);
            }
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(&value.replace(' ', "T")) {
            return SystemTime::UNIX_EPOCH + Duration::from_secs(dt.timestamp().max(0) as u64) + Duration::from_nanos(dt.timestamp_subsec_nanos() as u64);
        }
        UNIX_EPOCH
    }

    fn file_type_from_special(special_type: &str) -> FileType {
        match special_type {
            "fifo" => FileType::NamedPipe,
            "char" => FileType::CharDevice,
            "block" => FileType::BlockDevice,
            _ => FileType::RegularFile,
        }
    }

    fn decode_nul_fields(blob: &[u8]) -> Vec<String> {
        if blob.is_empty() {
            return Vec::new();
        }
        blob.split(|b| *b == 0)
            .map(|part| String::from_utf8_lossy(part).to_string())
            .collect()
    }

    fn file_id_for_path(&self, path: &str) -> Result<Option<u64>, libc::c_int> {
        let resolved = self.repo.resolve_path(path).map_err(|_| EIO)?;
        match resolved.kind.as_deref() {
            Some("hardlink") => {
                let hardlink_id = resolved.entry_id.ok_or(EIO)?;
                self.repo.get_hardlink_file_id(hardlink_id).map_err(|_| EIO)
            }
            Some("file") => Ok(resolved.entry_id),
            _ => Ok(None),
        }
    }

    fn resolved_entry_for_path(&self, path: &str) -> Result<(Option<String>, Option<u64>), libc::c_int> {
        let resolved = self.repo.resolve_path(path).map_err(|_| EIO)?;
        Ok((resolved.kind, resolved.entry_id))
    }

    fn raw_mode_for_path(&self, path: &str) -> Result<Option<String>, libc::c_int> {
        let blob = self.repo.fetch_path_attrs_blob(path).map_err(|_| EIO)?;
        let Some(blob) = blob else {
            return Ok(None);
        };
        let fields = Self::decode_nul_fields(&blob);
        if fields.len() < 4 {
            return Ok(None);
        }
        Ok(Some(fields[3].clone()))
    }

    fn current_group_ids() -> HashSet<u32> {
        let mut group_ids = HashSet::new();
        unsafe {
            let count = libc::getgroups(0, std::ptr::null_mut());
            if count > 0 {
                let mut groups = vec![0 as libc::gid_t; count as usize];
                let rc = libc::getgroups(count, groups.as_mut_ptr());
                if rc >= 0 {
                    for group in groups {
                        group_ids.insert(group as u32);
                    }
                }
            }
        }
        group_ids
    }

    fn can_access(&self, attrs: &FileAttr, mode: i32) -> bool {
        let current_uid = unsafe { libc::geteuid() } as u32;
        if current_uid == 0 {
            return true;
        }
        let required = Self::access_mask_from_mode(mode);
        let allowed = if current_uid == attrs.uid {
            (attrs.perm >> 6) & 0o7
        } else {
            let mut group_ids = Self::current_group_ids();
            group_ids.insert(unsafe { libc::getegid() } as u32);
            if group_ids.contains(&attrs.gid) {
                (attrs.perm >> 3) & 0o7
            } else {
                attrs.perm & 0o7
            }
        };
        (allowed & required) == required
    }

    fn access_mask_from_mode(mask: i32) -> u16 {
        let mut required = 0u16;
        if mask & libc::R_OK != 0 {
            required |= 0o4;
        }
        if mask & libc::W_OK != 0 {
            required |= 0o2;
        }
        if mask & libc::X_OK != 0 {
            required |= 0o1;
        }
        required
    }

    fn parse_posix_acl_xattr(value: &[u8]) -> Result<Vec<PosixAclEntry>, libc::c_int> {
        if value.len() < 4 {
            return Err(libc::EINVAL);
        }
        let version = u32::from_le_bytes(value[0..4].try_into().map_err(|_| libc::EINVAL)?);
        if version != 0x0002 {
            return Err(libc::EINVAL);
        }
        let mut entries = Vec::new();
        let mut idx = 4usize;
        while idx + 8 <= value.len() {
            let tag = u16::from_le_bytes(value[idx..idx + 2].try_into().map_err(|_| libc::EINVAL)?);
            let perm = u16::from_le_bytes(value[idx + 2..idx + 4].try_into().map_err(|_| libc::EINVAL)?);
            let mut id = i32::from_le_bytes(value[idx + 4..idx + 8].try_into().map_err(|_| libc::EINVAL)?);
            if tag == 0x0001 || tag == 0x0004 || tag == 0x0020 || tag == 0x0010 {
                id = -1;
            }
            entries.push(PosixAclEntry { tag, perm, id });
            idx += 8;
        }
        if idx != value.len() {
            return Err(libc::EINVAL);
        }
        Ok(entries)
    }

    fn acl_permission_from_entries(&self, entries: &[PosixAclEntry], attrs: &FileAttr, mode: i32) -> bool {
        let current_uid = unsafe { libc::geteuid() } as u32;
        if current_uid == 0 {
            return true;
        }
        let required = Self::access_mask_from_mode(mode);
        let mut mask_perm = 0o7u16;
        let mut user_obj_perm = None;
        let mut group_obj_perm = None;
        let mut other_perm = None;
        let mut named_user_perm = None;
        let mut named_group_matches: Vec<(u32, u16)> = Vec::new();
        for entry in entries {
            let perm = entry.perm & 0o7;
            match entry.tag {
                0x0001 => user_obj_perm = Some(perm),
                0x0002 if entry.id >= 0 && entry.id as u32 == current_uid => named_user_perm = Some(perm),
                0x0004 => group_obj_perm = Some(perm),
                0x0008 if entry.id >= 0 => named_group_matches.push((entry.id as u32, perm)),
                0x0010 => mask_perm = perm,
                0x0020 => other_perm = Some(perm),
                _ => {}
            }
        }
        if attrs.uid == current_uid {
            return user_obj_perm.unwrap_or(0) & required == required;
        }
        if let Some(named_user_perm) = named_user_perm {
            let allowed = named_user_perm & mask_perm;
            return (allowed & required) == required;
        }
        let mut group_ids = Self::current_group_ids();
        let current_gid = unsafe { libc::getegid() } as u32;
        group_ids.insert(current_gid);
        let mut group_allowed = 0u16;
        if group_ids.contains(&attrs.gid) {
            group_allowed |= group_obj_perm.unwrap_or(0);
        }
        for (group_id, perm) in named_group_matches {
            if group_ids.contains(&group_id) {
                group_allowed |= perm;
            }
        }
        if group_allowed != 0 {
            let allowed = group_allowed & mask_perm;
            return (allowed & required) == required;
        }
        (other_perm.unwrap_or(0) & required) == required
    }

    fn acl_allows(&self, path: &str, attrs: &FileAttr, mode: i32) -> Result<bool, libc::c_int> {
        if !self.acl_enabled {
            return Ok(self.can_access(attrs, mode));
        }
        let acl_value = self.repo.fetch_xattr_value(path, "system.posix_acl_access").map_err(|_| EIO)?;
        if let Some(value) = acl_value {
            let entries = Self::parse_posix_acl_xattr(&value)?;
            Ok(self.acl_permission_from_entries(&entries, attrs, mode))
        } else {
            Ok(self.can_access(attrs, mode))
        }
    }

    fn copy_default_acl_to_child(
        &self,
        parent_path: &str,
        owner_kind: &str,
        owner_id: u64,
        child_is_dir: bool,
    ) -> Result<(), libc::c_int> {
        if !self.acl_enabled {
            return Ok(());
        }
        let default_acl = self.repo.fetch_xattr_value(parent_path, "system.posix_acl_default").map_err(|_| EIO)?;
        let Some(default_acl) = default_acl else {
            return Ok(());
        };
        self.repo
            .store_xattr_value_for_owner(owner_kind, owner_id, "system.posix_acl_access", &default_acl)
            .map_err(|_| EIO)?;
        if child_is_dir {
            self.repo
                .store_xattr_value_for_owner(owner_kind, owner_id, "system.posix_acl_default", &default_acl)
                .map_err(|_| EIO)?;
        }
        Ok(())
    }

    fn enforce_sticky_bit(&self, parent_path: &str, entry_attrs: &FileAttr) -> Result<(), libc::c_int> {
        let parent_path = Self::normalize_path(parent_path);
        if parent_path == "/" {
            return Ok(());
        }
        let parent_attrs = match self.lookup_path(&parent_path) {
            Ok(Some(attrs)) => attrs.file_attr,
            Ok(None) => return Err(ENOENT),
            Err(errno) => return Err(errno),
        };
        if (parent_attrs.perm & libc::S_ISVTX as u16) == 0 {
            return Ok(());
        }
        let current_uid = unsafe { libc::geteuid() } as u32;
        if current_uid == 0 {
            return Ok(());
        }
        if current_uid == entry_attrs.uid || current_uid == parent_attrs.uid {
            return Ok(());
        }
        Err(libc::EPERM)
    }

    fn append_journal_event(&self, action: &str, path: &str, file_id: Option<u64>, directory_id: Option<u64>) -> Result<(), libc::c_int> {
        self.repo
            .append_journal_event(unsafe { libc::geteuid() } as u32, directory_id, file_id, &format!("{action}:{path}"))
            .map_err(|_| EIO)
    }

    fn posix_lock_type_conflicts(existing: i32, requested: i32) -> bool {
        if requested == libc::F_RDLCK {
            existing == libc::F_WRLCK
        } else if requested == libc::F_WRLCK {
            existing == libc::F_RDLCK || existing == libc::F_WRLCK
        } else {
            false
        }
    }

    fn range_overlaps(start_a: u64, end_a: Option<u64>, start_b: u64, end_b: Option<u64>) -> bool {
        let end_a = end_a.unwrap_or(u64::MAX);
        let end_b = end_b.unwrap_or(u64::MAX);
        start_a < end_b && start_b < end_a
    }

    fn resource_key_for_lock(&self, path: &str) -> Result<String, libc::c_int> {
        let resolved = self.repo.resolve_path(path).map_err(|_| EIO)?;
        match resolved.kind.as_deref() {
            Some("file") => {
                let file_id = resolved.entry_id.ok_or(EIO)?;
                Ok(format!("file:{file_id}"))
            }
            Some("hardlink") => {
                let hardlink_id = resolved.entry_id.ok_or(EIO)?;
                let file_id = self.repo.get_hardlink_file_id(hardlink_id).map_err(|_| EIO)?.ok_or(EIO)?;
                Ok(format!("file:{file_id}"))
            }
            Some("dir") => {
                let dir_id = resolved.entry_id.ok_or(EIO)?;
                Ok(format!("dir:{dir_id}"))
            }
            _ => Ok(format!("path:{path}")),
        }
    }

    fn get_posix_lock_conflict(
        &self,
        resource_key: &str,
        owner: u64,
        requested_type: i32,
        requested_start: u64,
        requested_end: u64,
    ) -> Option<PosixLockRecord> {
        let guard = self.posix_locks.lock().ok()?;
        let records = guard.get(resource_key)?;
        for record in records {
            if record.owner == owner {
                continue;
            }
            if !Self::range_overlaps(requested_start, Some(requested_end), record.start, record.end) {
                continue;
            }
            if Self::posix_lock_type_conflicts(record.typ, requested_type) {
                return Some(record.clone());
            }
        }
        None
    }

    fn set_posix_lock(
        &self,
        resource_key: &str,
        owner: u64,
        requested_type: i32,
        requested_start: u64,
        requested_end: u64,
        pid: u32,
    ) {
        if let Ok(mut guard) = self.posix_locks.lock() {
            let records = guard.entry(resource_key.to_string()).or_default();
            records.retain(|record| {
                if record.owner != owner {
                    return true;
                }
                !Self::range_overlaps(requested_start, Some(requested_end), record.start, record.end)
            });
            if requested_type != libc::F_UNLCK {
                records.push(PosixLockRecord {
                    owner,
                    typ: requested_type,
                    start: requested_start,
                    end: Some(requested_end),
                    pid,
                });
            }
            if records.is_empty() {
                guard.remove(resource_key);
            }
        }
    }

    fn xattr_owner_for_path(&self, path: &str) -> Result<Option<(String, u64)>, libc::c_int> {
        if path == "/" {
            return Ok(Some(("dir".to_string(), 0)));
        }
        let resolved = self.repo.resolve_path(path).map_err(|_| EIO)?;
        let owner = match resolved.kind.as_deref() {
            Some("file") => resolved.entry_id.map(|id| ("file".to_string(), id)),
            Some("hardlink") => match resolved.entry_id {
                Some(hardlink_id) => self
                    .repo
                    .get_hardlink_file_id(hardlink_id)
                    .map_err(|_| EIO)?
                    .map(|file_id| ("file".to_string(), file_id)),
                None => None,
            },
            Some("dir") => resolved.entry_id.map(|id| ("dir".to_string(), id)),
            Some("symlink") => resolved.entry_id.map(|id| ("symlink".to_string(), id)),
            _ => None,
        };
        Ok(owner)
    }

    fn file_id_for_handle(&self, fh: u64, ino: u64) -> Result<Option<u64>, libc::c_int> {
        if let Some(file_id) = self.fh_to_file_id.lock().ok().and_then(|guard| guard.get(&fh).copied()) {
            return Ok(Some(file_id));
        }
        if let Some(path) = self.path_for_inode(ino) {
            return self.file_id_for_path(&path);
        }
        Ok(None)
    }

    fn entry_path_for_ino(&self, ino: u64) -> Result<String, libc::c_int> {
        if ino == ROOT_INO {
            return Ok("/".to_string());
        }
        self.path_for_inode(ino).ok_or(ENOENT)
    }

    fn parent_entry_id_for_inode(&self, ino: u64) -> Result<Option<u64>, libc::c_int> {
        if ino == ROOT_INO {
            return Ok(None);
        }
        let path = self.entry_path_for_ino(ino)?;
        let resolved = self.repo.resolve_path(&path).map_err(|_| EIO)?;
        match (resolved.kind.as_deref(), resolved.entry_id) {
            (Some("dir"), entry_id) => Ok(entry_id),
            _ => Err(ENOENT),
        }
    }

    fn remove_cached_path(&self, path: &str) {
        if let Ok(mut guard) = self.path_to_inode.write() {
            if let Some(ino) = guard.remove(path) {
                if let Ok(mut inode_guard) = self.inode_to_path.write() {
                    inode_guard.remove(&ino);
                }
            }
        }
    }

    fn move_cached_path(&self, old_path: &str, new_path: &str, ino: u64) {
        self.remove_cached_path(old_path);
        self.register_path(new_path, ino);
        if let Ok(mut guard) = self.fh_to_path.lock() {
            for value in guard.values_mut() {
                if value == old_path {
                    *value = new_path.to_string();
                }
            }
        }
    }

    fn attrs_for_path(&self, path: &str) -> Result<Option<ParsedAttrs>, libc::c_int> {
        let blob = self.repo.fetch_path_attrs_blob(path).map_err(|_| EIO)?;
        let Some(blob) = blob else {
            return Ok(None);
        };
        let fields = Self::decode_nul_fields(&blob);
        if fields.is_empty() {
            return Ok(None);
        }
        let obj_type = fields[0].clone();
        let root_uid = unsafe { libc::geteuid() } as u32;
        let root_gid = unsafe { libc::getegid() } as u32;
        let file_attr = if obj_type == "symlink" {
            if fields.len() < 9 {
                return Err(EIO);
            }
            let raw_inode = fields[1].parse::<u64>().map_err(|_| EIO)?;
            let target = fields[2].clone();
            let mod_date = fields[3].clone();
            let acc_date = fields[4].clone();
            let chg_date = fields[5].clone();
            let uid = fields[6].parse::<u32>().unwrap_or(root_uid);
            let gid = fields[7].parse::<u32>().unwrap_or(root_gid);
            let inode_seed = fields[8].clone();
            let inode = self.stable_inode(&obj_type, &inode_seed, raw_inode);
            FileAttr {
                ino: inode,
                size: target.len() as u64,
                blocks: self.block_count(target.len() as u64, "symlink"),
                atime: Self::parse_time(&acc_date),
                mtime: Self::parse_time(&mod_date),
                ctime: Self::parse_time(&chg_date),
                crtime: Self::parse_time(&chg_date),
                kind: FileType::Symlink,
                perm: 0o777,
                nlink: 1,
                uid,
                gid,
                rdev: 0,
                flags: 0,
                blksize: self.block_size as u32,
            }
        } else {
            if fields.len() < 10 {
                return Err(EIO);
            }
            let raw_inode = fields[1].parse::<u64>().map_err(|_| EIO)?;
            let size = fields[2].parse::<u64>().unwrap_or(0);
            let mode = fields[3].clone();
            let mod_date = fields[4].clone();
            let acc_date = fields[5].clone();
            let chg_date = fields[6].clone();
            let uid = fields[7].parse::<u32>().unwrap_or(root_uid);
            let gid = fields[8].parse::<u32>().unwrap_or(root_gid);
            let inode_seed = fields[9].clone();
            let mut kind = if obj_type == "dir" { FileType::Directory } else { FileType::RegularFile };
            let mut perm = u16::from_str_radix(mode.trim_start_matches("0o"), 8).unwrap_or(0o644) as u16;
            let mut rdev = 0;
            if obj_type == "hardlink" {
                let file_id = self.repo.get_hardlink_file_id(raw_inode).map_err(|_| EIO)?;
                if let Some(file_id) = file_id {
                    if let Some((special_type, major, minor)) = self.repo.get_special_file_metadata(file_id).map_err(|_| EIO)? {
                        kind = Self::file_type_from_special(&special_type);
                        rdev = ((major as u64) << 8) | (minor as u64);
                    }
                }
            } else if let Some((special_type, major, minor)) = self.repo.get_special_file_metadata(raw_inode).map_err(|_| EIO)? {
                kind = Self::file_type_from_special(&special_type);
                rdev = ((major as u64) << 8) | (minor as u64);
            }
            if obj_type == "dir" && perm == 0o644 {
                perm = 0o755;
            }
            let inode = self.stable_inode(&obj_type, &inode_seed, raw_inode);
            let nlink = match obj_type.as_str() {
                "hardlink" => {
                    let file_id = self.repo.get_hardlink_file_id(raw_inode).map_err(|_| EIO)?;
                    let file_id = file_id.unwrap_or(raw_inode);
                    self.repo.count_file_links(file_id).map_err(|_| EIO)?
                }
                "file" => self.repo.count_file_links(raw_inode).map_err(|_| EIO)?,
                "dir" => 2 + self.repo.count_directory_subdirs(raw_inode).map_err(|_| EIO)?,
                _ => 1,
            };
            FileAttr {
                ino: inode,
                size,
                blocks: self.block_count(size, &obj_type),
                atime: Self::parse_time(&acc_date),
                mtime: Self::parse_time(&mod_date),
                ctime: Self::parse_time(&chg_date),
                crtime: Self::parse_time(&chg_date),
                kind,
                perm,
                nlink: nlink.try_into().unwrap_or(u32::MAX),
                uid,
                gid,
                rdev: rdev.try_into().unwrap_or(u32::MAX),
                flags: 0,
                blksize: self.block_size as u32,
            }
        };
        Ok(Some(ParsedAttrs { file_attr }))
    }

    fn block_count(&self, size: u64, kind: &str) -> u64 {
        if kind == "dir" {
            return 1;
        }
        let block_size = self.block_size.max(1);
        1 + size.saturating_sub(1) / block_size
    }

    fn lookup_path(&self, path: &str) -> Result<Option<ParsedAttrs>, libc::c_int> {
        let path = Self::normalize_path(path);
        if path == "/" {
            return Ok(Some(self.root_attr()));
        }
        self.attrs_for_path(&path)
    }

    fn root_attr(&self) -> ParsedAttrs {
        let now = SystemTime::now();
        let child_dirs = self.repo.count_root_directory_children().unwrap_or(0);
        ParsedAttrs {
            file_attr: FileAttr {
                ino: ROOT_INO,
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: (2 + child_dirs).try_into().unwrap_or(u32::MAX),
                uid: unsafe { libc::geteuid() } as u32,
                gid: unsafe { libc::getegid() } as u32,
                rdev: 0,
                flags: 0,
                blksize: self.block_size as u32,
            },
        }
    }

    fn inode_for_path(&self, path: &str) -> Option<u64> {
        self.path_to_inode.read().ok().and_then(|cache| cache.get(path).copied())
    }

    fn path_for_inode(&self, ino: u64) -> Option<String> {
        self.inode_to_path.read().ok().and_then(|cache| cache.get(&ino).cloned())
    }

    fn register_path(&self, path: &str, ino: u64) {
        if let Ok(mut cache) = self.path_to_inode.write() {
            cache.insert(path.to_string(), ino);
        }
        if let Ok(mut cache) = self.inode_to_path.write() {
            cache.insert(ino, path.to_string());
        }
    }

    fn next_handle(&self) -> u64 {
        let mut guard = self.next_fh.lock().unwrap();
        let fh = *guard;
        *guard += 1;
        fh
    }

    fn current_time() -> SystemTime {
        SystemTime::now()
    }

    fn system_time_to_db_string(value: SystemTime) -> String {
        let dt = DateTime::<Utc>::from(value);
        dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()
    }

    fn time_or_now_to_db_string(value: Option<TimeOrNow>) -> Option<String> {
        match value {
            Some(TimeOrNow::SpecificTime(time)) => Some(Self::system_time_to_db_string(time)),
            Some(TimeOrNow::Now) => Some(Self::system_time_to_db_string(Self::current_time())),
            None => None,
        }
    }

    fn new_write_state(file_id: u64, file_size: u64, truncate_pending: bool) -> WriteState {
        WriteState {
            file_id,
            file_size,
            truncate_pending,
            buffered_bytes: 0,
            blocks: BTreeMap::new(),
        }
    }

    fn load_write_block(&self, state: &mut WriteState, block_index: u64) -> Vec<u8> {
        if let Some(block) = state.blocks.get(&block_index) {
            return block.clone();
        }
        match self.repo.load_block(state.file_id, block_index, self.block_size) {
            Ok(Some(block)) => block,
            _ => vec![0u8; self.block_size as usize],
        }
    }

    fn update_write_buffer(&self, state: &mut WriteState, offset: u64, data: &[u8]) {
        let block_size = self.block_size.max(1);
        let end = offset.saturating_add(data.len() as u64);
        if end > state.file_size {
            state.file_size = end;
        }
        if data.is_empty() {
            return;
        }
        let first_block = offset / block_size;
        let last_block = (end.saturating_sub(1)) / block_size;
        let mut src_cursor = 0usize;
        for block_index in first_block..=last_block {
            let block_start = block_index * block_size;
            let block_end = block_start.saturating_add(block_size);
            let write_start = offset.max(block_start);
            let write_end = end.min(block_end);
            if write_end <= write_start {
                continue;
            }
            let mut block = self.load_write_block(state, block_index);
            if block.len() < block_size as usize {
                block.resize(block_size as usize, 0);
            }
            let block_slice_start = (write_start - block_start) as usize;
            let block_slice_end = (write_end - block_start) as usize;
            let src_len = block_slice_end.saturating_sub(block_slice_start);
            let src_end = src_cursor.saturating_add(src_len);
            if src_end > data.len() {
                break;
            }
            block[block_slice_start..block_slice_end].copy_from_slice(&data[src_cursor..src_end]);
            state.blocks.insert(block_index, block);
            src_cursor = src_end;
        }
    }

    fn flush_write_state(&self, state: &mut WriteState) -> Result<(), libc::c_int> {
        let block_size = self.block_size.max(1);
        let total_blocks = if state.file_size == 0 {
            0
        } else {
            1 + (state.file_size - 1) / block_size
        };
        let mut blocks = Vec::new();
        for (block_index, block) in state.blocks.iter() {
            let used_len = if *block_index >= total_blocks {
                continue;
            } else if *block_index == total_blocks.saturating_sub(1) {
                let tail = state.file_size % block_size;
                if tail == 0 { block_size } else { tail }
            } else {
                block_size
            };
            blocks.push(PersistBlockRow {
                block_index: *block_index,
                data: block.as_slice(),
                used_len,
            });
        }
        self.repo
            .persist_file_blocks(
                state.file_id,
                state.file_size,
                block_size,
                total_blocks,
                state.truncate_pending,
                &blocks,
            )
            .map_err(|_| EIO)?;
        state.truncate_pending = false;
        state.buffered_bytes = 0;
        state.blocks.clear();
        Ok(())
    }

    fn create_handle_for_file(
        &self,
        path: String,
        file_id: u64,
        file_size: u64,
        truncate_pending: bool,
        flags: i32,
        writable: bool,
    ) -> u64 {
        let fh = self.next_handle();
        if let Ok(mut guard) = self.fh_to_path.lock() {
            guard.insert(fh, path);
        }
        if let Ok(mut guard) = self.fh_to_file_id.lock() {
            guard.insert(fh, file_id);
        }
        if let Ok(mut guard) = self.fh_to_flags.lock() {
            guard.insert(fh, flags);
        }
        if writable {
            let _ = (file_id, file_size, truncate_pending);
        }
        let _ = flags;
        fh
    }

    fn read_from_write_state(
        &self,
        state: &mut WriteState,
        offset: u64,
        size: u64,
    ) -> Vec<u8> {
        let block_size = self.block_size.max(1);
        if offset >= state.file_size {
            return Vec::new();
        }
        let end_offset = (offset + size).min(state.file_size);
        let mut output = vec![0u8; (end_offset - offset) as usize];
        let first_block = offset / block_size;
        let last_block = (end_offset.saturating_sub(1)) / block_size;
        for block_index in first_block..=last_block {
            let block_start = block_index * block_size;
            let block_end = block_start.saturating_add(block_size);
            let read_start = offset.max(block_start);
            let read_end = end_offset.min(block_end);
            if read_end <= read_start {
                continue;
            }
            let block = self.load_write_block(state, block_index);
            let block_slice_start = (read_start - block_start) as usize;
            let block_slice_end = (read_end - block_start) as usize;
            let out_start = (read_start - offset) as usize;
            let out_end = out_start + block_slice_end - block_slice_start;
            output[out_start..out_end].copy_from_slice(&block[block_slice_start..block_slice_end]);
        }
        output
    }

    fn write_state_for_handle(&self, fh: u64) -> Option<WriteState> {
        self.write_states
            .lock()
            .ok()
            .and_then(|guard| guard.get(&fh).cloned())
    }

    fn update_write_state(&self, fh: u64, state: WriteState) {
        if let Ok(mut guard) = self.write_states.lock() {
            guard.insert(fh, state);
        }
    }

    fn remove_handle_state(&self, fh: u64) {
        if let Ok(mut guard) = self.fh_to_path.lock() {
            guard.remove(&fh);
        }
        if let Ok(mut guard) = self.fh_to_file_id.lock() {
            guard.remove(&fh);
        }
        if let Ok(mut guard) = self.fh_to_flags.lock() {
            guard.remove(&fh);
        }
        if let Ok(mut guard) = self.write_states.lock() {
            guard.remove(&fh);
        }
    }

    fn clear_locks_for_owner(&self, owner: u64) {
        if let Ok(mut guard) = self.posix_locks.lock() {
            let keys: Vec<String> = guard
                .iter_mut()
                .filter_map(|(resource_key, records)| {
                    records.retain(|record| record.owner != owner);
                    if records.is_empty() {
                        Some(resource_key.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for key in keys {
                guard.remove(&key);
            }
        }
    }

}

impl Filesystem for DbfsFuse {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        let _ = config.add_capabilities(FUSE_POSIX_LOCKS | FUSE_FLOCK_LOCKS);
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(parent_path) = self.path_for_inode(parent) else {
            reply.error(ENOENT);
            return;
        };
        let child_path = Self::join_path(&parent_path, name);
        match self.lookup_path(&child_path) {
            Ok(Some(attrs)) => {
                self.register_path(&child_path, attrs.file_attr.ino);
                reply.entry(&TTL, &attrs.file_attr, 0);
            }
            Ok(None) => reply.error(ENOENT),
            Err(errno) => reply.error(errno),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let path = if ino == ROOT_INO {
            "/".to_string()
        } else {
            match self.path_for_inode(ino) {
                Some(path) => path,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        match self.lookup_path(&path) {
            Ok(Some(attrs)) => {
                self.register_path(&path, attrs.file_attr.ino);
                reply.attr(&TTL, &attrs.file_attr);
            }
            Ok(None) => reply.error(ENOENT),
            Err(errno) => reply.error(errno),
        }
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        let path = if ino == ROOT_INO {
            "/".to_string()
        } else {
            match self.path_for_inode(ino) {
                Some(path) => path,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        let blob = match self.repo.list_directory_entries_blob(&path) {
            Ok(Some(blob)) => blob,
            Ok(None) => {
                reply.ok();
                return;
            }
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let entries = Self::decode_nul_fields(&blob);
        let mut next_offset = 1i64;
        if offset == 0 {
            let _ = reply.add(ino, 1, FileType::Directory, ".");
            let parent_ino = if ino == ROOT_INO {
                ROOT_INO
            } else {
                let parent_path = path.rsplit_once('/').map(|(parent, _)| if parent.is_empty() { "/" } else { parent }).unwrap_or("/");
                self.inode_for_path(parent_path).unwrap_or(ROOT_INO)
            };
            let _ = reply.add(parent_ino, 2, FileType::Directory, "..");
            next_offset = 2;
        }
        for (index, name) in entries.into_iter().enumerate().skip(offset.max(0) as usize) {
            let child_path = Self::join_path(&path, OsStr::from_bytes(name.as_bytes()));
            match self.lookup_path(&child_path) {
                Ok(Some(attrs)) => {
                    self.register_path(&child_path, attrs.file_attr.ino);
                    let kind = attrs.file_attr.kind;
                    let added = reply.add(attrs.file_attr.ino, (index + 3) as i64, kind, name);
                    if added {
                        break;
                    }
                }
                _ => {
                    continue;
                }
            }
            next_offset = (index + 3) as i64;
        }
        let _ = next_offset;
        reply.ok();
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let path = match self.path_for_inode(ino) {
            Some(path) => path,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        match self.repo.resolve_path(&path) {
            Ok(resolved) => {
                if let Some("symlink") = resolved.kind.as_deref() {
                    let symlink_id = match resolved.entry_id {
                        Some(value) => value,
                        None => {
                            reply.error(ENOENT);
                            return;
                        }
                    };
                    match self.repo.load_symlink_target(symlink_id) {
                        Ok(Some(target)) => reply.data(target.as_bytes()),
                        Ok(None) => reply.error(ENOENT),
                        Err(_) => reply.error(EIO),
                    }
                } else {
                    reply.error(ENOENT);
                }
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        let files = self.repo.count_files().unwrap_or(0);
        let dirs = self.repo.count_directories().unwrap_or(0);
        let total_data_size = self.repo.total_data_size().unwrap_or(0);
        let blocks = (total_data_size + self.block_size.saturating_sub(1)) / self.block_size;
        reply.statfs(
            blocks,
            blocks.saturating_sub(1),
            blocks.saturating_sub(1),
            files + dirs,
            0,
            self.block_size as u32,
            self.block_size as u32,
            255,
        );
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        if position != 0 {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        let path = match self.entry_path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let name = name.to_string_lossy().to_string();
        if !self.selinux_enabled && name == "security.selinux" {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        if !self.acl_enabled && name == "system.posix_acl_access" {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        let owner = match self.xattr_owner_for_path(&path) {
            Ok(Some(owner)) => owner,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        if flags & libc::XATTR_CREATE != 0 {
            if self.repo.fetch_xattr_value(&path, &name).ok().flatten().is_some() {
                reply.error(libc::EEXIST);
                return;
            }
        }
        if flags & libc::XATTR_REPLACE != 0 {
            if self.repo.fetch_xattr_value(&path, &name).ok().flatten().is_none() {
                reply.error(libc::ENODATA);
                return;
            }
        }
        let result = self.repo.store_xattr_value_for_owner(&owner.0, owner.1, &name, value);
        match result {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(EIO),
        }
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let path = match self.entry_path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let name = name.to_string_lossy().to_string();
        match self.repo.fetch_xattr_value(&path, &name) {
            Ok(Some(value)) => {
                if size == 0 {
                    reply.size(value.len() as u32);
                } else if size < value.len() as u32 {
                    reply.error(libc::ERANGE);
                } else {
                    reply.data(&value);
                }
            }
            Ok(None) => reply.error(libc::ENODATA),
            Err(_) => reply.error(EIO),
        }
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        let path = match self.entry_path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let owner = match self.xattr_owner_for_path(&path) {
            Ok(Some(owner)) => owner,
            Ok(None) => {
                reply.size(0);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self.repo.list_xattr_names_for_owner(&owner.0, owner.1) {
            Ok(names) => {
                let mut payload = Vec::new();
                for name in names {
                    payload.extend_from_slice(name.as_bytes());
                    payload.push(0);
                }
                if size == 0 {
                    reply.size(payload.len() as u32);
                } else if size < payload.len() as u32 {
                    reply.error(libc::ERANGE);
                } else {
                    reply.data(&payload);
                }
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn removexattr(&mut self, _req: &Request<'_>, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let path = match self.entry_path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let owner = match self.xattr_owner_for_path(&path) {
            Ok(Some(owner)) => owner,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let name = name.to_string_lossy().to_string();
        match self.repo.remove_xattr_for_owner(&owner.0, owner.1, &name) {
            Ok(deleted) if deleted > 0 => reply.ok(),
            Ok(_) => reply.error(libc::ENODATA),
            Err(_) => reply.error(EIO),
        }
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        let path = match self.entry_path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let attrs = match self.lookup_path(&path) {
            Ok(Some(attrs)) => attrs.file_attr,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self.acl_allows(&path, &attrs, mask) {
            Ok(true) => reply.ok(),
            Ok(false) => reply.error(libc::EACCES),
            Err(errno) => reply.error(errno),
        }
    }

    fn ioctl(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: u32,
        cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: ReplyIoctl,
    ) {
        if cmd != libc::FIONREAD as u32 {
            reply.error(ENOTTY);
            return;
        }
        let path = match self.path_for_inode(ino) {
            Some(path) => path,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let file_id = match self.file_id_for_handle(fh, ino) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self.repo.resolve_path(&path) {
            Ok(resolved) if matches!(resolved.kind.as_deref(), Some("file") | Some("hardlink")) => {}
            Ok(_) => {
                reply.error(ENOTTY);
                return;
            }
            Err(_) => {
                reply.error(EIO);
                return;
            }
        }
        let size = match self.repo.file_size(file_id) {
            Ok(Some(value)) => value,
            Ok(None) => 0,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let available = size.min(u32::MAX as u64) as u32;
        reply.ioctl(0, &available.to_ne_bytes());
    }

    fn poll(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _kh: u64,
        _events: u32,
        _flags: u32,
        reply: ReplyPoll,
    ) {
        let path = match self.path_for_inode(ino) {
            Some(path) => path,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let kind = match self.repo.resolve_path(&path) {
            Ok(resolved) => resolved.kind.unwrap_or_default(),
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        if kind != "file" && kind != "hardlink" {
            reply.error(ENOTTY);
            return;
        }
        let file_id = match self.file_id_for_handle(fh, ino) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let size = match self.repo.file_size(file_id) {
            Ok(Some(value)) => value,
            Ok(None) => 0,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let mut revents = 0u32;
        if size > 0 {
            revents |= POLLIN as u32;
        }
        if !self.read_only {
            revents |= POLLOUT as u32;
        }
        reply.poll(revents);
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let path = match self.path_for_inode(ino) {
            Some(path) => path,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let file_id = match self.file_id_for_path(&path) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let attrs = match self.lookup_path(&path) {
            Ok(Some(attrs)) => attrs.file_attr,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let access_mode = match flags & libc::O_ACCMODE {
            libc::O_WRONLY => libc::W_OK,
            libc::O_RDWR => libc::R_OK | libc::W_OK,
            _ => libc::R_OK,
        };
        if !self.acl_allows(&path, &attrs, access_mode).unwrap_or(false) {
            reply.error(libc::EACCES);
            return;
        }
        if self.read_only && (flags & libc::O_ACCMODE) != libc::O_RDONLY {
            reply.error(libc::EROFS);
            return;
        }
        let writable = (flags & libc::O_ACCMODE) != libc::O_RDONLY;
        let fh = self.create_handle_for_file(path, file_id, 0, false, flags, writable);
        reply.opened(fh, 0);
    }

    fn getlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        reply: ReplyLock,
    ) {
        let path = match self.path_for_inode(ino) {
            Some(path) => path,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let kind = match self.repo.resolve_path(&path) {
            Ok(resolved) => resolved.kind.unwrap_or_default(),
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        if kind != "file" && kind != "hardlink" {
            reply.error(ENOENT);
            return;
        }
        let owner = lock_owner;
        if let Some(conflict) = self.get_posix_lock_conflict(&self.resource_key_for_lock(&path).unwrap_or_default(), owner, typ, start, end) {
            reply.locked(conflict.start, conflict.end.unwrap_or(0), conflict.typ, conflict.pid);
        } else {
            reply.locked(start, end, libc::F_UNLCK, 0);
        }
    }

    fn setlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep_flag: bool,
        reply: ReplyEmpty,
    ) {
        let path = match self.path_for_inode(ino) {
            Some(path) => path,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let kind = match self.repo.resolve_path(&path) {
            Ok(resolved) => resolved.kind.unwrap_or_default(),
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        if kind != "file" && kind != "hardlink" {
            reply.error(ENOENT);
            return;
        }
        let resource_key = match self.resource_key_for_lock(&path) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let owner = lock_owner;
        if typ == libc::F_UNLCK {
            self.set_posix_lock(&resource_key, owner, typ, start, end, pid);
            reply.ok();
            return;
        }
        if self.read_only && typ == libc::F_WRLCK {
            reply.error(libc::EROFS);
            return;
        }

        loop {
            if self.get_posix_lock_conflict(&resource_key, owner, typ, start, end).is_none() {
                self.set_posix_lock(&resource_key, owner, typ, start, end, pid);
                reply.ok();
                return;
            }
            if !sleep_flag {
                reply.error(libc::EWOULDBLOCK);
                return;
            }
            sleep(Duration::from_millis(100));
        }
    }

    fn bmap(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        blocksize: u32,
        idx: u64,
        reply: ReplyBmap,
    ) {
        if blocksize == 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let path = match self.path_for_inode(ino) {
            Some(path) => path,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let file_id = match self.file_id_for_path(&path) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let size = match self.repo.file_size(file_id) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let logical_blocks = if size == 0 {
            0
        } else {
            (size + u64::from(blocksize) - 1) / u64::from(blocksize)
        }
        .max(1);
        if idx >= logical_blocks {
            reply.error(libc::EINVAL);
            return;
        }
        reply.bmap(idx);
    }

    fn flush(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let Some(mut state) = self.write_state_for_handle(fh) else {
            reply.ok();
            return;
        };
        if let Err(errno) = self.flush_write_state(&mut state) {
            reply.error(errno);
            return;
        }
        self.update_write_state(fh, state);
        self.clear_locks_for_owner(lock_owner);
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let file_id = match self.file_id_for_handle(fh, ino) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let offset = offset.max(0) as u64;
        let size = size as u64;
        if let Some(mut state) = self.write_state_for_handle(fh) {
            let data = self.read_from_write_state(&mut state, offset, size);
            reply.data(&data);
            return;
        }
        let file_size = match self.repo.file_size(file_id) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.data(&[]);
                return;
            }
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        if offset >= file_size {
            reply.data(&[]);
            return;
        }
        let end_offset = (offset + size).min(file_size);
        let first_block = offset / self.block_size;
        let last_block = (end_offset.saturating_sub(1)) / self.block_size;
        match self.repo.assemble_file_slice(
            file_id,
            first_block,
            last_block,
            offset,
            end_offset,
            self.block_size,
        ) {
            Ok(data) => reply.data(&data),
            Err(_) => reply.error(EIO),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if self.read_only {
            if let Some(owner) = lock_owner {
                self.clear_locks_for_owner(owner);
            }
            self.remove_handle_state(fh);
            reply.ok();
            return;
        }
        if let Some(mut state) = self.write_state_for_handle(fh) {
            if self.flush_write_state(&mut state).is_ok() {
                self.update_write_state(fh, state);
            }
        }
        if let Some(owner) = lock_owner {
            self.clear_locks_for_owner(owner);
        }
        self.remove_handle_state(fh);
        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if self.read_only && size.is_some() {
            reply.error(libc::EROFS);
            return;
        }
        let path = match self.entry_path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let (kind, entry_id) = match self.resolved_entry_for_path(&path) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let kind = kind.unwrap_or_default();
        let current_attrs = match self.lookup_path(&path) {
            Ok(Some(attrs)) => attrs.file_attr,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        if let Some(new_size) = size {
            if kind == "dir" {
                reply.error(libc::EISDIR);
                return;
            }
            let file_id = match self.file_id_for_path(&path) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    reply.error(ENOENT);
                    return;
                }
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            if let Some(fh) = fh {
                let mut state = self
                    .write_state_for_handle(fh)
                    .unwrap_or_else(|| Self::new_write_state(file_id, new_size, true));
                state.file_id = file_id;
                state.file_size = new_size;
                state.truncate_pending = true;
                self.update_write_state(fh, state);
            } else {
                let mut state = Self::new_write_state(file_id, new_size, true);
                if let Err(errno) = self.flush_write_state(&mut state) {
                    reply.error(errno);
                    return;
                }
            }
        }

        if let Some(new_mode) = mode {
            let mode_text = format!("{:o}", new_mode & 0o7777);
            let result = match kind.as_str() {
                "file" | "hardlink" => {
                    let file_id = match self.file_id_for_path(&path) {
                        Ok(Some(value)) => value,
                        Ok(None) => {
                            reply.error(ENOENT);
                            return;
                        }
                        Err(errno) => {
                            reply.error(errno);
                            return;
                        }
                    };
                    self.repo.update_file_mode(file_id, &mode_text)
                }
                "dir" => match entry_id {
                    Some(directory_id) => self.repo.update_directory_mode(directory_id, &mode_text),
                    None => Err("missing directory id".to_string()),
                },
                "symlink" => Ok(()),
                _ => Ok(()),
            };
            if result.is_err() {
                reply.error(EIO);
                return;
            }
        }

        if uid.is_some() || gid.is_some() {
            let new_uid = uid.unwrap_or_else(|| unsafe { libc::geteuid() } as u32);
            let new_gid = gid.unwrap_or_else(|| unsafe { libc::getegid() } as u32);
            let default_mode = if kind == "dir" { 0o755 } else { 0o644 };
            let mode_text = match self.raw_mode_for_path(&path) {
                Ok(Some(value)) => value,
                _ => format!("{:o}", mode.unwrap_or(default_mode) & 0o7777),
            };
            let result = match kind.as_str() {
                "file" | "hardlink" => {
                    let file_id = match self.file_id_for_path(&path) {
                        Ok(Some(value)) => value,
                        Ok(None) => {
                            reply.error(ENOENT);
                            return;
                        }
                        Err(errno) => {
                            reply.error(errno);
                            return;
                        }
                    };
                    self.repo.update_file_owner(file_id, new_uid, new_gid, &mode_text)
                }
                "dir" => match entry_id {
                    Some(directory_id) => self.repo.update_directory_owner(directory_id, new_uid, new_gid, &mode_text),
                    None => Err("missing directory id".to_string()),
                },
                "symlink" => match entry_id {
                    Some(symlink_id) => self.repo.update_symlink_owner(symlink_id, new_uid, new_gid),
                    None => Err("missing symlink id".to_string()),
                },
                _ => Ok(()),
            };
            if result.is_err() {
                reply.error(EIO);
                return;
            }
        }

        if atime.is_some() || mtime.is_some() {
            let atime_needs_update = match atime {
                Some(TimeOrNow::SpecificTime(time)) => time != current_attrs.atime,
                Some(TimeOrNow::Now) => true,
                None => false,
            };
            let mtime_needs_update = match mtime {
                Some(TimeOrNow::SpecificTime(time)) => time != current_attrs.mtime,
                Some(TimeOrNow::Now) => true,
                None => false,
            };
            if !atime_needs_update && !mtime_needs_update {
                match self.lookup_path(&path) {
                    Ok(Some(attrs)) => reply.attr(&TTL, &attrs.file_attr),
                    Ok(None) => reply.error(ENOENT),
                    Err(errno) => reply.error(errno),
                }
                return;
            }
            let atime_text = Self::time_or_now_to_db_string(atime).unwrap_or_else(|| Self::system_time_to_db_string(Self::current_time()));
            let mtime_text = Self::time_or_now_to_db_string(mtime).unwrap_or_else(|| Self::system_time_to_db_string(Self::current_time()));
            let result = match kind.as_str() {
                "file" | "hardlink" => {
                    let file_id = match self.file_id_for_path(&path) {
                        Ok(Some(value)) => value,
                        Ok(None) => {
                            reply.error(ENOENT);
                            return;
                        }
                        Err(errno) => {
                            reply.error(errno);
                            return;
                        }
                    };
                    self.repo.touch_file_times(file_id, &atime_text, &mtime_text)
                }
                "dir" => match entry_id {
                    Some(directory_id) => self.repo.touch_directory_times(directory_id, &atime_text, &mtime_text),
                    None => Err("missing directory id".to_string()),
                },
                "symlink" => match entry_id {
                    Some(symlink_id) => self.repo.touch_symlink_entry(symlink_id),
                    None => Err("missing symlink id".to_string()),
                },
                _ => Ok(()),
            };
            if result.is_err() {
                reply.error(EIO);
                return;
            }
        }

        match self.lookup_path(&path) {
            Ok(Some(attrs)) => reply.attr(&TTL, &attrs.file_attr),
            Ok(None) => reply.error(ENOENT),
            Err(errno) => reply.error(errno),
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let parent_path = match self.entry_path_for_ino(parent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let child_path = Self::join_path(&parent_path, name);
        if let Ok(Some(_)) = self.lookup_path(&child_path) {
            reply.error(libc::EEXIST);
            return;
        }
        let parent_id = match self.parent_entry_id_for_inode(parent) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let mut mode = mode & !umask;
        if req.uid() != 0 {
            mode &= !(libc::S_ISUID | libc::S_ISGID) as u32;
        }
        match self.repo.create_directory(parent_id, name.to_string_lossy().as_ref(), mode, req.uid(), req.gid(), &child_path) {
            Ok(directory_id) => {
                let _ = self.copy_default_acl_to_child(&parent_path, "dir", directory_id, true);
                let _ = self.append_journal_event("mkdir", &child_path, None, Some(directory_id));
                match self.lookup_path(&child_path) {
                    Ok(Some(attrs)) => {
                        self.register_path(&child_path, attrs.file_attr.ino);
                        reply.entry(&TTL, &attrs.file_attr, 0);
                    }
                    Ok(None) => reply.error(EIO),
                    Err(errno) => reply.error(errno),
                }
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let parent_path = match self.entry_path_for_ino(parent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let child_path = Self::join_path(&parent_path, name);
        let (kind, entry_id) = match self.resolved_entry_for_path(&child_path) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let result = match kind.as_deref() {
            Some("file") => {
                let file_id = match self.file_id_for_path(&child_path) {
                    Ok(Some(value)) => value,
                    Ok(None) => {
                        reply.error(ENOENT);
                        return;
                    }
                    Err(errno) => {
                        reply.error(errno);
                        return;
                    }
                };
                let entry_attrs = match self.lookup_path(&child_path) {
                    Ok(Some(attrs)) => attrs.file_attr,
                    _ => {
                        reply.error(EIO);
                        return;
                    }
                };
                if let Err(errno) = self.enforce_sticky_bit(&parent_path, &entry_attrs) {
                    reply.error(errno);
                    return;
                }
                self.repo.purge_primary_file(file_id)
            }
            Some("hardlink") => {
                let entry_attrs = match self.lookup_path(&child_path) {
                    Ok(Some(attrs)) => attrs.file_attr,
                    _ => {
                        reply.error(EIO);
                        return;
                    }
                };
                if let Err(errno) = self.enforce_sticky_bit(&parent_path, &entry_attrs) {
                    reply.error(errno);
                    return;
                }
                match entry_id {
                    Some(hardlink_id) => self.repo.delete_hardlink_entry(hardlink_id),
                    None => Err("missing hardlink id".to_string()),
                }
            }
            Some("symlink") => {
                let entry_attrs = match self.lookup_path(&child_path) {
                    Ok(Some(attrs)) => attrs.file_attr,
                    _ => {
                        reply.error(EIO);
                        return;
                    }
                };
                if let Err(errno) = self.enforce_sticky_bit(&parent_path, &entry_attrs) {
                    reply.error(errno);
                    return;
                }
                match entry_id {
                    Some(symlink_id) => self.repo.delete_symlink_entry(symlink_id),
                    None => Err("missing symlink id".to_string()),
                }
            }
            Some("dir") => {
                reply.error(libc::EISDIR);
                return;
            }
            _ => {
                reply.error(ENOENT);
                return;
            }
        };
        match result {
            Ok(_) => {
                let file_id = self.file_id_for_path(&child_path).ok().flatten();
                let dir_id = if parent == ROOT_INO { None } else { self.parent_entry_id_for_inode(parent).ok().flatten() };
                let _ = self.append_journal_event("unlink", &child_path, file_id, dir_id);
                self.remove_cached_path(&child_path);
                reply.ok();
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let parent_path = match self.entry_path_for_ino(parent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let child_path = Self::join_path(&parent_path, name);
        let (kind, entry_id) = match self.resolved_entry_for_path(&child_path) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        if kind.as_deref() != Some("dir") {
            reply.error(libc::ENOTDIR);
            return;
        }
        let entry_attrs = match self.lookup_path(&child_path) {
            Ok(Some(attrs)) => attrs.file_attr,
            _ => {
                reply.error(EIO);
                return;
            }
        };
        if let Err(errno) = self.enforce_sticky_bit(&parent_path, &entry_attrs) {
            reply.error(errno);
            return;
        }
        match self.repo.list_directory_entries_blob(&child_path) {
            Ok(Some(blob)) if !blob.is_empty() => {
                reply.error(libc::ENOTEMPTY);
                return;
            }
            Err(_) => {
                reply.error(EIO);
                return;
            }
            _ => {}
        }
        match entry_id {
            Some(directory_id) => match self.repo.delete_directory_entry(directory_id) {
                Ok(_) => {
                    let _ = self.append_journal_event("rmdir", &child_path, None, Some(directory_id));
                    self.remove_cached_path(&child_path);
                    reply.ok();
                }
                Err(_) => reply.error(EIO),
            },
            None => reply.error(ENOENT),
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let old_parent_path = match self.entry_path_for_ino(parent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let new_parent_path = match self.entry_path_for_ino(newparent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let old_path = Self::join_path(&old_parent_path, name);
        let new_path = Self::join_path(&new_parent_path, newname);
        let (kind, entry_id) = match self.resolved_entry_for_path(&old_path) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let new_parent_id = match self.parent_entry_id_for_inode(newparent) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let new_name = newname.to_string_lossy().to_string();
        let old_ino = self.lookup_path(&old_path).ok().flatten().map(|attrs| attrs.file_attr.ino).unwrap_or(ROOT_INO);
        if old_path != new_path {
            let existing = match self.resolved_entry_for_path(&new_path) {
                Ok(value) => value,
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            if let Some(existing_kind) = existing.0.as_deref() {
                let removal_result = match existing_kind {
                    "file" => match self.file_id_for_path(&new_path) {
                        Ok(Some(file_id)) => self.repo.purge_primary_file(file_id),
                        Ok(None) => Err("missing file id".to_string()),
                        Err(errno) => return reply.error(errno),
                    },
                    "hardlink" => match existing.1 {
                        Some(hardlink_id) => self.repo.delete_hardlink_entry(hardlink_id),
                        None => Err("missing hardlink id".to_string()),
                    },
                    "symlink" => match existing.1 {
                        Some(symlink_id) => self.repo.delete_symlink_entry(symlink_id),
                        None => Err("missing symlink id".to_string()),
                    },
                    "dir" => match (kind.as_deref(), existing.1) {
                        (Some("dir"), Some(directory_id)) => match self.repo.count_directory_children(directory_id) {
                            Ok(0) => self.repo.delete_directory_entry(directory_id),
                            Ok(_) => Err("target directory not empty".to_string()),
                            Err(_) => Err("failed to inspect target directory".to_string()),
                        },
                        (Some("dir"), None) => Err("missing directory id".to_string()),
                        _ => {
                            reply.error(libc::EISDIR);
                            return;
                        }
                    },
                    _ => Ok(()),
                };
                if removal_result.is_err() {
                    if matches!(existing_kind, "dir") && matches!(kind.as_deref(), Some("dir")) {
                        if let Some(directory_id) = existing.1 {
                            if let Ok(count) = self.repo.count_directory_children(directory_id) {
                                reply.error(if count == 0 { EIO } else { ENOTEMPTY });
                                return;
                            }
                        }
                    }
                    reply.error(EIO);
                    return;
                }
                self.remove_cached_path(&new_path);
            }
        }
        let result = match kind.as_deref() {
            Some("file") => match self.file_id_for_path(&old_path) {
                Ok(Some(file_id)) => self.repo.rename_file_entry(file_id, new_parent_id, &new_name),
                Ok(None) => Err("missing file id".to_string()),
                Err(errno) => return reply.error(errno),
            },
            Some("hardlink") => match entry_id {
                Some(hardlink_id) => self.repo.rename_hardlink_entry(hardlink_id, new_parent_id, &new_name),
                None => Err("missing hardlink id".to_string()),
            },
            Some("symlink") => match entry_id {
                Some(symlink_id) => self.repo.rename_symlink_entry(symlink_id, new_parent_id, &new_name),
                None => Err("missing symlink id".to_string()),
            },
            Some("dir") => match entry_id {
                Some(directory_id) => self.repo.rename_directory_entry(directory_id, new_parent_id, &new_name),
                None => Err("missing directory id".to_string()),
            },
            _ => Err("unsupported rename kind".to_string()),
        };
        match result {
            Ok(_) => {
                let _ = self.append_journal_event("rename", &format!("{old_path}->{new_path}"), None, None);
                self.move_cached_path(&old_path, &new_path, old_ino);
                reply.ok();
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let parent_path = match self.entry_path_for_ino(parent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let child_path = Self::join_path(&parent_path, name);
        if let Ok(Some(_)) = self.lookup_path(&child_path) {
            reply.error(libc::EEXIST);
            return;
        }
        let parent_id = match self.parent_entry_id_for_inode(parent) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let mut mode = mode & !umask;
        if req.uid() != 0 {
            mode &= !(libc::S_ISUID | libc::S_ISGID) as u32;
        }
        let file_id = match self.repo.create_file(parent_id, name.to_string_lossy().as_ref(), mode, req.uid(), req.gid(), &child_path) {
            Ok(file_id) => file_id,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let _ = self.copy_default_acl_to_child(&parent_path, "file", file_id, false);
        let fh = self.create_handle_for_file(child_path.clone(), file_id, 0, false, flags, true);
        match self.lookup_path(&child_path) {
            Ok(Some(attrs)) => {
                self.register_path(&child_path, attrs.file_attr.ino);
                let _ = self.append_journal_event("create", &child_path, Some(file_id), None);
                reply.created(&TTL, &attrs.file_attr, 0, fh, flags as u32);
            }
            Ok(None) => reply.error(EIO),
            Err(errno) => reply.error(errno),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let file_id = match self.file_id_for_handle(fh, ino) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let offset = offset.max(0) as u64;
        if data.is_empty() {
            reply.written(0);
            return;
        }
        let existing_size = self.repo.file_size(file_id).ok().flatten().unwrap_or(0);
        let end_offset = offset.saturating_add(data.len() as u64);
        if self.write_state_for_handle(fh).is_none() && offset <= existing_size && end_offset <= existing_size {
            let first_block = offset / self.block_size;
            let last_block = (end_offset.saturating_sub(1)) / self.block_size;
            if let Ok(existing) = self.repo.assemble_file_slice(
                file_id,
                first_block,
                last_block,
                offset,
                end_offset,
                self.block_size,
            ) {
                if existing == data {
                    reply.written(data.len() as u32);
                    return;
                }
            }
        }
        let mut state = self
            .write_state_for_handle(fh)
            .unwrap_or_else(|| Self::new_write_state(file_id, existing_size, false));
        state.file_id = file_id;
        self.update_write_buffer(&mut state, offset, data);
        state.buffered_bytes = state.buffered_bytes.saturating_add(data.len() as u64);
        if self.write_flush_threshold_bytes > 0 && state.buffered_bytes >= self.write_flush_threshold_bytes {
            if let Err(errno) = self.flush_write_state(&mut state) {
                reply.error(errno);
                return;
            }
        }
        self.update_write_state(fh, state);
        reply.written(data.len() as u32);
    }

    fn copy_file_range(
        &mut self,
        _req: &Request<'_>,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        _flags: u32,
        reply: ReplyWrite,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        if offset_in < 0 || offset_out < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        if len == 0 {
            reply.written(0);
            return;
        }
        let src_file_id = match self.file_id_for_handle(fh_in, ino_in) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let dst_file_id = match self.file_id_for_handle(fh_out, ino_out) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let src_size = match self.repo.file_size(src_file_id) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let src_offset = offset_in as u64;
        if src_offset >= src_size {
            reply.written(0);
            return;
        }
        let copy_len = len.min(src_size - src_offset);
        if copy_len == 0 {
            reply.written(0);
            return;
        }
        let end_offset = src_offset.saturating_add(copy_len);
        let first_block = src_offset / self.block_size;
        let last_block = (end_offset.saturating_sub(1)) / self.block_size;
        let data = match self.repo.assemble_file_slice(
            src_file_id,
            first_block,
            last_block,
            src_offset,
            end_offset,
            self.block_size,
        ) {
            Ok(data) => data,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        let dst_offset = offset_out as u64;
        let mut state = self
            .write_state_for_handle(fh_out)
            .unwrap_or_else(|| Self::new_write_state(dst_file_id, self.repo.file_size(dst_file_id).ok().flatten().unwrap_or(0), false));
        state.file_id = dst_file_id;
        self.update_write_buffer(&mut state, dst_offset, &data);
        state.buffered_bytes = state.buffered_bytes.saturating_add(data.len() as u64);
        if let Err(errno) = self.flush_write_state(&mut state) {
            reply.error(errno);
            return;
        }
        self.update_write_state(fh_out, state);
        reply.written(copy_len as u32);
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let file_type = mode & libc::S_IFMT as u32;
        if file_type == libc::S_IFREG as u32 {
            let parent_path = match self.entry_path_for_ino(parent) {
                Ok(path) => path,
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            let child_path = Self::join_path(&parent_path, name);
            if let Ok(Some(_)) = self.lookup_path(&child_path) {
                reply.error(libc::EEXIST);
                return;
            }
            let parent_id = match self.parent_entry_id_for_inode(parent) {
                Ok(value) => value,
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            let mut mode = mode & !umask;
            if req.uid() != 0 {
                mode &= !(libc::S_ISUID | libc::S_ISGID) as u32;
            }
            match self.repo.create_file(parent_id, name.to_string_lossy().as_ref(), mode, req.uid(), req.gid(), &child_path) {
                Ok(_) => match self.lookup_path(&child_path) {
                    Ok(Some(attrs)) => {
                        self.register_path(&child_path, attrs.file_attr.ino);
                        reply.entry(&TTL, &attrs.file_attr, 0);
                    }
                    Ok(None) => reply.error(EIO),
                    Err(errno) => reply.error(errno),
                },
                Err(_) => reply.error(EIO),
            }
            return;
        }
        let parent_path = match self.entry_path_for_ino(parent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let child_path = Self::join_path(&parent_path, name);
        if let Ok(Some(_)) = self.lookup_path(&child_path) {
            reply.error(libc::EEXIST);
            return;
        }
        let parent_id = match self.parent_entry_id_for_inode(parent) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let mut mode = mode & !umask;
        if req.uid() != 0 {
            mode &= !(libc::S_ISUID | libc::S_ISGID) as u32;
        }
        let file_kind = if file_type == libc::S_IFIFO as u32 {
            "fifo"
        } else if file_type == libc::S_IFCHR as u32 {
            "char"
        } else if file_type == libc::S_IFBLK as u32 {
            "block"
        } else {
            reply.error(libc::EINVAL);
            return;
        };
        match self.repo.create_special_file(
            parent_id,
            name.to_string_lossy().as_ref(),
            mode,
            req.uid(),
            req.gid(),
            &child_path,
            file_kind,
            libc::major(rdev as libc::dev_t) as u32,
            libc::minor(rdev as libc::dev_t) as u32,
        ) {
            Ok(_) => {}
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        match self.lookup_path(&child_path) {
            Ok(Some(attrs)) => {
                self.register_path(&child_path, attrs.file_attr.ino);
                reply.entry(&TTL, &attrs.file_attr, 0);
            }
            Ok(None) => reply.error(EIO),
            Err(errno) => reply.error(errno),
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let parent_path = match self.entry_path_for_ino(parent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let child_path = Self::join_path(&parent_path, link_name);
        if let Ok(Some(_)) = self.lookup_path(&child_path) {
            reply.error(libc::EEXIST);
            return;
        }
        let parent_id = match self.parent_entry_id_for_inode(parent) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let inode_seed = child_path.as_str();
        match self.repo.create_symlink(parent_id, link_name.to_string_lossy().as_ref(), &target.to_string_lossy(), req.uid(), req.gid(), inode_seed) {
            Ok(_) => match self.lookup_path(&child_path) {
                Ok(Some(attrs)) => {
                    self.register_path(&child_path, attrs.file_attr.ino);
                    reply.entry(&TTL, &attrs.file_attr, 0);
                }
                Ok(None) => reply.error(EIO),
                Err(errno) => reply.error(errno),
            },
            Err(_) => reply.error(EIO),
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }
        let source_path = match self.entry_path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let source_file_id = match self.file_id_for_path(&source_path) {
            Ok(Some(value)) => value,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let new_parent_path = match self.entry_path_for_ino(newparent) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let child_path = Self::join_path(&new_parent_path, newname);
        if let Ok(Some(_)) = self.lookup_path(&child_path) {
            reply.error(libc::EEXIST);
            return;
        }
        let new_parent_id = match self.parent_entry_id_for_inode(newparent) {
            Ok(value) => value,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        match self.repo.create_hardlink(source_file_id, new_parent_id, newname.to_string_lossy().as_ref(), unsafe { libc::geteuid() } as u32, unsafe { libc::getegid() } as u32) {
            Ok(_) => match self.lookup_path(&child_path) {
                Ok(Some(attrs)) => {
                    self.register_path(&child_path, attrs.file_attr.ino);
                    reply.entry(&TTL, &attrs.file_attr, 0);
                }
                Ok(None) => reply.error(EIO),
                Err(errno) => reply.error(errno),
            },
            Err(_) => reply.error(EIO),
        }
    }
}
