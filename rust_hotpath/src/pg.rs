use crate::crc32_bytes;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use std::ffi::{CStr, CString};
use std::fmt::Write as _;
use std::os::raw::{c_char, c_int, c_uint};
use std::sync::Mutex;

#[repr(C)]
struct PGconn {
    _private: [u8; 0],
}

#[repr(C)]
struct PGresult {
    _private: [u8; 0],
}

const CONNECTION_OK: c_int = 0;
const PGRES_TUPLES_OK: c_int = 2;
const PGRES_COMMAND_OK: c_int = 1;

#[link(name = "pq")]
unsafe extern "C" {
    fn PQconnectdb(conninfo: *const c_char) -> *mut PGconn;
    fn PQstatus(conn: *const PGconn) -> c_int;
    fn PQerrorMessage(conn: *const PGconn) -> *const c_char;
    fn PQexec(conn: *mut PGconn, command: *const c_char) -> *mut PGresult;
    fn PQexecParams(
        conn: *mut PGconn,
        command: *const c_char,
        nParams: c_int,
        paramTypes: *const c_uint,
        paramValues: *const *const c_char,
        paramLengths: *const c_int,
        paramFormats: *const c_int,
        resultFormat: c_int,
    ) -> *mut PGresult;
    fn PQresultStatus(res: *const PGresult) -> c_int;
    fn PQntuples(res: *const PGresult) -> c_int;
    fn PQnfields(res: *const PGresult) -> c_int;
    fn PQgetvalue(res: *const PGresult, row_number: c_int, field_number: c_int) -> *const c_char;
    fn PQclear(res: *mut PGresult);
    fn PQfinish(conn: *mut PGconn);
}

fn conn_error(conn: *const PGconn) -> String {
    if conn.is_null() {
        return "libpq returned a null connection".to_string();
    }
    unsafe {
        let error = PQerrorMessage(conn);
        if error.is_null() {
            return "postgres connection error".to_string();
        }
        CStr::from_ptr(error).to_string_lossy().trim().to_string()
    }
}

fn connect(conninfo: &str) -> Result<*mut PGconn, String> {
    let conninfo = CString::new(conninfo).map_err(|_| "connection string contains NUL byte".to_string())?;
    unsafe {
        let conn = PQconnectdb(conninfo.as_ptr());
        if conn.is_null() {
            return Err("failed to create PostgreSQL connection".to_string());
        }
        if PQstatus(conn) != CONNECTION_OK {
            let err = conn_error(conn);
            PQfinish(conn);
            return Err(err);
        }
        Ok(conn)
    }
}

unsafe fn fetch_single_text(res: *mut PGresult) -> Result<String, String> {
    let value = match PQresultStatus(res) {
        PGRES_TUPLES_OK => {
            let rows = PQntuples(res);
            let cols = PQnfields(res);
            if rows < 1 || cols < 1 {
                Ok(String::new())
            } else {
                let value_ptr = PQgetvalue(res, 0, 0);
                if value_ptr.is_null() {
                    Ok(String::new())
                } else {
                    Ok(CStr::from_ptr(value_ptr).to_string_lossy().to_string())
                }
            }
        }
        _ => Err("unexpected PostgreSQL result status".to_string()),
    };
    PQclear(res);
    value
}

unsafe fn fetch_first_row_texts(res: *mut PGresult) -> Result<Vec<String>, String> {
    let result = match PQresultStatus(res) {
        PGRES_TUPLES_OK => {
            let rows = PQntuples(res);
            let cols = PQnfields(res);
            if rows < 1 || cols < 1 {
                Ok(Vec::new())
            } else {
                let mut values = Vec::with_capacity(cols as usize);
                for col in 0..cols {
                    let value_ptr = PQgetvalue(res, 0, col);
                    if value_ptr.is_null() {
                        values.push(String::new());
                    } else {
                        values.push(CStr::from_ptr(value_ptr).to_string_lossy().to_string());
                    }
                }
                Ok(values)
            }
        }
        _ => Err("unexpected PostgreSQL result status".to_string()),
    };
    PQclear(res);
    result
}

unsafe fn fetch_first_column_texts(res: *mut PGresult) -> Result<Vec<String>, String> {
    let result = match PQresultStatus(res) {
        PGRES_TUPLES_OK => {
            let rows = PQntuples(res);
            if rows < 1 {
                Ok(Vec::new())
            } else {
                let cols = PQnfields(res);
                if cols < 1 {
                    Ok(Vec::new())
                } else {
                    let mut values = Vec::with_capacity(rows as usize);
                    for row in 0..rows {
                        let value_ptr = PQgetvalue(res, row, 0);
                        if value_ptr.is_null() {
                            values.push(String::new());
                        } else {
                            values.push(CStr::from_ptr(value_ptr).to_string_lossy().to_string());
                        }
                    }
                    Ok(values)
                }
            }
        }
        _ => Err("unexpected PostgreSQL result status".to_string()),
    };
    PQclear(res);
    result
}

fn join_nul_text(values: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            out.push(0);
        }
        out.extend_from_slice(value.as_bytes());
    }
    out
}

unsafe fn exec_command(conn: *mut PGconn, sql: &CString) -> Result<(), String> {
    let res = PQexec(conn, sql.as_ptr());
    if res.is_null() {
        return Err(conn_error(conn));
    }
    let status = PQresultStatus(res);
    PQclear(res);
    if status == PGRES_COMMAND_OK {
        Ok(())
    } else {
        Err(conn_error(conn))
    }
}

unsafe fn exec_params(conn: *mut PGconn, sql: &CString, params: &[&CString]) -> Result<*mut PGresult, String> {
    let param_values = params.iter().map(|value| value.as_ptr()).collect::<Vec<_>>();
    let param_lengths = params
        .iter()
        .map(|value| value.as_bytes().len() as c_int)
        .collect::<Vec<_>>();
    let param_formats = vec![0 as c_int; params.len()];
    let res = PQexecParams(
        conn,
        sql.as_ptr(),
        params.len() as c_int,
        std::ptr::null(),
        param_values.as_ptr(),
        param_lengths.as_ptr(),
        param_formats.as_ptr(),
        0,
    );
    if res.is_null() {
        Err(conn_error(conn))
    } else {
        Ok(res)
    }
}

unsafe fn exec_command_params(conn: *mut PGconn, sql: &CString, params: &[&CString]) -> Result<(), String> {
    let res = exec_params(conn, sql, params)?;
    let status = PQresultStatus(res);
    PQclear(res);
    if status == PGRES_COMMAND_OK {
        Ok(())
    } else {
        Err(conn_error(conn))
    }
}

fn hex_encode_bytes(data: &[u8]) -> String {
    let mut text = String::with_capacity(data.len() * 2);
    for byte in data {
        let _ = write!(&mut text, "{:02x}", byte);
    }
    text
}

unsafe fn transactional<T, F>(conn: *mut PGconn, mut f: F) -> Result<T, String>
where
    F: FnMut(*mut PGconn) -> Result<T, String>,
{
    let begin = CString::new("BEGIN").map_err(|_| "SQL contains NUL byte".to_string())?;
    let commit = CString::new("COMMIT").map_err(|_| "SQL contains NUL byte".to_string())?;
    let rollback = CString::new("ROLLBACK").map_err(|_| "SQL contains NUL byte".to_string())?;

    exec_command(conn, &begin)?;
    match f(conn) {
        Ok(value) => {
            if let Err(err) = exec_command(conn, &commit) {
                let _ = exec_command(conn, &rollback);
                Err(err)
            } else {
                Ok(value)
            }
        }
        Err(err) => {
            let _ = exec_command(conn, &rollback);
            Err(err)
        }
    }
}

pub struct DbRepo {
    conninfo: String,
    cached_conn: Mutex<Option<usize>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupSnapshot {
    pub block_size: Option<u32>,
    pub is_in_recovery: bool,
    pub schema_version: Option<u32>,
    pub schema_is_initialized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPath {
    pub parent_id: Option<u64>,
    pub kind: Option<String>,
    pub entry_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistBlockRow<'a> {
    pub block_index: u64,
    pub data: &'a [u8],
    pub used_len: u64,
}

impl DbRepo {
    pub fn new(conninfo: &str) -> Result<Self, String> {
        if conninfo.is_empty() {
            return Err("connection string is empty".to_string());
        }
        Ok(Self {
            conninfo: conninfo.to_string(),
            cached_conn: Mutex::new(None),
        })
    }

    fn with_cached_connection<T, F>(&self, f: F) -> Result<T, String>
    where
        F: FnOnce(*mut PGconn) -> Result<T, String>,
    {
        let conn = {
            let mut cached = self
                .cached_conn
                .lock()
                .map_err(|_| "connection cache is poisoned".to_string())?;
            match cached.take() {
                Some(value) => value as *mut PGconn,
                None => connect(&self.conninfo)?,
            }
        };

        let result = f(conn);
        match result {
            Ok(value) => {
                let mut cached = self
                    .cached_conn
                    .lock()
                    .map_err(|_| "connection cache is poisoned".to_string())?;
                if cached.is_none() {
                    *cached = Some(conn as usize);
                } else {
                    unsafe {
                        PQfinish(conn);
                    }
                }
                Ok(value)
            }
            Err(err) => {
                unsafe {
                    PQfinish(conn);
                }
                Err(err)
            }
        }
    }

    fn file_data_object_id_on_conn(&self, conn: *mut PGconn, file_id: u64) -> Result<Option<u64>, String> {
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        let sql = CString::new("SELECT data_object_id FROM files WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;

        unsafe {
            let params = [&file_id];
            let res = exec_params(conn, &sql, &params)?;
            let text = fetch_single_text(res)?;
            if text.is_empty() {
                return Ok(None);
            }
            let value = text
                .parse::<u64>()
                .map_err(|_| "invalid data_object_id value".to_string())?;
            Ok(Some(value))
        }
    }

    pub fn file_data_object_id(&self, file_id: u64) -> Result<Option<u64>, String> {
        self.with_cached_connection(|conn| self.file_data_object_id_on_conn(conn, file_id))
    }

    pub fn file_size(&self, file_id: u64) -> Result<Option<u64>, String> {
        let sql = CString::new("SELECT size FROM files WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&file_id];
            let res = exec_params(conn, &sql, &params)?;
            let text = fetch_single_text(res)?;
            if text.is_empty() {
                Ok(None)
            } else {
                let value = text
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| "invalid file size value".to_string())?;
                Ok(Some(value))
            }
        })
    }

    pub fn load_block(&self, file_id: u64, block_index: u64, block_size: u64) -> Result<Option<Vec<u8>>, String> {
        let sql = CString::new(
            "
            SELECT encode(data, 'base64')
            FROM data_blocks
            WHERE data_object_id = $1 AND _order = $2
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let data_object_id = match self.file_data_object_id(file_id)? {
            Some(value) => value,
            None => return Ok(None),
        };
        let data_object_id = CString::new(data_object_id.to_string())
            .map_err(|_| "data object id contains NUL byte".to_string())?;
        let block_index = CString::new(block_index.to_string())
            .map_err(|_| "block index contains NUL byte".to_string())?;
        let block_size = block_size.max(1) as usize;

        self.with_cached_connection(|conn| unsafe {
            let params = [&data_object_id, &block_index];
            let res = exec_params(conn, &sql, &params)?;
            let text = fetch_single_text(res)?;
            if text.is_empty() {
                return Ok(None);
            }
            let mut bytes = BASE64_STANDARD
                .decode(text.trim())
                .map_err(|_| "invalid base64 block data".to_string())?;
            if bytes.len() < block_size {
                bytes.resize(block_size, 0);
            } else if bytes.len() > block_size {
                bytes.truncate(block_size);
            }
            Ok(Some(bytes))
        })
    }

    pub fn fetch_block_range(
        &self,
        file_id: u64,
        first_block: u64,
        last_block: u64,
        block_size: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, String> {
        let mut blocks = Vec::new();
        if last_block < first_block {
            return Ok(blocks);
        }

        let block_len = block_size.max(1) as usize;
        for block_index in first_block..=last_block {
            match self.load_block(file_id, block_index, block_size)? {
                Some(block) => blocks.push((block_index, block)),
                None => blocks.push((block_index, vec![0u8; block_len])),
            }
        }

        Ok(blocks)
    }

    pub fn assemble_file_slice(
        &self,
        file_id: u64,
        first_block: u64,
        last_block: u64,
        offset: u64,
        end_offset: u64,
        block_size: u64,
    ) -> Result<Vec<u8>, String> {
        let blocks = self.fetch_block_range(file_id, first_block, last_block, block_size)?;
        Ok(crate::assemble_read_slice(
            first_block,
            last_block,
            offset,
            end_offset,
            block_size,
            &blocks,
        ))
    }

    pub fn create_data_object(&self, file_size: u64, content_hash: Option<&str>) -> Result<u64, String> {
        let sql = if content_hash.is_some() {
            CString::new(
                "INSERT INTO data_objects (file_size, content_hash, reference_count, creation_date, modification_date) \
                 VALUES ($1, $2, 1, NOW(), NOW()) RETURNING id_data_object",
            )
            .map_err(|_| "SQL contains NUL byte".to_string())?
        } else {
            CString::new(
                "INSERT INTO data_objects (file_size, content_hash, reference_count, creation_date, modification_date) \
                 VALUES ($1, NULL, 1, NOW(), NOW()) RETURNING id_data_object",
            )
            .map_err(|_| "SQL contains NUL byte".to_string())?
        };

        let file_size = CString::new(file_size.to_string()).map_err(|_| "file size contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let res = match content_hash {
                Some(value) => {
                    let content_hash = CString::new(value)
                        .map_err(|_| "content hash contains NUL byte".to_string())?;
                    let params = [&file_size, &content_hash];
                    exec_params(conn, &sql, &params)?
                }
                None => {
                    let params = [&file_size];
                    exec_params(conn, &sql, &params)?
                }
            };
            let text = fetch_single_text(res)?;
            let value = text
                .trim()
                .parse::<u64>()
                .map_err(|_| "invalid id_data_object value".to_string())?;
            Ok(value)
        })
    }

    pub fn touch_data_object(&self, data_object_id: u64, file_size: Option<u64>) -> Result<bool, String> {
        let sql = match file_size {
            Some(_) => CString::new(
                "UPDATE data_objects SET file_size = $1, modification_date = NOW() WHERE id_data_object = $2",
            )
            .map_err(|_| "SQL contains NUL byte".to_string())?,
            None => CString::new(
                "UPDATE data_objects SET modification_date = NOW() WHERE id_data_object = $1",
            )
            .map_err(|_| "SQL contains NUL byte".to_string())?,
        };

        let data_object_id = CString::new(data_object_id.to_string())
            .map_err(|_| "data object id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let status = match file_size {
                Some(value) => {
                    let file_size = CString::new(value.to_string())
                        .map_err(|_| "file size contains NUL byte".to_string())?;
                    let params = [&file_size, &data_object_id];
                    exec_command_params(conn, &sql, &params)
                }
                None => {
                    let params = [&data_object_id];
                    exec_command_params(conn, &sql, &params)
                }
            };
            match status {
                Ok(()) => Ok(true),
                Err(_) => Err("failed to update data object".to_string()),
            }
        })
    }

    pub fn query_scalar_text(&self, sql: &str) -> Result<String, String> {
        let sql = CString::new(sql).map_err(|_| "SQL contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let result = {
                let res = PQexec(conn, sql.as_ptr());
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    fetch_single_text(res)
                }
            };
            result
        })
    }

    pub fn query_config_value(&self, key: &str) -> Result<Option<String>, String> {
        let sql = CString::new("SELECT value FROM config WHERE key = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let key = CString::new(key).map_err(|_| "config key contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values = [key.as_ptr()];
                let param_lengths = [key.as_bytes().len() as c_int];
                let param_formats = [0 as c_int];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    1,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    Some(CStr::from_ptr(value_ptr).to_string_lossy().to_string())
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn is_in_recovery(&self) -> Result<bool, String> {
        let value = self.query_scalar_text("SELECT pg_is_in_recovery()")?;
        Ok(matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "t" | "true" | "1" | "on"
        ))
    }

    pub fn schema_version(&self) -> Result<Option<u32>, String> {
        let value = self.query_scalar_text(
            "SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1",
        )?;
        if value.trim().is_empty() {
            return Ok(None);
        }
        value
            .trim()
            .parse::<u32>()
            .map(Some)
            .map_err(|err| format!("invalid schema version returned by PostgreSQL: {err}"))
    }

    pub fn schema_is_initialized(&self) -> Result<bool, String> {
        let value = self.query_scalar_text(
            "SELECT to_regclass('public.directories') IS NOT NULL AND to_regclass('public.files') IS NOT NULL AND to_regclass('public.schema_version') IS NOT NULL",
        )?;
        Ok(matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "t" | "true" | "1" | "on"
        ))
    }

    pub fn startup_snapshot(&self) -> Result<StartupSnapshot, String> {
        let block_size = self
            .query_config_value("block_size")?
            .and_then(|value| value.trim().parse::<u32>().ok());
        let is_in_recovery = self.is_in_recovery()?;
        let schema_version = self.schema_version()?;
        let schema_is_initialized = self.schema_is_initialized()?;

        Ok(StartupSnapshot {
            block_size,
            is_in_recovery,
            schema_version,
            schema_is_initialized,
        })
    }

    pub fn ensure_lock_schema(&self) -> Result<(), String> {
        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                let statements = [
                    CString::new(
                        "
                        CREATE TABLE IF NOT EXISTS lock_leases (
                            id_lock SERIAL PRIMARY KEY,
                            resource_kind VARCHAR(20) NOT NULL,
                            resource_id BIGINT NOT NULL,
                            owner_key BIGINT NOT NULL,
                            lease_kind VARCHAR(20) NOT NULL,
                            lock_type INTEGER NOT NULL,
                            lease_expires_at TIMESTAMP NOT NULL,
                            heartbeat_at TIMESTAMP NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            UNIQUE(resource_kind, resource_id, owner_key, lease_kind)
                        )
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                    CString::new(
                        "
                        CREATE INDEX IF NOT EXISTS idx_lock_leases_resource
                        ON lock_leases (resource_kind, resource_id, lease_kind)
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                    CString::new(
                        "
                        CREATE INDEX IF NOT EXISTS idx_lock_leases_expires
                        ON lock_leases (lease_expires_at)
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                    CString::new(
                        "
                        CREATE TABLE IF NOT EXISTS lock_range_leases (
                            id_lock SERIAL PRIMARY KEY,
                            resource_kind VARCHAR(20) NOT NULL,
                            resource_id BIGINT NOT NULL,
                            owner_key BIGINT NOT NULL,
                            lock_type INTEGER NOT NULL,
                            range_start BIGINT NOT NULL,
                            range_end BIGINT NULL,
                            lease_expires_at TIMESTAMP NOT NULL,
                            heartbeat_at TIMESTAMP NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                        )
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                    CString::new(
                        "
                        CREATE INDEX IF NOT EXISTS idx_lock_range_leases_resource
                        ON lock_range_leases (resource_kind, resource_id)
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                    CString::new(
                        "
                        CREATE INDEX IF NOT EXISTS idx_lock_range_leases_expires
                        ON lock_range_leases (lease_expires_at)
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                ];
                for statement in statements.iter() {
                    exec_command(conn, statement)?;
                }
                Ok(())
            })
        })
    }

    pub fn prune_lock_leases(&self, resource_kind: Option<&str>, resource_id: Option<u64>) -> Result<(), String> {
        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                match (resource_kind, resource_id) {
                    (None, None) => {
                        let sql = CString::new("DELETE FROM lock_leases WHERE lease_expires_at <= NOW()")
                            .map_err(|_| "SQL contains NUL byte".to_string())?;
                        exec_command(conn, &sql)?;
                    }
                    (Some(resource_kind), Some(resource_id)) => {
                        let sql = CString::new(
                            "
                            DELETE FROM lock_leases
                            WHERE resource_kind = $1
                              AND resource_id = $2
                              AND lease_kind = $3
                              AND lease_expires_at <= NOW()
                            ",
                        )
                        .map_err(|_| "SQL contains NUL byte".to_string())?;
                        let resource_kind = CString::new(resource_kind)
                            .map_err(|_| "resource kind contains NUL byte".to_string())?;
                        let resource_id = CString::new(resource_id.to_string())
                            .map_err(|_| "resource id contains NUL byte".to_string())?;
                        let lease_kind = CString::new("flock")
                            .map_err(|_| "lease kind contains NUL byte".to_string())?;
                        let params = [&resource_kind, &resource_id, &lease_kind];
                        exec_command_params(conn, &sql, &params)?;
                    }
                    _ => return Err("resource kind and resource id must be provided together".to_string()),
                }
                Ok(())
            })
        })
    }

    pub fn delete_lock_lease(&self, resource_kind: &str, resource_id: u64, owner_key: u64) -> Result<(), String> {
        let sql = CString::new(
            "
            DELETE FROM lock_leases
            WHERE resource_kind = $1
              AND resource_id = $2
              AND owner_key = $3
              AND lease_kind = $4
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let resource_kind = CString::new(resource_kind).map_err(|_| "resource kind contains NUL byte".to_string())?;
        let resource_id = CString::new(resource_id.to_string()).map_err(|_| "resource id contains NUL byte".to_string())?;
        let owner_key = CString::new(owner_key.to_string()).map_err(|_| "owner key contains NUL byte".to_string())?;
        let lease_kind = CString::new("flock").map_err(|_| "lease kind contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&resource_kind, &resource_id, &owner_key, &lease_kind];
            exec_command_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn prune_lock_range_leases(&self, resource_kind: Option<&str>, resource_id: Option<u64>) -> Result<(), String> {
        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                match (resource_kind, resource_id) {
                    (None, None) => {
                        let sql = CString::new("DELETE FROM lock_range_leases WHERE lease_expires_at <= NOW()")
                            .map_err(|_| "SQL contains NUL byte".to_string())?;
                        exec_command(conn, &sql)?;
                    }
                    (Some(resource_kind), Some(resource_id)) => {
                        let sql = CString::new(
                            "
                            DELETE FROM lock_range_leases
                            WHERE resource_kind = $1
                              AND resource_id = $2
                              AND lease_expires_at <= NOW()
                            ",
                        )
                        .map_err(|_| "SQL contains NUL byte".to_string())?;
                        let resource_kind = CString::new(resource_kind)
                            .map_err(|_| "resource kind contains NUL byte".to_string())?;
                        let resource_id = CString::new(resource_id.to_string())
                            .map_err(|_| "resource id contains NUL byte".to_string())?;
                        let params = [&resource_kind, &resource_id];
                        exec_command_params(conn, &sql, &params)?;
                    }
                    _ => return Err("resource kind and resource id must be provided together".to_string()),
                }
                Ok(())
            })
        })
    }

    pub fn delete_range_leases(&self, resource_kind: &str, resource_id: u64, owner_key: Option<u64>) -> Result<(), String> {
        let (sql, params): (CString, Vec<CString>) = if let Some(owner_key) = owner_key {
            (
                CString::new(
                    "
                    DELETE FROM lock_range_leases
                    WHERE resource_kind = $1
                      AND resource_id = $2
                      AND owner_key = $3
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(resource_kind).map_err(|_| "resource kind contains NUL byte".to_string())?,
                    CString::new(resource_id.to_string()).map_err(|_| "resource id contains NUL byte".to_string())?,
                    CString::new(owner_key.to_string()).map_err(|_| "owner key contains NUL byte".to_string())?,
                ],
            )
        } else {
            (
                CString::new(
                    "
                    DELETE FROM lock_range_leases
                    WHERE resource_kind = $1
                      AND resource_id = $2
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(resource_kind).map_err(|_| "resource kind contains NUL byte".to_string())?,
                    CString::new(resource_id.to_string()).map_err(|_| "resource id contains NUL byte".to_string())?,
                ],
            )
        };

        self.with_cached_connection(|conn| unsafe {
            let params_ref = params.iter().collect::<Vec<_>>();
            exec_command_params(conn, &sql, &params_ref).map(|_| ())
        })
    }

    pub fn try_advisory_xact_lock(&self, resource_lock_id: i64) -> Result<bool, String> {
        let sql = format!("SELECT pg_try_advisory_xact_lock({resource_lock_id})");
        let value = self.query_scalar_text(&sql)?;
        Ok(matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "t" | "true" | "1" | "on"
        ))
    }

    pub fn acquire_flock_lease(
        &self,
        resource_kind: &str,
        resource_id: u64,
        owner_key: u64,
        requested_type: i32,
        lease_ttl_seconds: u64,
        resource_lock_id: i64,
    ) -> Result<bool, String> {
        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                let lock_granted = self.try_advisory_xact_lock(resource_lock_id)?;
                if !lock_granted {
                    return Ok(false);
                }

                let prune_sql = CString::new(
                    "
                    DELETE FROM lock_leases
                    WHERE resource_kind = $1
                      AND resource_id = $2
                      AND lease_kind = $3
                      AND lease_expires_at <= NOW()
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?;
                let resource_kind = CString::new(resource_kind)
                    .map_err(|_| "resource kind contains NUL byte".to_string())?;
                let resource_id = CString::new(resource_id.to_string())
                    .map_err(|_| "resource id contains NUL byte".to_string())?;
                let lease_kind = CString::new("flock").map_err(|_| "lease kind contains NUL byte".to_string())?;
                let prune_params = [&resource_kind, &resource_id, &lease_kind];
                exec_command_params(conn, &prune_sql, &prune_params)?;

                let conflict_sql = CString::new(
                    "
                    SELECT lock_type
                    FROM lock_leases
                    WHERE resource_kind = $1
                      AND resource_id = $2
                      AND lease_kind = $3
                      AND lease_expires_at > NOW()
                      AND owner_key <> $4
                    ORDER BY owner_key
                    LIMIT 1
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?;
                let owner_key = CString::new(owner_key.to_string())
                    .map_err(|_| "owner key contains NUL byte".to_string())?;
                let conflict_params = [&resource_kind, &resource_id, &lease_kind, &owner_key];
                let res = exec_params(conn, &conflict_sql, &conflict_params)?;
                let conflict = fetch_single_text(res)?;
                if !conflict.trim().is_empty() {
                    let other_type = conflict.trim().parse::<i32>().unwrap_or(0);
                    let blocked = match requested_type {
                        1 => other_type == 2,
                        2 => other_type == 1 || other_type == 2,
                        _ => false,
                    };
                    if blocked {
                        return Ok(false);
                    }
                }

                let upsert_sql = CString::new(
                    "
                    INSERT INTO lock_leases (
                        resource_kind,
                        resource_id,
                        owner_key,
                        lease_kind,
                        lock_type,
                        lease_expires_at,
                        heartbeat_at,
                        created_at,
                        updated_at
                    ) VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        NOW() + ($6 || ' seconds')::interval,
                        NOW(),
                        NOW(),
                        NOW()
                    )
                    ON CONFLICT (resource_kind, resource_id, owner_key, lease_kind)
                    DO UPDATE SET
                        lock_type = EXCLUDED.lock_type,
                        lease_expires_at = EXCLUDED.lease_expires_at,
                        heartbeat_at = EXCLUDED.heartbeat_at,
                        updated_at = NOW()
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?;
                let requested_type = CString::new(requested_type.to_string())
                    .map_err(|_| "lock type contains NUL byte".to_string())?;
                let lease_ttl_seconds = CString::new(lease_ttl_seconds.to_string())
                    .map_err(|_| "lease ttl contains NUL byte".to_string())?;
                let upsert_params = [
                    &resource_kind,
                    &resource_id,
                    &owner_key,
                    &lease_kind,
                    &requested_type,
                    &lease_ttl_seconds,
                ];
                exec_command_params(conn, &upsert_sql, &upsert_params)?;
                Ok(true)
            })
        })
    }

    pub fn release_flock_lease(&self, resource_kind: &str, resource_id: u64, owner_key: u64) -> Result<(), String> {
        self.delete_lock_lease(resource_kind, resource_id, owner_key)?;
        self.delete_range_leases(resource_kind, resource_id, Some(owner_key))?;
        Ok(())
    }

    pub fn heartbeat_lock_lease(
        &self,
        resource_kind: &str,
        resource_id: u64,
        owner_key: u64,
        lease_ttl_seconds: u64,
    ) -> Result<(), String> {
        let sql = CString::new(
            "
            UPDATE lock_leases
            SET lease_expires_at = NOW() + ($4 || ' seconds')::interval,
                heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE resource_kind = $1
              AND resource_id = $2
              AND owner_key = $3
              AND lease_kind = $5
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let resource_kind = CString::new(resource_kind).map_err(|_| "resource kind contains NUL byte".to_string())?;
        let resource_id = CString::new(resource_id.to_string()).map_err(|_| "resource id contains NUL byte".to_string())?;
        let owner_key = CString::new(owner_key.to_string()).map_err(|_| "owner key contains NUL byte".to_string())?;
        let lease_ttl_seconds = CString::new(lease_ttl_seconds.to_string())
            .map_err(|_| "lease ttl contains NUL byte".to_string())?;
        let lease_kind = CString::new("flock").map_err(|_| "lease kind contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&resource_kind, &resource_id, &owner_key, &lease_ttl_seconds, &lease_kind];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn heartbeat_lock_range_lease(
        &self,
        resource_kind: &str,
        resource_id: u64,
        owner_key: u64,
        range_start: u64,
        range_end: Option<u64>,
        lease_ttl_seconds: u64,
    ) -> Result<(), String> {
        let sql = CString::new(
            if range_end.is_some() {
                "
                UPDATE lock_range_leases
                SET lease_expires_at = NOW() + ($5 || ' seconds')::interval,
                    heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE resource_kind = $1
                  AND resource_id = $2
                  AND owner_key = $3
                  AND range_start = $4
                  AND range_end = $6
                "
            } else {
                "
                UPDATE lock_range_leases
                SET lease_expires_at = NOW() + ($5 || ' seconds')::interval,
                    heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE resource_kind = $1
                  AND resource_id = $2
                  AND owner_key = $3
                  AND range_start = $4
                  AND range_end IS NULL
                "
            },
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let resource_kind = CString::new(resource_kind).map_err(|_| "resource kind contains NUL byte".to_string())?;
        let resource_id = CString::new(resource_id.to_string()).map_err(|_| "resource id contains NUL byte".to_string())?;
        let owner_key = CString::new(owner_key.to_string()).map_err(|_| "owner key contains NUL byte".to_string())?;
        let range_start = CString::new(range_start.to_string()).map_err(|_| "range start contains NUL byte".to_string())?;
        let lease_ttl_seconds = CString::new(lease_ttl_seconds.to_string())
            .map_err(|_| "lease ttl contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            if let Some(range_end) = range_end {
                let range_end = CString::new(range_end.to_string())
                    .map_err(|_| "range end contains NUL byte".to_string())?;
                let params = [&resource_kind, &resource_id, &owner_key, &range_start, &lease_ttl_seconds, &range_end];
                exec_params(conn, &sql, &params).map(|_| ())
            } else {
                let params = [&resource_kind, &resource_id, &owner_key, &range_start, &lease_ttl_seconds];
                exec_params(conn, &sql, &params).map(|_| ())
            }
        })
    }

    pub fn load_lock_range_state_blob(&self, resource_kind: &str, resource_id: u64) -> Result<Vec<u8>, String> {
        let sql = CString::new(
            "
            SELECT owner_key, lock_type, range_start, range_end
            FROM lock_range_leases
            WHERE resource_kind = $1
              AND resource_id = $2
              AND lease_expires_at > NOW()
            ORDER BY owner_key, range_start, COALESCE(range_end, 9223372036854775807)
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let resource_kind = CString::new(resource_kind).map_err(|_| "resource kind contains NUL byte".to_string())?;
        let resource_id = CString::new(resource_id.to_string()).map_err(|_| "resource id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&resource_kind, &resource_id];
            let res = exec_params(conn, &sql, &params)?;
            let rows = PQntuples(res);
            let cols = PQnfields(res);
            let mut output = Vec::new();
            if rows >= 1 && cols >= 4 {
                for row in 0..rows {
                    if row > 0 {
                        output.push(b'\n');
                    }
                    let owner_ptr = PQgetvalue(res, row, 0);
                    let type_ptr = PQgetvalue(res, row, 1);
                    let start_ptr = PQgetvalue(res, row, 2);
                    let end_ptr = PQgetvalue(res, row, 3);
                    let owner = if owner_ptr.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(owner_ptr).to_string_lossy().to_string()
                    };
                    let lock_type = if type_ptr.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(type_ptr).to_string_lossy().to_string()
                    };
                    let start = if start_ptr.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(start_ptr).to_string_lossy().to_string()
                    };
                    let end = if end_ptr.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(end_ptr).to_string_lossy().to_string()
                    };
                    output.extend_from_slice(owner.as_bytes());
                    output.push(b'\t');
                    output.extend_from_slice(lock_type.as_bytes());
                    output.push(b'\t');
                    output.extend_from_slice(start.as_bytes());
                    output.push(b'\t');
                    output.extend_from_slice(end.as_bytes());
                }
            }
            PQclear(res);
            Ok(output)
        })
    }

    pub fn persist_lock_range_state_blob(
        &self,
        resource_kind: &str,
        resource_id: u64,
        lease_ttl_seconds: u64,
        payload: &str,
    ) -> Result<(), String> {
        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                self.delete_range_leases(resource_kind, resource_id, None)?;
                if payload.trim().is_empty() {
                    return Ok(());
                }
                let insert_sql = CString::new(
                    "
                    INSERT INTO lock_range_leases (
                        resource_kind,
                        resource_id,
                        owner_key,
                        lock_type,
                        range_start,
                        range_end,
                        lease_expires_at,
                        heartbeat_at,
                        created_at,
                        updated_at
                    ) VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        $6,
                        NOW() + ($7 || ' seconds')::interval,
                        NOW(),
                        NOW(),
                        NOW()
                    )
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?;
                let insert_sql_null_end = CString::new(
                    "
                    INSERT INTO lock_range_leases (
                        resource_kind,
                        resource_id,
                        owner_key,
                        lock_type,
                        range_start,
                        range_end,
                        lease_expires_at,
                        heartbeat_at,
                        created_at,
                        updated_at
                    ) VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        NULL,
                        NOW() + ($6 || ' seconds')::interval,
                        NOW(),
                        NOW(),
                        NOW()
                    )
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?;
                let resource_kind = CString::new(resource_kind)
                    .map_err(|_| "resource kind contains NUL byte".to_string())?;
                let resource_id = CString::new(resource_id.to_string())
                    .map_err(|_| "resource id contains NUL byte".to_string())?;
                let ttl = CString::new(lease_ttl_seconds.to_string())
                    .map_err(|_| "lease ttl contains NUL byte".to_string())?;

                for line in payload.lines() {
                    let mut parts = line.split('\t');
                    let owner_key = parts.next().ok_or_else(|| "invalid range state line".to_string())?;
                    let lock_type = parts.next().ok_or_else(|| "invalid range state line".to_string())?;
                    let range_start = parts.next().ok_or_else(|| "invalid range state line".to_string())?;
                    let range_end = parts.next().ok_or_else(|| "invalid range state line".to_string())?;
                    let owner_key = CString::new(owner_key).map_err(|_| "owner key contains NUL byte".to_string())?;
                    let lock_type = CString::new(lock_type).map_err(|_| "lock type contains NUL byte".to_string())?;
                    let range_start = CString::new(range_start).map_err(|_| "range start contains NUL byte".to_string())?;
                    let range_end = if range_end.is_empty() {
                        None
                    } else {
                        Some(CString::new(range_end).map_err(|_| "range end contains NUL byte".to_string())?)
                    };
                    if parts.next().is_some() {
                        return Err("invalid range state line".to_string());
                    }
                    if let Some(range_end) = range_end.as_ref() {
                        let params = [&resource_kind, &resource_id, &owner_key, &lock_type, &range_start, range_end, &ttl];
                        exec_command_params(conn, &insert_sql, &params)?;
                    } else {
                        let params = [&resource_kind, &resource_id, &owner_key, &lock_type, &range_start, &ttl];
                        exec_command_params(conn, &insert_sql_null_end, &params)?;
                    }
                }
                Ok(())
            })
        })
    }

    pub fn get_dir_id(&self, path: &str) -> Result<Option<u64>, String> {
        let sql = CString::new(
            "
            WITH RECURSIVE parts AS (
                SELECT part, ord
                FROM unnest(string_to_array(btrim($1, '/'), '/')) WITH ORDINALITY AS t(part, ord)
            ),
            walk AS (
                SELECT d.id_directory, p.ord
                FROM directories d
                JOIN parts p ON p.ord = 1
                WHERE d.id_parent IS NULL AND d.name = p.part
                UNION ALL
                SELECT d.id_directory, p.ord
                FROM walk w
                JOIN parts p ON p.ord = w.ord + 1
                JOIN directories d ON d.id_parent = w.id_directory AND d.name = p.part
            )
            SELECT id_directory
            FROM walk
            ORDER BY ord DESC
            LIMIT 1
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let path = CString::new(path).map_err(|_| "path contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values = [path.as_ptr()];
                let param_lengths = [path.as_bytes().len() as c_int];
                let param_formats = [0 as c_int];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    1,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    let value = CStr::from_ptr(value_ptr).to_string_lossy().to_string();
                                    value.trim().parse::<u64>().ok()
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn get_file_id(&self, path: &str) -> Result<Option<u64>, String> {
        let normalized = path.trim();
        let (parent_path, file_name) = match normalized.rsplit_once('/') {
            Some((parent, name)) if !name.is_empty() => (if parent.is_empty() { "/" } else { parent }, name),
            _ => ("/", normalized),
        };
        let parent_id = self.get_dir_id(parent_path)?;
        let has_parent = parent_id.is_some();
        let file_name = CString::new(file_name).map_err(|_| "path contains NUL byte".to_string())?;
        let parent_id_text = parent_id
            .map(|value| CString::new(value.to_string()).map_err(|_| "parent id contains NUL byte".to_string()))
            .transpose()?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let sql = if has_parent {
                    CString::new(
                        "
                        SELECT id_file FROM (
                            SELECT 1 AS precedence, id_file FROM hardlinks WHERE name = $1 AND id_directory = $2
                            UNION ALL
                            SELECT 2 AS precedence, id_file FROM files WHERE name = $1 AND id_directory = $2
                        ) entries
                        ORDER BY precedence
                        LIMIT 1
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?
                } else {
                    CString::new(
                        "
                        SELECT id_file FROM (
                            SELECT 1 AS precedence, id_file FROM hardlinks WHERE name = $1 AND id_directory IS NULL
                            UNION ALL
                            SELECT 2 AS precedence, id_file FROM files WHERE name = $1 AND id_directory IS NULL
                        ) entries
                        ORDER BY precedence
                        LIMIT 1
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?
                };
                let res = if let Some(ref parent_id_text) = parent_id_text {
                    let param_values = [file_name.as_ptr(), parent_id_text.as_ptr()];
                    let param_lengths = [file_name.as_bytes().len() as c_int, parent_id_text.as_bytes().len() as c_int];
                    let param_formats = [0 as c_int, 0 as c_int];
                    PQexecParams(
                        conn,
                        sql.as_ptr(),
                        2,
                        std::ptr::null(),
                        param_values.as_ptr(),
                        param_lengths.as_ptr(),
                        param_formats.as_ptr(),
                        0,
                    )
                } else {
                    let param_values = [file_name.as_ptr()];
                    let param_lengths = [file_name.as_bytes().len() as c_int];
                    let param_formats = [0 as c_int];
                    PQexecParams(
                        conn,
                        sql.as_ptr(),
                        1,
                        std::ptr::null(),
                        param_values.as_ptr(),
                        param_lengths.as_ptr(),
                        param_formats.as_ptr(),
                        0,
                    )
                };
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    let value = CStr::from_ptr(value_ptr).to_string_lossy().to_string();
                                    value.trim().parse::<u64>().ok()
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn get_file_mode_value(&self, path: &str) -> Result<Option<String>, String> {
        let normalized = path.trim();
        let (parent_path, file_name) = match normalized.rsplit_once('/') {
            Some((parent, name)) if !name.is_empty() => (if parent.is_empty() { "/" } else { parent }, name),
            _ => ("/", normalized),
        };
        let parent_id = self.get_dir_id(parent_path)?;
        let file_name = CString::new(file_name).map_err(|_| "path contains NUL byte".to_string())?;
        let parent_id_text = parent_id
            .map(|value| CString::new(value.to_string()).map_err(|_| "parent id contains NUL byte".to_string()))
            .transpose()?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let sql = if parent_id_text.is_some() {
                    CString::new(
                        "
                        SELECT mode FROM (
                            SELECT 1 AS precedence, mode
                            FROM hardlinks JOIN files ON hardlinks.id_file = files.id_file
                            WHERE hardlinks.name = $1 AND hardlinks.id_directory = $2
                            UNION ALL
                            SELECT 2 AS precedence, mode
                            FROM files
                            WHERE name = $1 AND id_directory = $2
                        ) entries
                        ORDER BY precedence
                        LIMIT 1
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?
                } else {
                    CString::new(
                        "
                        SELECT mode FROM (
                            SELECT 1 AS precedence, mode
                            FROM hardlinks JOIN files ON hardlinks.id_file = files.id_file
                            WHERE hardlinks.name = $1 AND hardlinks.id_directory IS NULL
                            UNION ALL
                            SELECT 2 AS precedence, mode
                            FROM files
                            WHERE name = $1 AND id_directory IS NULL
                        ) entries
                        ORDER BY precedence
                        LIMIT 1
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?
                };

                let res = if let Some(ref parent_id_text) = parent_id_text {
                    let param_values = [file_name.as_ptr(), parent_id_text.as_ptr()];
                    let param_lengths = [file_name.as_bytes().len() as c_int, parent_id_text.as_bytes().len() as c_int];
                    let param_formats = [0 as c_int, 0 as c_int];
                    PQexecParams(
                        conn,
                        sql.as_ptr(),
                        2,
                        std::ptr::null(),
                        param_values.as_ptr(),
                        param_lengths.as_ptr(),
                        param_formats.as_ptr(),
                        0,
                    )
                } else {
                    let param_values = [file_name.as_ptr()];
                    let param_lengths = [file_name.as_bytes().len() as c_int];
                    let param_formats = [0 as c_int];
                    PQexecParams(
                        conn,
                        sql.as_ptr(),
                        1,
                        std::ptr::null(),
                        param_values.as_ptr(),
                        param_lengths.as_ptr(),
                        param_formats.as_ptr(),
                        0,
                    )
                };
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    Some(CStr::from_ptr(value_ptr).to_string_lossy().to_string())
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn get_hardlink_id(&self, path: &str) -> Result<Option<u64>, String> {
        let normalized = path.trim();
        let (parent_path, link_name) = match normalized.rsplit_once('/') {
            Some((parent, name)) if !name.is_empty() => (if parent.is_empty() { "/" } else { parent }, name),
            _ => ("/", normalized),
        };
        let parent_id = self.get_dir_id(parent_path)?;
        let link_name = CString::new(link_name).map_err(|_| "path contains NUL byte".to_string())?;
        let parent_id_text = parent_id
            .map(|value| CString::new(value.to_string()).map_err(|_| "parent id contains NUL byte".to_string()))
            .transpose()?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let sql = if parent_id_text.is_some() {
                    CString::new(
                        "
                        SELECT id_hardlink
                        FROM hardlinks
                        WHERE name = $1 AND id_directory = $2
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?
                } else {
                    CString::new(
                        "
                        SELECT id_hardlink
                        FROM hardlinks
                        WHERE name = $1 AND id_directory IS NULL
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?
                };

                let res = if let Some(ref parent_id_text) = parent_id_text {
                    let param_values = [link_name.as_ptr(), parent_id_text.as_ptr()];
                    let param_lengths = [link_name.as_bytes().len() as c_int, parent_id_text.as_bytes().len() as c_int];
                    let param_formats = [0 as c_int, 0 as c_int];
                    PQexecParams(
                        conn,
                        sql.as_ptr(),
                        2,
                        std::ptr::null(),
                        param_values.as_ptr(),
                        param_lengths.as_ptr(),
                        param_formats.as_ptr(),
                        0,
                    )
                } else {
                    let param_values = [link_name.as_ptr()];
                    let param_lengths = [link_name.as_bytes().len() as c_int];
                    let param_formats = [0 as c_int];
                    PQexecParams(
                        conn,
                        sql.as_ptr(),
                        1,
                        std::ptr::null(),
                        param_values.as_ptr(),
                        param_lengths.as_ptr(),
                        param_formats.as_ptr(),
                        0,
                    )
                };
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    let value = CStr::from_ptr(value_ptr).to_string_lossy().to_string();
                                    value.trim().parse::<u64>().ok()
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn get_hardlink_file_id(&self, hardlink_id: u64) -> Result<Option<u64>, String> {
        let sql = CString::new("SELECT id_file FROM hardlinks WHERE id_hardlink = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let hardlink_id = CString::new(hardlink_id.to_string())
            .map_err(|_| "hardlink id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values = [hardlink_id.as_ptr()];
                let param_lengths = [hardlink_id.as_bytes().len() as c_int];
                let param_formats = [0 as c_int];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    1,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    let value = CStr::from_ptr(value_ptr).to_string_lossy().to_string();
                                    value.trim().parse::<u64>().ok()
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn choose_primary_hardlink(
        &self,
        file_id: u64,
    ) -> Result<Option<(u64, Option<u64>, String)>, String> {
        let sql = CString::new(
            "
            SELECT id_hardlink, id_directory, name
            FROM hardlinks
            WHERE id_file = $1
            ORDER BY id_hardlink ASC
            LIMIT 1
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string())
            .map_err(|_| "file id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values = [file_id.as_ptr()];
                let param_lengths = [file_id.as_bytes().len() as c_int];
                let param_formats = [0 as c_int];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    1,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 3 {
                                None
                            } else {
                                let hardlink_ptr = PQgetvalue(res, 0, 0);
                                let parent_ptr = PQgetvalue(res, 0, 1);
                                let name_ptr = PQgetvalue(res, 0, 2);
                                if hardlink_ptr.is_null() || parent_ptr.is_null() || name_ptr.is_null() {
                                    None
                                } else {
                                    let hardlink_id = CStr::from_ptr(hardlink_ptr)
                                        .to_string_lossy()
                                        .trim()
                                        .parse::<u64>()
                                        .ok();
                                    let parent_id = CStr::from_ptr(parent_ptr)
                                        .to_string_lossy()
                                        .trim()
                                        .parse::<u64>()
                                        .ok();
                                    let name = CStr::from_ptr(name_ptr).to_string_lossy().to_string();
                                    hardlink_id.map(|hardlink_id| (hardlink_id, parent_id, name))
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn promote_hardlink_to_primary(&self, file_id: u64) -> Result<bool, String> {
        let sql_choose = CString::new(
            "
            SELECT id_hardlink, id_directory, name
            FROM hardlinks
            WHERE id_file = $1
            ORDER BY id_hardlink ASC
            LIMIT 1
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_update_null_parent = CString::new(
            "
            UPDATE files
            SET id_directory = NULL, name = $1
            WHERE id_file = $2
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_update_parent = CString::new(
            "
            UPDATE files
            SET id_directory = $1, name = $2
            WHERE id_file = $3
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete = CString::new("DELETE FROM hardlinks WHERE id_hardlink = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string())
            .map_err(|_| "file id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = transactional(conn, |conn| {
                let params = [&file_id];
                let res = exec_params(conn, &sql_choose, &params)?;
                let chosen = match PQresultStatus(res) {
                    PGRES_TUPLES_OK => {
                        let rows = PQntuples(res);
                        let cols = PQnfields(res);
                        let value = if rows < 1 || cols < 3 {
                            None
                        } else {
                            let hardlink_ptr = PQgetvalue(res, 0, 0);
                            let parent_ptr = PQgetvalue(res, 0, 1);
                            let name_ptr = PQgetvalue(res, 0, 2);
                            if hardlink_ptr.is_null() || parent_ptr.is_null() || name_ptr.is_null() {
                                None
                            } else {
                                let hardlink_id = CStr::from_ptr(hardlink_ptr)
                                    .to_string_lossy()
                                    .trim()
                                    .parse::<u64>()
                                    .ok();
                                let parent_id = CStr::from_ptr(parent_ptr)
                                    .to_string_lossy()
                                    .trim()
                                    .parse::<u64>()
                                    .ok();
                                let name = CStr::from_ptr(name_ptr).to_string_lossy().to_string();
                                hardlink_id.map(|hardlink_id| (hardlink_id, parent_id, name))
                            }
                        };
                        PQclear(res);
                        value
                    }
                    _ => {
                        PQclear(res);
                        return Err(conn_error(conn));
                    }
                };

                let Some((hardlink_id, parent_id, name)) = chosen else {
                    return Ok(false);
                };

                let file_name = CString::new(name).map_err(|_| "hardlink name contains NUL byte".to_string())?;
                let hardlink_id = CString::new(hardlink_id.to_string())
                    .map_err(|_| "hardlink id contains NUL byte".to_string())?;
                if let Some(parent_id) = parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&parent_id, &file_name, &file_id];
                    let res = exec_params(conn, &sql_update_parent, &params)?;
                    PQclear(res);
                } else {
                    let params = [&file_name, &file_id];
                    let res = exec_params(conn, &sql_update_null_parent, &params)?;
                    PQclear(res);
                }
                let params = [&hardlink_id];
                let res = exec_params(conn, &sql_delete, &params)?;
                PQclear(res);
                Ok(true)
            });
            result
        })
    }

    pub fn touch_file_entry(&self, file_id: u64) -> Result<(), String> {
        let sql = CString::new("UPDATE files SET modification_date = NOW(), change_date = NOW() WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&file_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn touch_directory_entry(&self, directory_id: u64) -> Result<(), String> {
        let sql = CString::new("UPDATE directories SET modification_date = NOW(), change_date = NOW() WHERE id_directory = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&directory_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn touch_symlink_entry(&self, symlink_id: u64) -> Result<(), String> {
        let sql = CString::new("UPDATE symlinks SET modification_date = NOW(), change_date = NOW() WHERE id_symlink = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let symlink_id = CString::new(symlink_id.to_string())
            .map_err(|_| "symlink id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&symlink_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn rename_file_entry(&self, file_id: u64, new_parent_id: Option<u64>, new_name: &str) -> Result<(), String> {
        let (sql, params) = if let Some(parent_id) = new_parent_id {
            (
                CString::new("UPDATE files SET name = $1, id_directory = $2, change_date = NOW(), modification_date = NOW() WHERE id_file = $3")
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(new_name).map_err(|_| "name contains NUL byte".to_string())?,
                    CString::new(parent_id.to_string()).map_err(|_| "parent id contains NUL byte".to_string())?,
                    CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?,
                ],
            )
        } else {
            (
                CString::new("UPDATE files SET name = $1, id_directory = NULL, change_date = NOW(), modification_date = NOW() WHERE id_file = $2")
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(new_name).map_err(|_| "name contains NUL byte".to_string())?,
                    CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?,
                ],
            )
        };
        self.with_cached_connection(|conn| unsafe {
            let param_refs: Vec<&CString> = params.iter().collect();
            exec_params(conn, &sql, &param_refs).map(|_| ())
        })
    }

    pub fn rename_hardlink_entry(&self, hardlink_id: u64, new_parent_id: Option<u64>, new_name: &str) -> Result<(), String> {
        let (sql, params) = if let Some(parent_id) = new_parent_id {
            (
                CString::new("UPDATE hardlinks SET name = $1, id_directory = $2, modification_date = NOW() WHERE id_hardlink = $3")
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(new_name).map_err(|_| "name contains NUL byte".to_string())?,
                    CString::new(parent_id.to_string()).map_err(|_| "parent id contains NUL byte".to_string())?,
                    CString::new(hardlink_id.to_string()).map_err(|_| "hardlink id contains NUL byte".to_string())?,
                ],
            )
        } else {
            (
                CString::new("UPDATE hardlinks SET name = $1, id_directory = NULL, modification_date = NOW() WHERE id_hardlink = $2")
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(new_name).map_err(|_| "name contains NUL byte".to_string())?,
                    CString::new(hardlink_id.to_string()).map_err(|_| "hardlink id contains NUL byte".to_string())?,
                ],
            )
        };
        self.with_cached_connection(|conn| unsafe {
            let param_refs: Vec<&CString> = params.iter().collect();
            exec_params(conn, &sql, &param_refs).map(|_| ())
        })
    }

    pub fn rename_symlink_entry(&self, symlink_id: u64, new_parent_id: Option<u64>, new_name: &str) -> Result<(), String> {
        let (sql, params) = if let Some(parent_id) = new_parent_id {
            (
                CString::new("UPDATE symlinks SET name = $1, id_parent = $2, modification_date = NOW() WHERE id_symlink = $3")
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(new_name).map_err(|_| "name contains NUL byte".to_string())?,
                    CString::new(parent_id.to_string()).map_err(|_| "parent id contains NUL byte".to_string())?,
                    CString::new(symlink_id.to_string()).map_err(|_| "symlink id contains NUL byte".to_string())?,
                ],
            )
        } else {
            (
                CString::new("UPDATE symlinks SET name = $1, id_parent = NULL, modification_date = NOW() WHERE id_symlink = $2")
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(new_name).map_err(|_| "name contains NUL byte".to_string())?,
                    CString::new(symlink_id.to_string()).map_err(|_| "symlink id contains NUL byte".to_string())?,
                ],
            )
        };
        self.with_cached_connection(|conn| unsafe {
            let param_refs: Vec<&CString> = params.iter().collect();
            exec_params(conn, &sql, &param_refs).map(|_| ())
        })
    }

    pub fn rename_directory_entry(&self, directory_id: u64, new_parent_id: Option<u64>, new_name: &str) -> Result<(), String> {
        let (sql, params) = if let Some(parent_id) = new_parent_id {
            (
                CString::new("UPDATE directories SET name = $1, id_parent = $2, modification_date = NOW(), change_date = NOW() WHERE id_directory = $3")
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(new_name).map_err(|_| "name contains NUL byte".to_string())?,
                    CString::new(parent_id.to_string()).map_err(|_| "parent id contains NUL byte".to_string())?,
                    CString::new(directory_id.to_string()).map_err(|_| "directory id contains NUL byte".to_string())?,
                ],
            )
        } else {
            (
                CString::new("UPDATE directories SET name = $1, id_parent = NULL, modification_date = NOW(), change_date = NOW() WHERE id_directory = $2")
                    .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(new_name).map_err(|_| "name contains NUL byte".to_string())?,
                    CString::new(directory_id.to_string()).map_err(|_| "directory id contains NUL byte".to_string())?,
                ],
            )
        };
        self.with_cached_connection(|conn| unsafe {
            let param_refs: Vec<&CString> = params.iter().collect();
            exec_params(conn, &sql, &param_refs).map(|_| ())
        })
    }

    pub fn delete_hardlink_entry(&self, hardlink_id: u64) -> Result<(), String> {
        let sql = CString::new("DELETE FROM hardlinks WHERE id_hardlink = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let hardlink_id = CString::new(hardlink_id.to_string()).map_err(|_| "hardlink id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&hardlink_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn delete_symlink_entry(&self, symlink_id: u64) -> Result<(), String> {
        let sql = CString::new("DELETE FROM symlinks WHERE id_symlink = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let symlink_id = CString::new(symlink_id.to_string()).map_err(|_| "symlink id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&symlink_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn delete_directory_entry(&self, directory_id: u64) -> Result<(), String> {
        let sql = CString::new("DELETE FROM directories WHERE id_directory = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&directory_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn create_hardlink(
        &self,
        source_file_id: u64,
        target_parent_id: Option<u64>,
        target_name: &str,
        uid: u32,
        gid: u32,
    ) -> Result<u64, String> {
        let target_name = CString::new(target_name).map_err(|_| "target name contains NUL byte".to_string())?;
        let source_file_id = CString::new(source_file_id.to_string())
            .map_err(|_| "file id contains NUL byte".to_string())?;
        let uid = CString::new(uid.to_string()).map_err(|_| "uid contains NUL byte".to_string())?;
        let gid = CString::new(gid.to_string()).map_err(|_| "gid contains NUL byte".to_string())?;
        let sql_touch_parent = CString::new(
            "UPDATE directories SET modification_date = NOW(), change_date = NOW() WHERE id_directory = $1",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_null_parent = CString::new(
            "
            INSERT INTO hardlinks (id_file, id_directory, name, uid, gid, creation_date, modification_date, access_date)
            VALUES ($1, NULL, $2, $3, $4, NOW(), NOW(), NOW())
            RETURNING id_hardlink
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_parent = CString::new(
            "
            INSERT INTO hardlinks (id_file, id_directory, name, uid, gid, creation_date, modification_date, access_date)
            VALUES ($1, $5, $2, $3, $4, NOW(), NOW(), NOW())
            RETURNING id_hardlink
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                transactional(conn, |conn| {
                    let res = if let Some(parent_id) = target_parent_id {
                        let parent_id = CString::new(parent_id.to_string())
                            .map_err(|_| "parent id contains NUL byte".to_string())?;
                        let params = [
                            &source_file_id,
                            &target_name,
                            &uid,
                            &gid,
                            &parent_id,
                        ];
                        exec_params(conn, &sql_parent, &params)?
                    } else {
                        let params = [&source_file_id, &target_name, &uid, &gid];
                        exec_params(conn, &sql_null_parent, &params)?
                    };
                    let value = match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    CStr::from_ptr(value_ptr).to_string_lossy().trim().parse::<u64>().ok()
                                }
                            };
                            PQclear(res);
                            value.ok_or_else(|| "failed to create hardlink".to_string())
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }?;
                    if let Some(parent_id) = target_parent_id {
                        let parent_id = CString::new(parent_id.to_string())
                            .map_err(|_| "parent id contains NUL byte".to_string())?;
                        let params = [&parent_id];
                        let res = exec_params(conn, &sql_touch_parent, &params)?;
                        PQclear(res);
                    }
                    Ok(value)
                })
            };
            result
        })
    }

    pub fn create_symlink(
        &self,
        target_parent_id: Option<u64>,
        target_name: &str,
        target: &str,
        uid: u32,
        gid: u32,
        inode_seed: &str,
    ) -> Result<u64, String> {
        let target_name = CString::new(target_name).map_err(|_| "target name contains NUL byte".to_string())?;
        let target = CString::new(target).map_err(|_| "symlink target contains NUL byte".to_string())?;
        let uid = CString::new(uid.to_string()).map_err(|_| "uid contains NUL byte".to_string())?;
        let gid = CString::new(gid.to_string()).map_err(|_| "gid contains NUL byte".to_string())?;
        let inode_seed = CString::new(inode_seed).map_err(|_| "inode seed contains NUL byte".to_string())?;
        let sql_touch_parent = CString::new(
            "UPDATE directories SET modification_date = NOW(), change_date = NOW() WHERE id_directory = $1",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_null_parent = CString::new(
            "
            INSERT INTO symlinks (id_parent, name, target, uid, gid, inode_seed, change_date, creation_date, modification_date, access_date)
            VALUES (NULL, $1, $2, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
            RETURNING id_symlink
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_parent = CString::new(
            "
            INSERT INTO symlinks (id_parent, name, target, uid, gid, inode_seed, change_date, creation_date, modification_date, access_date)
            VALUES ($6, $1, $2, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
            RETURNING id_symlink
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = transactional(conn, |conn| {
                let res = if let Some(parent_id) = target_parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&target_name, &target, &uid, &gid, &inode_seed, &parent_id];
                    exec_params(conn, &sql_parent, &params)?
                } else {
                    let params = [&target_name, &target, &uid, &gid, &inode_seed];
                    exec_params(conn, &sql_null_parent, &params)?
                };
                let value = match PQresultStatus(res) {
                    PGRES_TUPLES_OK => {
                        let rows = PQntuples(res);
                        let cols = PQnfields(res);
                        let value = if rows < 1 || cols < 1 {
                            None
                        } else {
                            let value_ptr = PQgetvalue(res, 0, 0);
                            if value_ptr.is_null() {
                                None
                            } else {
                                CStr::from_ptr(value_ptr).to_string_lossy().trim().parse::<u64>().ok()
                            }
                        };
                        PQclear(res);
                        value.ok_or_else(|| "failed to create symlink".to_string())
                    }
                    _ => {
                        PQclear(res);
                        Err(conn_error(conn))
                    }
                }?;
                if let Some(parent_id) = target_parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&parent_id];
                    let res = exec_params(conn, &sql_touch_parent, &params)?;
                    PQclear(res);
                }
                Ok(value)
            });
            result
        })
    }

    pub fn create_directory(
        &self,
        target_parent_id: Option<u64>,
        target_name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        inode_seed: &str,
    ) -> Result<u64, String> {
        let target_name = CString::new(target_name).map_err(|_| "target name contains NUL byte".to_string())?;
        let uid = CString::new(uid.to_string()).map_err(|_| "uid contains NUL byte".to_string())?;
        let gid = CString::new(gid.to_string()).map_err(|_| "gid contains NUL byte".to_string())?;
        let inode_seed = CString::new(inode_seed).map_err(|_| "inode seed contains NUL byte".to_string())?;
        let mode = CString::new(format!("{:o}", mode)).map_err(|_| "mode contains NUL byte".to_string())?;
        let sql_touch_parent = CString::new(
            "UPDATE directories SET modification_date = NOW(), change_date = NOW() WHERE id_directory = $1",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_null_parent = CString::new(
            "
            INSERT INTO directories (id_parent, name, mode, uid, gid, inode_seed, change_date, creation_date, modification_date, access_date)
            VALUES (NULL, $1, $2, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
            RETURNING id_directory
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_parent = CString::new(
            "
            INSERT INTO directories (id_parent, name, mode, uid, gid, inode_seed, change_date, creation_date, modification_date, access_date)
            VALUES ($6, $1, $2, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
            RETURNING id_directory
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = transactional(conn, |conn| {
                let res = if let Some(parent_id) = target_parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&target_name, &mode, &uid, &gid, &inode_seed, &parent_id];
                    exec_params(conn, &sql_parent, &params)?
                } else {
                    let params = [&target_name, &mode, &uid, &gid, &inode_seed];
                    exec_params(conn, &sql_null_parent, &params)?
                };
                let value = match PQresultStatus(res) {
                    PGRES_TUPLES_OK => {
                        let rows = PQntuples(res);
                        let cols = PQnfields(res);
                        let value = if rows < 1 || cols < 1 {
                            None
                        } else {
                            let value_ptr = PQgetvalue(res, 0, 0);
                            if value_ptr.is_null() {
                                None
                            } else {
                                CStr::from_ptr(value_ptr).to_string_lossy().trim().parse::<u64>().ok()
                            }
                        };
                        PQclear(res);
                        value.ok_or_else(|| "failed to create directory".to_string())
                    }
                    _ => {
                        PQclear(res);
                        Err(conn_error(conn))
                    }
                }?;
                if let Some(parent_id) = target_parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&parent_id];
                    let res = exec_params(conn, &sql_touch_parent, &params)?;
                    PQclear(res);
                }
                Ok(value)
            });
            result
        })
    }

    pub fn create_file(
        &self,
        target_parent_id: Option<u64>,
        target_name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        inode_seed: &str,
    ) -> Result<u64, String> {
        let target_name = CString::new(target_name).map_err(|_| "target name contains NUL byte".to_string())?;
        let uid = CString::new(uid.to_string()).map_err(|_| "uid contains NUL byte".to_string())?;
        let gid = CString::new(gid.to_string()).map_err(|_| "gid contains NUL byte".to_string())?;
        let inode_seed = CString::new(inode_seed).map_err(|_| "inode seed contains NUL byte".to_string())?;
        let mode = CString::new(format!("{:o}", mode)).map_err(|_| "mode contains NUL byte".to_string())?;
        let sql_touch_parent = CString::new(
            "UPDATE directories SET modification_date = NOW(), change_date = NOW() WHERE id_directory = $1",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_null_parent = CString::new(
            "
            INSERT INTO files (id_directory, name, size, mode, uid, gid, inode_seed, change_date, modification_date, access_date, creation_date)
            VALUES (NULL, $1, 0, $2, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
            RETURNING id_file
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_parent = CString::new(
            "
            INSERT INTO files (id_directory, name, size, mode, uid, gid, inode_seed, change_date, modification_date, access_date, creation_date)
            VALUES ($6, $1, 0, $2, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
            RETURNING id_file
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = transactional(conn, |conn| {
                let res = if let Some(parent_id) = target_parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&target_name, &mode, &uid, &gid, &inode_seed, &parent_id];
                    exec_params(conn, &sql_parent, &params)?
                } else {
                    let params = [&target_name, &mode, &uid, &gid, &inode_seed];
                    exec_params(conn, &sql_null_parent, &params)?
                };
                let value = match PQresultStatus(res) {
                    PGRES_TUPLES_OK => {
                        let rows = PQntuples(res);
                        let cols = PQnfields(res);
                        let value = if rows < 1 || cols < 1 {
                            None
                        } else {
                            let value_ptr = PQgetvalue(res, 0, 0);
                            if value_ptr.is_null() {
                                None
                            } else {
                                CStr::from_ptr(value_ptr).to_string_lossy().trim().parse::<u64>().ok()
                            }
                        };
                        PQclear(res);
                        value.ok_or_else(|| "failed to create file".to_string())
                    }
                    _ => {
                        PQclear(res);
                        Err(conn_error(conn))
                    }
                }?;
                if let Some(parent_id) = target_parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&parent_id];
                    let res = exec_params(conn, &sql_touch_parent, &params)?;
                    PQclear(res);
                }
                Ok(value)
            });
            result
        })
    }

    pub fn create_special_file(
        &self,
        target_parent_id: Option<u64>,
        target_name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        inode_seed: &str,
        file_kind: &str,
        rdev_major: u32,
        rdev_minor: u32,
    ) -> Result<u64, String> {
        let target_name = CString::new(target_name).map_err(|_| "target name contains NUL byte".to_string())?;
        let uid = CString::new(uid.to_string()).map_err(|_| "uid contains NUL byte".to_string())?;
        let gid = CString::new(gid.to_string()).map_err(|_| "gid contains NUL byte".to_string())?;
        let inode_seed = CString::new(inode_seed).map_err(|_| "inode seed contains NUL byte".to_string())?;
        let mode = CString::new(format!("{:o}", mode)).map_err(|_| "mode contains NUL byte".to_string())?;
        let file_kind = CString::new(file_kind).map_err(|_| "file kind contains NUL byte".to_string())?;
        let rdev_major = CString::new(rdev_major.to_string()).map_err(|_| "rdev major contains NUL byte".to_string())?;
        let rdev_minor = CString::new(rdev_minor.to_string()).map_err(|_| "rdev minor contains NUL byte".to_string())?;
        let sql_touch_parent = CString::new(
            "UPDATE directories SET modification_date = NOW(), change_date = NOW() WHERE id_directory = $1",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_null_parent = CString::new(
            "
            INSERT INTO files (id_directory, name, size, mode, uid, gid, inode_seed, change_date, modification_date, access_date, creation_date)
            VALUES (NULL, $1, 0, $2, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
            RETURNING id_file
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_parent = CString::new(
            "
            INSERT INTO files (id_directory, name, size, mode, uid, gid, inode_seed, change_date, modification_date, access_date, creation_date)
            VALUES ($6, $1, 0, $2, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
            RETURNING id_file
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_special = CString::new(
            "
            INSERT INTO special_files (id_file, file_type, rdev_major, rdev_minor)
            VALUES ($1, $2, $3, $4)
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = transactional(conn, |conn| {
                let res = if let Some(parent_id) = target_parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&target_name, &mode, &uid, &gid, &inode_seed, &parent_id];
                    exec_params(conn, &sql_parent, &params)?
                } else {
                    let params = [&target_name, &mode, &uid, &gid, &inode_seed];
                    exec_params(conn, &sql_null_parent, &params)?
                };
                let id_file = match PQresultStatus(res) {
                    PGRES_TUPLES_OK => {
                        let rows = PQntuples(res);
                        let cols = PQnfields(res);
                        let value = if rows < 1 || cols < 1 {
                            None
                        } else {
                            let value_ptr = PQgetvalue(res, 0, 0);
                            if value_ptr.is_null() {
                                None
                            } else {
                                CStr::from_ptr(value_ptr).to_string_lossy().trim().parse::<u64>().ok()
                            }
                        };
                        PQclear(res);
                        value.ok_or_else(|| "failed to create special file".to_string())?
                    }
                    _ => {
                        PQclear(res);
                        return Err(conn_error(conn));
                    }
                };

                let id_file_text = CString::new(id_file.to_string())
                    .map_err(|_| "file id contains NUL byte".to_string())?;
                let special_params = [&id_file_text, &file_kind, &rdev_major, &rdev_minor];
                let res = exec_params(conn, &sql_special, &special_params)?;
                match PQresultStatus(res) {
                    PGRES_COMMAND_OK => {
                        PQclear(res);
                    }
                    _ => {
                        PQclear(res);
                        return Err(conn_error(conn));
                    }
                }

                if let Some(parent_id) = target_parent_id {
                    let parent_id = CString::new(parent_id.to_string())
                        .map_err(|_| "parent id contains NUL byte".to_string())?;
                    let params = [&parent_id];
                    let res = exec_params(conn, &sql_touch_parent, &params)?;
                    PQclear(res);
                }
                Ok(id_file)
            });
            result
        })
    }

    pub fn persist_copy_block_crc_rows<'a>(
        &self,
        file_id: u64,
        block_size: u64,
        blocks: &[PersistBlockRow<'a>],
    ) -> Result<(), String> {
        if blocks.is_empty() {
            return Ok(());
        }
        let block_size = block_size.max(1);
        let sql_upsert = CString::new(
            "
            INSERT INTO copy_block_crc (id_file, data_object_id, _order, crc32)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (data_object_id, _order)
            DO UPDATE SET id_file = EXCLUDED.id_file, crc32 = EXCLUDED.crc32, updated_at = NOW()
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete = CString::new(
            "
            DELETE FROM copy_block_crc
            WHERE data_object_id = $1 AND _order = $2
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                let data_object_id = match self.file_data_object_id_on_conn(conn, file_id)? {
                    Some(value) => value,
                    None => return Ok(()),
                };
                let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
                let data_object_id = CString::new(data_object_id.to_string())
                    .map_err(|_| "data object id contains NUL byte".to_string())?;
                for block in blocks {
                    let block_index = CString::new(block.block_index.to_string())
                        .map_err(|_| "block index contains NUL byte".to_string())?;
                    if block.used_len >= block_size {
                        let crc32 = CString::new(crc32_bytes(block.data).to_string())
                            .map_err(|_| "crc32 contains NUL byte".to_string())?;
                        let params = [&file_id, &data_object_id, &block_index, &crc32];
                        exec_command_params(conn, &sql_upsert, &params)?;
                    } else {
                        let params = [&data_object_id, &block_index];
                        exec_command_params(conn, &sql_delete, &params)?;
                    }
                }
                Ok(())
            })
        })
    }

    pub fn persist_file_blocks<'a>(
        &self,
        file_id: u64,
        file_size: u64,
        block_size: u64,
        total_blocks: u64,
        truncate_pending: bool,
        blocks: &[PersistBlockRow<'a>],
    ) -> Result<(), String> {
        let file_id_text = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        let file_size_text = CString::new(file_size.to_string()).map_err(|_| "file size contains NUL byte".to_string())?;
        let block_size = block_size.max(1);
        let total_blocks_text = CString::new(total_blocks.to_string())
            .map_err(|_| "total blocks contains NUL byte".to_string())?;
        let sql_delete_tail = CString::new(
            "
            DELETE FROM data_blocks
            WHERE data_object_id = $1 AND _order >= $2
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_crc_tail = CString::new(
            "
            DELETE FROM copy_block_crc
            WHERE data_object_id = $1 AND _order >= $2
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_upsert_data = CString::new(
            "
            INSERT INTO data_blocks (id_file, data_object_id, _order, data)
            VALUES ($1, $2, $3, decode($4, 'hex'))
            ON CONFLICT (data_object_id, _order)
            DO UPDATE SET id_file = EXCLUDED.id_file, data = EXCLUDED.data
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_upsert_crc = CString::new(
            "
            INSERT INTO copy_block_crc (id_file, data_object_id, _order, crc32)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (data_object_id, _order)
            DO UPDATE SET id_file = EXCLUDED.id_file, crc32 = EXCLUDED.crc32, updated_at = NOW()
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_crc = CString::new(
            "
            DELETE FROM copy_block_crc
            WHERE data_object_id = $1 AND _order = $2
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_update_file = CString::new(
            "UPDATE files SET size = $1, modification_date = NOW(), change_date = NOW() WHERE id_file = $2",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                let data_object_id = match self.file_data_object_id_on_conn(conn, file_id)? {
                    Some(value) => value,
                    None => return Ok(()),
                };
                let data_object_id = CString::new(data_object_id.to_string())
                    .map_err(|_| "data object id contains NUL byte".to_string())?;
                if truncate_pending {
                    let params = [&data_object_id, &total_blocks_text];
                    exec_command_params(conn, &sql_delete_tail, &params)?;
                    exec_command_params(conn, &sql_delete_crc_tail, &params)?;
                }

                for block in blocks {
                    if block.block_index >= total_blocks {
                        continue;
                    }

                    let block_index = CString::new(block.block_index.to_string())
                        .map_err(|_| "block index contains NUL byte".to_string())?;
                    let data_hex = CString::new(hex_encode_bytes(block.data))
                        .map_err(|_| "block data hex contains NUL byte".to_string())?;
                    let data_params = [&file_id_text, &data_object_id, &block_index, &data_hex];
                    exec_command_params(conn, &sql_upsert_data, &data_params)?;

                    if block.used_len >= block_size {
                        let crc32 = CString::new(crc32_bytes(block.data).to_string())
                            .map_err(|_| "crc32 contains NUL byte".to_string())?;
                        let crc_params = [&file_id_text, &data_object_id, &block_index, &crc32];
                        exec_command_params(conn, &sql_upsert_crc, &crc_params)?;
                    } else {
                        let crc_params = [&data_object_id, &block_index];
                        exec_command_params(conn, &sql_delete_crc, &crc_params)?;
                    }
                }

                let params = [&file_size_text, &file_id_text];
                exec_command_params(conn, &sql_update_file, &params)?;
                Ok(())
            })
        })
    }

    pub fn adopt_source_data_object(&self, src_file_id: u64, dst_file_id: u64) -> Result<bool, String> {
        let sql_file_info = CString::new("SELECT size, data_object_id FROM files WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_touch_src_object = CString::new(
            "UPDATE data_objects SET reference_count = reference_count + 1, modification_date = NOW() WHERE id_data_object = $1",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_update_dst_file = CString::new(
            "UPDATE files SET data_object_id = $1, size = $2, change_date = NOW(), modification_date = NOW() WHERE id_file = $3",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_count_dst_references = CString::new("SELECT COUNT(*) FROM files WHERE data_object_id = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_data = CString::new("DELETE FROM data_blocks WHERE data_object_id = $1 OR id_file = $2")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_crc = CString::new("DELETE FROM copy_block_crc WHERE data_object_id = $1 OR id_file = $2")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_data_object = CString::new("DELETE FROM data_objects WHERE id_data_object = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_touch_dst_object = CString::new(
            "UPDATE data_objects SET reference_count = GREATEST(reference_count - 1, 0), modification_date = NOW() WHERE id_data_object = $1",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let fetch_file_info = |conn: *mut PGconn, file_id: &CString| -> Result<Option<(u64, u64)>, String> {
            unsafe {
                let params = [file_id];
                let res = exec_params(conn, &sql_file_info, &params)?;
                let info = match PQresultStatus(res) {
                    PGRES_TUPLES_OK => {
                        let rows = PQntuples(res);
                        let cols = PQnfields(res);
                        let value = if rows < 1 || cols < 2 {
                            None
                        } else {
                            let size_ptr = PQgetvalue(res, 0, 0);
                            let data_object_ptr = PQgetvalue(res, 0, 1);
                            if size_ptr.is_null() || data_object_ptr.is_null() {
                                None
                            } else {
                                let size = CStr::from_ptr(size_ptr).to_string_lossy().trim().parse::<u64>().ok();
                                let data_object_id = CStr::from_ptr(data_object_ptr).to_string_lossy().trim().parse::<u64>().ok();
                                match (size, data_object_id) {
                                    (Some(size), Some(data_object_id)) => Some((size, data_object_id)),
                                    _ => None,
                                }
                            }
                        };
                        PQclear(res);
                        value
                    }
                    _ => {
                        PQclear(res);
                        return Err(conn_error(conn));
                    }
                };
                Ok(info)
            }
        };
        let src_file_id = CString::new(src_file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        let dst_file_id = CString::new(dst_file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                let src_info = fetch_file_info(conn, &src_file_id)?;
                let (src_size, src_data_object_id) = match src_info {
                    Some(value) => value,
                    None => return Ok(false),
                };
                if src_size == 0 {
                    return Ok(false);
                }

                let dst_info = fetch_file_info(conn, &dst_file_id)?;
                let (dst_size, dst_data_object_id) = match dst_info {
                    Some(value) => value,
                    None => return Ok(false),
                };
                if dst_size != 0 || src_data_object_id == dst_data_object_id {
                    return Ok(false);
                }

                let src_data_object_id = CString::new(src_data_object_id.to_string())
                    .map_err(|_| "data object id contains NUL byte".to_string())?;
                let dst_data_object_id = CString::new(dst_data_object_id.to_string())
                    .map_err(|_| "data object id contains NUL byte".to_string())?;
                let src_size_text = CString::new(src_size.to_string())
                    .map_err(|_| "file size contains NUL byte".to_string())?;

                let params = [&src_data_object_id];
                exec_command_params(conn, &sql_touch_src_object, &params)?;

                let params = [&src_data_object_id, &src_size_text, &dst_file_id];
                exec_command_params(conn, &sql_update_dst_file, &params)?;

                let params = [&dst_data_object_id];
                let res = exec_params(conn, &sql_count_dst_references, &params)?;
                let dst_reference_count = fetch_single_text(res)?
                    .trim()
                    .parse::<u64>()
                    .unwrap_or(0);

                if dst_reference_count <= 1 {
                    let params = [&dst_data_object_id, &dst_file_id];
                    exec_command_params(conn, &sql_delete_data, &params)?;
                    exec_command_params(conn, &sql_delete_crc, &params)?;
                    let params = [&dst_data_object_id];
                    exec_command_params(conn, &sql_delete_data_object, &params)?;
                } else {
                    let params = [&dst_data_object_id];
                    exec_command_params(conn, &sql_touch_dst_object, &params)?;
                }

                Ok(true)
            })
        })
    }

    pub fn set_file_size(&self, file_id: u64, file_size: u64) -> Result<(), String> {
        let sql_update_file = CString::new(
            "UPDATE files SET size = $1, modification_date = NOW(), change_date = NOW() WHERE id_file = $2",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        let file_size = CString::new(file_size.to_string()).map_err(|_| "file size contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                let params = [&file_size, &file_id];
                exec_command_params(conn, &sql_update_file, &params)?;
                Ok(())
            })
        })
    }

    pub fn purge_primary_file(&self, file_id: u64) -> Result<(), String> {
        let sql_lookup = CString::new("SELECT data_object_id FROM files WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_data = CString::new("DELETE FROM data_blocks WHERE data_object_id = $1 OR id_file = $2")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_crc = CString::new("DELETE FROM copy_block_crc WHERE data_object_id = $1 OR id_file = $2")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_file = CString::new("DELETE FROM files WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let sql_delete_data_object = CString::new("DELETE FROM data_objects WHERE id_data_object = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            transactional(conn, |conn| {
                let data_object_id = match {
                    let params = [&file_id];
                    let res = exec_params(conn, &sql_lookup, &params)?;
                    let text = fetch_single_text(res)?;
                    if text.is_empty() {
                        None
                    } else {
                        Some(text.parse::<u64>().map_err(|_| "invalid data_object_id value".to_string())?)
                    }
                } {
                    Some(value) => value,
                    None => {
                        let params = [&file_id];
                        exec_command_params(conn, &sql_delete_file, &params)?;
                        return Ok(());
                    }
                };
                let data_object_id = CString::new(data_object_id.to_string())
                    .map_err(|_| "data object id contains NUL byte".to_string())?;
                let params = [&data_object_id, &file_id];
                exec_command_params(conn, &sql_delete_data, &params)?;
                exec_command_params(conn, &sql_delete_crc, &params)?;
                let params = [&data_object_id];
                exec_command_params(conn, &sql_delete_data_object, &params)?;
                exec_command_params(conn, &sql_delete_file, &params)?;
                Ok(())
            })
        })
    }

    pub fn count_file_links(&self, file_id: u64) -> Result<u64, String> {
        let sql = CString::new("SELECT 1 + COUNT(*) FROM hardlinks WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string())
            .map_err(|_| "file id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values = [file_id.as_ptr()];
                let param_lengths = [file_id.as_bytes().len() as c_int];
                let param_formats = [0 as c_int];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    1,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                0
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    0
                                } else {
                                    CStr::from_ptr(value_ptr).to_string_lossy().trim().parse::<u64>().unwrap_or(0)
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn count_file_blocks(&self, file_id: u64) -> Result<u64, String> {
        let sql = CString::new(
            "
            SELECT COUNT(*)
            FROM data_blocks db
            JOIN files f ON f.data_object_id = db.data_object_id
            WHERE f.id_file = $1
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string())
            .map_err(|_| "file id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values = [file_id.as_ptr()];
                let param_lengths = [file_id.as_bytes().len() as c_int];
                let param_formats = [0 as c_int];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    1,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                0
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    0
                                } else {
                                    CStr::from_ptr(value_ptr).to_string_lossy().trim().parse::<u64>().unwrap_or(0)
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn path_has_children(&self, directory_id: u64) -> Result<bool, String> {
        let sql = CString::new(
            "
            SELECT 1
            FROM files
            WHERE id_directory = $1
            UNION ALL
            SELECT 1
            FROM directories
            WHERE id_parent = $1
            UNION ALL
            SELECT 1
            FROM hardlinks
            WHERE id_directory = $1
            UNION ALL
            SELECT 1
            FROM symlinks
            WHERE id_parent = $1
            LIMIT 1
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values = [directory_id.as_ptr()];
                let param_lengths = [directory_id.as_bytes().len() as c_int];
                let param_formats = [0 as c_int];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    1,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = rows >= 1 && cols >= 1;
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn count_directory_children(&self, directory_id: u64) -> Result<u64, String> {
        let sql = CString::new(
            "
            SELECT
                (SELECT COUNT(*) FROM directories WHERE id_parent = $1)
              + (SELECT COUNT(*) FROM files WHERE id_directory = $1)
              + (SELECT COUNT(*) FROM hardlinks WHERE id_directory = $1)
              + (SELECT COUNT(*) FROM symlinks WHERE id_parent = $1)
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&directory_id];
            let res = exec_params(conn, &sql, &params)?;
            let text = fetch_single_text(res)?;
            let value = text
                .trim()
                .parse::<u64>()
                .map_err(|_| "invalid directory children count".to_string())?;
            Ok(value)
        })
    }

    pub fn count_directory_subdirs(&self, directory_id: u64) -> Result<u64, String> {
        let sql = CString::new("SELECT COUNT(*) FROM directories WHERE id_parent = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&directory_id];
            let res = exec_params(conn, &sql, &params)?;
            let text = fetch_single_text(res)?;
            let value = text
                .trim()
                .parse::<u64>()
                .map_err(|_| "invalid directory subdir count".to_string())?;
            Ok(value)
        })
    }

    pub fn count_root_directory_children(&self) -> Result<u64, String> {
        let sql = CString::new(
            "
            SELECT COUNT(*)
            FROM directories
            WHERE id_parent IS NULL AND name != '/'
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let res = exec_params(conn, &sql, &[])?;
            let text = fetch_single_text(res)?;
            let value = text
                .trim()
                .parse::<u64>()
                .map_err(|_| "invalid root directory children count".to_string())?;
            Ok(value)
        })
    }

    pub fn count_symlinks(&self) -> Result<u64, String> {
        let sql = CString::new("SELECT COUNT(*) FROM symlinks")
            .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let res = exec_params(conn, &sql, &[])?;
            let text = fetch_single_text(res)?;
            let value = text
                .trim()
                .parse::<u64>()
                .map_err(|_| "invalid symlink count".to_string())?;
            Ok(value)
        })
    }

    pub fn count_files(&self) -> Result<u64, String> {
        let sql = CString::new("SELECT COUNT(*) FROM files")
            .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let res = exec_params(conn, &sql, &[])?;
            let text = fetch_single_text(res)?;
            let value = text
                .trim()
                .parse::<u64>()
                .map_err(|_| "invalid file count".to_string())?;
            Ok(value)
        })
    }

    pub fn count_directories(&self) -> Result<u64, String> {
        let sql = CString::new("SELECT COUNT(*) FROM directories")
            .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let res = exec_params(conn, &sql, &[])?;
            let text = fetch_single_text(res)?;
            let value = text
                .trim()
                .parse::<u64>()
                .map_err(|_| "invalid directory count".to_string())?;
            Ok(value)
        })
    }

    pub fn total_data_size(&self) -> Result<u64, String> {
        let sql = CString::new("SELECT COALESCE(SUM(LENGTH(data)), 0) FROM data_blocks")
            .map_err(|_| "SQL contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let res = exec_params(conn, &sql, &[])?;
            let text = fetch_single_text(res)?;
            let value = text
                .trim()
                .parse::<u64>()
                .map_err(|_| "invalid total data size".to_string())?;
            Ok(value)
        })
    }

    pub fn load_symlink_target(&self, symlink_id: u64) -> Result<Option<String>, String> {
        let sql = CString::new("SELECT target FROM symlinks WHERE id_symlink = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let symlink_id = CString::new(symlink_id.to_string())
            .map_err(|_| "symlink id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&symlink_id];
            let res = exec_params(conn, &sql, &params)?;
            let text = fetch_single_text(res)?;
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        })
    }

    pub fn get_special_file_metadata(&self, file_id: u64) -> Result<Option<(String, u32, u32)>, String> {
        let sql = CString::new("SELECT file_type, rdev_major, rdev_minor FROM special_files WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string())
            .map_err(|_| "file id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&file_id];
            let res = exec_params(conn, &sql, &params)?;
            let result = {
                if PQresultStatus(res) != PGRES_TUPLES_OK {
                    PQclear(res);
                    return Err(conn_error(conn));
                }
                let rows = PQntuples(res);
                let cols = PQnfields(res);
                if rows < 1 || cols < 3 {
                    PQclear(res);
                    return Ok(None);
                }
                let file_type_ptr = PQgetvalue(res, 0, 0);
                let major_ptr = PQgetvalue(res, 0, 1);
                let minor_ptr = PQgetvalue(res, 0, 2);
                if file_type_ptr.is_null() || major_ptr.is_null() || minor_ptr.is_null() {
                    PQclear(res);
                    return Ok(None);
                }
                let file_type = CStr::from_ptr(file_type_ptr).to_string_lossy().to_string();
                let rdev_major = CStr::from_ptr(major_ptr)
                    .to_string_lossy()
                    .trim()
                    .parse::<u32>()
                    .map_err(|_| "invalid special file major".to_string())?;
                let rdev_minor = CStr::from_ptr(minor_ptr)
                    .to_string_lossy()
                    .trim()
                    .parse::<u32>()
                    .map_err(|_| "invalid special file minor".to_string())?;
                PQclear(res);
                Ok(Some((file_type, rdev_major, rdev_minor)))
            };
            result
        })
    }

    pub fn get_symlink_id(&self, path: &str) -> Result<Option<u64>, String> {
        let normalized = path.trim();
        let (parent_path, link_name) = match normalized.rsplit_once('/') {
            Some((parent, name)) if !name.is_empty() => (if parent.is_empty() { "/" } else { parent }, name),
            _ => ("/", normalized),
        };
        let parent_id = self.get_dir_id(parent_path)?;
        let link_name = CString::new(link_name).map_err(|_| "path contains NUL byte".to_string())?;
        let parent_id_text = parent_id
            .map(|value| CString::new(value.to_string()).map_err(|_| "parent id contains NUL byte".to_string()))
            .transpose()?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let sql = if parent_id_text.is_some() {
                    CString::new(
                        "
                        SELECT id_symlink
                        FROM symlinks
                        WHERE name = $1 AND id_parent = $2
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?
                } else {
                    CString::new(
                        "
                        SELECT id_symlink
                        FROM symlinks
                        WHERE name = $1 AND id_parent IS NULL
                        ",
                    )
                    .map_err(|_| "SQL contains NUL byte".to_string())?
                };

                let res = if let Some(ref parent_id_text) = parent_id_text {
                    let param_values = [link_name.as_ptr(), parent_id_text.as_ptr()];
                    let param_lengths = [link_name.as_bytes().len() as c_int, parent_id_text.as_bytes().len() as c_int];
                    let param_formats = [0 as c_int, 0 as c_int];
                    PQexecParams(
                        conn,
                        sql.as_ptr(),
                        2,
                        std::ptr::null(),
                        param_values.as_ptr(),
                        param_lengths.as_ptr(),
                        param_formats.as_ptr(),
                        0,
                    )
                } else {
                    let param_values = [link_name.as_ptr()];
                    let param_lengths = [link_name.as_bytes().len() as c_int];
                    let param_formats = [0 as c_int];
                    PQexecParams(
                        conn,
                        sql.as_ptr(),
                        1,
                        std::ptr::null(),
                        param_values.as_ptr(),
                        param_lengths.as_ptr(),
                        param_formats.as_ptr(),
                        0,
                    )
                };
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    let value = CStr::from_ptr(value_ptr).to_string_lossy().to_string();
                                    value.trim().parse::<u64>().ok()
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn resolve_path(&self, path: &str) -> Result<ResolvedPath, String> {
        let normalized = path.trim();
        if normalized.is_empty() {
            return Ok(ResolvedPath {
                parent_id: None,
                kind: None,
                entry_id: None,
            });
        }

        let (parent_path, name) = match normalized.rsplit_once('/') {
            Some((parent, name)) if !name.is_empty() => (if parent.is_empty() { "/" } else { parent }, name),
            _ => ("/", normalized),
        };
        let parent_id = self.get_dir_id(parent_path)?;

        let (sql, params) = if parent_id.is_none() {
            (
                CString::new(
                    "
                    SELECT kind, entry_id FROM (
                        SELECT 1 AS precedence, 'hardlink' AS kind, h.id_hardlink AS entry_id
                        FROM hardlinks h
                        WHERE h.name = $1 AND h.id_directory IS NULL
                        UNION ALL
                        SELECT 2 AS precedence, 'symlink' AS kind, s.id_symlink AS entry_id
                        FROM symlinks s
                        WHERE s.name = $1 AND s.id_parent IS NULL
                        UNION ALL
                        SELECT 3 AS precedence, 'file' AS kind, f.id_file AS entry_id
                        FROM files f
                        WHERE f.name = $1 AND f.id_directory IS NULL
                        UNION ALL
                        SELECT 4 AS precedence, 'dir' AS kind, d.id_directory AS entry_id
                        FROM directories d
                        WHERE d.name = $1 AND d.id_parent IS NULL
                    ) entries
                    ORDER BY precedence
                    LIMIT 1
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![CString::new(name).map_err(|_| "path contains NUL byte".to_string())?],
            )
        } else {
            (
                CString::new(
                    "
                    SELECT kind, entry_id FROM (
                        SELECT 1 AS precedence, 'hardlink' AS kind, h.id_hardlink AS entry_id
                        FROM hardlinks h
                        WHERE h.name = $1 AND h.id_directory = $2
                        UNION ALL
                        SELECT 2 AS precedence, 'symlink' AS kind, s.id_symlink AS entry_id
                        FROM symlinks s
                        WHERE s.name = $1 AND s.id_parent = $2
                        UNION ALL
                        SELECT 3 AS precedence, 'file' AS kind, f.id_file AS entry_id
                        FROM files f
                        WHERE f.name = $1 AND f.id_directory = $2
                        UNION ALL
                        SELECT 4 AS precedence, 'dir' AS kind, d.id_directory AS entry_id
                        FROM directories d
                        WHERE d.name = $1 AND d.id_parent = $2
                    ) entries
                    ORDER BY precedence
                    LIMIT 1
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![
                    CString::new(name).map_err(|_| "path contains NUL byte".to_string())?,
                    CString::new(parent_id.unwrap().to_string()).map_err(|_| "parent id contains NUL byte".to_string())?,
                ],
            )
        };

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values: Vec<*const c_char> = params.iter().map(|value| value.as_ptr()).collect();
                let param_lengths: Vec<c_int> = params.iter().map(|value| value.as_bytes().len() as c_int).collect();
                let param_formats: Vec<c_int> = vec![0; params.len()];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    params.len() as c_int,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let value = if rows < 1 || cols < 2 {
                                None
                            } else {
                                let kind_ptr = PQgetvalue(res, 0, 0);
                                let entry_ptr = PQgetvalue(res, 0, 1);
                                if kind_ptr.is_null() || entry_ptr.is_null() {
                                    None
                                } else {
                                    let kind = CStr::from_ptr(kind_ptr).to_string_lossy().to_string();
                                    let entry_id = CStr::from_ptr(entry_ptr).to_string_lossy().parse::<u64>().ok();
                                    entry_id.map(|entry_id| (kind, entry_id))
                                }
                            };
                            PQclear(res);
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result.map(|entry| ResolvedPath {
                parent_id,
                kind: entry.as_ref().map(|(kind, _)| kind.clone()),
                entry_id: entry.map(|(_, entry_id)| entry_id),
            })
        })
    }

    pub fn fetch_xattr_value(&self, path: &str, name: &str) -> Result<Option<Vec<u8>>, String> {
        let resolved = self.resolve_path(path)?;
        let (owner_kind, owner_id) = match resolved.kind.as_deref() {
            Some("hardlink") => {
                let file_id = self.get_file_id(path)?;
                match file_id {
                    Some(file_id) => ("file".to_string(), file_id),
                    None => return Ok(None),
                }
            }
            Some("file") => match resolved.entry_id {
                Some(entry_id) => ("file".to_string(), entry_id),
                None => return Ok(None),
            },
            Some("dir") => ("dir".to_string(), resolved.entry_id.unwrap_or(0)),
            Some("symlink") => match resolved.entry_id {
                Some(entry_id) => ("symlink".to_string(), entry_id),
                None => return Ok(None),
            },
            _ => return Ok(None),
        };

        let sql = CString::new(
            "SELECT encode(value, 'base64') FROM xattrs WHERE owner_kind = $1 AND owner_id = $2 AND name = $3",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let owner_kind = CString::new(owner_kind).map_err(|_| "owner kind contains NUL byte".to_string())?;
        let owner_id = CString::new(owner_id.to_string()).map_err(|_| "owner id contains NUL byte".to_string())?;
        let name = CString::new(name).map_err(|_| "xattr name contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let result = {
                let param_values = [owner_kind.as_ptr(), owner_id.as_ptr(), name.as_ptr()];
                let param_lengths = [
                    owner_kind.as_bytes().len() as c_int,
                    owner_id.as_bytes().len() as c_int,
                    name.as_bytes().len() as c_int,
                ];
                let param_formats = [0 as c_int; 3];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    3,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let rows = PQntuples(res);
                            let cols = PQnfields(res);
                            let encoded = if rows < 1 || cols < 1 {
                                None
                            } else {
                                let value_ptr = PQgetvalue(res, 0, 0);
                                if value_ptr.is_null() {
                                    None
                                } else {
                                    Some(CStr::from_ptr(value_ptr).to_string_lossy().to_string())
                                }
                            };
                            PQclear(res);
                            let value = match encoded {
                                None => None,
                                Some(encoded) => {
                                    let decoded = BASE64_STANDARD
                                        .decode(encoded.trim())
                                        .map_err(|err| format!("invalid xattr payload returned by PostgreSQL: {err}"))?;
                                    Some(decoded)
                                }
                            };
                            Ok(value)
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            result
        })
    }

    pub fn list_xattr_names(&self, path: &str) -> Result<Option<Vec<String>>, String> {
        let resolved = self.resolve_path(path)?;
        let (owner_kind, owner_id) = match resolved.kind.as_deref() {
            Some("hardlink") => {
                let file_id = self.get_file_id(path)?;
                match file_id {
                    Some(file_id) => ("file".to_string(), file_id),
                    None => return Ok(None),
                }
            }
            Some("file") => match resolved.entry_id {
                Some(entry_id) => ("file".to_string(), entry_id),
                None => return Ok(None),
            },
            Some("dir") => ("dir".to_string(), resolved.entry_id.unwrap_or(0)),
            Some("symlink") => match resolved.entry_id {
                Some(entry_id) => ("symlink".to_string(), entry_id),
                None => return Ok(None),
            },
            _ => return Ok(None),
        };

        let sql = CString::new("SELECT name FROM xattrs WHERE owner_kind = $1 AND owner_id = $2 ORDER BY name")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let owner_kind = CString::new(owner_kind).map_err(|_| "owner kind contains NUL byte".to_string())?;
        let owner_id = CString::new(owner_id.to_string()).map_err(|_| "owner id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let param_values = [owner_kind.as_ptr(), owner_id.as_ptr()];
            let param_lengths = [owner_kind.as_bytes().len() as c_int, owner_id.as_bytes().len() as c_int];
            let param_formats = [0 as c_int, 0 as c_int];
            let res = PQexecParams(
                conn,
                sql.as_ptr(),
                2,
                std::ptr::null(),
                param_values.as_ptr(),
                param_lengths.as_ptr(),
                param_formats.as_ptr(),
                0,
            );
            if res.is_null() {
                return Err(conn_error(conn));
            }
            let result = match PQresultStatus(res) {
                PGRES_TUPLES_OK => {
                    let rows = PQntuples(res);
                    let cols = PQnfields(res);
                    let mut names = Vec::with_capacity(rows.max(0) as usize);
                    if rows >= 1 && cols >= 1 {
                        for row in 0..rows {
                            let value_ptr = PQgetvalue(res, row, 0);
                            if !value_ptr.is_null() {
                                names.push(CStr::from_ptr(value_ptr).to_string_lossy().to_string());
                            }
                        }
                    }
                    Ok(Some(names))
                }
                _ => Err(conn_error(conn)),
            };
            PQclear(res);
            result
        })
    }

    pub fn store_xattr_value_for_owner(&self, owner_kind: &str, owner_id: u64, name: &str, value: &[u8]) -> Result<(), String> {
        let sql = CString::new(
            "
            INSERT INTO xattrs (owner_kind, owner_id, name, value)
            VALUES ($1, $2, $3, decode($4, 'base64'))
            ON CONFLICT (owner_kind, owner_id, name) DO UPDATE
            SET value = EXCLUDED.value
            ",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let owner_kind = CString::new(owner_kind).map_err(|_| "owner kind contains NUL byte".to_string())?;
        let owner_id = CString::new(owner_id.to_string()).map_err(|_| "owner id contains NUL byte".to_string())?;
        let name = CString::new(name).map_err(|_| "xattr name contains NUL byte".to_string())?;
        let value_b64 = CString::new(BASE64_STANDARD.encode(value)).map_err(|_| "xattr value contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&owner_kind, &owner_id, &name, &value_b64];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn delete_owner_xattrs(&self, owner_kind: &str, owner_id: u64) -> Result<(), String> {
        let sql = CString::new("DELETE FROM xattrs WHERE owner_kind = $1 AND owner_id = $2")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let owner_kind = CString::new(owner_kind).map_err(|_| "owner kind contains NUL byte".to_string())?;
        let owner_id = CString::new(owner_id.to_string()).map_err(|_| "owner id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&owner_kind, &owner_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn remove_xattr_for_owner(&self, owner_kind: &str, owner_id: u64, name: &str) -> Result<u64, String> {
        let sql = CString::new("DELETE FROM xattrs WHERE owner_kind = $1 AND owner_id = $2 AND name = $3")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let owner_kind = CString::new(owner_kind).map_err(|_| "owner kind contains NUL byte".to_string())?;
        let owner_id = CString::new(owner_id.to_string()).map_err(|_| "owner id contains NUL byte".to_string())?;
        let name = CString::new(name).map_err(|_| "xattr name contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let params = [&owner_kind, &owner_id, &name];
            let res = exec_params(conn, &sql, &params)?;
            let rows = PQntuples(res);
            PQclear(res);
            Ok(rows as u64)
        })
    }

    pub fn list_directory_entries_blob(&self, path: &str) -> Result<Option<Vec<u8>>, String> {
        let normalized = path.trim();
        let parent_id = self.get_dir_id(normalized)?;
        let (sql, params) = if let Some(parent_id) = parent_id {
            (
                CString::new(
                    "
                    SELECT name FROM files WHERE id_directory = $1
                    UNION ALL
                    SELECT name FROM hardlinks WHERE id_directory = $1
                    UNION ALL
                    SELECT name FROM directories WHERE id_parent = $1
                    UNION ALL
                    SELECT name FROM symlinks WHERE id_parent = $1
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![CString::new(parent_id.to_string()).map_err(|_| "parent id contains NUL byte".to_string())?],
            )
        } else {
            (
                CString::new(
                    "
                    SELECT name FROM directories WHERE id_parent IS NULL AND name != '/'
                    UNION ALL
                    SELECT name FROM files WHERE id_directory IS NULL
                    UNION ALL
                    SELECT name FROM hardlinks WHERE id_directory IS NULL
                    UNION ALL
                    SELECT name FROM symlinks WHERE id_parent IS NULL
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                Vec::new(),
            )
        };

        self.with_cached_connection(|conn| unsafe {
            let res = {
                let param_values = params.iter().map(|value| value.as_ptr()).collect::<Vec<_>>();
                let param_lengths = params
                    .iter()
                    .map(|value| value.as_bytes().len() as c_int)
                    .collect::<Vec<_>>();
                let param_formats = vec![0 as c_int; params.len()];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    params.len() as c_int,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let names = fetch_first_column_texts(res)?;
                            Ok(Some(join_nul_text(&names)))
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            res
        })
    }

    pub fn fetch_path_attrs_blob(&self, path: &str) -> Result<Option<Vec<u8>>, String> {
        let resolved = self.resolve_path(path)?;
        let kind = match resolved.kind.as_deref() {
            Some(kind) => kind,
            None => return Ok(None),
        };
        let entry_id = match resolved.entry_id {
            Some(entry_id) => entry_id,
            None => return Ok(None),
        };

        let (sql, params) = match kind {
            "file" => (
                CString::new(
                    "
                    SELECT id_file, size, mode, modification_date, access_date, change_date, uid, gid, inode_seed
                    FROM files
                    WHERE id_file = $1
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![CString::new(entry_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?],
            ),
            "dir" => (
                CString::new(
                    "
                    SELECT id_directory, 0, mode, modification_date, access_date, change_date, uid, gid, inode_seed
                    FROM directories
                    WHERE id_directory = $1
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![CString::new(entry_id.to_string()).map_err(|_| "directory id contains NUL byte".to_string())?],
            ),
            "symlink" => (
                CString::new(
                    "
                    SELECT id_symlink, target, modification_date, access_date, change_date, uid, gid, inode_seed
                    FROM symlinks
                    WHERE id_symlink = $1
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![CString::new(entry_id.to_string()).map_err(|_| "symlink id contains NUL byte".to_string())?],
            ),
            "hardlink" => (
                CString::new(
                    "
                    SELECT id_hardlink, files.size, files.mode, files.modification_date, files.access_date, files.change_date, files.uid, files.gid, files.inode_seed
                    FROM hardlinks
                    JOIN files ON hardlinks.id_file = files.id_file
                    WHERE hardlinks.id_hardlink = $1
                    ",
                )
                .map_err(|_| "SQL contains NUL byte".to_string())?,
                vec![CString::new(entry_id.to_string()).map_err(|_| "hardlink id contains NUL byte".to_string())?],
            ),
            _ => return Ok(None),
        };

        self.with_cached_connection(|conn| unsafe {
            let res = {
                let param_values = params.iter().map(|value| value.as_ptr()).collect::<Vec<_>>();
                let param_lengths = params
                    .iter()
                    .map(|value| value.as_bytes().len() as c_int)
                    .collect::<Vec<_>>();
                let param_formats = vec![0 as c_int; params.len()];
                let res = PQexecParams(
                    conn,
                    sql.as_ptr(),
                    params.len() as c_int,
                    std::ptr::null(),
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    match PQresultStatus(res) {
                        PGRES_TUPLES_OK => {
                            let row = fetch_first_row_texts(res)?;
                            if row.is_empty() {
                                Ok(None)
                            } else {
                                let mut output = Vec::new();
                                output.extend_from_slice(kind.as_bytes());
                                output.push(0);
                                output.extend_from_slice(&join_nul_text(&row));
                                Ok(Some(output))
                            }
                        }
                        _ => {
                            PQclear(res);
                            Err(conn_error(conn))
                        }
                    }
                }
            };
            res
        })
    }

    pub fn update_file_mode(&self, file_id: u64, mode: &str) -> Result<(), String> {
        let sql = CString::new(
            "UPDATE files SET mode = $1, change_date = NOW(), modification_date = NOW() WHERE id_file = $2",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let mode = CString::new(mode).map_err(|_| "mode contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&mode, &file_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn update_directory_mode(&self, directory_id: u64, mode: &str) -> Result<(), String> {
        let sql = CString::new(
            "UPDATE directories SET mode = $1, change_date = NOW(), modification_date = NOW() WHERE id_directory = $2",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let mode = CString::new(mode).map_err(|_| "mode contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&mode, &directory_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn update_file_owner(&self, file_id: u64, uid: u32, gid: u32, mode: &str) -> Result<(), String> {
        let sql = CString::new(
            "UPDATE files SET uid = $1, gid = $2, mode = $3, change_date = NOW(), modification_date = NOW() WHERE id_file = $4",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let uid = CString::new(uid.to_string()).map_err(|_| "uid contains NUL byte".to_string())?;
        let gid = CString::new(gid.to_string()).map_err(|_| "gid contains NUL byte".to_string())?;
        let mode = CString::new(mode).map_err(|_| "mode contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&uid, &gid, &mode, &file_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn update_directory_owner(&self, directory_id: u64, uid: u32, gid: u32, mode: &str) -> Result<(), String> {
        let sql = CString::new(
            "UPDATE directories SET uid = $1, gid = $2, mode = $3, change_date = NOW(), modification_date = NOW() WHERE id_directory = $4",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let uid = CString::new(uid.to_string()).map_err(|_| "uid contains NUL byte".to_string())?;
        let gid = CString::new(gid.to_string()).map_err(|_| "gid contains NUL byte".to_string())?;
        let mode = CString::new(mode).map_err(|_| "mode contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&uid, &gid, &mode, &directory_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn update_symlink_owner(&self, symlink_id: u64, uid: u32, gid: u32) -> Result<(), String> {
        let sql = CString::new(
            "UPDATE symlinks SET uid = $1, gid = $2, change_date = NOW(), modification_date = NOW() WHERE id_symlink = $3",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let uid = CString::new(uid.to_string()).map_err(|_| "uid contains NUL byte".to_string())?;
        let gid = CString::new(gid.to_string()).map_err(|_| "gid contains NUL byte".to_string())?;
        let symlink_id = CString::new(symlink_id.to_string()).map_err(|_| "symlink id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&uid, &gid, &symlink_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn touch_file_times(&self, file_id: u64, atime: &str, mtime: &str) -> Result<(), String> {
        let sql = CString::new(
            "UPDATE files SET access_date = $1, modification_date = $2, change_date = NOW() WHERE id_file = $3",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let atime = CString::new(atime).map_err(|_| "atime contains NUL byte".to_string())?;
        let mtime = CString::new(mtime).map_err(|_| "mtime contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&atime, &mtime, &file_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn touch_directory_times(&self, directory_id: u64, atime: &str, mtime: &str) -> Result<(), String> {
        let sql = CString::new(
            "UPDATE directories SET access_date = $1, modification_date = $2, change_date = NOW() WHERE id_directory = $3",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let atime = CString::new(atime).map_err(|_| "atime contains NUL byte".to_string())?;
        let mtime = CString::new(mtime).map_err(|_| "mtime contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&atime, &mtime, &directory_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn update_file_access_date(&self, file_id: u64, atime: &str) -> Result<(), String> {
        let sql = CString::new("UPDATE files SET access_date = $1 WHERE id_file = $2")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let atime = CString::new(atime).map_err(|_| "atime contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string()).map_err(|_| "file id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&atime, &file_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn update_directory_access_date(&self, directory_id: u64, atime: &str) -> Result<(), String> {
        let sql = CString::new("UPDATE directories SET access_date = $1 WHERE id_directory = $2")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let atime = CString::new(atime).map_err(|_| "atime contains NUL byte".to_string())?;
        let directory_id = CString::new(directory_id.to_string())
            .map_err(|_| "directory id contains NUL byte".to_string())?;
        self.with_cached_connection(|conn| unsafe {
            let params = [&atime, &directory_id];
            exec_params(conn, &sql, &params).map(|_| ())
        })
    }

    pub fn append_journal_event(
        &self,
        id_user: u32,
        directory_id: Option<u64>,
        file_id: Option<u64>,
        action: &str,
    ) -> Result<(), String> {
        let sql = CString::new(
            "INSERT INTO journal (id_user, id_directory, id_file, action, date_time) VALUES ($1, $2, $3, $4, NOW())",
        )
        .map_err(|_| "SQL contains NUL byte".to_string())?;
        let id_user = CString::new(id_user.to_string()).map_err(|_| "user id contains NUL byte".to_string())?;
        let action = CString::new(action).map_err(|_| "journal action contains NUL byte".to_string())?;
        let directory_id = directory_id
            .map(|value| CString::new(value.to_string()).map_err(|_| "directory id contains NUL byte".to_string()))
            .transpose()?;
        let file_id = file_id
            .map(|value| CString::new(value.to_string()).map_err(|_| "file id contains NUL byte".to_string()))
            .transpose()?;

        self.with_cached_connection(|conn| unsafe {
            let (directory_ptr, directory_len) = match directory_id.as_ref() {
                Some(value) => (value.as_ptr(), value.as_bytes().len() as c_int),
                None => (std::ptr::null(), 0),
            };
            let (file_ptr, file_len) = match file_id.as_ref() {
                Some(value) => (value.as_ptr(), value.as_bytes().len() as c_int),
                None => (std::ptr::null(), 0),
            };
            let param_values = [id_user.as_ptr(), directory_ptr, file_ptr, action.as_ptr()];
            let param_lengths = [
                id_user.as_bytes().len() as c_int,
                directory_len,
                file_len,
                action.as_bytes().len() as c_int,
            ];
            let param_formats = [0 as c_int; 4];
            let res = PQexecParams(
                conn,
                sql.as_ptr(),
                4,
                std::ptr::null(),
                param_values.as_ptr(),
                param_lengths.as_ptr(),
                param_formats.as_ptr(),
                0,
            );
            if res.is_null() {
                return Err(conn_error(conn));
            }
            let status = PQresultStatus(res);
            PQclear(res);
            if status == PGRES_COMMAND_OK {
                Ok(())
            } else {
                Err(conn_error(conn))
            }
        })
    }

    pub fn list_xattr_names_for_owner(&self, owner_kind: &str, owner_id: u64) -> Result<Vec<String>, String> {
        let sql = CString::new("SELECT name FROM xattrs WHERE owner_kind = $1 AND owner_id = $2 ORDER BY name")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let owner_kind = CString::new(owner_kind).map_err(|_| "owner kind contains NUL byte".to_string())?;
        let owner_id = CString::new(owner_id.to_string()).map_err(|_| "owner id contains NUL byte".to_string())?;

        self.with_cached_connection(|conn| unsafe {
            let param_values = [owner_kind.as_ptr(), owner_id.as_ptr()];
            let param_lengths = [owner_kind.as_bytes().len() as c_int, owner_id.as_bytes().len() as c_int];
            let param_formats = [0 as c_int, 0 as c_int];
            let res = PQexecParams(
                conn,
                sql.as_ptr(),
                2,
                std::ptr::null(),
                param_values.as_ptr(),
                param_lengths.as_ptr(),
                param_formats.as_ptr(),
                0,
            );
            if res.is_null() {
                return Err(conn_error(conn));
            }
            let result = match PQresultStatus(res) {
                PGRES_TUPLES_OK => {
                    let rows = PQntuples(res);
                    let cols = PQnfields(res);
                    let mut names = Vec::with_capacity(rows.max(0) as usize);
                    if rows >= 1 && cols >= 1 {
                        for row in 0..rows {
                            let value_ptr = PQgetvalue(res, row, 0);
                            if !value_ptr.is_null() {
                                names.push(CStr::from_ptr(value_ptr).to_string_lossy().to_string());
                            }
                        }
                    }
                    Ok(names)
                }
                _ => Err(conn_error(conn)),
            };
            PQclear(res);
            result
        })
    }
}
