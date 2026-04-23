use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint};

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

pub struct DbRepo {
    conninfo: String,
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

impl DbRepo {
    pub fn new(conninfo: &str) -> Result<Self, String> {
        if conninfo.is_empty() {
            return Err("connection string is empty".to_string());
        }
        Ok(Self {
            conninfo: conninfo.to_string(),
        })
    }

    pub fn query_scalar_text(&self, sql: &str) -> Result<String, String> {
        let sql = CString::new(sql).map_err(|_| "SQL contains NUL byte".to_string())?;
        unsafe {
            let conn = connect(&self.conninfo)?;
            let result = {
                let res = PQexec(conn, sql.as_ptr());
                if res.is_null() {
                    Err(conn_error(conn))
                } else {
                    fetch_single_text(res)
                }
            };
            PQfinish(conn);
            result
        }
    }

    pub fn query_config_value(&self, key: &str) -> Result<Option<String>, String> {
        let sql = CString::new("SELECT value FROM config WHERE key = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let key = CString::new(key).map_err(|_| "config key contains NUL byte".to_string())?;

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
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

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
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

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
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

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
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

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
    }

    pub fn get_hardlink_file_id(&self, hardlink_id: u64) -> Result<Option<u64>, String> {
        let sql = CString::new("SELECT id_file FROM hardlinks WHERE id_hardlink = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let hardlink_id = CString::new(hardlink_id.to_string())
            .map_err(|_| "hardlink id contains NUL byte".to_string())?;

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
    }

    pub fn count_file_links(&self, file_id: u64) -> Result<u64, String> {
        let sql = CString::new("SELECT 1 + COUNT(*) FROM hardlinks WHERE id_file = $1")
            .map_err(|_| "SQL contains NUL byte".to_string())?;
        let file_id = CString::new(file_id.to_string())
            .map_err(|_| "file id contains NUL byte".to_string())?;

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
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

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
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

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result
        }
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

        unsafe {
            let conn = connect(&self.conninfo)?;
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
            PQfinish(conn);
            result.map(|entry| ResolvedPath {
                parent_id,
                kind: entry.as_ref().map(|(kind, _)| kind.clone()),
                entry_id: entry.map(|(_, entry_id)| entry_id),
            })
        }
    }
}
