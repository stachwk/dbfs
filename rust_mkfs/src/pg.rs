use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

#[repr(C)]
struct PGconn {
    _private: [u8; 0],
}

#[repr(C)]
struct PGresult {
    _private: [u8; 0],
}

const CONNECTION_OK: c_int = 0;
const PGRES_COMMAND_OK: c_int = 1;
const PGRES_TUPLES_OK: c_int = 2;

#[link(name = "pq")]
unsafe extern "C" {
    fn PQconnectdb(conninfo: *const c_char) -> *mut PGconn;
    fn PQstatus(conn: *const PGconn) -> c_int;
    fn PQerrorMessage(conn: *const PGconn) -> *const c_char;
    fn PQexec(conn: *mut PGconn, command: *const c_char) -> *mut PGresult;
    fn PQresultStatus(res: *const PGresult) -> c_int;
    fn PQntuples(res: *const PGresult) -> c_int;
    fn PQnfields(res: *const PGresult) -> c_int;
    fn PQgetvalue(res: *const PGresult, row_number: c_int, field_number: c_int) -> *const c_char;
    fn PQclear(res: *mut PGresult);
    fn PQfinish(conn: *mut PGconn);
}

pub struct DbConn {
    conn: *mut PGconn,
}

impl Drop for DbConn {
    fn drop(&mut self) {
        unsafe {
            if !self.conn.is_null() {
                PQfinish(self.conn);
            }
        }
    }
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

fn connect_raw(conninfo: &str) -> Result<*mut PGconn, String> {
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

impl DbConn {
    pub fn connect(conninfo: &str) -> Result<Self, String> {
        Ok(Self { conn: connect_raw(conninfo)? })
    }

    fn exec_raw(&self, sql: &str) -> Result<(), String> {
        let sql = CString::new(sql).map_err(|_| "SQL text contains NUL byte".to_string())?;
        unsafe {
            let res = PQexec(self.conn, sql.as_ptr());
            if res.is_null() {
                return Err(conn_error(self.conn));
            }
            let status = PQresultStatus(res);
            PQclear(res);
            match status {
                PGRES_COMMAND_OK | PGRES_TUPLES_OK => Ok(()),
                _ => Err(conn_error(self.conn)),
            }
        }
    }

    pub fn exec(&self, sql: &str) -> Result<(), String> {
        self.exec_raw(sql)
    }

    pub fn query_scalar_text(&self, sql: &str) -> Result<Option<String>, String> {
        let sql = CString::new(sql).map_err(|_| "SQL text contains NUL byte".to_string())?;
        unsafe {
            let res = PQexec(self.conn, sql.as_ptr());
            if res.is_null() {
                return Err(conn_error(self.conn));
            }
            let out = match PQresultStatus(res) {
                PGRES_TUPLES_OK => {
                    let rows = PQntuples(res);
                    let cols = PQnfields(res);
                    if rows < 1 || cols < 1 {
                        Ok(None)
                    } else {
                        let value_ptr = PQgetvalue(res, 0, 0);
                        if value_ptr.is_null() {
                            Ok(None)
                        } else {
                            Ok(Some(CStr::from_ptr(value_ptr).to_string_lossy().to_string()))
                        }
                    }
                }
                _ => Err(conn_error(self.conn)),
            };
            PQclear(res);
            out
        }
    }

    pub fn query_scalar_bool(&self, sql: &str) -> Result<bool, String> {
        let value = self.query_scalar_text(sql)?;
        Ok(matches!(value.as_deref(), Some("t") | Some("true") | Some("1") | Some("on")))
    }

    pub fn query_exists(&self, sql: &str) -> Result<bool, String> {
        self.query_scalar_bool(sql)
    }

    pub fn quote_identifier(ident: &str) -> String {
        format!("\"{}\"", ident.replace('\"', "\"\""))
    }

    pub fn quote_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    pub fn query_scalar_u64(&self, sql: &str) -> Result<Option<u64>, String> {
        match self.query_scalar_text(sql)? {
            Some(value) if !value.is_empty() => value.parse::<u64>().map(Some).map_err(|e| e.to_string()),
            Some(_) => Ok(None),
            None => Ok(None),
        }
    }
}
