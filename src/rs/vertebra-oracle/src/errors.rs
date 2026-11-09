//! Deterministic `ORA-xxxxx` to SQLSTATE and canonical `DbExceptionTypes` mapping.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyList, PyTuple, PyType};

/// Logical database exception category in the canonical Vertebra hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OracleErrorKind {
    Integrity,
    SerializationFailure,
    Operational,
    ConnectionFailure,
    QueryCancelled,
    NotFound,
    Data,
    Programming,
    Interface,
}

/// Deterministic classification result for an Oracle error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleErrorClassification {
    pub kind: OracleErrorKind,
    pub sqlstate: &'static str,
    pub vendor_code: Option<i32>,
    pub is_transient: bool,
}

/// Native Oracle driver error variants.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OracleError {
    /// Local configuration validation error (mapped to `InterfaceError`).
    #[error("{0}")]
    Config(String),
    /// Driver interface or lifecycle misuse error (mapped to `InterfaceError`).
    #[error("{0}")]
    Interface(String),
    /// Socket, TLS, wallet, or listener transport failure.
    #[error("{message}")]
    Transport {
        code: Option<i32>,
        message: String,
    },
    /// Oracle server or TNS error carrying a numeric `ORA-xxxxx` code.
    #[error("{message}")]
    Server {
        code: i32,
        message: String,
    },
    /// Statement cancelled via out-of-band `break_execution()` or future cancellation.
    #[error("{0}")]
    Cancelled(String),
}

/// Extract a numeric `ORA-xxxxx` vendor code from an Oracle error string if present.
#[must_use]
pub fn extract_ora_code(message: &str) -> Option<i32> {
    let bytes = message.as_bytes();
    let mut idx = 0;
    while idx + 8 <= bytes.len() {
        if bytes[idx..idx + 4].eq_ignore_ascii_case(b"ORA-") {
            let start = idx + 4;
            let mut end = start;
            while end < bytes.len() && end - start < 5 && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end - start >= 4
                && let Ok(digits) = std::str::from_utf8(&bytes[start..end])
                && let Ok(code) = digits.parse::<i32>()
            {
                return Some(code);
            }
        }
        idx += 1;
    }
    None
}

/// Classify an Oracle vendor code or message into a canonical `DbExceptionTypes` kind, SQLSTATE, and transience flag.
#[must_use]
pub fn classify_oracle_error(
    vendor_code: Option<i32>,
    message: &str,
) -> OracleErrorClassification {
    let resolved_code = vendor_code.or_else(|| extract_ora_code(message));
    match resolved_code {
        Some(1) => OracleErrorClassification {
            kind: OracleErrorKind::Integrity,
            sqlstate: "23505",
            vendor_code: Some(1),
            is_transient: false,
        },
        Some(code @ (2291 | 2292)) => OracleErrorClassification {
            kind: OracleErrorKind::Integrity,
            sqlstate: "23503",
            vendor_code: Some(code),
            is_transient: false,
        },
        Some(code @ (1400 | 1407)) => OracleErrorClassification {
            kind: OracleErrorKind::Integrity,
            sqlstate: "23502",
            vendor_code: Some(code),
            is_transient: false,
        },
        Some(2290) => OracleErrorClassification {
            kind: OracleErrorKind::Integrity,
            sqlstate: "23514",
            vendor_code: Some(2290),
            is_transient: false,
        },
        Some(8177) => OracleErrorClassification {
            kind: OracleErrorKind::SerializationFailure,
            sqlstate: "40001",
            vendor_code: Some(8177),
            is_transient: true,
        },
        Some(60) => OracleErrorClassification {
            kind: OracleErrorKind::SerializationFailure,
            sqlstate: "40P01",
            vendor_code: Some(60),
            is_transient: true,
        },
        Some(code @ (54 | 30006)) => OracleErrorClassification {
            kind: OracleErrorKind::Operational,
            sqlstate: "55P03",
            vendor_code: Some(code),
            is_transient: true,
        },
        Some(1013) => OracleErrorClassification {
            kind: OracleErrorKind::QueryCancelled,
            sqlstate: "57014",
            vendor_code: Some(1013),
            is_transient: false,
        },
        Some(code @ (1017 | 28000)) => OracleErrorClassification {
            kind: OracleErrorKind::ConnectionFailure,
            sqlstate: "28P01",
            vendor_code: Some(code),
            is_transient: false,
        },
        Some(12154) => OracleErrorClassification {
            kind: OracleErrorKind::ConnectionFailure,
            sqlstate: "08006",
            vendor_code: Some(12154),
            is_transient: false,
        },
        Some(code @ (12170 | 12514 | 12541 | 12545)) => OracleErrorClassification {
            kind: OracleErrorKind::ConnectionFailure,
            sqlstate: "08006",
            vendor_code: Some(code),
            is_transient: true,
        },
        Some(1403) => OracleErrorClassification {
            kind: OracleErrorKind::NotFound,
            sqlstate: "02000",
            vendor_code: Some(1403),
            is_transient: false,
        },
        Some(code @ (1438 | 1476 | 1722 | 1830..=1899 | 12899)) => OracleErrorClassification {
            kind: OracleErrorKind::Data,
            sqlstate: "22000",
            vendor_code: Some(code),
            is_transient: false,
        },
        Some(code @ (900..=999 | 6550)) => OracleErrorClassification {
            kind: OracleErrorKind::Programming,
            sqlstate: "42000",
            vendor_code: Some(code),
            is_transient: false,
        },
        Some(code @ (3113 | 3114 | 3135 | 4061 | 4068 | 12537)) => OracleErrorClassification {
            kind: OracleErrorKind::Operational,
            sqlstate: "08006",
            vendor_code: Some(code),
            is_transient: true,
        },
        Some(code) => OracleErrorClassification {
            kind: OracleErrorKind::Operational,
            sqlstate: "HY000",
            vendor_code: Some(code),
            is_transient: false,
        },
        None => OracleErrorClassification {
            kind: OracleErrorKind::ConnectionFailure,
            sqlstate: "08006",
            vendor_code: None,
            is_transient: true,
        },
    }
}

struct CachedOracleExceptionTypes {
    database: Py<PyType>,
    interface: Py<PyType>,
    data: Py<PyType>,
    not_found: Py<PyType>,
    operational: Py<PyType>,
    connection_failure: Py<PyType>,
    query_cancelled: Py<PyType>,
    serialization_failure: Py<PyType>,
    integrity: Py<PyType>,
    programming: Py<PyType>,
}

static ORACLE_EXCEPTION_TYPES: PyOnceLock<CachedOracleExceptionTypes> = PyOnceLock::new();

fn new_stand_in_class<'py>(
    py: Python<'py>,
    name: &str,
    bases: &Bound<'py, PyTuple>,
) -> PyResult<Py<PyType>> {
    let dict = PyDict::new(py);
    dict.set_item("__module__", "vertebra.db.driver")?;
    let ty = py
        .get_type::<PyType>()
        .call1((name, bases, dict))?
        .cast_into::<PyType>()?;
    Ok(ty.unbind())
}

fn build_stand_in_types(py: Python<'_>) -> PyResult<CachedOracleExceptionTypes> {
    let root = py.get_type::<pyo3::exceptions::PyException>();
    let database = new_stand_in_class(py, "DatabaseError", &PyTuple::new(py, [&root])?)?;
    let db_bound = database.bind(py);
    let interface = new_stand_in_class(py, "InterfaceError", &PyTuple::new(py, [db_bound])?)?;
    let data = new_stand_in_class(py, "DataError", &PyTuple::new(py, [db_bound])?)?;
    let not_found = new_stand_in_class(
        py,
        "NotFoundError",
        &PyTuple::new(py, [data.bind(py), &py.get_type::<pyo3::exceptions::PyValueError>()])?,
    )?;
    let operational = new_stand_in_class(py, "OperationalError", &PyTuple::new(py, [db_bound])?)?;
    let op_bound = operational.bind(py);
    let connection_failure =
        new_stand_in_class(py, "ConnectionFailure", &PyTuple::new(py, [op_bound])?)?;
    let query_cancelled =
        new_stand_in_class(py, "QueryCancelledError", &PyTuple::new(py, [op_bound])?)?;
    let serialization_failure =
        new_stand_in_class(py, "SerializationFailure", &PyTuple::new(py, [op_bound])?)?;
    let integrity = new_stand_in_class(py, "IntegrityError", &PyTuple::new(py, [db_bound])?)?;
    let programming = new_stand_in_class(py, "ProgrammingError", &PyTuple::new(py, [db_bound])?)?;
    Ok(CachedOracleExceptionTypes {
        database,
        interface,
        data,
        not_found,
        operational,
        connection_failure,
        query_cancelled,
        serialization_failure,
        integrity,
        programming,
    })
}

fn load_from_driver_module(
    driver_mod: &Bound<'_, PyModule>,
) -> PyResult<CachedOracleExceptionTypes> {
    let get = |name: &str| -> PyResult<Py<PyType>> {
        Ok(driver_mod.getattr(name)?.cast_into::<PyType>()?.unbind())
    };
    Ok(CachedOracleExceptionTypes {
        database: get("DatabaseError")?,
        interface: get("InterfaceError")?,
        data: get("DataError")?,
        not_found: get("NotFoundError")?,
        operational: get("OperationalError")?,
        connection_failure: get("ConnectionFailure")?,
        query_cancelled: get("QueryCancelledError")?,
        serialization_failure: get("SerializationFailure")?,
        integrity: get("IntegrityError")?,
        programming: get("ProgrammingError")?,
    })
}

fn resolve_exception_types(py: Python<'_>) -> PyResult<&'static CachedOracleExceptionTypes> {
    ORACLE_EXCEPTION_TYPES.get_or_try_init(py, || match py.import("vertebra.db.driver") {
        Ok(driver_mod) => load_from_driver_module(&driver_mod),
        Err(_) => {
            if let Ok(sys) = py.import("sys")
                && let Ok(path_any) = sys.getattr("path")
                && let Ok(path_list) = path_any.cast::<PyList>()
            {
                let repo_py = concat!(env!("CARGO_MANIFEST_DIR"), "/../../py");
                let _ = path_list.insert(0, repo_py);
            }
            match py.import("vertebra.db.driver") {
                Ok(driver_mod) => load_from_driver_module(&driver_mod),
                Err(_) => build_stand_in_types(py),
            }
        }
    })
}

fn select_type_for_kind(
    types: &CachedOracleExceptionTypes,
    kind: OracleErrorKind,
) -> &Py<PyType> {
    match kind {
        OracleErrorKind::Integrity => &types.integrity,
        OracleErrorKind::SerializationFailure => &types.serialization_failure,
        OracleErrorKind::Operational => &types.operational,
        OracleErrorKind::ConnectionFailure => &types.connection_failure,
        OracleErrorKind::QueryCancelled => &types.query_cancelled,
        OracleErrorKind::NotFound => &types.not_found,
        OracleErrorKind::Data => &types.data,
        OracleErrorKind::Programming => &types.programming,
        OracleErrorKind::Interface => &types.interface,
    }
}

impl OracleError {
    /// Construct an [`OracleError`] from an upstream `oracledb::Error`.
    #[must_use]
    pub fn from_oracledb(err: &oracledb::Error) -> Self {
        let message = err.to_string();
        if let Some(code) = extract_ora_code(&message) {
            if code == 1013 {
                return Self::Cancelled(message);
            }
            return Self::Server { code, message };
        }
        Self::Transport {
            code: None,
            message,
        }
    }

    /// Return the deterministic classification for this error.
    #[must_use]
    pub fn classify(&self) -> OracleErrorClassification {
        match self {
            Self::Config(_) | Self::Interface(_) => OracleErrorClassification {
                kind: OracleErrorKind::Interface,
                sqlstate: "HY000",
                vendor_code: None,
                is_transient: false,
            },
            Self::Cancelled(msg) => OracleErrorClassification {
                kind: OracleErrorKind::QueryCancelled,
                sqlstate: "57014",
                vendor_code: extract_ora_code(msg).or(Some(1013)),
                is_transient: false,
            },
            Self::Server { code, message } => classify_oracle_error(Some(*code), message),
            Self::Transport { code, message } => classify_oracle_error(*code, message),
        }
    }

    /// Convert this [`OracleError`] into a canonical `vertebra.db.driver` Python exception with `.sqlstate`, `.vendor_code`, and `.is_transient` populated.
    pub fn to_py_err(self, py: Python<'_>) -> PyErr {
        let types = match resolve_exception_types(py) {
            Ok(t) => t,
            Err(e) => return e,
        };
        let _ = &types.database;
        let classification = self.classify();
        let message = self.to_string();
        let ty = select_type_for_kind(types, classification.kind);
        let py_err = PyErr::from_type(ty.bind(py).clone(), (message,));
        let val = py_err.value(py);
        let sqlstate_opt: Option<&str> = match self {
            Self::Config(_) | Self::Interface(_) => None,
            _ => Some(classification.sqlstate),
        };
        if let Err(attr_err) = val
            .setattr("sqlstate", sqlstate_opt)
            .and_then(|()| val.setattr("vendor_code", classification.vendor_code))
            .and_then(|()| val.setattr("is_transient", classification.is_transient))
        {
            return attr_err;
        }
        py_err
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::{
        OracleError, OracleErrorKind, classify_oracle_error, extract_ora_code,
    };

    #[test]
    fn extract_ora_code_parses_standard_messages() {
        assert_eq!(
            extract_ora_code("ORA-00001: unique constraint (APP.PK_USERS) violated"),
            Some(1)
        );
        assert_eq!(
            extract_ora_code("transport error: ORA-12541: TNS:no listener"),
            Some(12541)
        );
        assert_eq!(extract_ora_code("socket reset by peer"), None);
    }

    #[test]
    fn deterministic_ora_to_sqlstate_classification_matrix() {
        let cases: &[(i32, OracleErrorKind, &str, bool)] = &[
            (1, OracleErrorKind::Integrity, "23505", false),
            (2291, OracleErrorKind::Integrity, "23503", false),
            (2292, OracleErrorKind::Integrity, "23503", false),
            (1400, OracleErrorKind::Integrity, "23502", false),
            (1407, OracleErrorKind::Integrity, "23502", false),
            (2290, OracleErrorKind::Integrity, "23514", false),
            (8177, OracleErrorKind::SerializationFailure, "40001", true),
            (60, OracleErrorKind::SerializationFailure, "40P01", true),
            (54, OracleErrorKind::Operational, "55P03", true),
            (30006, OracleErrorKind::Operational, "55P03", true),
            (1013, OracleErrorKind::QueryCancelled, "57014", false),
            (1017, OracleErrorKind::ConnectionFailure, "28P01", false),
            (28000, OracleErrorKind::ConnectionFailure, "28P01", false),
            (1403, OracleErrorKind::NotFound, "02000", false),
            (1438, OracleErrorKind::Data, "22000", false),
            (1476, OracleErrorKind::Data, "22000", false),
            (1722, OracleErrorKind::Data, "22000", false),
            (1843, OracleErrorKind::Data, "22000", false),
            (12899, OracleErrorKind::Data, "22000", false),
            (942, OracleErrorKind::Programming, "42000", false),
            (6550, OracleErrorKind::Programming, "42000", false),
            (12154, OracleErrorKind::ConnectionFailure, "08006", false),
            (12170, OracleErrorKind::ConnectionFailure, "08006", true),
            (12514, OracleErrorKind::ConnectionFailure, "08006", true),
            (12541, OracleErrorKind::ConnectionFailure, "08006", true),
            (12545, OracleErrorKind::ConnectionFailure, "08006", true),
            (3113, OracleErrorKind::Operational, "08006", true),
            (3114, OracleErrorKind::Operational, "08006", true),
            (3135, OracleErrorKind::Operational, "08006", true),
            (4061, OracleErrorKind::Operational, "08006", true),
            (4068, OracleErrorKind::Operational, "08006", true),
            (12537, OracleErrorKind::Operational, "08006", true),
        ];

        for &(code, expected_kind, expected_sqlstate, expected_transient) in cases {
            let msg = format!("ORA-{code:05}: simulated failure");
            let classified = classify_oracle_error(None, &msg);
            assert_eq!(classified.kind, expected_kind, "code={code}");
            assert_eq!(classified.sqlstate, expected_sqlstate, "code={code}");
            assert_eq!(classified.vendor_code, Some(code), "code={code}");
            assert_eq!(classified.is_transient, expected_transient, "code={code}");
        }
    }

    #[test]
    fn to_py_err_attaches_sqlstate_vendor_code_and_transience() {
        Python::initialize();
        Python::attach(|py| {
            let dup = OracleError::Server {
                code: 1,
                message: "ORA-00001: unique constraint violated".to_owned(),
            }
            .to_py_err(py);
            let dup_val = dup.value(py);
            assert_eq!(
                dup_val.get_type().name().expect("name"),
                "IntegrityError"
            );
            assert_eq!(
                dup_val
                    .getattr("sqlstate")
                    .expect("sqlstate")
                    .extract::<Option<String>>()
                    .expect("extract")
                    .as_deref(),
                Some("23505")
            );
            assert_eq!(
                dup_val
                    .getattr("vendor_code")
                    .expect("vendor_code")
                    .extract::<Option<i32>>()
                    .expect("extract"),
                Some(1)
            );
            assert!(
                !dup_val
                    .getattr("is_transient")
                    .expect("is_transient")
                    .extract::<bool>()
                    .expect("extract")
            );

            let ser = OracleError::Server {
                code: 8177,
                message: "ORA-08177: can't serialize access for this transaction".to_owned(),
            }
            .to_py_err(py);
            let ser_val = ser.value(py);
            assert_eq!(
                ser_val.get_type().name().expect("name"),
                "SerializationFailure"
            );
            assert_eq!(
                ser_val
                    .getattr("sqlstate")
                    .expect("sqlstate")
                    .extract::<Option<String>>()
                    .expect("extract")
                    .as_deref(),
                Some("40001")
            );
            assert_eq!(
                ser_val
                    .getattr("vendor_code")
                    .expect("vendor_code")
                    .extract::<Option<i32>>()
                    .expect("extract"),
                Some(8177)
            );
            assert!(
                ser_val
                    .getattr("is_transient")
                    .expect("is_transient")
                    .extract::<bool>()
                    .expect("extract")
            );

            let tns_bad_alias = OracleError::Server {
                code: 12154,
                message: "ORA-12154: TNS:could not resolve the connect identifier specified"
                    .to_owned(),
            }
            .to_py_err(py);
            let tns_bad_val = tns_bad_alias.value(py);
            assert_eq!(
                tns_bad_val.get_type().name().expect("name"),
                "ConnectionFailure"
            );
            assert_eq!(
                tns_bad_val
                    .getattr("sqlstate")
                    .expect("sqlstate")
                    .extract::<Option<String>>()
                    .expect("extract")
                    .as_deref(),
                Some("08006")
            );
            assert!(
                !tns_bad_val
                    .getattr("is_transient")
                    .expect("is_transient")
                    .extract::<bool>()
                    .expect("extract")
            );

            let tns_no_listener = OracleError::Server {
                code: 12541,
                message: "ORA-12541: TNS:no listener".to_owned(),
            }
            .to_py_err(py);
            let tns_listener_val = tns_no_listener.value(py);
            assert_eq!(
                tns_listener_val.get_type().name().expect("name"),
                "ConnectionFailure"
            );
            assert!(
                tns_listener_val
                    .getattr("is_transient")
                    .expect("is_transient")
                    .extract::<bool>()
                    .expect("extract")
            );

            let cancelled = OracleError::Server {
                code: 1013,
                message: "ORA-01013: user requested cancel of current operation".to_owned(),
            }
            .to_py_err(py);
            let cancelled_val = cancelled.value(py);
            assert_eq!(
                cancelled_val.get_type().name().expect("name"),
                "QueryCancelledError"
            );
            assert_eq!(
                cancelled_val
                    .getattr("sqlstate")
                    .expect("sqlstate")
                    .extract::<Option<String>>()
                    .expect("extract")
                    .as_deref(),
                Some("57014")
            );
            assert_eq!(
                cancelled_val
                    .getattr("vendor_code")
                    .expect("vendor_code")
                    .extract::<Option<i32>>()
                    .expect("extract"),
                Some(1013)
            );
        });
    }
}
