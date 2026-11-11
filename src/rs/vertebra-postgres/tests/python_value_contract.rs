//! Integration tests for PyO3 extraction and projection of `PgValue` / `QueryResult` (`ch14:t03`).

extern crate _postgres as vertebra_postgres;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use vertebra_postgres::codecs::{
    BOOL_OID, BYTEA_OID, DATE_OID, FLOAT8_OID, INT4_OID, INT8_OID, INTERVAL_OID, JSONB_OID,
    NUMERIC_OID, TEXT_OID, TIMESTAMPTZ_OID, UUID_OID,
};
use vertebra_postgres::python_values::{
    extract_parameters, pg_value_to_python, query_result_to_python,
};
use vertebra_postgres::result::{
    Column, Format, PG_TS_POS_INF, PgNumeric, PgValue, QueryResult, TemporalKind,
    TransactionStatus, VectorKind,
};

#[test]
fn extract_and_project_parameters_and_query_result_across_lossless_wrappers() {
    Python::initialize();
    Python::attach(|py| {
        if let Ok(sys) = py.import("sys")
            && let Ok(path_any) = sys.getattr("path")
            && let Ok(path_list) = path_any.cast::<PyList>()
        {
            let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let py_ver = py.version_info();
            let venv_unix = repo_root.join(".venv/lib").join(format!(
                "python{}.{}/site-packages",
                py_ver.major, py_ver.minor
            ));
            if venv_unix.is_dir() {
                let _ = path_list.insert(0, venv_unix.to_string_lossy().as_ref());
            }
            let src_py = repo_root.join("src/py");
            if src_py.is_dir() {
                let _ = path_list.insert(0, src_py.to_string_lossy().as_ref());
            }
        }
        let Ok(types_mod) = py.import("vertebra.db.adapters.postgres.types") else {
            return;
        };
        let decimal_cls = py.import("decimal").unwrap().getattr("Decimal").unwrap();
        let uuid_cls = py.import("uuid").unwrap().getattr("UUID").unwrap();

        let dec_nan = decimal_cls.call1(("NaN",)).unwrap();
        let u = uuid_cls
            .call1(("12345678-1234-5678-1234-567812345678",))
            .unwrap();
        let pg_json = types_mod
            .getattr("PgJson")
            .unwrap()
            .call1((PyDict::new(py),))
            .unwrap();
        let pg_vec = types_mod
            .getattr("PgVector")
            .unwrap()
            .call1((PyTuple::new(py, [1.0f64, 2.5f64]).unwrap(), "vector"))
            .unwrap();

        let params_seq = PyList::new(
            py,
            [
                py.None().into_bound(py),
                true.into_pyobject(py).unwrap().to_owned().into_any(),
                42i32.into_pyobject(py).unwrap().into_any(),
                dec_nan,
                u,
                pg_json,
                pg_vec,
            ],
        )
        .unwrap();

        let (extracted, oids) = extract_parameters(py, &params_seq, None).unwrap();
        assert_eq!(extracted.len(), 7);
        assert_eq!(extracted[0].value, PgValue::Null);
        assert_eq!(extracted[1].value, PgValue::Bool(true));
        assert_eq!(extracted[2].value, PgValue::Int4(42));
        assert_eq!(extracted[3].value, PgValue::Numeric(PgNumeric::NaN));
        assert!(matches!(extracted[4].value, PgValue::Uuid(_)));
        assert!(matches!(extracted[5].value, PgValue::Json(_)));
        assert_eq!(extracted[5].type_oid, None);
        assert_eq!(
            extracted[6].value,
            PgValue::Vector {
                kind: VectorKind::Vector,
                values: vec![1.0, 2.5],
            }
        );
        let _ = oids;

        // Reject NaN float inside JSON mapped dict via parameter_oids=[JSONB_OID]
        let bad_dict = PyDict::new(py);
        bad_dict.set_item("x", f64::NAN).unwrap();
        let bad_seq = PyList::new(py, [bad_dict]).unwrap();
        let jsonb_oids = PyList::new(py, [JSONB_OID]).unwrap();
        assert!(extract_parameters(py, &bad_seq, Some(jsonb_oids.as_any())).is_err());

        // Reject unresolved wire_text PgTemporal on input
        let wire_temp = types_mod
            .getattr("PgTemporal")
            .unwrap()
            .call1((
                "timestamptz",
                py.None(),
                "wire_text",
                "2026-03-29 01:30:00 MET",
                "Postgres, MDY",
                "Europe/Berlin",
            ))
            .unwrap();
        let wire_seq = PyList::new(py, [wire_temp]).unwrap();
        assert!(extract_parameters(py, &wire_seq, None).is_err());

        // Project QueryResult with duplicate column names and PgTemporal infinity
        let cols = vec![
            Column::try_new("id", INT4_OID, Format::Binary, -1, 0, 0).unwrap(),
            Column::try_new("id", TEXT_OID, Format::Text, -1, 0, 0).unwrap(),
            Column::try_new("ts", TIMESTAMPTZ_OID, Format::Binary, -1, 0, 0).unwrap(),
        ];
        let rows = vec![vec![
            PgValue::Int4(9),
            PgValue::Text("nine".to_string()),
            PgValue::TimestampTz(PG_TS_POS_INF),
        ]];
        let qr = QueryResult::try_new(cols, rows, 1, "SELECT 1", TransactionStatus::Idle).unwrap();
        let py_qr = query_result_to_python(py, &qr).unwrap();
        let bound_qr = py_qr.bind(py);
        assert_eq!(
            bound_qr
                .getattr("affected_rows")
                .unwrap()
                .extract::<u64>()
                .unwrap(),
            1
        );
        assert_eq!(
            bound_qr
                .getattr("command_tag")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "SELECT 1"
        );
        let py_rows = bound_qr.getattr("rows").unwrap();
        let first_row = py_rows.get_item(0).unwrap();
        assert_eq!(first_row.get_item(0).unwrap().extract::<i32>().unwrap(), 9);
        assert_eq!(
            first_row.get_item(1).unwrap().extract::<String>().unwrap(),
            "nine"
        );
        let ts_cell = first_row.get_item(2).unwrap();
        assert_eq!(
            ts_cell
                .getattr("special")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "positive_infinity"
        );

        let _ = (
            BOOL_OID,
            BYTEA_OID,
            DATE_OID,
            FLOAT8_OID,
            INT8_OID,
            INTERVAL_OID,
            NUMERIC_OID,
            UUID_OID,
            TemporalKind::TimestampTz,
            pg_value_to_python,
        );
    });
}
