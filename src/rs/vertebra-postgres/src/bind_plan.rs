//! Cached PostgreSQL lexical bind scanner, placeholder rewriter, and parameter ordering.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyByteArray, PyBytes, PyDict, PyList, PyMemoryView, PySequence, PyString, PyTuple, PyType,
};

use crate::errors::{PostgresError, postgres_error_to_py};

/// Maximum number of distinct SQL bind plans retained in the process-wide LRU cache.
pub const BIND_PLAN_CACHE_CAPACITY: usize = 1024;

/// Placeholder syntax detected in a PostgreSQL SQL statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindStyle {
    /// No placeholders present in the SQL statement.
    None,
    /// Native PostgreSQL `$N` numeric placeholders.
    Numeric,
    /// DB-API `%s` positional format placeholders.
    Format,
    /// DB-API `%(name)s` named pyformat placeholders.
    Named,
}

/// Cached lexical bind rewrite plan for a SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindPlan {
    /// SQL text ready to send to the PostgreSQL server.
    pub sql: Box<str>,
    /// Detected placeholder style.
    pub style: BindStyle,
    /// Expected parameter count (positional count, distinct named count, or max `$N` index).
    pub param_count: usize,
    /// Distinct named placeholder keys in first-use order (empty unless `style == BindStyle::Named`).
    pub names: Box<[Box<str>]>,
}

#[derive(Default)]
struct ScanState {
    positional_count: usize,
    named_order: Option<Vec<String>>,
    named_slots: Option<HashMap<String, usize>>,
    numeric_indices: Option<Vec<usize>>,
    has_escaped_percent: bool,
    has_ambiguous_backslash_quote: bool,
    out: Option<String>,
}

impl ScanState {
    fn ensure_named(&mut self) -> (&mut Vec<String>, &mut HashMap<String, usize>) {
        let order = self.named_order.get_or_insert_with(Vec::new);
        let slots = self.named_slots.get_or_insert_with(HashMap::new);
        (order, slots)
    }

    fn record_numeric(&mut self, index: usize) -> Result<(), PostgresError> {
        if self.positional_count > 0 || self.named_order.is_some() {
            return Err(PostgresError::Interface(
                "Cannot mix numeric `$N` placeholders with positional `%s` or named `%(name)s` placeholders"
                    .to_string(),
            ));
        }
        self.numeric_indices
            .get_or_insert_with(Vec::new)
            .push(index);
        Ok(())
    }

    fn activate_rewrite(&mut self, sql: &str, copy_until: usize) -> &mut String {
        self.out.get_or_insert_with(|| {
            let mut buf = String::with_capacity(sql.len());
            if copy_until > 0 {
                buf.push_str(&sql[..copy_until]);
            }
            buf
        })
    }

    fn emit_chunk(&mut self, sql: &str, start: usize, end: usize) {
        if let Some(out) = self.out.as_mut()
            && end > start
        {
            out.push_str(&sql[start..end]);
        }
    }
}

fn is_ident_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '$'
}

fn is_dollar_tag_start(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_'
}

fn is_dollar_tag_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

fn match_dollar_quote(sql: &str, pos: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    if pos + 1 >= n {
        return None;
    }
    if bytes[pos + 1] == b'$' {
        return Some(pos + 2);
    }
    let first = sql[pos + 1..].chars().next()?;
    if !is_dollar_tag_start(first) {
        return None;
    }
    let mut j = pos + 1 + first.len_utf8();
    while j < n {
        let Some(ch) = sql[j..].chars().next() else {
            break;
        };
        if !is_dollar_tag_char(ch) {
            break;
        }
        j += ch.len_utf8();
    }
    if j < n && bytes[j] == b'$' {
        return Some(j + 1);
    }
    None
}

fn scan_single_quoted(sql: &str, pos: usize, state: &mut ScanState, c_escape: bool) -> usize {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut i = pos + if c_escape { 2 } else { 1 };
    let mut trailing_backslashes: usize = 0;

    while i < n {
        let b = bytes[i];
        if c_escape {
            if b == b'\\' {
                i += 1;
                if i < n {
                    i += sql[i..].chars().next().map_or(0, char::len_utf8);
                }
                continue;
            }
            if b == b'\'' {
                if i + 1 < n && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i += 1;
            continue;
        }
        if b == b'\\' {
            trailing_backslashes += 1;
            i += 1;
            continue;
        }
        if b == b'\'' {
            if trailing_backslashes % 2 == 1 {
                state.has_ambiguous_backslash_quote = true;
            }
            trailing_backslashes = 0;
            if i + 1 < n && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            return i + 1;
        }
        trailing_backslashes = 0;
        i += 1;
    }
    n
}

fn scan_double_quoted(sql: &str, pos: usize) -> usize {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut i = pos + 1;
    while i < n {
        if bytes[i] == b'"' {
            if i + 1 < n && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    n
}

fn scan_block_comment(sql: &str, pos: usize) -> usize {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut depth: usize = 1;
    let mut i = pos + 2;
    while i < n && depth > 0 {
        if bytes[i] == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && i + 1 < n && bytes[i + 1] == b'/' {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

/// Scan `sql` once and build its PostgreSQL placeholder rewrite plan.
///
/// Placeholders inside single-quoted literals, `E'...'` strings, double-quoted identifiers,
/// dollar-quoted bodies, `--` line comments, and nested `/* ... */` block comments are
/// ignored. Escaped `%%` sequences collapse to `%` only when `Format` or `Named`
/// placeholders are present.
pub fn scan(sql: &str) -> Result<BindPlan, PostgresError> {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut i: usize = 0;
    let mut last_emit: usize = 0;
    let mut state = ScanState::default();

    while i < n {
        let b = bytes[i];
        if (b == b'E' || b == b'e')
            && i + 1 < n
            && bytes[i + 1] == b'\''
            && (i == 0 || !sql[..i].chars().next_back().is_some_and(is_ident_char))
        {
            i = scan_single_quoted(sql, i, &mut state, true);
            continue;
        }
        if b == b'\'' {
            i = scan_single_quoted(sql, i, &mut state, false);
            continue;
        }
        if b == b'"' {
            i = scan_double_quoted(sql, i);
            continue;
        }
        if b == b'-' && i + 1 < n && bytes[i + 1] == b'-' {
            i = match sql[i + 2..].find('\n') {
                Some(rel) => i + 2 + rel + 1,
                None => n,
            };
            continue;
        }
        if b == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
            i = scan_block_comment(sql, i);
            continue;
        }
        if b == b'$' {
            let preceded_by_ident =
                i > 0 && sql[..i].chars().next_back().is_some_and(is_ident_char);
            if !preceded_by_ident {
                if let Some(tag_end) = match_dollar_quote(sql, i) {
                    let tag = &sql[i..tag_end];
                    i = match sql[tag_end..].find(tag) {
                        Some(rel) => tag_end + rel + tag.len(),
                        None => n,
                    };
                    continue;
                }
                if i + 1 < n && bytes[i + 1].is_ascii_digit() {
                    let mut j = i + 1;
                    while j < n && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    if j == n || !sql[j..].chars().next().is_some_and(is_ident_char) {
                        let idx_val = sql[i + 1..j].parse::<usize>().map_err(|_| {
                            PostgresError::Interface(
                                "PostgreSQL numeric bind placeholder index out of range"
                                    .to_string(),
                            )
                        })?;
                        state.record_numeric(idx_val)?;
                    }
                    i = j;
                    continue;
                }
            }
            i += 1;
            continue;
        }
        if b == b'%' {
            if i + 1 < n && bytes[i + 1] == b'%' {
                state.activate_rewrite(sql, last_emit);
                state.emit_chunk(sql, last_emit, i);
                if let Some(out) = state.out.as_mut() {
                    out.push('%');
                }
                state.has_escaped_percent = true;
                i += 2;
                last_emit = i;
                continue;
            }
            if i + 1 < n && bytes[i + 1] == b's' {
                if state.named_order.is_some() || state.numeric_indices.is_some() {
                    return Err(PostgresError::Interface(
                        "Cannot mix positional `%s` placeholders with named `%(name)s` or numeric `$N` placeholders"
                            .to_string(),
                    ));
                }
                state.positional_count += 1;
                let slot = state.positional_count;
                state.activate_rewrite(sql, last_emit);
                state.emit_chunk(sql, last_emit, i);
                if let Some(out) = state.out.as_mut() {
                    let _ = write!(out, "${slot}");
                }
                i += 2;
                last_emit = i;
                continue;
            }
            if i + 1 < n && bytes[i + 1] == b'(' {
                if state.positional_count > 0 || state.numeric_indices.is_some() {
                    return Err(PostgresError::Interface(
                        "Cannot mix named `%(name)s` placeholders with positional `%s` or numeric `$N` placeholders"
                            .to_string(),
                    ));
                }
                let Some(rel_close) = sql[i + 2..].find(")s") else {
                    return Err(PostgresError::Interface(
                        "Malformed named `%(name)s` placeholder in SQL".to_string(),
                    ));
                };
                let close_paren = i + 2 + rel_close;
                let name = &sql[i + 2..close_paren];
                if name.is_empty() {
                    return Err(PostgresError::Interface(
                        "Empty named `%()s` placeholder in SQL".to_string(),
                    ));
                }
                let slot = {
                    let (named_order, named_slots) = state.ensure_named();
                    if let Some(&existing) = named_slots.get(name) {
                        existing
                    } else {
                        let next_slot = named_order.len() + 1;
                        let owned = name.to_owned();
                        named_slots.insert(owned.clone(), next_slot);
                        named_order.push(owned);
                        next_slot
                    }
                };
                state.activate_rewrite(sql, last_emit);
                state.emit_chunk(sql, last_emit, i);
                if let Some(out) = state.out.as_mut() {
                    let _ = write!(out, "${slot}");
                }
                i = close_paren + 2;
                last_emit = i;
                continue;
            }
        }
        i += 1;
    }

    let has_pyformat = state.positional_count > 0 || state.named_order.is_some();
    if has_pyformat && state.has_ambiguous_backslash_quote {
        return Err(PostgresError::Interface(
            "Ambiguous `\\'` escape inside ordinary single-quoted literal with pyformat placeholders; use `''` or `E'...'`"
                .to_string(),
        ));
    }

    if let Some(indices) = state.numeric_indices {
        if indices.iter().any(|&idx| idx < 1) {
            return Err(PostgresError::Interface(
                "PostgreSQL numeric bind placeholders must start at `$1`".to_string(),
            ));
        }
        let max_idx = indices.iter().copied().max().unwrap_or(0);
        return Ok(BindPlan {
            sql: sql.into(),
            style: BindStyle::Numeric,
            param_count: max_idx,
            names: Box::default(),
        });
    }

    if let Some(named_order) = state.named_order.take() {
        state.emit_chunk(sql, last_emit, n);
        let compiled_sql = state.out.unwrap_or_default().into_boxed_str();
        let param_count = named_order.len();
        let names = named_order
            .into_iter()
            .map(String::into_boxed_str)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        return Ok(BindPlan {
            sql: compiled_sql,
            style: BindStyle::Named,
            param_count,
            names,
        });
    }

    if state.positional_count > 0 {
        state.emit_chunk(sql, last_emit, n);
        let compiled_sql = state.out.unwrap_or_default().into_boxed_str();
        return Ok(BindPlan {
            sql: compiled_sql,
            style: BindStyle::Format,
            param_count: state.positional_count,
            names: Box::default(),
        });
    }

    Ok(BindPlan {
        sql: sql.into(),
        style: BindStyle::None,
        param_count: 0,
        names: Box::default(),
    })
}

struct CacheSlot {
    entry: Option<(Box<str>, Arc<BindPlan>)>,
    prev: Option<usize>,
    next: Option<usize>,
    next_free: Option<usize>,
}

struct BindPlanCache {
    capacity: usize,
    slots: Vec<CacheSlot>,
    index: HashMap<Box<str>, usize>,
    head: Option<usize>,
    tail: Option<usize>,
    free: Option<usize>,
}

impl BindPlanCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            slots: Vec::new(),
            index: HashMap::new(),
            head: None,
            tail: None,
            free: None,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.index.len()
    }

    fn unlink(&mut self, idx: usize) {
        let prev = self.slots[idx].prev;
        let next = self.slots[idx].next;
        match prev {
            Some(p) => self.slots[p].next = next,
            None => self.head = next,
        }
        match next {
            Some(n) => self.slots[n].prev = prev,
            None => self.tail = prev,
        }
        self.slots[idx].prev = None;
        self.slots[idx].next = None;
    }

    fn push_front(&mut self, idx: usize) {
        let old_head = self.head;
        self.slots[idx].prev = None;
        self.slots[idx].next = old_head;
        match old_head {
            Some(h) => self.slots[h].prev = Some(idx),
            None => self.tail = Some(idx),
        }
        self.head = Some(idx);
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }
        self.unlink(idx);
        self.push_front(idx);
    }

    fn get(&mut self, sql: &str) -> Option<Arc<BindPlan>> {
        let &idx = self.index.get(sql)?;
        self.move_to_front(idx);
        self.slots[idx]
            .entry
            .as_ref()
            .map(|(_, plan)| Arc::clone(plan))
    }

    fn insert(&mut self, sql: &str, plan: Arc<BindPlan>) -> Arc<BindPlan> {
        if self.capacity == 0 {
            return plan;
        }
        if let Some(&idx) = self.index.get(sql) {
            self.move_to_front(idx);
            if let Some((_, existing)) = self.slots[idx].entry.as_ref() {
                return Arc::clone(existing);
            }
        }
        if self.index.len() >= self.capacity
            && let Some(evict_idx) = self.tail
        {
            self.unlink(evict_idx);
            if let Some((old_key, _)) = self.slots[evict_idx].entry.take() {
                self.index.remove(&old_key);
            }
            self.slots[evict_idx].next_free = self.free;
            self.free = Some(evict_idx);
        }
        let slot_idx = if let Some(free_idx) = self.free {
            self.free = self.slots[free_idx].next_free;
            self.slots[free_idx].next_free = None;
            free_idx
        } else {
            let idx = self.slots.len();
            self.slots.push(CacheSlot {
                entry: None,
                prev: None,
                next: None,
                next_free: None,
            });
            idx
        };
        let key: Box<str> = sql.into();
        self.slots[slot_idx].entry = Some((key.clone(), Arc::clone(&plan)));
        self.index.insert(key, slot_idx);
        self.push_front(slot_idx);
        plan
    }
}

static BIND_PLAN_CACHE: OnceLock<Mutex<BindPlanCache>> = OnceLock::new();

fn global_cache() -> &'static Mutex<BindPlanCache> {
    BIND_PLAN_CACHE.get_or_init(|| Mutex::new(BindPlanCache::new(BIND_PLAN_CACHE_CAPACITY)))
}

/// Return a cached [`BindPlan`] for `sql`, scanning and inserting on a cache miss.
///
/// Scan errors are returned to the caller and never stored in the cache.
pub fn plan_for(sql: &str) -> Result<Arc<BindPlan>, PostgresError> {
    {
        let mut cache = global_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(plan) = cache.get(sql) {
            return Ok(plan);
        }
    }

    let plan = Arc::new(scan(sql)?);
    let mut cache = global_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(cache.insert(sql, plan))
}

struct BindPlanPyTypes {
    programming_error: Py<PyType>,
    abc_mapping: Py<PyType>,
    abc_sequence: Py<PyType>,
}

static BIND_PLAN_PY_TYPES: PyOnceLock<BindPlanPyTypes> = PyOnceLock::new();

fn resolve_py_types(py: Python<'_>) -> PyResult<&'static BindPlanPyTypes> {
    BIND_PLAN_PY_TYPES.get_or_try_init(py, || {
        let programming_error = match py
            .import("vertebra._vertebra")
            .and_then(|m| m.getattr("ProgrammingError"))
            .and_then(|attr| Ok(attr.cast_into::<PyType>()?.unbind()))
        {
            Ok(ty) => ty,
            Err(_) => {
                let root = py.get_type::<pyo3::exceptions::PyException>();
                let bases = PyTuple::new(py, [&root])?;
                let dict = PyDict::new(py);
                dict.set_item("__module__", "vertebra.db.driver")?;
                py.get_type::<PyType>()
                    .call1(("ProgrammingError", bases, dict))?
                    .cast_into::<PyType>()?
                    .unbind()
            }
        };
        let abc = py.import("collections.abc")?;
        let abc_mapping = abc.getattr("Mapping")?.cast_into::<PyType>()?.unbind();
        let abc_sequence = abc.getattr("Sequence")?.cast_into::<PyType>()?.unbind();
        Ok(BindPlanPyTypes {
            programming_error,
            abc_mapping,
            abc_sequence,
        })
    })
}

fn programming_err(py: Python<'_>, message: &str) -> PyErr {
    let types = match resolve_py_types(py) {
        Ok(t) => t,
        Err(err) => return err,
    };
    match types.programming_error.bind(py).call1((message,)) {
        Ok(inst) => {
            let _ = inst.setattr("sqlstate", py.None());
            let _ = inst.setattr("vendor_code", py.None());
            let _ = inst.setattr("is_transient", false);
            let _ = inst.setattr("completed_result", py.None());
            PyErr::from_value(inst)
        }
        Err(err) => err,
    }
}

fn sequence_to_tuple<'py>(
    py: Python<'py>,
    parameters: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyTuple>> {
    if parameters.is_none() {
        return Ok(PyTuple::empty(py));
    }
    if parameters.is_exact_instance_of::<PyTuple>() {
        return Ok(parameters.cast::<PyTuple>()?.clone());
    }
    if let Ok(list) = parameters.cast::<PyList>() {
        return Ok(list.to_tuple());
    }
    if let Ok(seq) = parameters.cast::<PySequence>() {
        return seq.to_tuple();
    }
    let items: Vec<Bound<'py, PyAny>> = parameters.try_iter()?.collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(py, items)
}

/// Validate and order `parameters` according to `plan`, returning a tuple of values to bind.
///
/// `None` and empty inputs return an empty tuple. A mapping with [`BindStyle::Named`] returns
/// a tuple ordered by `plan.names`. A sequence is returned unchanged when it is already an
/// exact `tuple` and converted to a `tuple` otherwise.
pub fn order_parameters<'py>(
    py: Python<'py>,
    plan: &BindPlan,
    parameters: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let is_none = parameters.is_none();
    let is_mapping = if is_none {
        if plan.style == BindStyle::None {
            return Ok(PyTuple::empty(py).into_any());
        }
        false
    } else if parameters.is_exact_instance_of::<PyTuple>() {
        if plan.style == BindStyle::None && parameters.cast::<PyTuple>()?.is_empty() {
            return Ok(PyTuple::empty(py).into_any());
        }
        false
    } else if parameters.is_exact_instance_of::<PyList>() {
        if plan.style == BindStyle::None && parameters.cast::<PyList>()?.is_empty() {
            return Ok(PyTuple::empty(py).into_any());
        }
        false
    } else if parameters.is_exact_instance_of::<PyDict>() {
        if plan.style == BindStyle::None && parameters.cast::<PyDict>()?.is_empty() {
            return Ok(PyTuple::empty(py).into_any());
        }
        true
    } else if parameters.is_instance_of::<PyString>()
        || parameters.is_instance_of::<PyBytes>()
        || parameters.is_instance_of::<PyByteArray>()
        || parameters.is_instance_of::<PyMemoryView>()
    {
        return Err(programming_err(
            py,
            "Query parameters must be a Sequence, Mapping, or None, not str or bytes",
        ));
    } else if parameters.is_instance_of::<PyDict>() {
        true
    } else if parameters.is_instance_of::<PyTuple>() || parameters.is_instance_of::<PyList>() {
        false
    } else {
        let types = resolve_py_types(py)?;
        if parameters.is_instance(types.abc_mapping.bind(py))? {
            true
        } else if parameters.is_instance(types.abc_sequence.bind(py))? {
            false
        } else {
            let type_name = parameters.get_type().name()?;
            return Err(programming_err(
                py,
                &format!("Unsupported parameters container type: {type_name}"),
            ));
        }
    };

    match plan.style {
        BindStyle::Numeric => {
            if is_mapping {
                return Err(programming_err(
                    py,
                    "Numeric `$N` SQL placeholders require a positional Sequence of parameters, not a Mapping",
                ));
            }
            let seq = sequence_to_tuple(py, parameters)?;
            if seq.len() < plan.param_count {
                return Err(programming_err(
                    py,
                    &format!(
                        "SQL references `${}` but only {} positional parameters were supplied",
                        plan.param_count,
                        seq.len()
                    ),
                ));
            }
            Ok(seq.into_any())
        }
        BindStyle::Named => {
            if !is_mapping {
                return Err(programming_err(
                    py,
                    "Named `%(name)s` SQL placeholders require a Mapping of parameters",
                ));
            }
            let mut bound = Vec::with_capacity(plan.names.len());
            if let Ok(dict) = parameters.cast::<PyDict>() {
                for name in &plan.names {
                    match dict.get_item(name.as_ref())? {
                        Some(val) => bound.push(val),
                        None => {
                            let repr = PyString::new(py, name.as_ref()).repr()?;
                            return Err(programming_err(
                                py,
                                &format!("Missing required named SQL parameter: {repr}"),
                            ));
                        }
                    }
                }
            } else {
                for name in &plan.names {
                    if !parameters.contains(name.as_ref())? {
                        let repr = PyString::new(py, name.as_ref()).repr()?;
                        return Err(programming_err(
                            py,
                            &format!("Missing required named SQL parameter: {repr}"),
                        ));
                    }
                    bound.push(parameters.get_item(name.as_ref())?);
                }
            }
            Ok(PyTuple::new(py, bound)?.into_any())
        }
        BindStyle::Format => {
            if is_mapping {
                return Err(programming_err(
                    py,
                    "Positional `%s` SQL placeholders require a Sequence of parameters, not a Mapping",
                ));
            }
            let seq = sequence_to_tuple(py, parameters)?;
            if seq.len() != plan.param_count {
                return Err(programming_err(
                    py,
                    &format!(
                        "SQL has {} `%s` placeholders but {} parameters were supplied",
                        plan.param_count,
                        seq.len()
                    ),
                ));
            }
            Ok(seq.into_any())
        }
        BindStyle::None => {
            if is_none {
                return Ok(PyTuple::empty(py).into_any());
            }
            let count = parameters.len()?;
            if count == 0 {
                return Ok(PyTuple::empty(py).into_any());
            }
            if is_mapping {
                return Err(programming_err(
                    py,
                    "Mapping parameters were supplied for a SQL statement without named `%(name)s` placeholders",
                ));
            }
            Err(programming_err(
                py,
                &format!(
                    "Positional parameters ({count}) were supplied for a SQL statement with 0 placeholders"
                ),
            ))
        }
    }
}

/// Compile PostgreSQL SQL placeholders and order parameters in a single cached Rust pass.
#[pyfunction]
#[pyo3(signature = (sql, parameters = None))]
pub fn compile_binds(
    py: Python<'_>,
    sql: &str,
    parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Py<PyAny>)> {
    let plan = plan_for(sql).map_err(|err| match err {
        PostgresError::Interface(msg) => programming_err(py, &msg),
        other => postgres_error_to_py(py, other),
    })?;
    let none_obj;
    let params_ref = match parameters {
        Some(obj) => obj,
        None => {
            none_obj = py.None().into_bound(py);
            &none_obj
        }
    };
    let ordered = order_parameters(py, &plan, params_ref)?;
    Ok((plan.sql.to_string(), ordered.unbind()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn bind_plan_no_placeholders() {
        let plan = scan("SELECT 1").unwrap();
        assert_eq!(plan.sql.as_ref(), "SELECT 1");
        assert_eq!(plan.style, BindStyle::None);
        assert_eq!(plan.param_count, 0);
        assert!(plan.names.is_empty());
    }

    #[test]
    fn bind_plan_format() {
        let plan = scan("SELECT %s, %s").unwrap();
        assert_eq!(plan.sql.as_ref(), "SELECT $1, $2");
        assert_eq!(plan.style, BindStyle::Format);
        assert_eq!(plan.param_count, 2);
        assert!(plan.names.is_empty());
    }

    #[test]
    fn bind_plan_named_reuse() {
        let plan = scan("WHERE a = %(x)s OR b = %(x)s AND c = %(y)s").unwrap();
        assert_eq!(plan.sql.as_ref(), "WHERE a = $1 OR b = $1 AND c = $2");
        assert_eq!(plan.style, BindStyle::Named);
        assert_eq!(plan.param_count, 2);
        let names: Vec<&str> = plan.names.iter().map(Box::as_ref).collect();
        assert_eq!(names, vec!["x", "y"]);
    }

    #[test]
    fn bind_plan_numeric_passthrough() {
        let plan = scan("SELECT $2, $1").unwrap();
        assert_eq!(plan.sql.as_ref(), "SELECT $2, $1");
        assert_eq!(plan.style, BindStyle::Numeric);
        assert_eq!(plan.param_count, 2);
        assert!(plan.names.is_empty());
    }

    #[test]
    fn bind_plan_quoted_text() {
        let sql = r#"SELECT '%s', "%s", $$ %s $$, E'it\'s %s', $tag$ %(a)s $tag$"#;
        let plan = scan(sql).unwrap();
        assert_eq!(plan.sql.as_ref(), sql);
        assert_eq!(plan.style, BindStyle::None);
        assert_eq!(plan.param_count, 0);
        assert!(plan.names.is_empty());
    }

    #[test]
    fn bind_plan_comments() {
        let line_sql = "SELECT 1 -- %s";
        let line_plan = scan(line_sql).unwrap();
        assert_eq!(line_plan.sql.as_ref(), line_sql);
        assert_eq!(line_plan.style, BindStyle::None);
        assert_eq!(line_plan.param_count, 0);

        let block_sql = "SELECT /* /* %s */ */ 1";
        let block_plan = scan(block_sql).unwrap();
        assert_eq!(block_plan.sql.as_ref(), block_sql);
        assert_eq!(block_plan.style, BindStyle::None);
        assert_eq!(block_plan.param_count, 0);
    }

    #[test]
    fn bind_plan_escaped_percent() {
        let format_plan = scan("SELECT 10 %% 3, %s").unwrap();
        assert_eq!(format_plan.sql.as_ref(), "SELECT 10 % 3, $1");
        assert_eq!(format_plan.style, BindStyle::Format);
        assert_eq!(format_plan.param_count, 1);

        let named_plan = scan("SELECT 10 %% 3, %(x)s").unwrap();
        assert_eq!(named_plan.sql.as_ref(), "SELECT 10 % 3, $1");
        assert_eq!(named_plan.style, BindStyle::Named);
        assert_eq!(named_plan.param_count, 1);

        let none_plan = scan("SELECT 10 %% 3").unwrap();
        assert_eq!(none_plan.sql.as_ref(), "SELECT 10 %% 3");
        assert_eq!(none_plan.style, BindStyle::None);
        assert_eq!(none_plan.param_count, 0);

        let numeric_plan = scan("SELECT 10 %% 3, $1").unwrap();
        assert_eq!(numeric_plan.sql.as_ref(), "SELECT 10 %% 3, $1");
        assert_eq!(numeric_plan.style, BindStyle::Numeric);
        assert_eq!(numeric_plan.param_count, 1);
    }

    #[test]
    fn bind_plan_mixed_styles_and_scan_errors() {
        let err = scan("SELECT %s, $1").unwrap_err();
        assert_eq!(
            err,
            PostgresError::Interface(
                "Cannot mix numeric `$N` placeholders with positional `%s` or named `%(name)s` placeholders"
                    .to_string()
            )
        );

        let err2 = scan("SELECT $1, %s").unwrap_err();
        assert_eq!(
            err2,
            PostgresError::Interface(
                "Cannot mix positional `%s` placeholders with named `%(name)s` or numeric `$N` placeholders"
                    .to_string()
            )
        );

        let err3 = scan("SELECT %s, %(x)s").unwrap_err();
        assert_eq!(
            err3,
            PostgresError::Interface(
                "Cannot mix named `%(name)s` placeholders with positional `%s` or numeric `$N` placeholders"
                    .to_string()
            )
        );

        let err4 = scan("SELECT %(x").unwrap_err();
        assert_eq!(
            err4,
            PostgresError::Interface("Malformed named `%(name)s` placeholder in SQL".to_string())
        );

        let err5 = scan("SELECT %()s").unwrap_err();
        assert_eq!(
            err5,
            PostgresError::Interface("Empty named `%()s` placeholder in SQL".to_string())
        );

        let err6 = scan("SELECT $0").unwrap_err();
        assert_eq!(
            err6,
            PostgresError::Interface(
                "PostgreSQL numeric bind placeholders must start at `$1`".to_string()
            )
        );

        let err7 = scan(r"SELECT 'foo\', %s").unwrap_err();
        assert_eq!(
            err7,
            PostgresError::Interface(
                "Ambiguous `\\'` escape inside ordinary single-quoted literal with pyformat placeholders; use `''` or `E'...'`"
                    .to_string()
            )
        );
    }

    #[test]
    fn bind_plan_cache_reuse_and_errors_not_cached() {
        let sql = "SELECT 'bind_plan_cache_reuse_unique', %s, %s";
        let first = plan_for(sql).unwrap();
        let second = plan_for(sql).unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        let bad_sql = "SELECT 'bind_plan_bad_unique', %s, $1";
        assert!(plan_for(bad_sql).is_err());
        let cache = global_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!cache.index.contains_key(bad_sql));
    }

    #[test]
    fn bind_plan_cache_bound() {
        for idx in 0..=BIND_PLAN_CACHE_CAPACITY {
            let sql = format!("SELECT {idx} AS bind_plan_bound_col, %s");
            let plan = plan_for(&sql).unwrap();
            assert_eq!(plan.param_count, 1);
        }
        let cache = global_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(cache.len(), BIND_PLAN_CACHE_CAPACITY);
    }

    #[test]
    fn bind_plan_order_parameters_and_compile_binds() {
        Python::attach(|py| {
            let (sql_none, params_none) = compile_binds(py, "SELECT 1", None).unwrap();
            assert_eq!(sql_none, "SELECT 1");
            assert_eq!(params_none.bind(py).cast::<PyTuple>().unwrap().len(), 0);

            let input_tuple = PyTuple::new(py, [10_i64, 20_i64]).unwrap();
            let (sql_fmt, params_fmt) =
                compile_binds(py, "SELECT %s, %s", Some(input_tuple.as_any())).unwrap();
            assert_eq!(sql_fmt, "SELECT $1, $2");
            assert!(params_fmt.bind(py).is(&input_tuple));

            let input_list = PyList::new(py, [10_i64, 20_i64]).unwrap();
            let (_, params_list) =
                compile_binds(py, "SELECT %s, %s", Some(input_list.as_any())).unwrap();
            let out_tuple = params_list.bind(py).cast::<PyTuple>().unwrap();
            assert_eq!(out_tuple.len(), 2);
            assert_eq!(out_tuple.get_item(0).unwrap().extract::<i64>().unwrap(), 10);
            assert_eq!(out_tuple.get_item(1).unwrap().extract::<i64>().unwrap(), 20);

            let mapping = PyDict::new(py);
            mapping.set_item("y", 2_i64).unwrap();
            mapping.set_item("x", 1_i64).unwrap();
            let (sql_named, params_named) = compile_binds(
                py,
                "WHERE a = %(x)s OR b = %(x)s AND c = %(y)s",
                Some(mapping.as_any()),
            )
            .unwrap();
            assert_eq!(sql_named, "WHERE a = $1 OR b = $1 AND c = $2");
            let named_tuple = params_named.bind(py).cast::<PyTuple>().unwrap();
            assert_eq!(named_tuple.len(), 2);
            assert_eq!(
                named_tuple.get_item(0).unwrap().extract::<i64>().unwrap(),
                1
            );
            assert_eq!(
                named_tuple.get_item(1).unwrap().extract::<i64>().unwrap(),
                2
            );

            let empty_dict = PyDict::new(py);
            let missing_err =
                compile_binds(py, "SELECT %(x)s", Some(empty_dict.as_any())).unwrap_err();
            assert_eq!(
                missing_err.get_type(py).name().unwrap().to_string(),
                "ProgrammingError"
            );
            assert_eq!(
                missing_err.value(py).to_string(),
                "Missing required named SQL parameter: 'x'"
            );

            let map_for_fmt_err =
                compile_binds(py, "SELECT %s", Some(mapping.as_any())).unwrap_err();
            assert_eq!(
                map_for_fmt_err.get_type(py).name().unwrap().to_string(),
                "ProgrammingError"
            );
            assert_eq!(
                map_for_fmt_err.value(py).to_string(),
                "Positional `%s` SQL placeholders require a Sequence of parameters, not a Mapping"
            );

            let map_for_num_err =
                compile_binds(py, "SELECT $1", Some(mapping.as_any())).unwrap_err();
            assert_eq!(
                map_for_num_err.value(py).to_string(),
                "Numeric `$N` SQL placeholders require a positional Sequence of parameters, not a Mapping"
            );

            let map_for_none_err =
                compile_binds(py, "SELECT 1", Some(mapping.as_any())).unwrap_err();
            assert_eq!(
                map_for_none_err.value(py).to_string(),
                "Mapping parameters were supplied for a SQL statement without named `%(name)s` placeholders"
            );

            let seq_for_named_err =
                compile_binds(py, "SELECT %(x)s", Some(input_tuple.as_any())).unwrap_err();
            assert_eq!(
                seq_for_named_err.value(py).to_string(),
                "Named `%(name)s` SQL placeholders require a Mapping of parameters"
            );

            let mixed_err = compile_binds(py, "SELECT %s, $1", None).unwrap_err();
            assert_eq!(
                mixed_err.get_type(py).name().unwrap().to_string(),
                "ProgrammingError"
            );
            assert_eq!(
                mixed_err.value(py).to_string(),
                "Cannot mix numeric `$N` placeholders with positional `%s` or named `%(name)s` placeholders"
            );

            let count_fmt_err =
                compile_binds(py, "SELECT %s, %s, %s", Some(input_tuple.as_any())).unwrap_err();
            assert_eq!(
                count_fmt_err.value(py).to_string(),
                "SQL has 3 `%s` placeholders but 2 parameters were supplied"
            );

            let count_num_err =
                compile_binds(py, "SELECT $3", Some(input_tuple.as_any())).unwrap_err();
            assert_eq!(
                count_num_err.value(py).to_string(),
                "SQL references `$3` but only 2 positional parameters were supplied"
            );

            let count_none_err =
                compile_binds(py, "SELECT 1", Some(input_tuple.as_any())).unwrap_err();
            assert_eq!(
                count_none_err.value(py).to_string(),
                "Positional parameters (2) were supplied for a SQL statement with 0 placeholders"
            );

            let str_param = PyString::new(py, "abc");
            let str_err = compile_binds(py, "SELECT %s", Some(str_param.as_any())).unwrap_err();
            assert_eq!(
                str_err.value(py).to_string(),
                "Query parameters must be a Sequence, Mapping, or None, not str or bytes"
            );

            let bad_container = py
                .eval(&CString::new("{1, 2}").unwrap(), None, None)
                .unwrap();
            let set_err = compile_binds(py, "SELECT %s", Some(&bad_container)).unwrap_err();
            assert_eq!(
                set_err.value(py).to_string(),
                "Unsupported parameters container type: set"
            );
        });
    }
}
