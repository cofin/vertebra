//! Shared PEP 249 buffered cursor state, generic SQL plan LRU cache, and column-description helpers
//! for `vertebra-*` SQL PyO3 extensions.

use std::collections::{HashMap, VecDeque};

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use crate::py_errors::{cursor_interface_err, cursor_programming_err};

/// Default per-process capacity for compiled SQL/parameter plan LRU caches.
pub const DEFAULT_PLAN_CACHE_CAPACITY: usize = 512;

/// Bounded LRU cache keyed by SQL text (`Box<str>`).
pub struct PlanLruCache<V> {
    capacity: usize,
    entries: HashMap<Box<str>, V>,
    order: VecDeque<Box<str>>,
}

impl<V: Clone> PlanLruCache<V> {
    /// Construct a bounded LRU cache holding at most `capacity` entries (clamped to `>= 1`).
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            capacity: cap,
            entries: HashMap::with_capacity(cap),
            order: VecDeque::with_capacity(cap),
        }
    }

    /// Look up `key`, promoting it to most-recently-used on hit.
    pub fn get(&mut self, key: &str) -> Option<V> {
        let val = self.entries.get(key).cloned()?;
        if let Some(pos) = self.order.iter().position(|k| k.as_ref() == key)
            && let Some(entry_key) = self.order.remove(pos)
        {
            self.order.push_back(entry_key);
        }
        Some(val)
    }

    /// Insert `(key, value)` if absent, evicting the least-recently-used key when at capacity.
    pub fn insert(&mut self, key: Box<str>, value: V) -> V {
        if let Some(existing) = self.get(key.as_ref()) {
            return existing;
        }
        while self.entries.len() >= self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(evicted.as_ref());
            } else {
                break;
            }
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, value.clone());
        value
    }

    /// Return the current number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` when the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove all cached entries.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

/// Buffered result and PEP 249 state shared by sync and async Python cursors.
pub struct BufferedCursorState {
    pub rows: Vec<Py<PyTuple>>,
    pub pos: usize,
    pub description: Option<Py<PyTuple>>,
    pub rowcount: i64,
    pub lastrowid: Option<Py<PyAny>>,
    pub arraysize: usize,
    pub executed: bool,
    pub closed: bool,
}

impl Default for BufferedCursorState {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferedCursorState {
    /// Construct a fresh cursor state with `rowcount = -1` and `arraysize = 1`.
    pub const fn new() -> Self {
        Self {
            rows: Vec::new(),
            pos: 0,
            description: None,
            rowcount: -1,
            lastrowid: None,
            arraysize: 1,
            executed: false,
            closed: false,
        }
    }

    /// Return `InterfaceError` when the cursor has been closed.
    pub fn ensure_open(&self, py: Python<'_>) -> PyResult<()> {
        if self.closed {
            Err(cursor_interface_err(py, "cursor is closed"))
        } else {
            Ok(())
        }
    }

    /// Replace the buffered result with a new execution outcome.
    pub fn store(
        &mut self,
        description: Option<Py<PyTuple>>,
        rows: Vec<Py<PyTuple>>,
        rowcount: i64,
        lastrowid: Option<Py<PyAny>>,
    ) {
        self.rows = rows;
        self.pos = 0;
        self.description = description;
        self.rowcount = rowcount;
        self.lastrowid = lastrowid;
        self.executed = true;
    }

    /// Clear any buffered result and reset statement attributes after a failed execute or close.
    pub fn clear_result(&mut self) {
        self.rows.clear();
        self.pos = 0;
        self.description = None;
        self.rowcount = -1;
        self.lastrowid = None;
        self.executed = false;
    }

    /// Fetch the next buffered row, or `None` when no result set is active or rows are exhausted.
    pub fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        self.ensure_open(py)?;
        if self.description.is_none() || self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.pos].clone_ref(py);
        self.pos += 1;
        Ok(Some(row))
    }

    /// Fetch up to `size` (or `self.arraysize`) buffered rows as a Python `list[tuple]`.
    pub fn fetchmany(&mut self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<PyList>> {
        self.ensure_open(py)?;
        let count = match size {
            Some(n) if n < 0 => {
                return Err(cursor_programming_err(
                    py,
                    "fetchmany size must be non-negative",
                ));
            }
            Some(n) => usize::try_from(n).unwrap_or(usize::MAX),
            None => self.arraysize,
        };
        if self.description.is_none() || count == 0 || self.pos >= self.rows.len() {
            return Ok(PyList::empty(py).unbind());
        }
        let end = self.pos.saturating_add(count).min(self.rows.len());
        let slice = &self.rows[self.pos..end];
        let items: Vec<Bound<'_, PyTuple>> = slice.iter().map(|r| r.bind(py).clone()).collect();
        self.pos = end;
        Ok(PyList::new(py, items)?.unbind())
    }

    /// Fetch all remaining buffered rows as a Python `list[tuple]`.
    pub fn fetchall(&mut self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.ensure_open(py)?;
        if self.description.is_none() || self.pos >= self.rows.len() {
            return Ok(PyList::empty(py).unbind());
        }
        let slice = &self.rows[self.pos..];
        let items: Vec<Bound<'_, PyTuple>> = slice.iter().map(|r| r.bind(py).clone()).collect();
        self.pos = self.rows.len();
        Ok(PyList::new(py, items)?.unbind())
    }

    /// Validate and update `arraysize` (`>= 1`).
    pub fn set_arraysize(&mut self, py: Python<'_>, value: i64) -> PyResult<()> {
        match usize::try_from(value) {
            Ok(size) if size >= 1 => {
                self.arraysize = size;
                Ok(())
            }
            _ => Err(cursor_programming_err(
                py,
                "arraysize must be a positive integer",
            )),
        }
    }

    /// Close the cursor and discard any buffered rows or metadata.
    pub fn close(&mut self) {
        self.closed = true;
        self.clear_result();
    }
}

/// Build a PEP 249 `description` tuple of 7-tuples `(name, type_code, None, None, None, None, null_ok)`.
pub fn build_description_7tuple<'py, I, S>(py: Python<'py>, columns: I) -> PyResult<Py<PyTuple>>
where
    I: IntoIterator<Item = (S, Option<Py<PyAny>>, Option<bool>)>,
    S: AsRef<str>,
{
    let mut entries: Vec<Bound<'py, PyTuple>> = Vec::new();
    for (name, type_code, null_ok) in columns {
        let none = py.None();
        let tc = type_code.unwrap_or_else(|| py.None());
        let null_obj: Py<PyAny> = match null_ok {
            Some(flag) => flag.into_pyobject(py)?.to_owned().into_any().unbind(),
            None => py.None(),
        };
        let entry = PyTuple::new(
            py,
            [
                name.as_ref().into_pyobject(py)?.into_any().unbind(),
                tc,
                none.clone_ref(py),
                none.clone_ref(py),
                none.clone_ref(py),
                none,
                null_obj,
            ],
        )?;
        entries.push(entry);
    }
    Ok(PyTuple::new(py, entries)?.unbind())
}
