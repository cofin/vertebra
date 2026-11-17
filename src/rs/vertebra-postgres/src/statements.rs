//! Protocol-level prepared statement cache and pooler capability negotiation.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use crate::errors::PostgresError;
use crate::extended::PreparedDescription;

/// Connection pooler topology mode governing prepared statement and session feature safety.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PoolerMode {
    /// Direct connection to PostgreSQL without an intermediate connection pooler.
    #[default]
    Direct,
    /// Session-pooled connection where backend assignment persists for the client lifetime.
    Session,
    /// Transaction-pooled connection where backends may rotate at `ReadyForQuery('I')`.
    Transaction,
    /// Statement-pooled connection where multi-statement transactions and portals are forbidden.
    Statement,
    /// Automatic capability detection from server `ParameterStatus` (`pgbouncer.*`).
    Auto,
}

impl PoolerMode {
    /// Parse one of the five canonical `PoolerMode` spellings (`direct`, `session`, `transaction`, `statement`, `auto`).
    pub fn parse(raw: &str) -> Result<Self, PostgresError> {
        match raw {
            "direct" => Ok(Self::Direct),
            "session" => Ok(Self::Session),
            "transaction" => Ok(Self::Transaction),
            "statement" => Ok(Self::Statement),
            "auto" => Ok(Self::Auto),
            other => Err(PostgresError::Config(format!(
                "unsupported pooler_mode `{other}` (expected `direct`, `session`, `transaction`, `statement`, or `auto`)"
            ))),
        }
    }

    /// Return the canonical lowercase string spelling for this [`PoolerMode`].
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Session => "session",
            Self::Transaction => "transaction",
            Self::Statement => "statement",
            Self::Auto => "auto",
        }
    }
}

impl FromStr for PoolerMode {
    type Err = PostgresError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Resolved per-session pooler capabilities derived from configuration and server `ParameterStatus`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolerCapabilities {
    /// Configured [`PoolerMode`] requested by the caller.
    pub configured_mode: PoolerMode,
    /// Effective [`PoolerMode`] after consulting `pgbouncer.pool_mode` when `configured_mode == Auto`.
    pub effective_mode: PoolerMode,
    /// Whether explicit protocol-level prepared statement caching across a transaction pooler was enabled.
    pub protocol_level_pooler_cache: bool,
    /// Effective bounded prepared statement cache capacity for this physical session (`0` disables caching).
    pub effective_capacity: usize,
    /// Optional `pgbouncer.version` advertised via `ParameterStatus`.
    pub advertised_version: Option<String>,
    /// Optional `pgbouncer.max_prepared_statements` advertised via `ParameterStatus`.
    pub advertised_max_prepared: Option<usize>,
}

impl PoolerCapabilities {
    /// Resolve [`PoolerCapabilities`] from configured options and session `ParameterStatus` entries.
    pub fn resolve(
        configured_mode: PoolerMode,
        configured_capacity: usize,
        protocol_level_pooler_cache: bool,
        parameter_status: &BTreeMap<String, String>,
    ) -> Self {
        let advertised_version = parameter_status.get("pgbouncer.version").cloned();
        let advertised_max_prepared = parameter_status
            .get("pgbouncer.max_prepared_statements")
            .and_then(|v| v.trim().parse::<usize>().ok());
        let advertised_pool_mode =
            parameter_status.get("pgbouncer.pool_mode").and_then(|v| {
                match v.trim().to_ascii_lowercase().as_str() {
                    "session" => Some(PoolerMode::Session),
                    "transaction" => Some(PoolerMode::Transaction),
                    "statement" => Some(PoolerMode::Statement),
                    "direct" => Some(PoolerMode::Direct),
                    _ => None,
                }
            });

        let effective_mode = match configured_mode {
            PoolerMode::Auto => advertised_pool_mode.unwrap_or(PoolerMode::Auto),
            explicit => explicit,
        };

        let effective_capacity = match effective_mode {
            PoolerMode::Direct | PoolerMode::Session => match advertised_max_prepared {
                Some(0) => 0,
                Some(max) => configured_capacity.min(max),
                None => configured_capacity,
            },
            PoolerMode::Transaction | PoolerMode::Auto => match advertised_max_prepared {
                Some(0) => 0,
                Some(max) => configured_capacity.min(max),
                None => {
                    if protocol_level_pooler_cache {
                        configured_capacity
                    } else {
                        0
                    }
                }
            },
            PoolerMode::Statement => 0,
        };

        Self {
            configured_mode,
            effective_mode,
            protocol_level_pooler_cache,
            effective_capacity,
            advertised_version,
            advertised_max_prepared,
        }
    }

    /// Return `true` when multi-statement transactions (`BEGIN`), portals, and pipeline scopes are permitted.
    pub const fn allows_scoped_transactions(&self) -> bool {
        !matches!(self.effective_mode, PoolerMode::Statement)
    }

    /// Return `true` when multi-statement transactions (`BEGIN`), portals, and pipeline scopes are permitted.
    pub const fn supports_scoped_state(&self) -> bool {
        !matches!(self.effective_mode, PoolerMode::Statement)
    }

    /// Return `true` when `DISCARD ALL` session reset is safe and meaningful on lease return.
    pub const fn allows_discard_all_reset(&self) -> bool {
        matches!(
            self.effective_mode,
            PoolerMode::Direct | PoolerMode::Session
        )
    }
}

/// Return `true` if `name` is a PgBouncer/PgCat pooler `ParameterStatus` banner key.
pub fn is_pooler_banner_parameter(name: &str) -> bool {
    name.eq_ignore_ascii_case("pgbouncer.version")
        || name.eq_ignore_ascii_case("pgbouncer.pool_mode")
        || name.eq_ignore_ascii_case("pgbouncer.max_prepared_statements")
}

/// Return `true` if `sqlstate` indicates a cached plan or statement invalidation (`0A000`, `26000`, `42883`, `42P01`, `XX000`).
pub fn is_statement_invalidation_sqlstate(sqlstate: &str) -> bool {
    matches!(sqlstate, "0A000" | "26000" | "42883" | "42P01" | "XX000")
}

/// Composite lookup key for a protocol-level prepared statement in [`StatementCache`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StatementKey {
    /// Exact SQL command string.
    pub sql: String,
    /// Resolved parameter type OIDs (`>= 1`).
    pub param_oids: Vec<u32>,
    /// Session schema / `search_path` generation when the statement was prepared.
    pub schema_generation: u64,
}

impl StatementKey {
    /// Construct a new [`StatementKey`].
    pub fn new(sql: impl Into<String>, param_oids: Vec<u32>, schema_generation: u64) -> Self {
        Self {
            sql: sql.into(),
            param_oids,
            schema_generation,
        }
    }
}

/// Cached protocol-level prepared statement descriptor owned by a physical session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedStatement {
    /// Protocol statement name (`vtb_s_<u64>`).
    pub name: String,
    /// Negotiated parameter OID and result column metadata from `Describe('S')`.
    pub description: PreparedDescription,
    /// Schema generation under which this statement was prepared.
    pub schema_generation: u64,
}

#[derive(Clone, Debug)]
struct LruSlot {
    entry: Option<(StatementKey, CachedStatement)>,
    prev: Option<usize>,
    next: Option<usize>,
    next_free: Option<usize>,
}

/// Per-session bounded LRU cache of named protocol-level prepared statements.
#[derive(Clone, Debug)]
pub struct StatementCache {
    capacity: usize,
    schema_generation: u64,
    next_statement_id: u64,
    slots: Vec<LruSlot>,
    index: HashMap<StatementKey, usize>,
    head: Option<usize>,
    tail: Option<usize>,
    free: Option<usize>,
    evicted_names: Vec<String>,
}

impl StatementCache {
    /// Construct a new [`StatementCache`] with `capacity` maximum entries (`0` disables caching).
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            schema_generation: 1,
            next_statement_id: 1,
            slots: Vec::new(),
            index: HashMap::new(),
            head: None,
            tail: None,
            free: None,
            evicted_names: Vec::new(),
        }
    }

    /// Return the configured maximum entry capacity (`0` when disabled).
    pub const fn capacity(&self) -> usize {
        self.capacity
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

    fn push_tail(&mut self, idx: usize) {
        let tail = self.tail;
        self.slots[idx].prev = tail;
        self.slots[idx].next = None;
        match tail {
            Some(t) => self.slots[t].next = Some(idx),
            None => self.head = Some(idx),
        }
        self.tail = Some(idx);
    }

    fn move_to_tail(&mut self, idx: usize) {
        if self.tail == Some(idx) {
            return;
        }
        self.unlink(idx);
        self.push_tail(idx);
    }

    fn alloc_slot(&mut self, key: StatementKey, value: CachedStatement) -> usize {
        if let Some(idx) = self.free {
            self.free = self.slots[idx].next_free.take();
            self.slots[idx].entry = Some((key, value));
            self.slots[idx].prev = None;
            self.slots[idx].next = None;
            idx
        } else {
            let idx = self.slots.len();
            self.slots.push(LruSlot {
                entry: Some((key, value)),
                prev: None,
                next: None,
                next_free: None,
            });
            idx
        }
    }

    fn free_slot(&mut self, idx: usize) -> Option<(StatementKey, CachedStatement)> {
        self.unlink(idx);
        let removed = self.slots[idx].entry.take();
        self.slots[idx].next_free = self.free;
        self.free = Some(idx);
        removed
    }

    fn pop_head(&mut self) -> Option<(StatementKey, CachedStatement)> {
        let head_idx = self.head?;
        let (key, value) = self.free_slot(head_idx)?;
        self.index.remove(&key);
        Some((key, value))
    }

    /// Update the effective capacity, evicting oldest entries into `evicted_names` if shrunk.
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        if capacity == 0 {
            while let Some((_, evicted)) = self.pop_head() {
                self.evicted_names.push(evicted.name);
            }
            self.slots.clear();
            self.index.clear();
            self.head = None;
            self.tail = None;
            self.free = None;
            return;
        }
        while self.index.len() > capacity {
            let Some((_, evicted)) = self.pop_head() else {
                break;
            };
            self.evicted_names.push(evicted.name);
        }
    }

    /// Return the number of currently live cached statements.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Return `true` when the cache holds no live statements.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Return `true` if a cached statement exists for `(sql, param_oids)` at the current `schema_generation`.
    pub fn contains(&self, sql: &str, param_oids: &[u32]) -> bool {
        if self.capacity == 0 {
            return false;
        }
        let key = StatementKey {
            sql: sql.to_owned(),
            param_oids: param_oids.to_vec(),
            schema_generation: self.schema_generation,
        };
        self.index.contains_key(&key)
    }

    /// Return the current schema generation counter.
    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    /// Allocate the next monotonic statement name (`vtb_s_<u64>`).
    pub fn allocate_statement_name(&mut self) -> String {
        let id = self.next_statement_id;
        self.next_statement_id += 1;
        format!("vtb_s_{id}")
    }

    /// Return the current statement ID seed without mutating the cache.
    pub(crate) const fn peek_next_id_seed(&self) -> u64 {
        self.next_statement_id
    }

    /// Advance the monotonic statement ID seed after reserving batch-scoped names.
    pub(crate) fn advance_id_seed(&mut self, delta: u64) {
        self.next_statement_id = self.next_statement_id.saturating_add(delta);
    }

    /// Alias for [`StatementCache::allocate_statement_name`].
    pub fn next_statement_name(&mut self) -> String {
        self.allocate_statement_name()
    }

    /// Resolve effective prepared-statement cache capacity from pooler mode and `ParameterStatus`.
    pub fn resolve_effective_capacity(
        mode: PoolerMode,
        configured_capacity: usize,
        protocol_level_pooler_cache: bool,
        params: &BTreeMap<String, String>,
    ) -> Result<usize, PostgresError> {
        Ok(PoolerCapabilities::resolve(
            mode,
            configured_capacity,
            protocol_level_pooler_cache,
            params,
        )
        .effective_capacity)
    }

    /// Look up a cached statement by `(sql, param_oids)` at the current `schema_generation`, promoting it to MRU.
    pub fn get(&mut self, sql: &str, param_oids: &[u32]) -> Option<CachedStatement> {
        if self.capacity == 0 {
            return None;
        }
        let key = StatementKey {
            sql: sql.to_owned(),
            param_oids: param_oids.to_vec(),
            schema_generation: self.schema_generation,
        };
        self.get_by_key(&key)
    }

    /// Look up a cached statement by [`StatementKey`], promoting it to MRU.
    pub fn get_by_key(&mut self, key: &StatementKey) -> Option<CachedStatement> {
        if self.capacity == 0 || key.schema_generation != self.schema_generation {
            return None;
        }
        let idx = *self.index.get(key)?;
        self.move_to_tail(idx);
        self.slots[idx]
            .entry
            .as_ref()
            .map(|(_, cached)| cached.clone())
    }

    /// Look up a cached statement by `sql` when the caller provided no explicit parameter OIDs and the statement has a unique match at the current generation.
    pub fn get_by_sql(&mut self, sql: &str, param_count: usize) -> Option<CachedStatement> {
        if self.capacity == 0 {
            return None;
        }
        let generation = self.schema_generation;
        let mut cur = self.tail;
        let mut found = None;
        while let Some(idx) = cur {
            let slot = &self.slots[idx];
            if let Some((k, _)) = &slot.entry
                && k.schema_generation == generation
                && k.sql == sql
                && k.param_oids.len() == param_count
            {
                found = Some(idx);
                break;
            }
            cur = slot.prev;
        }
        let idx = found?;
        self.move_to_tail(idx);
        self.slots[idx]
            .entry
            .as_ref()
            .map(|(_, cached)| cached.clone())
    }

    /// Insert a prepared statement with a pre-allocated `name` into the LRU cache.
    pub fn insert_named(
        &mut self,
        name: String,
        sql: String,
        param_oids: Vec<u32>,
        description: PreparedDescription,
    ) -> Option<CachedStatement> {
        if self.capacity == 0 {
            return None;
        }
        let key = StatementKey {
            sql,
            param_oids,
            schema_generation: self.schema_generation,
        };
        let cached = CachedStatement {
            name,
            description,
            schema_generation: self.schema_generation,
        };
        if let Some(&idx) = self.index.get(&key) {
            if let Some((_, old)) = &mut self.slots[idx].entry {
                if old.name != cached.name {
                    self.evicted_names
                        .push(std::mem::replace(&mut old.name, cached.name.clone()));
                }
                *old = cached.clone();
            }
            self.move_to_tail(idx);
            return Some(cached);
        }
        while self.index.len() >= self.capacity {
            let Some((_, evicted)) = self.pop_head() else {
                break;
            };
            self.evicted_names.push(evicted.name);
        }
        let idx = self.alloc_slot(key.clone(), cached.clone());
        self.index.insert(key, idx);
        self.push_tail(idx);
        Some(cached)
    }

    /// Allocate a fresh statement name and insert `(sql, param_oids, description)` into the LRU cache.
    pub fn insert(
        &mut self,
        sql: String,
        param_oids: Vec<u32>,
        description: PreparedDescription,
    ) -> Option<CachedStatement> {
        if self.capacity == 0 {
            return None;
        }
        let name = self.allocate_statement_name();
        self.insert_named(name, sql, param_oids, description)
    }

    /// Drain all queued LRU-evicted statement names awaiting protocol `Close(b'S', name)`.
    pub fn take_evicted_names(&mut self) -> Vec<String> {
        std::mem::take(&mut self.evicted_names)
    }

    /// Borrow queued LRU-evicted statement names awaiting protocol `Close(b'S', name)`.
    pub fn evicted_names(&self) -> &[String] {
        &self.evicted_names
    }

    /// Evict a specific statement by `name` (e.g. after `0A000` or `26000`), optionally queuing `Close(b'S', name)`.
    pub fn evict_by_name(&mut self, name: &str, queue_close: bool) {
        let mut cur = self.head;
        let mut found = None;
        while let Some(idx) = cur {
            let slot = &self.slots[idx];
            if let Some((_, cached)) = &slot.entry
                && cached.name == name
            {
                found = Some(idx);
                break;
            }
            cur = slot.next;
        }
        if let Some(idx) = found
            && let Some((key, removed)) = self.free_slot(idx)
        {
            self.index.remove(&key);
            if queue_close {
                self.evicted_names.push(removed.name);
            }
        }
    }

    /// Remove and unlink a specific statement by [`StatementKey`].
    pub fn invalidate_key(&mut self, key: &StatementKey) -> Option<CachedStatement> {
        let idx = self.index.remove(key)?;
        self.free_slot(idx).map(|(_, cached)| cached)
    }

    /// Remove and unlink a cached statement by `(sql, param_oids)` at the current `schema_generation`.
    pub fn remove(&mut self, sql: &str, param_oids: &[u32]) -> Option<CachedStatement> {
        if self.capacity == 0 {
            return None;
        }
        let key = StatementKey {
            sql: sql.to_owned(),
            param_oids: param_oids.to_vec(),
            schema_generation: self.schema_generation,
        };
        self.invalidate_key(&key)
    }

    /// Bump `schema_generation` and clear all cached statements without emitting SQL `DEALLOCATE`.
    pub fn invalidate_all(&mut self) {
        self.schema_generation = self.schema_generation.wrapping_add(1);
        self.slots.clear();
        self.index.clear();
        self.head = None;
        self.tail = None;
        self.free = None;
        self.evicted_names.clear();
    }

    /// Bump `schema_generation` and clear all cached statements.
    pub fn clear(&mut self) {
        self.invalidate_all();
    }

    /// Clear all cached statements and queued evictions during `DISCARD ALL` pool return reset.
    pub fn clear_for_reset(&mut self) {
        self.invalidate_all();
    }

    /// Bump `schema_generation` and queue all live cached statements for protocol `Close(b'S', name)`.
    pub fn invalidate_and_queue_close(&mut self) {
        self.schema_generation = self.schema_generation.wrapping_add(1);
        let mut cur = self.head;
        while let Some(idx) = cur {
            cur = self.slots[idx].next;
            if let Some((_, cached)) = self.slots[idx].entry.take() {
                self.evicted_names.push(cached.name);
            }
        }
        self.slots.clear();
        self.index.clear();
        self.head = None;
        self.tail = None;
        self.free = None;
    }
}
