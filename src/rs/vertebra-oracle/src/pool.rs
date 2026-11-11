//! Bounded physical connection pools (`SyncOraclePool` and `AsyncOraclePool`), DRCP purity/tagging, and session reset.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::Notify;
use vertebra_db::py_async::{RustDbTaskAwaitable, spawn_rust_db_task_with_attr};

use crate::client::{
    AsyncOracleClient, ORACLE_COMPLETION_DRAINER_ATTR, OracleWorkerHandle, SyncOracleClient,
    WorkerCommand, build_raw_oracle_options,
};
use crate::config::NativeOracleConfig;
use crate::errors::OracleError;

/// Validated connection pool configuration options.
#[derive(Clone, Debug)]
pub struct OraclePoolOptions {
    pub config: NativeOracleConfig,
    pub min_size: usize,
    pub max_size: usize,
    pub acquire_timeout: Duration,
    pub reset_timeout: Duration,
    pub close_timeout: Duration,
    pub max_lifetime: Duration,
    pub idle_timeout: Duration,
    pub health_check_interval: Duration,
}

impl OraclePoolOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn validate(
        config: NativeOracleConfig,
        min_size: i64,
        max_size: i64,
        acquire_timeout: f64,
        reset_timeout: f64,
        close_timeout: f64,
        max_lifetime_seconds: f64,
        idle_timeout_seconds: f64,
        health_check_interval_seconds: f64,
    ) -> Result<Self, OracleError> {
        if max_size < 1 {
            return Err(OracleError::Config(
                "Oracle pool max_size must be >= 1".to_owned(),
            ));
        }
        if min_size < 0 {
            return Err(OracleError::Config(
                "Oracle pool min_size must be >= 0".to_owned(),
            ));
        }
        if min_size > max_size {
            return Err(OracleError::Config(
                "Oracle pool min_size cannot exceed max_size".to_owned(),
            ));
        }
        if !acquire_timeout.is_finite() || acquire_timeout <= 0.0 {
            return Err(OracleError::Config(
                "Oracle pool acquire_timeout must be > 0".to_owned(),
            ));
        }
        if !reset_timeout.is_finite() || reset_timeout <= 0.0 {
            return Err(OracleError::Config(
                "Oracle pool reset_timeout must be > 0".to_owned(),
            ));
        }
        if !close_timeout.is_finite() || close_timeout <= 0.0 {
            return Err(OracleError::Config(
                "Oracle pool close_timeout must be > 0".to_owned(),
            ));
        }
        if !max_lifetime_seconds.is_finite() || max_lifetime_seconds <= 0.0 {
            return Err(OracleError::Config(
                "Oracle pool max_lifetime_seconds must be > 0".to_owned(),
            ));
        }
        if !idle_timeout_seconds.is_finite() || idle_timeout_seconds <= 0.0 {
            return Err(OracleError::Config(
                "Oracle pool idle_timeout_seconds must be > 0".to_owned(),
            ));
        }
        if !health_check_interval_seconds.is_finite() || health_check_interval_seconds < 0.0 {
            return Err(OracleError::Config(
                "Oracle pool health_check_interval_seconds must be >= 0".to_owned(),
            ));
        }
        Ok(Self {
            config,
            min_size: min_size as usize,
            max_size: max_size as usize,
            acquire_timeout: Duration::from_secs_f64(acquire_timeout),
            reset_timeout: Duration::from_secs_f64(reset_timeout),
            close_timeout: Duration::from_secs_f64(close_timeout),
            max_lifetime: Duration::from_secs_f64(max_lifetime_seconds),
            idle_timeout: Duration::from_secs_f64(idle_timeout_seconds),
            health_check_interval: Duration::from_secs_f64(health_check_interval_seconds),
        })
    }
}

struct PoolState {
    closed: bool,
    idle: VecDeque<Arc<OracleWorkerHandle>>,
    borrowed: HashMap<u64, Arc<OracleWorkerHandle>>,
    opening: usize,
}

enum CheckoutSlot {
    Existing(Arc<OracleWorkerHandle>),
    ReservedOpening,
    Wait,
}

pub(crate) struct OraclePoolCore {
    options: OraclePoolOptions,
    state: Mutex<PoolState>,
    cvar: Condvar,
    async_notify: Notify,
}

impl OraclePoolCore {
    pub fn new(options: OraclePoolOptions) -> Self {
        Self {
            options,
            state: Mutex::new(PoolState {
                closed: false,
                idle: VecDeque::new(),
                borrowed: HashMap::new(),
                opening: 0,
            }),
            cvar: Condvar::new(),
            async_notify: Notify::new(),
        }
    }

    pub fn min_size(&self) -> usize {
        self.options.min_size
    }

    pub fn max_size(&self) -> usize {
        self.options.max_size
    }

    pub fn size(&self) -> usize {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.idle.len() + guard.borrowed.len() + guard.opening
    }

    pub fn idle_size(&self) -> usize {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.idle.len()
    }

    pub fn borrowed_size(&self) -> usize {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.borrowed.len()
    }

    pub fn is_closed(&self) -> bool {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.closed
    }

    pub fn stats_snapshot(&self) -> (usize, usize, usize, usize, usize, usize, bool) {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let idle = guard.idle.len();
        let borrowed = guard.borrowed.len();
        let opening = guard.opening;
        let size = idle + borrowed + opening;
        (
            self.options.min_size,
            self.options.max_size,
            size,
            idle,
            borrowed,
            opening,
            guard.closed,
        )
    }

    fn notify_all_waiters(&self) {
        self.cvar.notify_all();
        self.async_notify.notify_waiters();
    }

    fn normalize_purity(&self, purity: Option<&str>) -> Result<String, OracleError> {
        let raw = purity.unwrap_or_else(|| self.options.config.purity.as_str());
        let up = raw.trim().to_ascii_uppercase();
        if up != "SELF" && up != "NEW" {
            return Err(OracleError::Config(format!(
                "Invalid DRCP purity `{raw}`; expected 'SELF' or 'NEW'"
            )));
        }
        Ok(up)
    }

    fn resolve_timeout(&self, timeout: Option<f64>) -> Result<Duration, OracleError> {
        if let Some(t) = timeout {
            if !t.is_finite() || t <= 0.0 {
                return Err(OracleError::Config(
                    "Oracle pool acquire timeout must be > 0".to_owned(),
                ));
            }
            Ok(Duration::from_secs_f64(t))
        } else {
            Ok(self.options.acquire_timeout)
        }
    }

    fn sweep_and_try_checkout_locked(
        &self,
        guard: &mut PoolState,
        requested_tag: Option<&str>,
        purity: &str,
    ) -> Result<(CheckoutSlot, Vec<Arc<OracleWorkerHandle>>), OracleError> {
        if guard.closed {
            return Err(OracleError::Interface("Oracle pool is closed".to_owned()));
        }
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut remaining_idle = VecDeque::with_capacity(guard.idle.len());
        while let Some(w) = guard.idle.pop_front() {
            let lifetime_expired = now.duration_since(w.created_at()) >= self.options.max_lifetime;
            let idle_expired = now.duration_since(w.last_used_at()) >= self.options.idle_timeout;
            if w.is_closed() || lifetime_expired || idle_expired {
                expired.push(w);
            } else {
                remaining_idle.push_back(w);
            }
        }
        guard.idle = remaining_idle;

        if !guard.idle.is_empty() {
            let chosen = if purity == "SELF" {
                if let Some(req_tag) = requested_tag {
                    if let Some(pos) = guard
                        .idle
                        .iter()
                        .position(|w| w.connection_tag().as_deref() == Some(req_tag))
                    {
                        let w = guard.idle.remove(pos).expect("valid index");
                        w.set_tag_matched(true);
                        w.set_purity("SELF".to_owned());
                        w
                    } else {
                        let w = guard.idle.pop_front().expect("non-empty idle");
                        w.set_tag_matched(false);
                        w.set_purity("SELF".to_owned());
                        w
                    }
                } else {
                    let w = guard.idle.pop_front().expect("non-empty idle");
                    w.set_tag_matched(false);
                    w.set_purity("SELF".to_owned());
                    w
                }
            } else {
                let w = guard.idle.pop_front().expect("non-empty idle");
                w.reset_for_new_purity();
                w.set_purity("NEW".to_owned());
                w
            };
            guard.borrowed.insert(chosen.id(), Arc::clone(&chosen));
            return Ok((CheckoutSlot::Existing(chosen), expired));
        }

        if guard.opening + guard.idle.len() + guard.borrowed.len() < self.options.max_size {
            guard.opening += 1;
            return Ok((CheckoutSlot::ReservedOpening, expired));
        }

        Ok((CheckoutSlot::Wait, expired))
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_checkout(
        &self,
        slot: CheckoutSlot,
        purity: &str,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> Result<Option<Arc<OracleWorkerHandle>>, OracleError> {
        match slot {
            CheckoutSlot::Existing(worker) => {
                let now = Instant::now();
                if self.options.health_check_interval > Duration::ZERO
                    && now.duration_since(worker.last_health_check_at())
                        >= self.options.health_check_interval
                {
                    if worker.dispatch_sync(WorkerCommand::Ping).is_err() {
                        let _ = worker.close_sync();
                        {
                            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                            guard.borrowed.remove(&worker.id());
                        }
                        self.notify_all_waiters();
                        return Ok(None);
                    }
                    worker.touch_health_check();
                }
                worker.touch_last_used();
                worker.stage_session_attrs(client_identifier, module, action, ecid);
                Ok(Some(worker))
            }
            CheckoutSlot::ReservedOpening => {
                let spawn_res = OracleWorkerHandle::spawn(self.options.config.clone());
                let worker = match spawn_res {
                    Ok(w) => Arc::new(w),
                    Err(err) => {
                        {
                            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                            guard.opening = guard.opening.saturating_sub(1);
                        }
                        self.notify_all_waiters();
                        return Err(err);
                    }
                };
                if purity == "NEW" {
                    worker.reset_for_new_purity();
                }
                worker.set_purity(purity.to_owned());
                worker.set_tag_matched(false);
                worker.stage_session_attrs(client_identifier, module, action, ecid);

                if let Err(err) = worker.dispatch_sync(WorkerCommand::Connect) {
                    let _ = worker.close_sync();
                    {
                        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                        guard.opening = guard.opening.saturating_sub(1);
                    }
                    self.notify_all_waiters();
                    return Err(err);
                }

                let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                guard.opening = guard.opening.saturating_sub(1);
                if guard.closed {
                    drop(guard);
                    let _ = worker.close_sync();
                    self.notify_all_waiters();
                    return Err(OracleError::Interface("Oracle pool is closed".to_owned()));
                }
                guard.borrowed.insert(worker.id(), Arc::clone(&worker));
                Ok(Some(worker))
            }
            CheckoutSlot::Wait => Ok(None),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn acquire_sync(
        &self,
        timeout: Option<f64>,
        connection_tag: Option<String>,
        purity: Option<&str>,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> Result<Arc<OracleWorkerHandle>, OracleError> {
        let effective_purity = self.normalize_purity(purity)?;
        let wait_dur = self.resolve_timeout(timeout)?;
        let deadline = Instant::now() + wait_dur;
        let effective_tag = connection_tag.or_else(|| self.options.config.connection_tag.clone());

        loop {
            let (slot, expired) = {
                let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    let (slot, expired) = self.sweep_and_try_checkout_locked(
                        &mut guard,
                        effective_tag.as_deref(),
                        &effective_purity,
                    )?;
                    if !matches!(slot, CheckoutSlot::Wait) || !expired.is_empty() {
                        break (slot, expired);
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(OracleError::PoolExhausted(
                            "Oracle connection pool exhausted before acquire_timeout".to_owned(),
                        ));
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    let (next_guard, timeout_res) = self
                        .cvar
                        .wait_timeout(guard, remaining)
                        .unwrap_or_else(|e| e.into_inner());
                    guard = next_guard;
                    if timeout_res.timed_out() && Instant::now() >= deadline {
                        return Err(OracleError::PoolExhausted(
                            "Oracle connection pool exhausted before acquire_timeout".to_owned(),
                        ));
                    }
                }
            };

            for w in expired {
                let _ = w.close_sync();
            }

            if matches!(slot, CheckoutSlot::Wait) {
                continue;
            }

            if let Some(worker) = self.finalize_checkout(
                slot,
                &effective_purity,
                client_identifier.clone(),
                module.clone(),
                action.clone(),
                ecid.clone(),
            )? {
                return Ok(worker);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn acquire_async(
        self: Arc<Self>,
        timeout: Option<f64>,
        connection_tag: Option<String>,
        purity: Option<String>,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> Result<Arc<OracleWorkerHandle>, OracleError> {
        let effective_purity = self.normalize_purity(purity.as_deref())?;
        let wait_dur = self.resolve_timeout(timeout)?;
        let deadline = Instant::now() + wait_dur;
        let effective_tag = connection_tag.or_else(|| self.options.config.connection_tag.clone());

        loop {
            let notified = self.async_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let (slot, expired) = {
                let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
                self.sweep_and_try_checkout_locked(
                    &mut guard,
                    effective_tag.as_deref(),
                    &effective_purity,
                )?
            };

            for w in expired {
                let _ = w.close_sync();
            }

            if !matches!(slot, CheckoutSlot::Wait) {
                let core = Arc::clone(&self);
                let purity_clone = effective_purity.clone();
                let cid_clone = client_identifier.clone();
                let mod_clone = module.clone();
                let act_clone = action.clone();
                let ecid_clone = ecid.clone();
                let finalized = tokio::task::spawn_blocking(move || {
                    core.finalize_checkout(
                        slot,
                        &purity_clone,
                        cid_clone,
                        mod_clone,
                        act_clone,
                        ecid_clone,
                    )
                })
                .await
                .map_err(|e| {
                    OracleError::Interface(format!("pool checkout task panicked: {e}"))
                })??;

                if let Some(worker) = finalized {
                    return Ok(worker);
                }
                continue;
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(OracleError::PoolExhausted(
                    "Oracle connection pool exhausted before acquire_timeout".to_owned(),
                ));
            }
            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, notified).await.is_err()
                && Instant::now() >= deadline
            {
                return Err(OracleError::PoolExhausted(
                    "Oracle connection pool exhausted before acquire_timeout".to_owned(),
                ));
            }
        }
    }

    pub fn release_sync(
        &self,
        worker: &Arc<OracleWorkerHandle>,
        connection_tag: Option<String>,
        discard: bool,
    ) -> Result<(), OracleError> {
        let (was_borrowed, pool_closed) = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let removed = guard.borrowed.remove(&worker.id()).is_some();
            (removed, guard.closed)
        };

        if !was_borrowed {
            if discard {
                let _ = worker.close_sync();
            }
            return Ok(());
        }

        let now = Instant::now();
        let lifetime_expired = now.duration_since(worker.created_at()) >= self.options.max_lifetime;
        let idle_expired = now.duration_since(worker.last_used_at()) >= self.options.idle_timeout;

        if discard || pool_closed || worker.is_closed() || lifetime_expired || idle_expired {
            let _ = worker.close_sync();
            self.notify_all_waiters();
            return Ok(());
        }

        if worker
            .rollback_with_timeout(self.options.reset_timeout)
            .is_err()
            || worker.is_closed()
        {
            let _ = worker.close_sync();
            self.notify_all_waiters();
            return Ok(());
        }

        if let Some(tag) = connection_tag {
            worker.set_connection_tag(Some(tag));
        } else {
            worker.set_connection_tag(None);
            worker.reset_untagged_session_attrs();
        }
        worker.set_tag_matched(false);
        worker.touch_last_used();

        let should_close = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if guard.closed
                || guard.idle.len() + guard.borrowed.len() + guard.opening > self.options.max_size
            {
                true
            } else {
                guard.idle.push_back(Arc::clone(worker));
                false
            }
        };

        if should_close {
            let _ = worker.close_sync();
        }
        self.notify_all_waiters();
        Ok(())
    }

    pub fn evict_sync(&self, worker: &Arc<OracleWorkerHandle>) -> Result<(), OracleError> {
        {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            guard.borrowed.remove(&worker.id());
            guard.idle.retain(|w| w.id() != worker.id());
        }
        let _ = worker.close_sync();
        self.notify_all_waiters();
        Ok(())
    }

    pub fn close_sync(&self) -> Result<(), OracleError> {
        let (idle_workers, borrowed_workers) = {
            let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            guard.closed = true;
            let idle: Vec<Arc<OracleWorkerHandle>> = guard.idle.drain(..).collect();
            let borrowed: Vec<Arc<OracleWorkerHandle>> =
                guard.borrowed.drain().map(|(_, w)| w).collect();
            (idle, borrowed)
        };
        self.notify_all_waiters();
        for w in idle_workers.into_iter().chain(borrowed_workers) {
            let _ = w.close_sync();
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn build_pool_options_from_py(
    user: Option<String>,
    password: Option<String>,
    dsn: Option<String>,
    host: &str,
    port: i32,
    service_name: &str,
    sid: Option<String>,
    tns_admin: Option<String>,
    connect_descriptor: Option<String>,
    protocol: &str,
    ssl_server_dn_match: bool,
    ssl_server_cert_dn: Option<String>,
    wallet_location: Option<String>,
    wallet_password: Option<String>,
    pem_cert_data: Option<Vec<u8>>,
    cwallet_sso_data: Option<Vec<u8>>,
    auth_mode: &str,
    access_token: Option<String>,
    token_auth_options: Option<BTreeMap<String, String>>,
    cclass: Option<String>,
    purity: &str,
    connection_tag: Option<String>,
    stmtcachesize: i32,
    prefetchrows: i64,
    arraysize: i64,
    call_timeout_ms: Option<i64>,
    connect_timeout: f64,
    enable_lowercase_column_names: bool,
    client_identifier: Option<String>,
    module: Option<String>,
    action: Option<String>,
    ecid: Option<String>,
    min_size: i64,
    max_size: i64,
    acquire_timeout: f64,
    reset_timeout: f64,
    close_timeout: f64,
    max_lifetime_seconds: f64,
    idle_timeout_seconds: f64,
    health_check_interval_seconds: f64,
) -> Result<OraclePoolOptions, OracleError> {
    let raw = build_raw_oracle_options(
        user,
        password,
        dsn,
        host.to_owned(),
        port,
        service_name.to_owned(),
        sid,
        tns_admin,
        connect_descriptor,
        protocol.to_owned(),
        ssl_server_dn_match,
        ssl_server_cert_dn,
        wallet_location,
        wallet_password,
        pem_cert_data,
        cwallet_sso_data,
        auth_mode.to_owned(),
        access_token,
        token_auth_options,
        cclass,
        purity.to_owned(),
        connection_tag,
        stmtcachesize,
        prefetchrows,
        arraysize,
        call_timeout_ms,
        connect_timeout,
        enable_lowercase_column_names,
        client_identifier,
        module,
        action,
        ecid,
    );
    let cfg = NativeOracleConfig::validate(raw)?;
    OraclePoolOptions::validate(
        cfg,
        min_size,
        max_size,
        acquire_timeout,
        reset_timeout,
        close_timeout,
        max_lifetime_seconds,
        idle_timeout_seconds,
        health_check_interval_seconds,
    )
}

fn build_stats_dict(py: Python<'_>, core: &OraclePoolCore) -> PyResult<Py<PyDict>> {
    let (min_size, max_size, size, idle, borrowed, opening, closed) = core.stats_snapshot();
    let dict = PyDict::new(py);
    dict.set_item("min_size", min_size)?;
    dict.set_item("max_size", max_size)?;
    dict.set_item("size", size)?;
    dict.set_item("idle", idle)?;
    dict.set_item("idle_size", idle)?;
    dict.set_item("borrowed", borrowed)?;
    dict.set_item("borrowed_size", borrowed)?;
    dict.set_item("opening", opening)?;
    dict.set_item("closed", usize::from(closed))?;
    Ok(dict.unbind())
}

/// Bounded synchronous physical Oracle connection pool (`vertebra._oracle.SyncOraclePool`).
#[pyclass(
    name = "SyncOraclePool",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct SyncOraclePool {
    inner: Arc<OraclePoolCore>,
}

#[pymethods]
impl SyncOraclePool {
    #[new]
    #[pyo3(signature = (
        *,
        user=None,
        password=None,
        dsn=None,
        host="localhost",
        port=1521,
        service_name="FREEPDB1",
        sid=None,
        tns_admin=None,
        connect_descriptor=None,
        protocol="tcp",
        ssl_server_dn_match=true,
        ssl_server_cert_dn=None,
        wallet_location=None,
        wallet_password=None,
        pem_cert_data=None,
        cwallet_sso_data=None,
        auth_mode="password",
        access_token=None,
        token_auth_options=None,
        cclass=None,
        purity="SELF",
        connection_tag=None,
        stmtcachesize=64,
        prefetchrows=256,
        arraysize=256,
        call_timeout_ms=None,
        connect_timeout=10.0,
        enable_lowercase_column_names=true,
        client_identifier=None,
        module=None,
        action=None,
        ecid=None,
        min_size=1,
        max_size=10,
        acquire_timeout=30.0,
        reset_timeout=10.0,
        close_timeout=30.0,
        max_lifetime_seconds=3600.0,
        idle_timeout_seconds=600.0,
        health_check_interval_seconds=30.0,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        user: Option<String>,
        password: Option<String>,
        dsn: Option<String>,
        host: &str,
        port: i32,
        service_name: &str,
        sid: Option<String>,
        tns_admin: Option<String>,
        connect_descriptor: Option<String>,
        protocol: &str,
        ssl_server_dn_match: bool,
        ssl_server_cert_dn: Option<String>,
        wallet_location: Option<String>,
        wallet_password: Option<String>,
        pem_cert_data: Option<Vec<u8>>,
        cwallet_sso_data: Option<Vec<u8>>,
        auth_mode: &str,
        access_token: Option<String>,
        token_auth_options: Option<BTreeMap<String, String>>,
        cclass: Option<String>,
        purity: &str,
        connection_tag: Option<String>,
        stmtcachesize: i32,
        prefetchrows: i64,
        arraysize: i64,
        call_timeout_ms: Option<i64>,
        connect_timeout: f64,
        enable_lowercase_column_names: bool,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
        min_size: i64,
        max_size: i64,
        acquire_timeout: f64,
        reset_timeout: f64,
        close_timeout: f64,
        max_lifetime_seconds: f64,
        idle_timeout_seconds: f64,
        health_check_interval_seconds: f64,
    ) -> PyResult<Self> {
        let options = build_pool_options_from_py(
            user,
            password,
            dsn,
            host,
            port,
            service_name,
            sid,
            tns_admin,
            connect_descriptor,
            protocol,
            ssl_server_dn_match,
            ssl_server_cert_dn,
            wallet_location,
            wallet_password,
            pem_cert_data,
            cwallet_sso_data,
            auth_mode,
            access_token,
            token_auth_options,
            cclass,
            purity,
            connection_tag,
            stmtcachesize,
            prefetchrows,
            arraysize,
            call_timeout_ms,
            connect_timeout,
            enable_lowercase_column_names,
            client_identifier,
            module,
            action,
            ecid,
            min_size,
            max_size,
            acquire_timeout,
            reset_timeout,
            close_timeout,
            max_lifetime_seconds,
            idle_timeout_seconds,
            health_check_interval_seconds,
        )
        .map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            inner: Arc::new(OraclePoolCore::new(options)),
        })
    }

    #[getter]
    pub fn min_size(&self) -> usize {
        self.inner.min_size()
    }

    #[getter]
    pub fn max_size(&self) -> usize {
        self.inner.max_size()
    }

    #[getter]
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    #[getter]
    pub fn idle_size(&self) -> usize {
        self.inner.idle_size()
    }

    #[getter]
    pub fn borrowed_size(&self) -> usize {
        self.inner.borrowed_size()
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    #[pyo3(signature = (
        *,
        timeout=None,
        connection_tag=None,
        purity=None,
        client_identifier=None,
        module=None,
        action=None,
        ecid=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn acquire(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
        connection_tag: Option<String>,
        purity: Option<String>,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> PyResult<SyncOracleClient> {
        let inner = Arc::clone(&self.inner);
        let worker = py
            .detach(|| {
                inner.acquire_sync(
                    timeout,
                    connection_tag,
                    purity.as_deref(),
                    client_identifier,
                    module,
                    action,
                    ecid,
                )
            })
            .map_err(|e| e.to_py_err(py))?;
        Ok(SyncOracleClient::from_worker(worker))
    }

    #[pyo3(signature = (
        client,
        *,
        connection_tag=None,
        discard=false,
    ))]
    pub fn release(
        &self,
        py: Python<'_>,
        client: &SyncOracleClient,
        connection_tag: Option<String>,
        discard: bool,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let worker = Arc::clone(client.worker());
        py.detach(|| inner.release_sync(&worker, connection_tag, discard))
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (
        *,
        timeout=None,
        connection_tag=None,
        purity=None,
        client_identifier=None,
        module=None,
        action=None,
        ecid=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn acquire_from_pool(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
        connection_tag: Option<String>,
        purity: Option<String>,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> PyResult<SyncOracleClient> {
        self.acquire(
            py,
            timeout,
            connection_tag,
            purity,
            client_identifier,
            module,
            action,
            ecid,
        )
    }

    #[pyo3(signature = (
        client,
        *,
        connection_tag=None,
        discard=false,
    ))]
    pub fn release_to_pool(
        &self,
        py: Python<'_>,
        client: &SyncOracleClient,
        connection_tag: Option<String>,
        discard: bool,
    ) -> PyResult<()> {
        self.release(py, client, connection_tag, discard)
    }

    pub fn evict(&self, py: Python<'_>, client: &SyncOracleClient) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let worker = Arc::clone(client.worker());
        py.detach(|| inner.evict_sync(&worker))
            .map_err(|e| e.to_py_err(py))
    }

    pub fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        build_stats_dict(py, &self.inner)
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.detach(|| inner.close_sync())
            .map_err(|e| e.to_py_err(py))
    }
}

/// Bounded asynchronous physical Oracle connection pool (`vertebra._oracle.AsyncOraclePool`).
#[pyclass(
    name = "AsyncOraclePool",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct AsyncOraclePool {
    inner: Arc<OraclePoolCore>,
}

#[pymethods]
impl AsyncOraclePool {
    #[new]
    #[pyo3(signature = (
        *,
        user=None,
        password=None,
        dsn=None,
        host="localhost",
        port=1521,
        service_name="FREEPDB1",
        sid=None,
        tns_admin=None,
        connect_descriptor=None,
        protocol="tcp",
        ssl_server_dn_match=true,
        ssl_server_cert_dn=None,
        wallet_location=None,
        wallet_password=None,
        pem_cert_data=None,
        cwallet_sso_data=None,
        auth_mode="password",
        access_token=None,
        token_auth_options=None,
        cclass=None,
        purity="SELF",
        connection_tag=None,
        stmtcachesize=64,
        prefetchrows=256,
        arraysize=256,
        call_timeout_ms=None,
        connect_timeout=10.0,
        enable_lowercase_column_names=true,
        client_identifier=None,
        module=None,
        action=None,
        ecid=None,
        min_size=1,
        max_size=10,
        acquire_timeout=30.0,
        reset_timeout=10.0,
        close_timeout=30.0,
        max_lifetime_seconds=3600.0,
        idle_timeout_seconds=600.0,
        health_check_interval_seconds=30.0,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        user: Option<String>,
        password: Option<String>,
        dsn: Option<String>,
        host: &str,
        port: i32,
        service_name: &str,
        sid: Option<String>,
        tns_admin: Option<String>,
        connect_descriptor: Option<String>,
        protocol: &str,
        ssl_server_dn_match: bool,
        ssl_server_cert_dn: Option<String>,
        wallet_location: Option<String>,
        wallet_password: Option<String>,
        pem_cert_data: Option<Vec<u8>>,
        cwallet_sso_data: Option<Vec<u8>>,
        auth_mode: &str,
        access_token: Option<String>,
        token_auth_options: Option<BTreeMap<String, String>>,
        cclass: Option<String>,
        purity: &str,
        connection_tag: Option<String>,
        stmtcachesize: i32,
        prefetchrows: i64,
        arraysize: i64,
        call_timeout_ms: Option<i64>,
        connect_timeout: f64,
        enable_lowercase_column_names: bool,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
        min_size: i64,
        max_size: i64,
        acquire_timeout: f64,
        reset_timeout: f64,
        close_timeout: f64,
        max_lifetime_seconds: f64,
        idle_timeout_seconds: f64,
        health_check_interval_seconds: f64,
    ) -> PyResult<Self> {
        let options = build_pool_options_from_py(
            user,
            password,
            dsn,
            host,
            port,
            service_name,
            sid,
            tns_admin,
            connect_descriptor,
            protocol,
            ssl_server_dn_match,
            ssl_server_cert_dn,
            wallet_location,
            wallet_password,
            pem_cert_data,
            cwallet_sso_data,
            auth_mode,
            access_token,
            token_auth_options,
            cclass,
            purity,
            connection_tag,
            stmtcachesize,
            prefetchrows,
            arraysize,
            call_timeout_ms,
            connect_timeout,
            enable_lowercase_column_names,
            client_identifier,
            module,
            action,
            ecid,
            min_size,
            max_size,
            acquire_timeout,
            reset_timeout,
            close_timeout,
            max_lifetime_seconds,
            idle_timeout_seconds,
            health_check_interval_seconds,
        )
        .map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            inner: Arc::new(OraclePoolCore::new(options)),
        })
    }

    #[getter]
    pub fn min_size(&self) -> usize {
        self.inner.min_size()
    }

    #[getter]
    pub fn max_size(&self) -> usize {
        self.inner.max_size()
    }

    #[getter]
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    #[getter]
    pub fn idle_size(&self) -> usize {
        self.inner.idle_size()
    }

    #[getter]
    pub fn borrowed_size(&self) -> usize {
        self.inner.borrowed_size()
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.inner.is_closed()
    }

    #[pyo3(signature = (
        *,
        timeout=None,
        connection_tag=None,
        purity=None,
        client_identifier=None,
        module=None,
        action=None,
        ecid=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn acquire(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
        connection_tag: Option<String>,
        purity: Option<String>,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                inner
                    .acquire_async(
                        timeout,
                        connection_tag,
                        purity,
                        client_identifier,
                        module,
                        action,
                        ecid,
                    )
                    .await
            },
            |py, worker| {
                let client = AsyncOracleClient::from_worker(worker);
                Ok(Py::new(py, client)?.into_any())
            },
            |py, err| err.to_py_err(py),
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (
        client,
        *,
        connection_tag=None,
        discard=false,
    ))]
    pub fn release(
        &self,
        py: Python<'_>,
        client: &AsyncOracleClient,
        connection_tag: Option<String>,
        discard: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        let worker = Arc::clone(client.worker());
        spawn_rust_db_task_with_attr(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    inner.release_sync(&worker, connection_tag, discard)
                })
                .await
                .map_err(|e| OracleError::Interface(format!("pool release task panicked: {e}")))?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.to_py_err(py),
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (
        *,
        timeout=None,
        connection_tag=None,
        purity=None,
        client_identifier=None,
        module=None,
        action=None,
        ecid=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn acquire_from_pool(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
        connection_tag: Option<String>,
        purity: Option<String>,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.acquire(
            py,
            timeout,
            connection_tag,
            purity,
            client_identifier,
            module,
            action,
            ecid,
        )
    }

    #[pyo3(signature = (
        client,
        *,
        connection_tag=None,
        discard=false,
    ))]
    pub fn release_to_pool(
        &self,
        py: Python<'_>,
        client: &AsyncOracleClient,
        connection_tag: Option<String>,
        discard: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.release(py, client, connection_tag, discard)
    }

    pub fn evict(
        &self,
        py: Python<'_>,
        client: &AsyncOracleClient,
    ) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        let worker = Arc::clone(client.worker());
        spawn_rust_db_task_with_attr(
            py,
            async move {
                tokio::task::spawn_blocking(move || inner.evict_sync(&worker))
                    .await
                    .map_err(|e| OracleError::Interface(format!("pool evict task panicked: {e}")))?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.to_py_err(py),
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn stats(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        build_stats_dict(py, &self.inner)
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                tokio::task::spawn_blocking(move || inner.close_sync())
                    .await
                    .map_err(|e| OracleError::Interface(format!("pool close task panicked: {e}")))?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.to_py_err(py),
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }
}
