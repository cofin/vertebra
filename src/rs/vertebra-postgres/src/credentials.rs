//! Scoped Google Cloud OAuth2 and static IAM login credentials for PostgreSQL, Cloud SQL, and AlloyDB.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::time::Instant;
use vertebra_core::gcp_auth::{
    DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, GcpTokenCache, GcpTokenSnapshot, ProxyPolicy,
};

use crate::cloud::material::Clock;
use crate::errors::PostgresError;

/// Default OAuth2 scope for Cloud SQL and AlloyDB Admin API discovery and certificate signing.
pub const ADMIN_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Default OAuth2 scope for Cloud SQL and direct PostgreSQL IAM database login.
pub const CLOUDSQL_LOGIN_SCOPE: &str = "https://www.googleapis.com/auth/sqlservice.login";

/// Default OAuth2 scope for AlloyDB metadata exchange and IAM database login.
pub const ALLOYDB_LOGIN_SCOPE: &str = "https://www.googleapis.com/auth/alloydb.login";

/// Validated designated static IAM login credential (`iam_auth_options`) with explicit scope provenance and unsigned decimal expiry.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct StaticLoginCredential {
    pub access_token: String,
    pub expires_at_unix_ms: u64,
    pub declared_scopes: Vec<String>,
    pub quota_project_id: Option<String>,
    pub refresh_skew_ms: u64,
}

impl fmt::Debug for StaticLoginCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticLoginCredential")
            .field("access_token", &"<redacted>")
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .field("declared_scopes", &self.declared_scopes)
            .field("quota_project_id", &self.quota_project_id)
            .field("refresh_skew_ms", &self.refresh_skew_ms)
            .finish()
    }
}

impl StaticLoginCredential {
    /// Parse and validate a designated static login credential map against `required_scopes` and `now_ms`.
    pub(crate) fn from_options(
        options: &HashMap<String, String>,
        required_scopes: &[String],
        now_ms: u64,
        refresh_skew_ms: u64,
    ) -> Result<Self, PostgresError> {
        let access_opt = options.get("access_token");
        let bearer_opt = options.get("bearer_token");
        if let (Some(a), Some(b)) = (access_opt, bearer_opt)
            && a != b
        {
            return Err(PostgresError::Auth(
                "conflicting access_token and bearer_token values in static login options"
                    .to_string(),
            ));
        }

        let raw_token = access_opt.or(bearer_opt).ok_or_else(|| {
            PostgresError::Auth(
                "missing access_token or bearer_token in static login options".to_string(),
            )
        })?;
        let trimmed_token = raw_token.trim();
        if trimmed_token.is_empty() {
            return Err(PostgresError::Auth(
                "static login access_token must not be empty".to_string(),
            ));
        }

        let raw_expiry = options.get("expires_at_unix_ms").ok_or_else(|| {
            PostgresError::Auth(
                "static login credential requires canonical unsigned decimal expires_at_unix_ms"
                    .to_string(),
            )
        })?;
        let expires_at_unix_ms = parse_canonical_u64_decimal(raw_expiry)?;

        let raw_scopes = options.get("scopes").ok_or_else(|| {
            PostgresError::Auth(
                "static login credential requires explicit scopes declaration".to_string(),
            )
        })?;
        let declared_scopes: Vec<String> = raw_scopes
            .split(|c: char| c == ',' || c.is_ascii_whitespace())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if declared_scopes.is_empty() {
            return Err(PostgresError::Auth(
                "static login credential declared scopes must not be empty".to_string(),
            ));
        }
        for req in required_scopes {
            if !declared_scopes.iter().any(|d| d == req) {
                return Err(PostgresError::Auth(format!(
                    "static login credential scopes do not include required login scope `{req}`"
                )));
            }
        }

        let quota_project_id = options
            .get("quota_project_id")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let cred = Self {
            access_token: trimmed_token.to_string(),
            expires_at_unix_ms,
            declared_scopes,
            quota_project_id,
            refresh_skew_ms,
        };
        cred.ensure_usable(now_ms)?;
        Ok(cred)
    }

    /// Verify that `now_ms + refresh_skew_ms < expires_at_unix_ms`.
    pub(crate) fn ensure_usable(&self, now_ms: u64) -> Result<(), PostgresError> {
        if now_ms.saturating_add(self.refresh_skew_ms) < self.expires_at_unix_ms {
            Ok(())
        } else {
            Err(PostgresError::Auth(format!(
                "static login token is expired or within refresh skew (now_ms={now_ms}, skew_ms={}, expires_at_unix_ms={})",
                self.refresh_skew_ms, self.expires_at_unix_ms
            )))
        }
    }

    /// Construct a [`GcpTokenCache`] backed by [`GcpAuthSource::Static`] with the exact supplied `expires_at_unix_ms`.
    pub(crate) fn to_token_cache(&self, proxy_policy: ProxyPolicy) -> GcpTokenCache {
        GcpTokenCache::with_proxy_policy(
            GcpAuthSource::Static {
                access_token: self.access_token.clone(),
                expires_at_unix_ms: Some(self.expires_at_unix_ms),
                quota_project_id: self.quota_project_id.clone(),
            },
            self.refresh_skew_ms,
            proxy_policy,
        )
        .with_requested_scopes(self.declared_scopes.clone())
    }
}

fn parse_canonical_u64_decimal(raw: &str) -> Result<u64, PostgresError> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(PostgresError::Auth(format!(
            "invalid expires_at_unix_ms `{raw}` (expected canonical unsigned decimal u64)"
        )));
    }
    let parsed = raw.parse::<u64>().map_err(|_| {
        PostgresError::Auth(format!(
            "expires_at_unix_ms `{raw}` overflowed unsigned 64-bit integer"
        ))
    })?;
    if parsed.to_string() != raw {
        return Err(PostgresError::Auth(format!(
            "non-canonical expires_at_unix_ms `{raw}` (leading zeros are not allowed)"
        )));
    }
    Ok(parsed)
}

/// Resolved login token snapshot paired with scope provenance and refresh-skew enforcement.
#[derive(Clone)]
pub(crate) struct ScopedLoginSnapshot {
    pub snapshot: GcpTokenSnapshot,
    pub required_scopes: Vec<String>,
    pub declared_scopes: Option<Vec<String>>,
    pub refresh_skew_ms: u64,
    pub is_static: bool,
    cache: Option<Arc<GcpTokenCache>>,
}

impl PartialEq for ScopedLoginSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot.access_token == other.snapshot.access_token
            && self.snapshot.expires_at_unix_ms == other.snapshot.expires_at_unix_ms
            && self.snapshot.generation == other.snapshot.generation
            && self.required_scopes == other.required_scopes
            && self.declared_scopes == other.declared_scopes
            && self.refresh_skew_ms == other.refresh_skew_ms
            && self.is_static == other.is_static
    }
}

impl Eq for ScopedLoginSnapshot {}

impl fmt::Debug for ScopedLoginSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedLoginSnapshot")
            .field("token", &"<redacted>")
            .field("expires_at_unix_ms", &self.snapshot.expires_at_unix_ms)
            .field("generation", &self.snapshot.generation)
            .field("required_scopes", &self.required_scopes)
            .field("declared_scopes", &self.declared_scopes)
            .field("refresh_skew_ms", &self.refresh_skew_ms)
            .field("is_static", &self.is_static)
            .finish()
    }
}

impl ScopedLoginSnapshot {
    /// Construct a [`ScopedLoginSnapshot`] from a [`GcpTokenSnapshot`].
    pub(crate) fn new(
        snapshot: GcpTokenSnapshot,
        required_scopes: Vec<String>,
        declared_scopes: Option<Vec<String>>,
        refresh_skew_ms: u64,
        is_static: bool,
        cache: Option<Arc<GcpTokenCache>>,
    ) -> Self {
        Self {
            snapshot,
            required_scopes,
            declared_scopes,
            refresh_skew_ms,
            is_static,
            cache,
        }
    }

    /// Return the access token string slice.
    pub(crate) fn access_token(&self) -> &str {
        self.snapshot.access_token.as_str()
    }

    /// Verify that `now_ms + refresh_skew_ms < snapshot.expires_at_unix_ms` before transmitting credentials on the wire.
    pub(crate) fn ensure_usable(&self, now_ms: u64) -> Result<(), PostgresError> {
        if now_ms.saturating_add(self.refresh_skew_ms) < self.snapshot.expires_at_unix_ms {
            Ok(())
        } else {
            Err(PostgresError::Auth(format!(
                "IAM login token expired or entered refresh skew before transmission (now_ms={now_ms}, skew_ms={}, expires_at_unix_ms={})",
                self.refresh_skew_ms, self.snapshot.expires_at_unix_ms
            )))
        }
    }

    /// Invalidate the underlying [`GcpTokenCache`] generation when the server rejects authentication (`28xxx`).
    pub(crate) fn invalidate_generation(&self) {
        if let Some(ref cache) = self.cache {
            cache.invalidate(self.snapshot.generation);
        }
    }
}

/// Scope-keyed Admin and login token provider isolating `cloud_auth_options` and `iam_auth_options`.
#[derive(Clone)]
pub(crate) struct ScopedTokenProvider {
    cloud_auth_options: Option<HashMap<String, String>>,
    iam_auth_options: Option<HashMap<String, String>>,
    clock: Arc<dyn Clock>,
    admin_cache: Arc<Mutex<Option<Arc<GcpTokenCache>>>>,
    login_caches: Arc<Mutex<HashMap<Vec<String>, Arc<GcpTokenCache>>>>,
}

impl fmt::Debug for ScopedTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedTokenProvider")
            .field("has_cloud_auth_options", &self.cloud_auth_options.is_some())
            .field("has_iam_auth_options", &self.iam_auth_options.is_some())
            .finish()
    }
}

impl ScopedTokenProvider {
    /// Pure constructor storing owned option maps and clock without performing file I/O or ADC discovery.
    pub(crate) fn new(
        cloud_auth_options: Option<HashMap<String, String>>,
        iam_auth_options: Option<HashMap<String, String>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            cloud_auth_options,
            iam_auth_options,
            clock,
            admin_cache: Arc::new(Mutex::new(None)),
            login_caches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Resolve a fresh or cached Admin API token (`cloud-platform`) under `absolute_deadline`.
    pub(crate) async fn resolve_admin_token(
        &self,
        absolute_deadline: Instant,
    ) -> Result<GcpTokenSnapshot, PostgresError> {
        let empty = HashMap::new();
        let opts = self.cloud_auth_options.as_ref().unwrap_or(&empty);
        let now_ms = self.clock.now_unix_ms();

        if has_static_token_key(opts) {
            let token = extract_static_token(opts)?;
            let expiry_ms = parse_optional_admin_expiry(opts, now_ms)?;
            return Ok(GcpTokenSnapshot {
                access_token: token,
                expires_at_unix_ms: expiry_ms.unwrap_or(u64::MAX / 2),
                generation: 1,
                quota_project_id: opts.get("quota_project_id").cloned(),
            });
        }

        let cached = self
            .admin_cache
            .lock()
            .expect("admin_cache lock poisoned")
            .clone();
        let cache = match cached {
            Some(c) => c,
            None => {
                let opts_owned = opts.clone();
                let built = run_blocking_until(absolute_deadline, move || {
                    let scopes = vec![ADMIN_CLOUD_PLATFORM_SCOPE.to_string()];
                    GcpTokenCache::from_scoped_options(Some(&opts_owned), &scopes)
                        .map_err(PostgresError::Auth)
                })
                .await?;
                let mut guard = self.admin_cache.lock().expect("admin_cache lock poisoned");
                *guard = Some(Arc::clone(&built));
                built
            }
        };

        let now_for_cache = self.clock.now_unix_ms();
        if let Some(snap) = cache.get_cached(now_for_cache) {
            return Ok((*snap).clone());
        }

        let snap = tokio::time::timeout_at(absolute_deadline, cache.get_or_refresh_with_force(true))
            .await
            .map_err(|_| {
                PostgresError::Transport("GCP Admin OAuth2 token refresh timed out".to_string())
            })?
            .map_err(PostgresError::Auth)?;
        Ok((*snap).clone())
    }

    /// Resolve a fresh or cached login token snapshot for `required_scopes` under `absolute_deadline`.
    pub(crate) async fn resolve_login_snapshot(
        &self,
        required_scopes: &[String],
        absolute_deadline: Instant,
    ) -> Result<ScopedLoginSnapshot, PostgresError> {
        let now_ms = self.clock.now_unix_ms();
        let iam_non_empty = self.iam_auth_options.as_ref().filter(|m| !m.is_empty());

        if let Some(iam_opts) = iam_non_empty {
            let skew_ms = parse_refresh_skew_ms(iam_opts);
            if has_static_token_key(iam_opts) {
                let static_cred =
                    StaticLoginCredential::from_options(iam_opts, required_scopes, now_ms, skew_ms)?;
                let proxy = parse_proxy_policy(iam_opts);
                let cache = Arc::new(static_cred.to_token_cache(proxy));
                let snap = cache
                    .get_or_refresh()
                    .await
                    .map_err(PostgresError::Auth)?;
                let login_snap = ScopedLoginSnapshot::new(
                    (*snap).clone(),
                    required_scopes.to_vec(),
                    Some(static_cred.declared_scopes),
                    skew_ms,
                    true,
                    Some(cache),
                );
                login_snap.ensure_usable(self.clock.now_unix_ms())?;
                return Ok(login_snap);
            }

            let effective_scopes = extract_explicit_scopes(iam_opts)
                .unwrap_or_else(|| required_scopes.to_vec());
            for req in required_scopes {
                if !effective_scopes.iter().any(|s| s == req) {
                    return Err(PostgresError::Auth(format!(
                        "iam_auth_options scopes do not include required login scope `{req}`"
                    )));
                }
            }
            return self
                .resolve_dynamic_login_with_opts(iam_opts, &effective_scopes, skew_ms, absolute_deadline)
                .await;
        }

        let empty = HashMap::new();
        let cloud_opts = self.cloud_auth_options.as_ref().unwrap_or(&empty);
        if has_static_token_key(cloud_opts) {
            return Err(PostgresError::Auth(
                "opaque static access_token in cloud_auth_options cannot be used for database IAM login without designated iam_auth_options".to_string(),
            ));
        }

        let skew_ms = parse_refresh_skew_ms(cloud_opts);
        let mut derived_opts = cloud_opts.clone();
        derived_opts.remove("scopes");
        self.resolve_dynamic_login_with_opts(
            &derived_opts,
            required_scopes,
            skew_ms,
            absolute_deadline,
        )
        .await
    }

    /// Resolve a direct-host IAM login token snapshot (where `iam_auth_options["scopes"]` overrides `default_scopes`).
    pub(crate) async fn resolve_direct_login(
        &self,
        default_scopes: &[String],
        absolute_deadline: Instant,
    ) -> Result<ScopedLoginSnapshot, PostgresError> {
        let iam_non_empty = self.iam_auth_options.as_ref().filter(|m| !m.is_empty());
        let authoritative_scopes = iam_non_empty
            .and_then(extract_explicit_scopes)
            .unwrap_or_else(|| default_scopes.to_vec());
        self.resolve_login_snapshot(&authoritative_scopes, absolute_deadline)
            .await
    }

    async fn resolve_dynamic_login_with_opts(
        &self,
        opts: &HashMap<String, String>,
        effective_scopes: &[String],
        skew_ms: u64,
        absolute_deadline: Instant,
    ) -> Result<ScopedLoginSnapshot, PostgresError> {
        let key = effective_scopes.to_vec();
        let cached = self
            .login_caches
            .lock()
            .expect("login_caches lock poisoned")
            .get(&key)
            .cloned();

        let cache = match cached {
            Some(c) => c,
            None => {
                let opts_owned = opts.clone();
                let scopes_owned = key.clone();
                let built = run_blocking_until(absolute_deadline, move || {
                    GcpTokenCache::from_scoped_options(Some(&opts_owned), &scopes_owned)
                        .map_err(PostgresError::Auth)
                })
                .await?;
                let mut guard = self
                    .login_caches
                    .lock()
                    .expect("login_caches lock poisoned");
                guard.insert(key.clone(), Arc::clone(&built));
                built
            }
        };

        let now_before = self.clock.now_unix_ms();
        let snap = if let Some(cached_snap) = cache.get_cached(now_before) {
            cached_snap
        } else {
            tokio::time::timeout_at(absolute_deadline, cache.get_or_refresh_with_force(true))
                .await
                .map_err(|_| {
                    PostgresError::Transport("GCP IAM login token refresh timed out".to_string())
                })?
                .map_err(PostgresError::Auth)?
        };

        let login_snap = ScopedLoginSnapshot::new(
            (*snap).clone(),
            effective_scopes.to_vec(),
            extract_explicit_scopes(opts),
            skew_ms,
            false,
            Some(cache),
        );
        login_snap.ensure_usable(self.clock.now_unix_ms())?;
        Ok(login_snap)
    }
}

pub(crate) fn has_static_token_key(opts: &HashMap<String, String>) -> bool {
    opts.contains_key("access_token") || opts.contains_key("bearer_token")
}

pub(crate) fn extract_explicit_scopes(opts: &HashMap<String, String>) -> Option<Vec<String>> {
    let raw = opts.get("scopes")?;
    let parsed: Vec<String> = raw
        .split(|c: char| c == ',' || c.is_ascii_whitespace())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

fn extract_static_token(opts: &HashMap<String, String>) -> Result<String, PostgresError> {
    if let (Some(a), Some(b)) = (opts.get("access_token"), opts.get("bearer_token"))
        && a != b
    {
        return Err(PostgresError::Auth(
            "conflicting access_token and bearer_token values".to_string(),
        ));
    }
    let raw = opts
        .get("access_token")
        .or_else(|| opts.get("bearer_token"))
        .map(String::as_str)
        .unwrap_or("");
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(PostgresError::Auth(
            "static access_token must not be empty".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

fn parse_optional_admin_expiry(
    opts: &HashMap<String, String>,
    now_ms: u64,
) -> Result<Option<u64>, PostgresError> {
    if let Some(raw_exp) = opts
        .get("expires_at_unix_ms")
        .or_else(|| opts.get("expires_at_ms"))
        .or_else(|| opts.get("token_expiry_ms"))
    {
        let exp = raw_exp.trim().parse::<u64>().map_err(|_| {
            PostgresError::Auth("invalid static Admin token expiry timestamp".to_string())
        })?;
        if exp <= now_ms {
            return Err(PostgresError::Auth(
                "static Admin OAuth2 token is already expired".to_string(),
            ));
        }
        return Ok(Some(exp));
    }
    if let Some(raw_ttl) = opts.get("expires_in_secs") {
        let secs = raw_ttl.trim().parse::<u64>().map_err(|_| {
            PostgresError::Auth("invalid static Admin token expires_in_secs".to_string())
        })?;
        if secs == 0 {
            return Err(PostgresError::Auth(
                "static Admin OAuth2 token is already expired".to_string(),
            ));
        }
        return Ok(Some(now_ms.saturating_add(secs.saturating_mul(1_000))));
    }
    Ok(None)
}

fn parse_refresh_skew_ms(opts: &HashMap<String, String>) -> u64 {
    opts.get("refresh_skew_ms")
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_REFRESH_SKEW_MS)
}

fn parse_proxy_policy(opts: &HashMap<String, String>) -> ProxyPolicy {
    match opts
        .get("proxy_policy")
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("direct") => ProxyPolicy::Direct,
        _ => ProxyPolicy::Environment,
    }
}

async fn run_blocking_until<F, T>(
    absolute_deadline: Instant,
    f: F,
) -> Result<T, PostgresError>
where
    F: FnOnce() -> Result<T, PostgresError> + Send + 'static,
    T: Send + 'static,
{
    if Instant::now() >= absolute_deadline {
        return Err(PostgresError::Transport(
            "credential discovery deadline exceeded".to_string(),
        ));
    }
    let handle = tokio::task::spawn_blocking(f);
    tokio::time::timeout_at(absolute_deadline, handle)
        .await
        .map_err(|_| {
            PostgresError::Transport("credential discovery timed out".to_string())
        })?
        .map_err(|e| PostgresError::Auth(format!("credential discovery task panicked: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_login_contract() {
        let required = vec![CLOUDSQL_LOGIN_SCOPE.to_string()];
        let now_ms = 1_000_000_u64;
        let skew_ms = 300_000_u64;

        let mut missing_tok = HashMap::new();
        missing_tok.insert("expires_at_unix_ms".to_string(), "2000000".to_string());
        missing_tok.insert("scopes".to_string(), CLOUDSQL_LOGIN_SCOPE.to_string());
        assert!(matches!(
            StaticLoginCredential::from_options(&missing_tok, &required, now_ms, skew_ms),
            Err(PostgresError::Auth(_))
        ));

        let mut conflict = missing_tok.clone();
        conflict.insert("access_token".to_string(), "secret-alpha".to_string());
        conflict.insert("bearer_token".to_string(), "secret-beta".to_string());
        let conflict_err =
            StaticLoginCredential::from_options(&conflict, &required, now_ms, skew_ms)
                .expect_err("conflicting tokens must fail");
        let conflict_msg = conflict_err.to_string();
        assert!(!conflict_msg.contains("secret-alpha"));
        assert!(!conflict_msg.contains("secret-beta"));

        for bad_expiry in [
            "",
            "01700000000000",
            "+1700000000000",
            "-1700000000000",
            " 1700000000000",
            "1700.5",
            "1e12",
            "18446744073709551616",
        ] {
            let mut opts = HashMap::new();
            opts.insert("access_token".to_string(), "valid-tok".to_string());
            opts.insert("expires_at_unix_ms".to_string(), bad_expiry.to_string());
            opts.insert("scopes".to_string(), CLOUDSQL_LOGIN_SCOPE.to_string());
            assert!(
                matches!(
                    StaticLoginCredential::from_options(&opts, &required, now_ms, skew_ms),
                    Err(PostgresError::Auth(_))
                ),
                "bad expiry `{bad_expiry}` should be rejected"
            );
        }

        for bad_scope in ["", ADMIN_CLOUD_PLATFORM_SCOPE] {
            let mut opts = HashMap::new();
            opts.insert("access_token".to_string(), "valid-tok".to_string());
            opts.insert("expires_at_unix_ms".to_string(), "2000000".to_string());
            opts.insert("scopes".to_string(), bad_scope.to_string());
            assert!(matches!(
                StaticLoginCredential::from_options(&opts, &required, now_ms, skew_ms),
                Err(PostgresError::Auth(_))
            ));
        }

        let mut exact_boundary = HashMap::new();
        exact_boundary.insert("access_token".to_string(), "valid-tok".to_string());
        exact_boundary.insert("expires_at_unix_ms".to_string(), "1300000".to_string());
        exact_boundary.insert(
            "scopes".to_string(),
            format!("{ADMIN_CLOUD_PLATFORM_SCOPE}, {CLOUDSQL_LOGIN_SCOPE}"),
        );
        assert!(matches!(
            StaticLoginCredential::from_options(&exact_boundary, &required, now_ms, skew_ms),
            Err(PostgresError::Auth(_))
        ));

        let mut plus_one = exact_boundary.clone();
        plus_one.insert("expires_at_unix_ms".to_string(), "1300001".to_string());
        let cred = StaticLoginCredential::from_options(&plus_one, &required, now_ms, skew_ms)
            .expect("1 ms outside skew should succeed");
        assert!(cred.ensure_usable(1_000_000).is_ok());
        assert!(matches!(
            cred.ensure_usable(1_000_001),
            Err(PostgresError::Auth(_))
        ));
        assert!(matches!(
            cred.ensure_usable(u64::MAX),
            Err(PostgresError::Auth(_))
        ));

        let mut far_future = plus_one;
        far_future.insert(
            "expires_at_unix_ms".to_string(),
            "4102444800000".to_string(),
        );
        let far_cred =
            StaticLoginCredential::from_options(&far_future, &required, now_ms, skew_ms)
                .expect("far future static cred");
        let cache = far_cred.to_token_cache(ProxyPolicy::Direct);
        let snap1 = cache.get_or_refresh().await.expect("first refresh");
        let snap2 = cache.get_or_refresh().await.expect("second refresh");
        assert_eq!(snap1.expires_at_unix_ms, 4_102_444_800_000);
        assert_eq!(snap2.expires_at_unix_ms, 4_102_444_800_000);
    }
}
