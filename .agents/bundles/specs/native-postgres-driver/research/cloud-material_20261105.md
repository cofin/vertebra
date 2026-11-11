---
type: Research
flow_id: native-postgres-driver
title: "Native PostgreSQL cloud connection material and scoped authentication audit"
description: "Primary-source cloud material, mTLS identity, and scoped credential contracts for Cloud SQL and AlloyDB in native PostgreSQL."
status: stable
state: promoted
promoted_to: native-postgres-driver
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T04:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: rs-conn
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Cloud mTLS TransportStream and EndpointTarget in vertebra-db
  - id: rs-dialer
    resource: src/rs/vertebra-db/src/dialer.rs
    title: CloudSqlDialer and AlloyDbDialer URI and metadata framing
  - id: rs-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Core GCP single-flight token cache
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL cloud_sql_instance, alloydb_instance_uri, and IAM config
  - id: cloudsql-get
    resource: https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/connect/get
    title: Cloud SQL Admin API connectSettings endpoint
  - id: cloudsql-ephemeral
    resource: https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/connect/generateEphemeralCert
    title: Cloud SQL Admin API generateEphemeralCert endpoint
  - id: alloydb-conninfo
    resource: https://docs.cloud.google.com/alloydb/docs/reference/rest/v1/projects.locations.clusters.instances/getConnectionInfo
    title: AlloyDB connectionInfo REST endpoint
  - id: alloydb-proto
    resource: https://raw.githubusercontent.com/googleapis/googleapis/master/google/cloud/alloydb/v1alpha/service.proto
    title: AlloyDB v1alpha service and certificate issuance protobuf
tags:
  - postgres
  - gcp
  - cloudsql
  - alloydb
  - mtls
  - iam
---

# Native PostgreSQL cloud connection material audit

<!-- truth: start -->
- **Control-Plane Material Lifecycle**: Cloud SQL (`connectSettings` + `generateEphemeralCert` with RSA-2048 public key and `sqlservice.login` scope) [^cloudsql-get] [^cloudsql-ephemeral] and AlloyDB (`v1alpha` `connectionInfo` + `generateClientCertificate` + post-TLS1.3 `MetadataExchangeRequest` with `alloydb.login` scope) [^alloydb-conninfo] [^alloydb-proto] fetch, validate, and renew certificates natively.
- **Strict TLS 1.3 Peer Identity & Scoped Tokens**: Cloud mTLS enforces TLS 1.3 plus SAN/CN peer verification (`connection.rs` / `dialer.rs`) [^rs-conn] [^rs-dialer] and separates admin vs. login scopes across owned `ScopedTokenProvider` caches without cross-`cdylib` `Arc` extraction [^rs-gcp-auth] [^pg-config].
<!-- truth: end -->

URI + IAM connections require adapter-owned control-plane fetching and renewal. Existing native transport consumes prepared material. It does not discover endpoints or obtain issued client certificates. Required cloud functionality belongs in the complete native-postgres-driver PR; prepared-material injection is an internal seam and test facility, not the end-user completion boundary.

No credentials were read, no Google Cloud resources were accessed, and no source or .agents files were changed during this audit. Public documentation and public upstream source were read.

## Existing source contracts

- src/rs/vertebra-db/src/connection.rs::EndpointTarget::{CloudSql,AlloyDb} requires a resolved SocketAddr, server CA PEM, signed client certificate PEM, and private key PEM. AlloyDb also requires user_agent, oauth2_token, auth_type.
- connection.rs::TransportStream::connect_inner immediately performs cloud mTLS. AlloyDB then invokes exchange_alloydb_metadata_over_stream. The cloud proxy stream is already TLS before PostgreSQL startup; do not issue a PostgreSQL SSLRequest or nest another PostgreSQL TLS session. Direct ordinary PostgreSQL TCP still requires its own SSLRequest negotiation.
- src/rs/vertebra-db/src/dialer.rs::{CloudSqlDialer,AlloyDbDialer,CloudSqlInstanceUri,AlloyDbClusterUri} provides parsing, URL paths, metadata framing, and key helper only. No native Admin API fetching, discovery, material expiry cache, or automatic renewal exists.
- dialer.rs::generate_ephemeral_keypair_pem produces a self-signed CERTIFICATE and private key, not a public key and private key. Its first result cannot be sent as public_key/publicKey. src/rs/vertebra-db/Cargo.toml already depends on rsa 0.9 with sha2. Implement real RSA public/private key generation. Official connectors generate RSA2048 and emit PKCS1 RSA PUBLIC KEY / RSA PRIVATE KEY PEM; this is an accepted implementation convention, not an API minimum established by this audit.
- src/py/vertebra/db/dialer/cloudsql.py::_execute_ephemeral_cert_http expects serverCaCert and ipAddress beside ephemeralCert; Google returns those from a separate metadata request. It defaults endpoint to localhost and expiry to time.time()+3600. refresh_certificate stores a bundle but connect does not automatically discover/refresh it. The fallback key generator can return placeholder PEM when dependencies are unavailable; do not reuse that fallback.
- src/py/vertebra/db/dialer/alloydb.py::_execute_client_cert_http sends pem_csr/access_token and expects clientCert. Those are not the current issuance API fields. It fabricates localhost and one-hour expiry. connect requires a supplied host/context and does not automatically obtain the signed bundle.
- src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig has cloud_sql_instance, cloud_sql_ip_type, alloydb_instance_uri, alloydb_ip_type, enable_iam_auth, iam_auth_options. to_cloud_dialer_config currently drops both IP preferences; preserve them in the native handoff. Engine-owned configuration selects admin versus login scopes; generic shared auth owns scope-aware acquisition.

## Cloud SQL control-plane contract

1. GET https://sqladmin.googleapis.com/sql/v1beta4/projects/{project}/instances/{instance}/connectSettings using an admin bearer token.
2. Parse serverCaCert.cert; ipAddresses entries contain type and ipAddress. PRIMARY is the public-address type, PRIVATE selects the private address. Validate region against the connection name and databaseVersion as PostgreSQL. PSC needs pscEnabled plus dnsName/dnsNames and DNS resolution; never substitute localhost or another IP preference when the requested mode is missing.
3. POST https://sqladmin.googleapis.com/sql/v1beta4/projects/{project}/instances/{instance}:generateEphemeralCert with JSON public_key containing the real PEM public key. Include access_token only for IAM certificate binding, using the separately scoped login token. Authorization header uses the admin token.
4. Response contains ephemeralCert as SslCert, with cert and certificate expiration metadata. It does not also provide the CA/IP. Parse signed leaf NotAfter and validate the private key matches it. Effective IAM material expiry is min(client certificate NotAfter, embedded IAM login-token expiry). Do not assume one hour from local clock.
5. Cloud SQL admin permits cloud-platform, cloudsql, or sqlservice.admin scopes. Login token scope is https://www.googleapis.com/auth/sqlservice.login; maintain separate token caches by requested scope and identity.

Official sources:

- [connect.get](https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/connect/get)
- [ConnectSettings](https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/ConnectSettings)
- [connect.generateEphemeralCert](https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/connect/generateEphemeralCert)
- [SslCert](https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/sslCerts)
- [Google-maintained connector metadata/issuance/expiry implementation](https://raw.githubusercontent.com/GoogleCloudPlatform/cloud-sql-python-connector/main/google/cloud/sql/connector/client.py)
- [Google-maintained login downscoping implementation](https://raw.githubusercontent.com/GoogleCloudPlatform/cloud-sql-python-connector/main/google/cloud/sql/connector/refresh_utils.py)

## AlloyDB control-plane contract

1. GET https://alloydb.googleapis.com/v1/{instance-resource}/connectionInfo using an admin bearer token. instance-resource is projects/{project}/locations/{location}/clusters/{cluster}/instances/{instance}. Response contains private ipAddress, optional publicIpAddress, instanceUid. Choose only the requested connectivity mode.
2. POST https://alloydb.googleapis.com/v1/{cluster-resource}:generateClientCertificate. cluster-resource excludes the instances suffix. Request JSON uses publicKey, certDuration:"3600s", useMetadataExchange:true. requestId may be a UUID. No pem_csr or login access_token is sent in this issuance body.
3. Response fields are pemCertificateChain (array of PEM strings) and caCert. Join the chain in supplied order, validate matching private key, parse client leaf NotAfter. certDuration is a hint; accepted request range is 1–24 hours, and the service need not honor the hint. No separate clientCert/pemCertificate or fixed expiry field should be invented.
4. The v1 method remains defined in Google's authoritative service.proto, although the public REST page for generateClientCertificate was inaccessible and current discovery output omitted it during research. The source describes it as intended for connectors/Auth Proxy and subject to change. Implement fixture tests against the explicit schema/version and do not claim an unrelated stable public API guarantee.
5. The maintained Go connector uses v1alpha. Its connection information includes pscDnsName/pscAutoDnsName. Existing Vertebra URL helper uses v1beta. For full PUBLIC/PRIVATE/PSC scope, choose the explicit coherent connector API version with those endpoint fields and test exact URLs; using v1alpha for both calls matches the maintained reference. Do not advertise PSC while implementing only the v1 public/private metadata subset.
6. Admin scope is cloud-platform. Login scope is https://www.googleapis.com/auth/alloydb.login. After TLS1.3, send length-prefixed MetadataExchangeRequest with user_agent, auth_type 1=DB_NATIVE or 2=AUTO_IAM, oauth2_token. The official connector fetches the login-scope token even for DB_NATIVE metadata mode. Parse the response before PostgreSQL startup. Certificate lifetime and per-connection login-token lifetime remain distinct; refresh token before each new metadata exchange when necessary.

Official sources:

- [v1 connectionInfo](https://docs.cloud.google.com/alloydb/docs/reference/rest/v1/projects.locations.clusters.instances/getConnectionInfo)
- [Authoritative v1 service and issuance messages](https://raw.githubusercontent.com/googleapis/googleapis/master/google/cloud/alloydb/v1/service.proto)
- [Authoritative v1alpha service](https://raw.githubusercontent.com/googleapis/googleapis/master/google/cloud/alloydb/v1alpha/service.proto)
- [v1alpha discovery schema including PSC fields](https://raw.githubusercontent.com/googleapis/google-api-go-client/main/alloydb/v1alpha/alloydb-api.json)
- [Maintained endpoint discovery, real key format, chain and expiry handling](https://raw.githubusercontent.com/GoogleCloudPlatform/alloydb-go-connector/main/internal/alloydb/refresh.go)
- [Maintained TLS and metadata exchange](https://raw.githubusercontent.com/GoogleCloudPlatform/alloydb-go-connector/main/dialer.go)
- [Maintained separate admin/login scopes](https://raw.githubusercontent.com/GoogleCloudPlatform/alloydb-go-connector/main/options.go)
- [Connector TLS1.3/network-path overview](https://docs.cloud.google.com/alloydb/docs/language-connectors-overview)

## Native token cache and HTTP reuse

src/rs/vertebra-py/src/gcp_auth.rs::{PySyncGcpTokenCache,PyAsyncGcpTokenCache}::inner returns &Arc<vertebra_core::gcp_auth::GcpTokenCache>. Within that same extension only, clone the Arc before detached sync execution or native async spawn. The PostgreSQL extension never extracts or transfers these wrappers/Arcs across cdylibs; it constructs its own ScopedTokenProvider from owned primitive option maps as specified in the driver contracts. Both existing wrappers use the same async native cache: sync get_token detaches around block_on_db; async get_token returns RustDbTaskAwaitable through spawn_rust_db_task.

src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache semantics:

- get_cached(now) accepts a snapshot only when now+refresh_skew_ms < expires_at_unix_ms; default skew is 300000ms.
- get_or_refresh_with_force(false) checks the cache, acquires async refresh_lock, checks again, obtains a source token, increments generation, and publishes an Arc snapshot. Source acquisition is single-flight; force=true bypasses both cache checks.
- Snapshot fields: access_token, expires_at_unix_ms, generation, quota_project_id.
- invalidate(failed_generation) clears only a matching snapshot, preserving newer concurrent generations.
- GcpAuthSource and cache configuration are fixed after construction. Existing from_options defaults apply to service-account/impersonation; metadata currently uses only explicit custom scopes. AuthorizedUser and GcloudCli currently carry no requested scopes.
- Static tokens default to an extremely distant expiry when omitted. That cannot establish actual expiry or scope for the new strict cloud-login provider contract. Do not turn a scope request into a claim about unknown token scope.

src/rs/vertebra-bigquery/src/client.rs::{send_rest_json,dispatch_http_once} is a private reusable pattern, not a public generic API: reqwest, bearer and quota headers, generation-aware invalidate and one 401 retry. src/rs/vertebra-db/src/gcp.rs::GcpTokenCredentials offers public credentials for official SDKs under optional gcp. Token HTTP clients are private. Reuse generic reqwest/auth pieces without making postgres depend on unrelated BigQuery/Spanner engines. Preserve existing proxy policy and direct metadata-server routing, timeouts, quota headers, and sanitized diagnostics.

## Final source-specific scope policy

Add an opt-in shared native scope-aware provider path. Preserve old behavior for unrelated GCP adapters using the existing unscoped path. The engine requests cloud-platform/admin scopes versus sqlservice.login/alloydb.login; credentials transport belongs to shared auth.

| Source | Required behavior for an explicit scope request |
| --- | --- |
| Service account | Include exact requested scope list in JWT scope claim; exchange normally; retain actual expiry. |
| Metadata | Include explicit requested scopes in metadata token request, even when engine chose them as defaults. Preserve no-proxy metadata behavior. Respect server rejection for grants the VM does not have. |
| Impersonation | Send exact requested list in IAM generateAccessToken scope field; keep base-source/admin credential acquisition separate. |
| Authorized user | Store requested scopes and add form scope as space-separated values to OAuth refresh grant. This is supported by Google's own auth client. Refresh token must already authorize them; rejection is canonical authentication error, not fabricated success. If response declares granted scope and omits a required login scope, reject that result for the strict login path. |
| Gcloud ADC CLI | Execute gcloud auth application-default print-access-token --scopes=<comma-separated-exact-scopes>. Public CLI supports this for user/service accounts. For end-user ADC, sqlservice.login is in the documented default permitted set; alloydb.login requires earlier consent via application-default login --scopes. A service refusal is an auth/config error. |
| Gcloud ordinary fallback | gcloud auth print-access-token exposes no --scopes. Never invoke this fallback to satisfy an explicit scoped request. Unsupported scope-capability/old CLI becomes canonical Unsupported with explicit scoped-token-provider injection supported in native cloud configuration. Keep old fallback behavior only for existing unscoped users. |
| Explicit external/scoped provider | Provider receives requested scopes, returns token plus real expiry and scope contract. Unknown scope/client mode fails canonical Unsupported/Auth rather than relabeling a broad token as narrow or manufacturing a token. |
| Static token | Require an explicit scoped-provider/static-scope contract for strict cloud login. Configured scope labels alone are caller assertions, not proof that Google granted them; service rejection remains authoritative. No fake expiration. |

Google's official OAuth client refresh_grant adds scope to the refresh form. Requesting scopes cannot grant permissions that the user's existing consent does not contain. The implementation promises accurate scope requests and canonical failures; it cannot promise Google grants unavailable scopes.

Primary verification:

- [ADC print-access-token --scopes and end-user restrictions](https://docs.cloud.google.com/sdk/gcloud/reference/auth/application-default/print-access-token)
- [Ordinary print-access-token, without --scopes](https://docs.cloud.google.com/sdk/gcloud/reference/auth/print-access-token)
- [Google auth refresh_grant scope serialization](https://raw.githubusercontent.com/googleapis/google-auth-library-python/main/google/oauth2/_client.py)
- [Google auth authorized-user requested/granted scope handling](https://raw.githubusercontent.com/googleapis/google-auth-library-python/main/google/oauth2/credentials.py)

Tests must inspect exact requested scopes at every provider call/HTTP form/JWT/CLI argument and reject missing-grant responses. CLI tests use a fake executable only; no installed CLI, user ADC, token files, or live API access is needed.

## Cloud identity and TLS policy

Current connection.rs::build_cloud_mtls_config uses safe default versions (permits TLS1.2). CaAndOptionalCnVerifier ignores NotValidForName errors; CloudSQL supplies Some(project:instance), AlloyDb passes None and discards cluster. AlloyDB therefore validates CA without endpoint identity. Existing prepared material does not fix that security gap.

Implement explicit cloud-owned verification policies, leaving ordinary PostgreSQL TlsMode behavior intact:

- Cloud SQL: validate supplied CA chain, signature, validity; validate configured or authenticated metadata DNS SAN, including trailing-dot normalization; fall back to exact nonempty Subject CN project:instance; reject if both identity checks fail. Current maintained Google verifier uses SAN then CN fallback, not solely a serverCaMode switch. Metadata needs DNS identity independently of the resolved socket IP.
- AlloyDB: validate chain/signature/validity and selected actual endpoint IP SAN or PSC DNS SAN. Preserve original DNS identity across resolution. Never ignore the name mismatch. Cluster CA alone does not identify the selected instance.
- Both cloud modes require TLS1.3. Never expose tokens through plaintext metadata exchange or downgrade through ordinary sslmode options.
- Match client certificate and private key before publishing a material generation. Reject empty/malformed CA/chain, missing endpoint, mismatched URI region/resource, expired leaf, untrusted chain, wrong SAN/CN. Keep private keys/tokens out of Debug and HTTP diagnostic bodies.

Primary reference algorithms:

- [Cloud SQL chain, SAN, exact CN fallback](https://raw.githubusercontent.com/GoogleCloudPlatform/cloud-sql-go-connector/main/internal/cloudsql/tls_verify.go)
- [Cloud SQL TLS1.3 and metadata DNS](https://raw.githubusercontent.com/GoogleCloudPlatform/cloud-sql-go-connector/main/internal/cloudsql/instance.go)
- [AlloyDB selected IP/DNS SAN and TLS1.3](https://raw.githubusercontent.com/GoogleCloudPlatform/alloydb-go-connector/main/dialer.go)

## Bounded implementation tasks and review evidence

The implementation worksheets assign bounded ownership across these seams:

1. Shared scope-aware token provider extension: source-specific exact scopes, typed Unsupported/Auth errors, explicit provider injection; preserve unrelated adapter behavior.
2. Cloud config, typed PreparedCloudMaterial, deadline-aware authenticated HTTP requests, quota/proxy behavior, bounded retry, cache ownership/generation/single-flight. Cache keys include resource, IP type, API endpoint, auth mode and credential-provider identity; never coalesce different users/scopes by URI alone.
3. Cloud SQL Admin API metadata and real-key certificate material fetch/validation/expiry.
4. AlloyDB coherent connector API metadata and real-key certificate material fetch/validation/expiry.
5. Explicit cloud verification policies and TLS1.3; adapt EndpointTarget to preserve TLS DNS identity separately from socket address.
6. Connect/reconnect and pool integration: automatically obtain fresh material, refresh before new physical connections, retain established socket ownership, bounded failed-renewal policy, close/cancellation cleanup. Prepared-material-only tests are not completion evidence.

Use deterministic local mock Admin APIs and mTLS peers to verify fields, scoped tokens, selected IP/PSC DNS, certificate issuance responses, unknown/malformed data, exact deadline, 401 generation invalidation, concurrent single-flight, real expiry, failed refresh, TLS1.2 rejection, wrong CA/SAN/CN, metadata failure, cancellation and no token leakage. Keep live cloud checks explicitly separate and optional where credentials/infrastructure are absent. The complete PR still needs automatic URI+IAM functionality; no silent limited-ADC fallback or caller-supplied-material requirement substitutes for it.

[^rs-conn]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Cloud mTLS TransportStream and EndpointTarget in vertebra-db.
[^rs-dialer]: `src/rs/vertebra-db/src/dialer.rs` — CloudSqlDialer and AlloyDbDialer URI and metadata framing.
[^rs-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Core GCP single-flight token cache.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL cloud_sql_instance, alloydb_instance_uri, and IAM config.
[^cloudsql-get]: Cloud SQL Admin API connectSettings endpoint (https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/connect/get).
[^cloudsql-ephemeral]: Cloud SQL Admin API generateEphemeralCert endpoint (https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/connect/generateEphemeralCert).
[^alloydb-conninfo]: AlloyDB connectionInfo REST endpoint (https://docs.cloud.google.com/alloydb/docs/reference/rest/v1/projects.locations.clusters.instances/getConnectionInfo).
[^alloydb-proto]: AlloyDB v1alpha service and certificate issuance protobuf (https://raw.githubusercontent.com/googleapis/googleapis/master/google/cloud/alloydb/v1alpha/service.proto).
