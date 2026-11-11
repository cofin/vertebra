//! Off-GIL PKCS#1 RSA-2048 ephemeral keypair generation and issued X.509 certificate chain validation.

use std::fmt;
use std::io::BufReader;

use rsa::pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey, LineEnding};
use rsa::pkcs8::DecodePublicKey;
use rsa::rand_core::OsRng;
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use tokio::time::Instant;
use x509_parser::parse_x509_certificate;

use crate::errors::PostgresError;

/// Ephemeral 2048-bit PKCS#1 RSA keypair generated for Cloud SQL (`:3307`) or AlloyDB (`:5433`) mTLS.
#[derive(Clone, PartialEq, Eq)]
pub struct EphemeralRsaKeyPair {
    public_key_pem: String,
    private_key_pem: Vec<u8>,
    public_key: RsaPublicKey,
}

impl EphemeralRsaKeyPair {
    /// Borrow the PKCS#1 `RSA PUBLIC KEY` PEM string (LF line endings).
    pub fn public_key_pem(&self) -> &str {
        &self.public_key_pem
    }

    /// Borrow the PKCS#1 `RSA PRIVATE KEY` PEM bytes (LF line endings).
    pub fn private_key_pem(&self) -> &[u8] {
        &self.private_key_pem
    }

    /// Borrow the parsed [`RsaPublicKey`].
    pub const fn public_key(&self) -> &RsaPublicKey {
        &self.public_key
    }
}

impl fmt::Debug for EphemeralRsaKeyPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EphemeralRsaKeyPair")
            .field("public_key_pem", &self.public_key_pem)
            .field("private_key_pem", &"<redacted>")
            .finish()
    }
}

/// Validated leaf-first client certificate chain and matching private key with authoritative `NotAfter`.
#[derive(Clone, PartialEq, Eq)]
pub struct ValidatedClientIdentity {
    pub cert_chain_pem: Vec<u8>,
    pub private_key_pem: Vec<u8>,
    pub signed_not_after_ms: u64,
}

impl fmt::Debug for ValidatedClientIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidatedClientIdentity")
            .field("cert_chain_pem_len", &self.cert_chain_pem.len())
            .field("private_key_pem", &"<redacted>")
            .field("signed_not_after_ms", &self.signed_not_after_ms)
            .finish()
    }
}

/// Generate a 2048-bit RSA keypair on `tokio::task::spawn_blocking` bounded by `deadline`.
pub async fn generate_ephemeral_rsa_keypair(
    deadline: Instant,
) -> Result<EphemeralRsaKeyPair, PostgresError> {
    if Instant::now() >= deadline {
        return Err(PostgresError::Transport(
            "connection deadline exceeded before ephemeral RSA key generation".to_string(),
        ));
    }

    let handle = tokio::task::spawn_blocking(generate_rsa_keypair_blocking);

    tokio::time::timeout_at(deadline, handle)
        .await
        .map_err(|_| {
            PostgresError::Transport("ephemeral RSA key generation timed out".to_string())
        })?
        .map_err(|e| {
            PostgresError::Transport(format!("ephemeral RSA key generation task panicked: {e}"))
        })?
}

fn encode_rsa_keypair(priv_key: RsaPrivateKey) -> Result<EphemeralRsaKeyPair, PostgresError> {
    let pub_key = RsaPublicKey::from(&priv_key);
    let public_key_pem = pub_key.to_pkcs1_pem(LineEnding::LF).map_err(|e| {
        PostgresError::Transport(format!("failed to encode PKCS#1 RSA public key PEM: {e}"))
    })?;
    let priv_pem_zeroizing = priv_key.to_pkcs1_pem(LineEnding::LF).map_err(|e| {
        PostgresError::Transport(format!("failed to encode PKCS#1 RSA private key PEM: {e}"))
    })?;
    Ok(EphemeralRsaKeyPair {
        public_key_pem,
        private_key_pem: priv_pem_zeroizing.as_bytes().to_vec(),
        public_key: pub_key,
    })
}

fn generate_rsa_keypair_blocking() -> Result<EphemeralRsaKeyPair, PostgresError> {
    let priv_key = RsaPrivateKey::new(&mut OsRng, 2048).map_err(|e| {
        PostgresError::Transport(format!("failed to generate 2048-bit RSA keypair: {e}"))
    })?;
    encode_rsa_keypair(priv_key)
}

/// Validate an Admin-issued X.509 certificate chain against `expected_key` and `now_unix_ms`.
///
/// Enforces:
/// 1. Non-empty `chain_pems`, each containing exactly one valid X.509 `CERTIFICATE` PEM block with
///    zero trailing DER bytes and zero unparsed trailing PEM data.
/// 2. `chain_pems[0]` (leaf) SubjectPublicKeyInfo decodes as an RSA public key whose `(n, e)`
///    matches `expected_key.public_key()`.
/// 3. `chain_pems[0]` signed `NotAfter` is strictly greater than `now_unix_ms`.
/// 4. Returns leaf-first normalized LF PEM chain bytes.
pub fn validate_issued_certificate_chain(
    chain_pems: &[String],
    expected_key: &EphemeralRsaKeyPair,
    now_unix_ms: u64,
) -> Result<ValidatedClientIdentity, PostgresError> {
    if chain_pems.is_empty() {
        return Err(PostgresError::Protocol(
            "issued client certificate chain is empty".to_string(),
        ));
    }

    let mut normalized_chain = Vec::new();
    let mut leaf_not_after_ms: Option<u64> = None;

    for (idx, raw_pem) in chain_pems.iter().enumerate() {
        let trimmed = raw_pem.trim();
        if !trimmed.starts_with("-----BEGIN CERTIFICATE-----")
            || !trimmed.ends_with("-----END CERTIFICATE-----")
        {
            return Err(PostgresError::Protocol(format!(
                "certificate chain entry {idx} is not a single CERTIFICATE PEM block"
            )));
        }

        let mut reader = BufReader::new(trimmed.as_bytes());
        let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                PostgresError::Protocol(format!(
                    "failed to decode CERTIFICATE PEM at index {idx}: {e}"
                ))
            })?;
        if certs.len() != 1 {
            return Err(PostgresError::Protocol(format!(
                "expected exactly 1 CERTIFICATE in PEM entry {idx}, found {}",
                certs.len()
            )));
        }

        let der = certs[0].as_ref();
        let (rem, x509) = parse_x509_certificate(der).map_err(|e| {
            PostgresError::Protocol(format!(
                "failed to parse X.509 DER in certificate chain entry {idx}: {e}"
            ))
        })?;
        if !rem.is_empty() {
            return Err(PostgresError::Protocol(format!(
                "certificate chain entry {idx} contains {} trailing DER bytes",
                rem.len()
            )));
        }

        if idx == 0 {
            let spki_der = x509.public_key().raw;
            let cert_rsa_pub = RsaPublicKey::from_public_key_der(spki_der).map_err(|e| {
                PostgresError::Protocol(format!(
                    "issued leaf certificate SPKI is not a valid RSA public key: {e}"
                ))
            })?;
            if cert_rsa_pub.n() != expected_key.public_key().n()
                || cert_rsa_pub.e() != expected_key.public_key().e()
            {
                return Err(PostgresError::Protocol(
                    "issued leaf certificate RSA public key (n, e) does not match generated keypair"
                        .to_string(),
                ));
            }

            let not_after_secs = x509.validity().not_after.timestamp();
            if not_after_secs <= 0 {
                return Err(PostgresError::Protocol(format!(
                    "issued leaf certificate has non-positive NotAfter timestamp {not_after_secs}"
                )));
            }
            let not_after_ms = (not_after_secs as u64).saturating_mul(1_000);
            if not_after_ms <= now_unix_ms {
                return Err(PostgresError::Transport(format!(
                    "issued leaf certificate is already expired (NotAfter={not_after_ms}ms <= now={now_unix_ms}ms)"
                )));
            }
            leaf_not_after_ms = Some(not_after_ms);
        }

        let lf_normalized = trimmed.replace("\r\n", "\n");
        normalized_chain.extend_from_slice(lf_normalized.as_bytes());
        normalized_chain.push(b'\n');
    }

    Ok(ValidatedClientIdentity {
        cert_chain_pem: normalized_chain,
        private_key_pem: expected_key.private_key_pem.clone(),
        signed_not_after_ms: leaf_not_after_ms.expect("non-empty chain checked above"),
    })
}
