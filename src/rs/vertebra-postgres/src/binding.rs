//! RFC 5929 §4.1 `tls-server-end-point` channel-binding digest computation over X.509 leaf DER.

use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use x509_parser::parse_x509_certificate;
use x509_parser::signature_algorithm::SignatureAlgorithm;

use crate::errors::PostgresError;

/// Compute the RFC 5929 §4.1 `tls-server-end-point` channel-binding digest over a peer leaf DER certificate.
///
/// Maps MD5 and SHA-1 signature algorithms (including RSASSA-PSS with default or explicit SHA-1)
/// to SHA-256, preserves SHA-224/256/384/512, and returns [`PostgresError::UnsupportedAuth`] for
/// signature algorithms without an RFC 5929 hash (such as Ed25519 `1.3.101.112` and Ed448 `1.3.101.113`).
pub fn tls_server_end_point(der: &[u8]) -> Result<Vec<u8>, PostgresError> {
    let (rem, cert) = parse_x509_certificate(der).map_err(|e| {
        PostgresError::Transport(format!("failed to parse peer X.509 leaf certificate: {e}"))
    })?;
    if !rem.is_empty() {
        return Err(PostgresError::Transport(format!(
            "peer X.509 leaf certificate has {} trailing bytes",
            rem.len()
        )));
    }

    let oid = cert.signature_algorithm.algorithm.to_id_string();
    match oid.as_str() {
        // MD5 / SHA-1 signature OIDs -> RFC 5929 §4.1 substitutes SHA-256
        "1.2.840.113549.1.1.4"
        | "1.2.840.113549.1.1.5"
        | "1.2.840.10045.4.1"
        | "1.2.840.10040.4.3"
        // SHA-256 signature OIDs (RSA, ECDSA)
        | "1.2.840.113549.1.1.11"
        | "1.2.840.10045.4.3.2" => Ok(Sha256::digest(der).to_vec()),
        // SHA-224 signature OIDs (RSA, ECDSA)
        "1.2.840.113549.1.1.14" | "1.2.840.10045.4.3.1" => Ok(Sha224::digest(der).to_vec()),
        // SHA-384 signature OIDs (RSA, ECDSA)
        "1.2.840.113549.1.1.12" | "1.2.840.10045.4.3.3" => Ok(Sha384::digest(der).to_vec()),
        // SHA-512 signature OIDs (RSA, ECDSA)
        "1.2.840.113549.1.1.13" | "1.2.840.10045.4.3.4" => Ok(Sha512::digest(der).to_vec()),
        // RSASSA-PSS (`1.2.840.113549.1.1.10`) -> inspect RSASSA-PSS-params.hashAlgorithm
        "1.2.840.113549.1.1.10" => {
            if cert.signature_algorithm.parameters.is_none() {
                // RFC 4055 default hashAlgorithm is SHA-1 -> substitute SHA-256
                return Ok(Sha256::digest(der).to_vec());
            }
            match SignatureAlgorithm::try_from(&cert.signature_algorithm) {
                Ok(SignatureAlgorithm::RSASSA_PSS(params)) => {
                    let hash_oid = params.hash_algorithm_oid().to_id_string();
                    match hash_oid.as_str() {
                        // SHA-1 (`1.3.14.3.2.26`) or SHA-256 (`2.16.840.1.101.3.4.2.1`)
                        "1.3.14.3.2.26" | "2.16.840.1.101.3.4.2.1" => {
                            Ok(Sha256::digest(der).to_vec())
                        }
                        // SHA-224 (`2.16.840.1.101.3.4.2.4`)
                        "2.16.840.1.101.3.4.2.4" => Ok(Sha224::digest(der).to_vec()),
                        // SHA-384 (`2.16.840.1.101.3.4.2.2`)
                        "2.16.840.1.101.3.4.2.2" => Ok(Sha384::digest(der).to_vec()),
                        // SHA-512 (`2.16.840.1.101.3.4.2.3`)
                        "2.16.840.1.101.3.4.2.3" => Ok(Sha512::digest(der).to_vec()),
                        other => Err(PostgresError::UnsupportedAuth(format!(
                            "unsupported RSASSA-PSS hash algorithm OID {other} for tls-server-end-point"
                        ))),
                    }
                }
                Ok(_) => Err(PostgresError::Transport(
                    "unexpected non-PSS SignatureAlgorithm variant for OID 1.2.840.113549.1.1.10"
                        .to_string(),
                )),
                Err(err) => Err(PostgresError::Transport(format!(
                    "invalid RSASSA-PSS signature parameters: {err}"
                ))),
            }
        }
        other => Err(PostgresError::UnsupportedAuth(format!(
            "RFC 5929 §4.1 defines no tls-server-end-point hash for certificate signature OID {other}"
        ))),
    }
}
