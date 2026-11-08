//! RFC 5929 certificate channel binding for PostgreSQL SCRAM.

use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use x509_parser::{
    asn1_rs::{Any, Class, FromDer, Oid, Tag},
    signature_algorithm::SignatureAlgorithm,
};

use crate::errors::PostgresError;

/// Hash the entire leaf certificate with its signature digest under RFC 5929.
///
/// Unsupported algorithms make certificate binding unavailable. Invalid or
/// trailing certificate DER is a protocol error, independently of TLS trust.
pub fn tls_server_end_point(der: &[u8]) -> Result<Vec<u8>, PostgresError> {
    let (remaining, cert) = x509_parser::parse_x509_certificate(der)
        .map_err(|_| PostgresError::Protocol("invalid peer certificate DER".into()))?;
    if !remaining.is_empty() {
        return Err(PostgresError::Protocol(
            "trailing peer certificate DER".into(),
        ));
    }
    let unavailable =
        || PostgresError::UnsupportedAuth("peer signature has no supported endpoint digest".into());
    let algorithm = &cert.signature_algorithm;
    let bits = match algorithm.algorithm.to_id_string().as_str() {
        "1.2.840.113549.1.1.14" | "1.2.840.10045.4.3.1" | "2.16.840.1.101.3.4.3.1" => 224,
        "1.2.840.113549.1.1.11"
        | "1.2.840.10045.4.3.2"
        | "2.16.840.1.101.3.4.3.2"
        | "1.2.840.113549.1.1.5"
        | "1.2.840.10040.4.3"
        | "1.2.840.10045.4.1"
        | "1.2.840.113549.1.1.4" => 256,
        "1.2.840.113549.1.1.12" | "1.2.840.10045.4.3.3" => 384,
        "1.2.840.113549.1.1.13" | "1.2.840.10045.4.3.4" => 512,
        "1.2.840.113549.1.1.10" => {
            validate_pss(algorithm.parameters.as_ref().ok_or_else(unavailable)?)
                .ok_or_else(unavailable)?;
            let SignatureAlgorithm::RSASSA_PSS(params) =
                SignatureAlgorithm::try_from(algorithm).map_err(|_| unavailable())?
            else {
                return Err(unavailable());
            };
            match params.hash_algorithm_oid().to_id_string().as_str() {
                "1.3.14.3.2.26" | "2.16.840.1.101.3.4.2.1" => 256,
                "2.16.840.1.101.3.4.2.4" => 224,
                "2.16.840.1.101.3.4.2.2" => 384,
                "2.16.840.1.101.3.4.2.3" => 512,
                _ => return Err(unavailable()),
            }
        }
        _ => return Err(unavailable()),
    };
    Ok(match bits {
        224 => Sha224::digest(der).to_vec(),
        256 => Sha256::digest(der).to_vec(),
        384 => Sha384::digest(der).to_vec(),
        _ => Sha512::digest(der).to_vec(),
    })
}

fn sequence(value: &Any<'_>) -> Option<()> {
    (value.class() == Class::Universal
        && value.tag() == Tag::Sequence
        && value.header.is_constructed())
    .then_some(())
}

fn hash_identifier(encoded: &[u8], mgf: bool) -> Option<()> {
    let (remaining, value) = Any::from_der(encoded).ok()?;
    sequence(&value)?;
    if !remaining.is_empty() {
        return None;
    }
    let (parameters, oid) = Oid::from_der(value.data).ok()?;
    if mgf {
        if oid.to_id_string() != "1.2.840.113549.1.1.8" {
            return None;
        }
        hash_identifier(parameters, false)
    } else if parameters.is_empty() {
        Some(())
    } else {
        let (remaining, ()) = <()>::from_der(parameters).ok()?;
        remaining.is_empty().then_some(())
    }
}

fn validate_pss(parameters: &Any<'_>) -> Option<()> {
    sequence(parameters)?;
    let mut input = parameters.data;
    let mut previous = None;
    while !input.is_empty() {
        let (remaining, field) = Any::from_der(input).ok()?;
        let tag = field.tag().0;
        if field.class() != Class::ContextSpecific
            || !field.header.is_constructed()
            || tag > 3
            || previous.is_some_and(|previous| tag <= previous)
        {
            return None;
        }
        match tag {
            0 | 1 => hash_identifier(field.data, tag == 1)?,
            _ => {
                let (remaining, value) = u32::from_der(field.data).ok()?;
                if !remaining.is_empty() || (tag == 3 && value != 1) {
                    return None;
                }
            }
        }
        previous = Some(tag);
        input = remaining;
    }
    Some(())
}
