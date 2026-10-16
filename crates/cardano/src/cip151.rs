use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::{primitives::Metadatum, traverse::MultiEraTx},
};
use thiserror::Error;

pub const CIP151_METADATA_LABEL: u64 = 867;

const POOL_SCOPE_KIND: u64 = 1;
const CALIDUS_KEY_HASH_HEADER: u8 = 0xa1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cip151PoolRegistration {
    pub version: u64,
    pub pool_id: Hash<28>,
    pub validation_method: u64,
    pub nonce: u64,
    pub calidus_pub_key: [u8; 32],
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Cip151Error {
    #[error("missing field {0}")]
    MissingField(&'static str),

    #[error("{field} has invalid type, expected {expected}")]
    InvalidType {
        field: &'static str,
        expected: &'static str,
    },

    #[error("unsupported registration version {0}")]
    UnsupportedVersion(u64),

    #[error("unsupported scope kind {0}")]
    UnsupportedScopeKind(u64),

    #[error("invalid pool id length {0}")]
    InvalidPoolIdLength(usize),

    #[error("invalid validation method {0}")]
    InvalidValidationMethod(u64),

    #[error("invalid calidus public key length {0}")]
    InvalidCalidusKeyLength(usize),
}

pub fn cip151_metadata_for_tx(tx: &MultiEraTx) -> Option<Metadatum> {
    tx.metadata().find(CIP151_METADATA_LABEL).cloned()
}

pub fn parse_cip151_pool_registration(
    metadata: &Metadatum,
) -> Result<Cip151PoolRegistration, Cip151Error> {
    let registration = as_map(metadata, "registration")?;

    let version = lookup(registration, 0)
        .ok_or(Cip151Error::MissingField("version"))
        .and_then(|x| as_uint(x, "version"))?;

    if version != 2 {
        return Err(Cip151Error::UnsupportedVersion(version));
    }

    let payload = lookup(registration, 1)
        .ok_or(Cip151Error::MissingField("payload"))
        .and_then(|x| as_map(x, "payload"))?;

    let scope = lookup(payload, 1)
        .ok_or(Cip151Error::MissingField("scope"))
        .and_then(|x| as_array(x, "scope"))?;

    let scope_kind = scope
        .first()
        .ok_or(Cip151Error::MissingField("scope[0]"))
        .and_then(|x| as_uint(x, "scope[0]"))?;

    if scope_kind != POOL_SCOPE_KIND {
        return Err(Cip151Error::UnsupportedScopeKind(scope_kind));
    }

    let pool_id = scope
        .get(1)
        .ok_or(Cip151Error::MissingField("scope[1]"))
        .and_then(|x| as_bytes(x, "scope[1]"))?;
    let pool_id: [u8; 28] = pool_id
        .try_into()
        .map_err(|_| Cip151Error::InvalidPoolIdLength(pool_id.len()))?;

    let feature_set = lookup(payload, 2)
        .ok_or(Cip151Error::MissingField("feature_set"))
        .and_then(|x| as_array(x, "feature_set"))?;
    for item in feature_set {
        let _ = as_uint(item, "feature_set[]")?;
    }

    let validation = lookup(payload, 3)
        .ok_or(Cip151Error::MissingField("validation_method"))
        .and_then(|x| as_array(x, "validation_method"))?;
    let validation_method = validation
        .first()
        .ok_or(Cip151Error::MissingField("validation_method[0]"))
        .and_then(|x| as_uint(x, "validation_method[0]"))?;

    if !matches!(validation_method, 0..=2) {
        return Err(Cip151Error::InvalidValidationMethod(validation_method));
    }

    let nonce = lookup(payload, 4)
        .ok_or(Cip151Error::MissingField("nonce"))
        .and_then(|x| as_uint(x, "nonce"))?;

    let calidus_pub_key = lookup(payload, 7)
        .ok_or(Cip151Error::MissingField("calidus_key"))
        .and_then(|x| as_bytes(x, "calidus_key"))?;
    let calidus_pub_key: [u8; 32] = calidus_pub_key
        .try_into()
        .map_err(|_| Cip151Error::InvalidCalidusKeyLength(calidus_pub_key.len()))?;

    Ok(Cip151PoolRegistration {
        version,
        pool_id: Hash::from(pool_id),
        validation_method,
        nonce,
        calidus_pub_key,
    })
}

pub fn calidus_key_is_revoked(pub_key: &[u8; 32]) -> bool {
    pub_key.iter().all(|byte| *byte == 0)
}

pub fn calidus_key_id_bytes(pub_key: &[u8; 32]) -> [u8; 29] {
    let hash: Hash<28> = Hasher::<224>::hash(pub_key.as_slice());
    let mut out = [0u8; 29];
    out[0] = CALIDUS_KEY_HASH_HEADER;
    out[1..].copy_from_slice(hash.as_slice());
    out
}

fn lookup(items: &[(Metadatum, Metadatum)], key: i128) -> Option<&Metadatum> {
    items.iter().find_map(|(k, v)| match k {
        Metadatum::Int(value) if i128::from(*value) == key => Some(v),
        _ => None,
    })
}

fn as_map<'a>(
    value: &'a Metadatum,
    field: &'static str,
) -> Result<&'a [(Metadatum, Metadatum)], Cip151Error> {
    match value {
        Metadatum::Map(items) => Ok(items.as_slice()),
        _ => Err(Cip151Error::InvalidType {
            field,
            expected: "map",
        }),
    }
}

fn as_array<'a>(value: &'a Metadatum, field: &'static str) -> Result<&'a [Metadatum], Cip151Error> {
    match value {
        Metadatum::Array(items) => Ok(items.as_slice()),
        _ => Err(Cip151Error::InvalidType {
            field,
            expected: "array",
        }),
    }
}

fn as_uint(value: &Metadatum, field: &'static str) -> Result<u64, Cip151Error> {
    match value {
        Metadatum::Int(value) => {
            let value = i128::from(*value);
            value.try_into().map_err(|_| Cip151Error::InvalidType {
                field,
                expected: "uint",
            })
        }
        _ => Err(Cip151Error::InvalidType {
            field,
            expected: "uint",
        }),
    }
}

fn as_bytes<'a>(value: &'a Metadatum, field: &'static str) -> Result<&'a [u8], Cip151Error> {
    match value {
        Metadatum::Bytes(bytes) => Ok(bytes.as_slice()),
        _ => Err(Cip151Error::InvalidType {
            field,
            expected: "bytes",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas::{codec::utils::Bytes, ledger::primitives::Int};

    fn md_int(value: i64) -> Metadatum {
        Metadatum::Int(Int::from(value))
    }

    fn md_map(items: Vec<(Metadatum, Metadatum)>) -> Metadatum {
        Metadatum::Map(items.into())
    }

    fn valid_metadata(calidus_key: [u8; 32]) -> Metadatum {
        md_map(vec![
            (md_int(0), md_int(2)),
            (
                md_int(1),
                md_map(vec![
                    (
                        md_int(1),
                        Metadatum::Array(vec![
                            md_int(1),
                            Metadatum::Bytes(Bytes::from(vec![0x11; 28])),
                        ]),
                    ),
                    (md_int(2), Metadatum::Array(vec![])),
                    (md_int(3), Metadatum::Array(vec![md_int(2)])),
                    (md_int(4), md_int(12345)),
                    (
                        md_int(7),
                        Metadatum::Bytes(Bytes::from(calidus_key.to_vec())),
                    ),
                ]),
            ),
        ])
    }

    #[test]
    fn parses_valid_pool_registration() {
        let metadata = valid_metadata([0x57; 32]);
        let parsed = parse_cip151_pool_registration(&metadata).expect("valid registration");

        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.pool_id, Hash::from([0x11; 28]));
        assert_eq!(parsed.validation_method, 2);
        assert_eq!(parsed.nonce, 12345);
        assert_eq!(parsed.calidus_pub_key, [0x57; 32]);
    }

    #[test]
    fn rejects_wrong_version() {
        let metadata = md_map(vec![
            (md_int(0), md_int(1)),
            (
                md_int(1),
                md_map(vec![
                    (
                        md_int(1),
                        Metadatum::Array(vec![
                            md_int(1),
                            Metadatum::Bytes(Bytes::from(vec![0x11; 28])),
                        ]),
                    ),
                    (md_int(2), Metadatum::Array(vec![])),
                    (md_int(3), Metadatum::Array(vec![md_int(2)])),
                    (md_int(4), md_int(12345)),
                    (md_int(7), Metadatum::Bytes(Bytes::from(vec![0x57; 32]))),
                ]),
            ),
        ]);

        assert_eq!(
            parse_cip151_pool_registration(&metadata),
            Err(Cip151Error::UnsupportedVersion(1))
        );
    }

    #[test]
    fn rejects_wrong_scope() {
        let metadata = md_map(vec![
            (md_int(0), md_int(2)),
            (
                md_int(1),
                md_map(vec![
                    (
                        md_int(1),
                        Metadatum::Array(vec![
                            md_int(0),
                            Metadatum::Bytes(Bytes::from(vec![0x11; 28])),
                        ]),
                    ),
                    (md_int(2), Metadatum::Array(vec![])),
                    (md_int(3), Metadatum::Array(vec![md_int(2)])),
                    (md_int(4), md_int(12345)),
                    (md_int(7), Metadatum::Bytes(Bytes::from(vec![0x57; 32]))),
                ]),
            ),
        ]);

        assert_eq!(
            parse_cip151_pool_registration(&metadata),
            Err(Cip151Error::UnsupportedScopeKind(0))
        );
    }

    #[test]
    fn rejects_bad_calidus_key_length() {
        let metadata = md_map(vec![
            (md_int(0), md_int(2)),
            (
                md_int(1),
                md_map(vec![
                    (
                        md_int(1),
                        Metadatum::Array(vec![
                            md_int(1),
                            Metadatum::Bytes(Bytes::from(vec![0x11; 28])),
                        ]),
                    ),
                    (md_int(2), Metadatum::Array(vec![])),
                    (md_int(3), Metadatum::Array(vec![md_int(2)])),
                    (md_int(4), md_int(12345)),
                    (md_int(7), Metadatum::Bytes(Bytes::from(vec![0x57; 31]))),
                ]),
            ),
        ]);

        assert_eq!(
            parse_cip151_pool_registration(&metadata),
            Err(Cip151Error::InvalidCalidusKeyLength(31))
        );
    }

    #[test]
    fn calidus_key_id_matches_spec_example() {
        let pub_key =
            hex::decode("57758911253f6b31df2a87c10eb08a2c9b8450768cb8dd0d378d93f7c2e220f0")
                .expect("valid hex");
        let pub_key: [u8; 32] = pub_key.try_into().expect("32-byte key");

        assert_eq!(
            hex::encode(calidus_key_id_bytes(&pub_key)),
            "a1171983a1178a55b02afacfd6ad6b516da375469fd7dbcf54a2f95823"
        );
    }

    #[test]
    fn zero_key_is_revocation() {
        assert!(calidus_key_is_revoked(&[0u8; 32]));
        assert!(!calidus_key_is_revoked(&[1u8; 32]));
    }
}
