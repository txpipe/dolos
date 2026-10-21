use pallas::{
    codec::minicbor,
    crypto::{
        hash::{Hash, Hasher},
        key::ed25519::{PublicKey, Signature},
    },
    ledger::{primitives::Metadatum, traverse::MultiEraTx},
};
use thiserror::Error;

pub const CIP151_METADATA_LABEL: u64 = 867;

const POOL_SCOPE_KIND: u64 = 1;
const CALIDUS_KEY_HASH_HEADER: u8 = 0xa1;
const CIP8_VALIDATION_METHOD: u64 = 2;
const COSE_WITNESS_TYPE: u64 = 0;

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

/// Whether `metadata`, the label-867 entry `registration` was parsed from,
/// carries a CIP-8 (validation method 2) signature by the pool's cold key.
///
/// It follows the check Blockfrost and Koios run (pg_cardano's
/// `tools_verify_cip88_pool_key_registration`): a COSE_Sign1 over the
/// blake2b-256 hash of the payload's CBOR, whose protected header names the
/// signer's key hash as `address`. It also requires that key to be the pool's
/// cold key, so no other key can register for the pool.
pub fn cip151_registration_is_signed(
    metadata: &Metadatum,
    registration: &Cip151PoolRegistration,
) -> bool {
    if registration.validation_method != CIP8_VALIDATION_METHOD {
        return false;
    }

    let Ok(items) = as_map(metadata, "registration") else {
        return false;
    };

    let Some(Ok(payload)) = lookup(items, 1).map(minicbor::to_vec) else {
        return false;
    };

    let Some(Ok(witnesses)) = lookup(items, 2).map(|x| as_array(x, "witnesses")) else {
        return false;
    };

    let digest = Hasher::<256>::hash(&payload);

    witnesses
        .iter()
        .any(|witness| cose_witness_signs(witness, &registration.pool_id, digest.as_slice()))
}

/// Whether `witness` is a COSE witness by the key hashing to `pool_id` over
/// `digest`.
fn cose_witness_signs(witness: &Metadatum, pool_id: &Hash<28>, digest: &[u8]) -> bool {
    let Ok(witness) = as_map(witness, "witness") else {
        return false;
    };

    if let Some(kind) = lookup(witness, 0) {
        if as_uint(kind, "witness_type") != Ok(COSE_WITNESS_TYPE) {
            return false;
        }
    }

    let Some(pub_key) = lookup(witness, 1)
        .and_then(|x| as_map(x, "cose_key").ok())
        .and_then(|x| lookup(x, -2))
        .and_then(|x| as_bytes(x, "cose_key_pub").ok())
        .and_then(|x| <[u8; 32]>::try_from(x).ok())
    else {
        return false;
    };

    if Hasher::<224>::hash(&pub_key) != *pool_id {
        return false;
    }

    let Some(Ok(sign1)) = lookup(witness, 2).map(|x| as_array(x, "cose_sign1")) else {
        return false;
    };

    let [protected, hashed, _, signature] = sign1 else {
        return false;
    };

    let Ok(protected) = as_bytes(protected, "protected_header") else {
        return false;
    };

    if protected_header_address(protected) != Some(pool_id.as_slice()) {
        return false;
    }

    let payload = match as_uint(hashed, "hashed") {
        Ok(0) => digest.to_vec(),
        Ok(1) => Hasher::<224>::hash(digest).to_vec(),
        _ => return false,
    };

    let Some(signature) = as_bytes(signature, "signature")
        .ok()
        .and_then(|x| <[u8; 64]>::try_from(x).ok())
    else {
        return false;
    };

    PublicKey::from(pub_key).verify(
        cose_sig_structure(protected, &payload),
        &Signature::from(signature),
    )
}

/// The `address` entry of a COSE protected header.
fn protected_header_address(protected: &[u8]) -> Option<&[u8]> {
    let mut decoder = minicbor::Decoder::new(protected);
    let entries = decoder.map().ok()??;

    for _ in 0..entries {
        let is_address = match decoder.datatype().ok()? {
            minicbor::data::Type::String => decoder.str().ok()? == "address",
            _ => {
                decoder.skip().ok()?;
                false
            }
        };

        if is_address {
            return decoder.bytes().ok();
        }

        decoder.skip().ok()?;
    }

    None
}

/// The COSE `Sig_structure` a COSE_Sign1 signs, with no external data.
fn cose_sig_structure(protected: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut encoder = minicbor::Encoder::new(Vec::new());
    encoder
        .array(4)
        .and_then(|e| e.str("Signature1"))
        .and_then(|e| e.bytes(protected))
        .and_then(|e| e.bytes(&[]))
        .and_then(|e| e.bytes(payload))
        .expect("encoding into a vec is infallible");
    encoder.into_writer()
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
    use dolos_testing::cip151::{self as signer, Witness};
    use pallas::{
        codec::utils::{Bytes, KeyValuePairs},
        ledger::primitives::Int,
    };

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

    /// The registration pg_cardano's README verifies, which Blockfrost and
    /// Koios accept.
    const SIGNED_REGISTRATION: &str = "a1190363a3000201a5018201581c6648d73a09b282c120c8476789f8232b7eead94ff560917ae8fc4eb10280038102041a08d77f10075820fb01193646bb8820cc8c1fc70752d068e1b37be2f1aef88a2bc70cf598de6a630281a201a40101032720062158201ca00b7625ea6a25a6c8c128e308877a563d133ff108147fd1edc36eca3a447c02845829a201276761646472657373581c6648d73a09b282c120c8476789f8232b7eead94ff560917ae8fc4eb10058203a1807ae73acd6dc91f39fa2f6d24e0996d348bb21939e1d9bc5cd75dd778a6858406b8978326e011a9531ac55040adeba410edd9ad8d28b59ad2b7b7d5923b82745ed85155c0ba367b2e54aa8e1f91eb96a58e8249f89d5d0b1f83ba4946d808607";

    fn signed_registration() -> Metadatum {
        let bytes = hex::decode(SIGNED_REGISTRATION).unwrap();
        let entries: KeyValuePairs<u64, Metadatum> = minicbor::decode(&bytes).unwrap();
        entries.to_vec().remove(0).1
    }

    fn is_signed(metadata: &Metadatum) -> bool {
        let registration = parse_cip151_pool_registration(metadata).unwrap();
        cip151_registration_is_signed(metadata, &registration)
    }

    fn set_payload_field(metadata: &mut Metadatum, key: i64, value: Metadatum) {
        let Metadatum::Map(items) = metadata else {
            panic!("registration is a map");
        };
        let mut items: Vec<_> = items.iter().cloned().collect();
        let Metadatum::Map(payload) = &mut items[1].1 else {
            panic!("payload is a map");
        };
        let mut payload: Vec<_> = payload.iter().cloned().collect();
        payload
            .iter_mut()
            .find(|(k, _)| *k == md_int(key))
            .unwrap()
            .1 = value;
        items[1].1 = Metadatum::Map(payload.into());
        *metadata = Metadatum::Map(items.into());
    }

    #[test]
    fn verifies_a_published_registration() {
        assert!(is_signed(&signed_registration()));
    }

    #[test]
    fn a_changed_payload_breaks_the_signature() {
        let mut metadata = signed_registration();
        set_payload_field(&mut metadata, 4, md_int(0x08d77f11));

        assert!(!is_signed(&metadata));
    }

    #[test]
    fn a_registration_without_witnesses_is_not_signed() {
        let mut metadata = signed_registration();
        let Metadatum::Map(items) = &metadata else {
            unreachable!()
        };
        let items: Vec<_> = items.iter().take(2).cloned().collect();
        metadata = Metadatum::Map(items.into());

        assert!(!is_signed(&metadata));
    }

    #[test]
    fn verifies_the_testing_signer() {
        let cold_key = signer::cold_key(1);
        let (_, metadata) = signer::signed_registration(&cold_key, 7, [0x57; 32]).remove(0);

        assert!(is_signed(&metadata));
    }

    #[test]
    fn a_signature_by_another_key_is_not_the_pools() {
        let cold_key = signer::cold_key(1);
        let pool = signer::pool_id(&cold_key);
        let other = signer::cold_key(2);
        let (_, metadata) = signer::registration_signed_by(pool, &other, 7, [0x57; 32]).remove(0);

        assert!(!is_signed(&metadata));
    }

    #[test]
    fn an_unsigned_registration_is_not_signed() {
        let pool = signer::pool_id(&signer::cold_key(1));
        let (_, metadata) = signer::unsigned_registration(pool, 7, [0x57; 32]).remove(0);

        assert!(!is_signed(&metadata));
    }

    fn is_signed_with(witness: &Witness) -> bool {
        let pool = signer::pool_id(&signer::cold_key(1));
        let (_, metadata) = signer::registration_with(pool, 7, [0x57; 32], witness).remove(0);

        is_signed(&metadata)
    }

    #[test]
    fn another_key_naming_the_pool_as_address_is_not_the_pools() {
        let cold_key = signer::cold_key(1);
        let other = signer::cold_key(2);
        let witness = Witness {
            address: Some(signer::pool_id(&cold_key).to_vec()),
            ..Witness::by(&other)
        };

        assert!(!is_signed_with(&witness));
    }

    #[test]
    fn a_cold_key_signature_without_an_address_is_not_signed() {
        let cold_key = signer::cold_key(1);
        let witness = Witness {
            address: None,
            ..Witness::by(&cold_key)
        };

        assert!(!is_signed_with(&witness));
    }

    #[test]
    fn a_cold_key_signature_naming_another_address_is_not_signed() {
        let cold_key = signer::cold_key(1);
        let other = signer::cold_key(2);
        let witness = Witness {
            address: Some(signer::pool_id(&other).to_vec()),
            ..Witness::by(&cold_key)
        };

        assert!(!is_signed_with(&witness));
    }

    #[test]
    fn a_hashed_signature_over_the_digest_hash_is_signed() {
        let cold_key = signer::cold_key(1);
        let witness = Witness {
            hashed: true,
            signs_digest_hash: true,
            ..Witness::by(&cold_key)
        };

        assert!(is_signed_with(&witness));
    }

    #[test]
    fn a_hashed_signature_over_the_bare_digest_is_not_signed() {
        let cold_key = signer::cold_key(1);
        let witness = Witness {
            hashed: true,
            ..Witness::by(&cold_key)
        };

        assert!(!is_signed_with(&witness));
    }

    #[test]
    fn a_payload_declaring_another_validation_method_is_not_signed() {
        let cold_key = signer::cold_key(1);
        let witness = Witness {
            validation_method: 0,
            ..Witness::by(&cold_key)
        };

        assert!(!is_signed_with(&witness));
    }

    #[test]
    fn only_a_cose_witness_type_is_signed() {
        let cold_key = signer::cold_key(1);
        let typed = |kind| Witness {
            kind: Some(kind),
            ..Witness::by(&cold_key)
        };

        assert!(is_signed_with(&typed(0)));
        assert!(!is_signed_with(&typed(1)));
    }

    #[test]
    fn zero_key_is_revocation() {
        assert!(calidus_key_is_revoked(&[0u8; 32]));
        assert!(!calidus_key_is_revoked(&[1u8; 32]));
    }
}
