//! CIP-151 calidus registrations for tests, signed the way cardano-signer
//! signs one with a pool cold key (CIP-8, validation method 2).

use pallas::{
    codec::{minicbor, utils::Bytes},
    crypto::{
        hash::{Hash, Hasher},
        key::ed25519::SecretKey,
    },
    ledger::primitives::{alonzo::Metadatum, Int},
};

use dolos_cardano::cip151::CIP151_METADATA_LABEL;

/// A pool cold key derived from `seed`.
pub fn cold_key(seed: u8) -> SecretKey {
    SecretKey::from([seed; 32])
}

/// The pool id of `cold_key`.
pub fn pool_id(cold_key: &SecretKey) -> Hash<28> {
    Hasher::<224>::hash(cold_key.public_key().as_ref())
}

/// The bech32 pool id of `cold_key`, for a synthetic chain's `pool_id`.
pub fn pool_bech32(cold_key: &SecretKey) -> String {
    use bech32::ToBase32 as _;

    bech32::encode(
        "pool",
        pool_id(cold_key).as_ref().to_base32(),
        bech32::Variant::Bech32,
    )
    .unwrap()
}

/// The label-867 metadata of a registration of `calidus_key` for the pool
/// of `cold_key`, signed by it.
pub fn signed_registration(
    cold_key: &SecretKey,
    nonce: u64,
    calidus_key: [u8; 32],
) -> Vec<(u64, Metadatum)> {
    registration_signed_by(pool_id(cold_key), cold_key, nonce, calidus_key)
}

/// A registration of `calidus_key` for `pool`, signed by `signer`, which
/// need not be the pool's cold key.
pub fn registration_signed_by(
    pool: Hash<28>,
    signer: &SecretKey,
    nonce: u64,
    calidus_key: [u8; 32],
) -> Vec<(u64, Metadatum)> {
    let payload = payload(pool, nonce, calidus_key);
    let digest = Hasher::<256>::hash(&minicbor::to_vec(&payload).unwrap());
    let signer_hash = Hasher::<224>::hash(signer.public_key().as_ref());

    let mut protected = minicbor::Encoder::new(Vec::new());
    protected
        .map(2)
        .unwrap()
        .i8(1)
        .unwrap()
        .i8(-8)
        .unwrap()
        .str("address")
        .unwrap()
        .bytes(signer_hash.as_slice())
        .unwrap();
    let protected = protected.into_writer();

    let mut sig_structure = minicbor::Encoder::new(Vec::new());
    sig_structure
        .array(4)
        .unwrap()
        .str("Signature1")
        .unwrap()
        .bytes(&protected)
        .unwrap()
        .bytes(&[])
        .unwrap()
        .bytes(digest.as_slice())
        .unwrap();
    let signature = signer.sign(sig_structure.into_writer());

    let cose_key = Metadatum::Map(
        vec![
            (int(1), int(1)),
            (int(3), int(-8)),
            (int(-1), int(6)),
            (int(-2), bytes(signer.public_key().as_ref())),
        ]
        .into(),
    );

    let witness = Metadatum::Map(
        vec![
            (int(1), cose_key),
            (
                int(2),
                Metadatum::Array(vec![
                    bytes(&protected),
                    int(0),
                    bytes(digest.as_slice()),
                    bytes(signature.as_ref()),
                ]),
            ),
        ]
        .into(),
    );

    entry(payload, vec![witness])
}

/// A registration of `calidus_key` for `pool` with no witness.
pub fn unsigned_registration(
    pool: Hash<28>,
    nonce: u64,
    calidus_key: [u8; 32],
) -> Vec<(u64, Metadatum)> {
    entry(payload(pool, nonce, calidus_key), vec![])
}

fn payload(pool: Hash<28>, nonce: u64, calidus_key: [u8; 32]) -> Metadatum {
    Metadatum::Map(
        vec![
            (
                int(1),
                Metadatum::Array(vec![int(1), bytes(pool.as_slice())]),
            ),
            (int(2), Metadatum::Array(vec![])),
            (int(3), Metadatum::Array(vec![int(2)])),
            (
                int(4),
                Metadatum::Int(Int::try_from(i128::from(nonce)).unwrap()),
            ),
            (int(7), bytes(&calidus_key)),
        ]
        .into(),
    )
}

fn entry(payload: Metadatum, witnesses: Vec<Metadatum>) -> Vec<(u64, Metadatum)> {
    vec![(
        CIP151_METADATA_LABEL,
        Metadatum::Map(
            vec![
                (int(0), int(2)),
                (int(1), payload),
                (int(2), Metadatum::Array(witnesses)),
            ]
            .into(),
        ),
    )]
}

fn int(value: i64) -> Metadatum {
    Metadatum::Int(Int::from(value))
}

fn bytes(value: &[u8]) -> Metadatum {
    Metadatum::Bytes(Bytes::from(value.to_vec()))
}
