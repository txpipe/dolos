//! CIP-151 calidus registrations for tests, signed the way cardano-signer
//! signs one with a pool cold key (CIP-8, validation method 2).

use curve25519_dalek::scalar::{clamp_integer, Scalar};
use pallas::{
    codec::{minicbor, utils::Bytes},
    crypto::{
        hash::{Hash, Hasher},
        key::ed25519::SecretKey,
    },
    ledger::primitives::{alonzo::Metadatum, Int},
};

use sha2::{Digest as _, Sha512};

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
    registration_with(pool, nonce, calidus_key, &Witness::by(signer))
}

/// How [`registration_with`] signs a registration. [`Witness::by`] signs the
/// way cardano-signer does; each field varies one part of it.
pub struct Witness<'a> {
    pub signer: &'a SecretKey,
    /// The protected header's `address`, if any.
    pub address: Option<Vec<u8>>,
    /// The COSE_Sign1 `hashed` flag.
    pub hashed: i64,
    /// Sign the blake2b-224 of the payload digest rather than the digest.
    pub signs_digest_hash: bool,
    /// The witness type entry (key 0), if any.
    pub kind: Option<i64>,
    /// The validation method the payload declares.
    pub validation_method: i64,
    /// Sign with `R` set to the identity point, which plain Ed25519
    /// verification accepts and the Cardano node's rules reject.
    pub identity_r: bool,
}

impl<'a> Witness<'a> {
    pub fn by(signer: &'a SecretKey) -> Self {
        Self {
            signer,
            address: Some(pool_id(signer).to_vec()),
            hashed: 0,
            signs_digest_hash: false,
            kind: None,
            validation_method: 2,
            identity_r: false,
        }
    }
}

/// A registration of `calidus_key` for `pool` with one witness, built as
/// `witness` says.
pub fn registration_with(
    pool: Hash<28>,
    nonce: u64,
    calidus_key: [u8; 32],
    witness: &Witness,
) -> Vec<(u64, Metadatum)> {
    let payload = payload(pool, nonce, calidus_key, witness.validation_method);
    let digest = Hasher::<256>::hash(&minicbor::to_vec(&payload).unwrap());

    let mut protected = minicbor::Encoder::new(Vec::new());
    match &witness.address {
        Some(address) => protected
            .map(2)
            .unwrap()
            .i8(1)
            .unwrap()
            .i8(-8)
            .unwrap()
            .str("address")
            .unwrap()
            .bytes(address)
            .unwrap(),
        None => protected.map(1).unwrap().i8(1).unwrap().i8(-8).unwrap(),
    };
    let protected = protected.into_writer();

    let signed = if witness.signs_digest_hash {
        Hasher::<224>::hash(digest.as_slice()).to_vec()
    } else {
        digest.to_vec()
    };

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
        .bytes(&signed)
        .unwrap();
    let sig_structure = sig_structure.into_writer();
    let signature = if witness.identity_r {
        identity_r_signature(witness.signer, &sig_structure).to_vec()
    } else {
        witness.signer.sign(&sig_structure).as_ref().to_vec()
    };

    let cose_key = Metadatum::Map(
        vec![
            (int(1), int(1)),
            (int(3), int(-8)),
            (int(-1), int(6)),
            (int(-2), bytes(witness.signer.public_key().as_ref())),
        ]
        .into(),
    );

    let mut entries = Vec::new();
    if let Some(kind) = witness.kind {
        entries.push((int(0), int(kind)));
    }
    entries.push((int(1), cose_key));
    entries.push((
        int(2),
        Metadatum::Array(vec![
            bytes(&protected),
            int(witness.hashed),
            bytes(&signed),
            bytes(&signature),
        ]),
    ));

    entry(payload, vec![Metadatum::Map(entries.into())])
}

/// A signature of `message` by `signer` whose `R` is the identity point.
/// With `S = k·a`, where `a` is the signer's secret scalar and
/// `k = H(R ‖ A ‖ message)`, the check `[S]B = R + [k]A` holds.
fn identity_r_signature(signer: &SecretKey, message: &[u8]) -> [u8; 64] {
    // SAFETY: a test key; its bytes never leave this function.
    let seed = unsafe { SecretKey::leak_into_bytes(signer.clone()) };
    let expanded = Sha512::digest(seed);
    let a = Scalar::from_bytes_mod_order(clamp_integer(expanded[..32].try_into().unwrap()));

    let mut r = [0u8; 32];
    r[0] = 1;
    let k = Sha512::new()
        .chain_update(r)
        .chain_update(signer.public_key().as_ref())
        .chain_update(message)
        .finalize();
    let s = Scalar::from_bytes_mod_order_wide(&k.into()) * a;

    let mut signature = [0u8; 64];
    signature[..32].copy_from_slice(&r);
    signature[32..].copy_from_slice(s.as_bytes());
    signature
}

/// A registration of `calidus_key` for `pool` with no witness.
pub fn unsigned_registration(
    pool: Hash<28>,
    nonce: u64,
    calidus_key: [u8; 32],
) -> Vec<(u64, Metadatum)> {
    entry(payload(pool, nonce, calidus_key, 2), vec![])
}

fn payload(pool: Hash<28>, nonce: u64, calidus_key: [u8; 32], validation_method: i64) -> Metadatum {
    Metadatum::Map(
        vec![
            (
                int(1),
                Metadatum::Array(vec![int(1), bytes(pool.as_slice())]),
            ),
            (int(2), Metadatum::Array(vec![])),
            (int(3), Metadatum::Array(vec![int(validation_method)])),
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

#[cfg(test)]
mod tests {
    use super::*;
    use curve25519_dalek::{edwards::CompressedEdwardsY, EdwardsPoint};

    #[test]
    fn an_identity_r_signature_meets_the_plain_verification_equation() {
        let signer = cold_key(1);
        let message = b"message";
        let signature = identity_r_signature(&signer, message);
        let (r, s) = signature.split_at(32);

        let k = Sha512::new()
            .chain_update(r)
            .chain_update(signer.public_key().as_ref())
            .chain_update(message)
            .finalize();
        let k = Scalar::from_bytes_mod_order_wide(&k.into());
        let a = CompressedEdwardsY(signer.public_key().as_ref().try_into().unwrap())
            .decompress()
            .unwrap();
        let r = CompressedEdwardsY(r.try_into().unwrap())
            .decompress()
            .unwrap();
        let s = Scalar::from_canonical_bytes(s.try_into().unwrap()).unwrap();

        assert_eq!(r, EdwardsPoint::default());
        assert_eq!(EdwardsPoint::mul_base(&s), r + k * a);
    }
}
