//! Shard range helper for account-shard key-range partitioning.
//!
//! `EntityKey`s for credential-keyed entities (`AccountState`,
//! `PendingRewardState`, `PendingMirState`) are produced by CBOR-encoding a
//! `StakeCredential`. The encoding has a fixed 4-byte prefix:
//!
//! ```text
//!   0x82                 CBOR array(2)
//!   0x00 | 0x01          variant tag (AddrKeyhash | ScriptHash)
//!   0x58 0x1c            CBOR bytes(28) header
//!   <28 hash bytes>      the actual hash (entropic)
//! ```
//!
//! The first 4 bytes carry no entropy across credentials, so partitioning by
//! `key[0]` collapses every credential into a single bucket. We instead shard
//! on `key[4]` — the first byte of the underlying hash — and emit one range
//! per credential variant. Each shard `i` of `total_shards` therefore covers
//! two ranges:
//!
//! ```text
//!   AddrKeyhash:  [0x82, 0x00, 0x58, 0x1c, i*step,     0, ..., 0]
//!              ..  [0x82, 0x00, 0x58, 0x1c, (i+1)*step, 0, ..., 0]
//!   ScriptHash:   same with key[1] = 0x01
//! ```
//!
//! `total_shards` must divide 256 so each shard owns an equal whole number of
//! `key[4]` buckets. The final shard's range end "rolls over" the bytes(28)
//! header (0x1c → 0x1d), which sorts past every credential key in the
//! variant's subspace, expressing "until the end of the variant".
//!
//! The CBOR layout above is a Cardano-wide invariant (every client encodes
//! `StakeCredential` the same way), so it is checked once as a unit test
//! rather than at startup.

use std::ops::Range;

use dolos_core::{EntityKey, KEY_SIZE};

pub const PREFIX_SPACE: u32 = 256;

/// Number of shards used to partition the per-credential leg of the
/// epoch-boundary pipeline (RUPD, Ewrap, Estart). Must divide 256 (so
/// shards are whole first-byte prefix buckets) and be >= 1.
pub const ACCOUNT_SHARDS: u32 = 32;

const _: () = assert!(
    ACCOUNT_SHARDS >= 1 && PREFIX_SPACE.is_multiple_of(ACCOUNT_SHARDS),
    "ACCOUNT_SHARDS must be >= 1 and divide PREFIX_SPACE (256)"
);

/// Layout constants for the CBOR-encoded `StakeCredential` key prefix.
/// Asserted by the `cbor_layout_invariant_holds` unit test.
pub const CRED_KEY_ARRAY_TAG: u8 = 0x82;
pub const CRED_KEY_VARIANT_ADDRKEYHASH: u8 = 0x00;
pub const CRED_KEY_VARIANT_SCRIPTHASH: u8 = 0x01;
pub const CRED_KEY_BYTES_HEADER: [u8; 2] = [0x58, 0x1c];
pub const CRED_KEY_HASH_OFFSET: usize = 4;

/// Return `Ok(())` if `total_shards` is a valid sharding factor (>= 1 and
/// divides 256). Used by `shard_key_ranges` invariants and unit tests.
pub fn validate_total_shards(total_shards: u32) -> Result<(), String> {
    if total_shards == 0 {
        return Err("total_shards must be >= 1".into());
    }
    if !PREFIX_SPACE.is_multiple_of(total_shards) {
        return Err(format!(
            "total_shards ({total_shards}) must divide {PREFIX_SPACE}"
        ));
    }
    Ok(())
}

/// Compute the per-variant key ranges for `shard_index` of `total_shards`.
/// Returns one range per `StakeCredential` variant (AddrKeyhash and
/// ScriptHash). Both must be iterated to cover the shard's full slice.
///
/// Panics in **all** build profiles (not just debug) on invalid inputs:
/// `total_shards == 0` would divide by zero in `variant_range`, and a
/// non-divisor of 256 would silently produce broken partitions. The
/// `ACCOUNT_SHARDS` constant is validated at compile time, but
/// `total_shards` can also come from persisted `ShardProgress.total`,
/// so a release-mode `debug_assert!` would let storage corruption pass
/// silently — hence the unconditional `assert!`/`panic!` here.
pub fn shard_key_ranges(shard_index: u32, total_shards: u32) -> Vec<Range<EntityKey>> {
    if let Err(e) = validate_total_shards(total_shards) {
        panic!("shard_key_ranges: {e}");
    }
    assert!(
        shard_index < total_shards,
        "shard_key_ranges: shard_index ({shard_index}) must be < total_shards ({total_shards})",
    );

    [CRED_KEY_VARIANT_ADDRKEYHASH, CRED_KEY_VARIANT_SCRIPTHASH]
        .into_iter()
        .map(|variant| variant_range(variant, shard_index, total_shards))
        .collect()
}

fn variant_range(variant: u8, shard_index: u32, total_shards: u32) -> Range<EntityKey> {
    let step = PREFIX_SPACE / total_shards;
    let lo = (shard_index * step) as u8;

    let mut start = [0u8; KEY_SIZE];
    start[0] = CRED_KEY_ARRAY_TAG;
    start[1] = variant;
    start[2] = CRED_KEY_BYTES_HEADER[0];
    start[3] = CRED_KEY_BYTES_HEADER[1];
    start[CRED_KEY_HASH_OFFSET] = lo;

    let mut end = [0u8; KEY_SIZE];
    end[0] = CRED_KEY_ARRAY_TAG;
    end[1] = variant;
    end[2] = CRED_KEY_BYTES_HEADER[0];

    if shard_index + 1 == total_shards {
        // Roll over the bytes(28) header so the end bound sits past every
        // valid credential key for this variant in lex order.
        end[3] = CRED_KEY_BYTES_HEADER[1] + 1;
    } else {
        end[3] = CRED_KEY_BYTES_HEADER[1];
        end[CRED_KEY_HASH_OFFSET] = ((shard_index + 1) * step) as u8;
    }

    Range {
        start: EntityKey::from(&start),
        end: EntityKey::from(&end),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas::{codec::minicbor, crypto::hash::Hash, ledger::primitives::StakeCredential};

    fn first_bytes(r: &Range<EntityKey>, n: usize) -> Vec<u8> {
        r.start.as_ref()[..n].to_vec()
    }

    #[test]
    fn each_shard_yields_two_variant_ranges() {
        let ranges = shard_key_ranges(0, 16);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start.as_ref()[1], CRED_KEY_VARIANT_ADDRKEYHASH);
        assert_eq!(ranges[1].start.as_ref()[1], CRED_KEY_VARIANT_SCRIPTHASH);
    }

    #[test]
    fn ranges_carry_full_cbor_prefix() {
        let ranges = shard_key_ranges(3, 16);
        for r in &ranges {
            assert_eq!(r.start.as_ref()[0], CRED_KEY_ARRAY_TAG);
            assert_eq!(r.start.as_ref()[2], CRED_KEY_BYTES_HEADER[0]);
            assert_eq!(r.start.as_ref()[3], CRED_KEY_BYTES_HEADER[1]);
        }
    }

    #[test]
    fn shards_partition_byte4_at_16() {
        // Across all 16 shards, byte[4] starts march from 0x00 to 0xF0 in
        // steps of 0x10 for each variant.
        for i in 0..16u32 {
            let ranges = shard_key_ranges(i, 16);
            for r in &ranges {
                assert_eq!(r.start.as_ref()[CRED_KEY_HASH_OFFSET], (i * 16) as u8);
            }
        }
    }

    #[test]
    fn adjacent_shards_meet_within_each_variant() {
        for i in 0..15u32 {
            let curr = shard_key_ranges(i, 16);
            let next = shard_key_ranges(i + 1, 16);
            for v in 0..2 {
                assert_eq!(
                    first_bytes(&curr[v], 5),
                    {
                        // start of curr[v]: [0x82, variant, 0x58, 0x1c, i*16]
                        let mut e = vec![0u8; 5];
                        e[0] = CRED_KEY_ARRAY_TAG;
                        e[1] = if v == 0 {
                            CRED_KEY_VARIANT_ADDRKEYHASH
                        } else {
                            CRED_KEY_VARIANT_SCRIPTHASH
                        };
                        e[2] = CRED_KEY_BYTES_HEADER[0];
                        e[3] = CRED_KEY_BYTES_HEADER[1];
                        e[4] = (i * 16) as u8;
                        e
                    }
                );
                assert_eq!(curr[v].end.as_ref()[CRED_KEY_HASH_OFFSET], next[v].start.as_ref()[CRED_KEY_HASH_OFFSET]);
            }
        }
    }

    #[test]
    fn final_shard_rolls_bytes_header() {
        let ranges = shard_key_ranges(15, 16);
        for r in &ranges {
            assert_eq!(r.start.as_ref()[CRED_KEY_HASH_OFFSET], 0xF0);
            // End bumps the bytes(28) header byte instead of overflowing the
            // u8 byte-4 counter.
            assert_eq!(r.end.as_ref()[2], CRED_KEY_BYTES_HEADER[0]);
            assert_eq!(r.end.as_ref()[3], CRED_KEY_BYTES_HEADER[1] + 1);
            assert_eq!(r.end.as_ref()[CRED_KEY_HASH_OFFSET], 0x00);
        }
    }

    #[test]
    fn single_shard_covers_full_variant_subspace() {
        let ranges = shard_key_ranges(0, 1);
        assert_eq!(ranges.len(), 2);
        for r in &ranges {
            assert_eq!(r.start.as_ref()[CRED_KEY_HASH_OFFSET], 0x00);
            assert_eq!(r.end.as_ref()[3], CRED_KEY_BYTES_HEADER[1] + 1);
        }
    }

    #[test]
    fn validates_total_shards() {
        assert!(validate_total_shards(1).is_ok());
        assert!(validate_total_shards(2).is_ok());
        assert!(validate_total_shards(16).is_ok());
        assert!(validate_total_shards(256).is_ok());

        assert!(validate_total_shards(0).is_err());
        assert!(validate_total_shards(3).is_err());
        assert!(validate_total_shards(100).is_err());
    }

    #[test]
    fn cbor_layout_invariant_holds() {
        // Cardano-wide invariant: `StakeCredential` CBOR-encodes as
        // [0x82, variant, 0x58, 0x1c, <28 hash bytes>]. The whole shard
        // partition rests on this layout — if a pallas upgrade ever changes
        // it, this test fires before anything else does.
        let dummy: Hash<28> = Hash::new([0u8; 28]);
        for (cred, expected_variant) in [
            (StakeCredential::AddrKeyhash(dummy), CRED_KEY_VARIANT_ADDRKEYHASH),
            (StakeCredential::ScriptHash(dummy), CRED_KEY_VARIANT_SCRIPTHASH),
        ] {
            let enc = minicbor::to_vec(&cred).unwrap();
            assert_eq!(enc.len(), KEY_SIZE);
            assert_eq!(enc[0], CRED_KEY_ARRAY_TAG);
            assert_eq!(enc[1], expected_variant);
            assert_eq!(&enc[2..4], &CRED_KEY_BYTES_HEADER);
        }
    }

    #[test]
    fn ranges_actually_contain_real_credential_keys() {
        // Encode a credential, then check that exactly one shard's range
        // (across both variants) contains the resulting key.
        let h: Hash<28> = Hash::new([0x42; 28]);
        let cred = StakeCredential::AddrKeyhash(h);
        let key = EntityKey::from(minicbor::to_vec(&cred).unwrap());

        let total_shards = 16;
        let mut hits = 0;
        for i in 0..total_shards {
            for r in shard_key_ranges(i, total_shards) {
                if key.as_ref() >= r.start.as_ref() && key.as_ref() < r.end.as_ref() {
                    hits += 1;
                }
            }
        }
        assert_eq!(hits, 1, "credential key must belong to exactly one shard range");
    }
}
