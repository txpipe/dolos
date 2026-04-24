//! Shard range helper for EWRAP key-range partitioning.
//!
//! Shards partition per-account EWRAP work by first-byte prefix of the
//! `EntityKey`. `total_shards` must divide 256 so that each shard covers an
//! equal whole number of prefix buckets (e.g. at `total_shards = 16` each
//! shard owns 16 consecutive prefix values).
//!
//! The state-store iterator takes a half-open `Range<EntityKey>`, so shard
//! `i` gets:
//!
//! - `start = [i*step, 0, 0, ..., 0]`
//! - `end   = [(i+1)*step, 0, 0, ..., 0]` for `i < total - 1`
//! - `end   = [0xFF; KEY_SIZE]` for the final shard, matching
//!   `EntityKey::full_range`'s convention.

use std::ops::Range;

use dolos_core::{EntityKey, KEY_SIZE};

pub const PREFIX_SPACE: u32 = 256;

/// Return `Ok(())` if `total_shards` is a valid sharding factor (>= 1 and
/// divides 256).
pub fn validate_total_shards(total_shards: u32) -> Result<(), String> {
    if total_shards == 0 {
        return Err("ewrap_total_shards must be >= 1".into());
    }
    if !PREFIX_SPACE.is_multiple_of(total_shards) {
        return Err(format!(
            "ewrap_total_shards ({total_shards}) must divide {PREFIX_SPACE}"
        ));
    }
    Ok(())
}

/// Compute the key range for `shard_index` out of `total_shards`.
pub fn shard_key_range(shard_index: u32, total_shards: u32) -> Range<EntityKey> {
    debug_assert!(validate_total_shards(total_shards).is_ok());
    debug_assert!(shard_index < total_shards);

    let step = PREFIX_SPACE / total_shards;
    let first_byte = (shard_index * step) as u8;

    let mut start = [0u8; KEY_SIZE];
    start[0] = first_byte;

    let end = if shard_index + 1 == total_shards {
        [0xFFu8; KEY_SIZE]
    } else {
        let next_first_byte = ((shard_index + 1) * step) as u8;
        let mut end = [0u8; KEY_SIZE];
        end[0] = next_first_byte;
        end
    };

    Range {
        start: EntityKey::from(&start),
        end: EntityKey::from(&end),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_cover_full_range_at_16() {
        let ranges: Vec<_> = (0..16).map(|i| shard_key_range(i, 16)).collect();

        assert_eq!(ranges[0].start.as_ref()[0], 0x00);
        assert_eq!(ranges[0].end.as_ref()[0], 0x10);
        assert_eq!(ranges[1].start.as_ref()[0], 0x10);
        assert_eq!(ranges[15].start.as_ref()[0], 0xF0);
        assert_eq!(ranges[15].end.as_ref(), &[0xFFu8; KEY_SIZE]);

        // Adjacent ranges meet: end_i == start_{i+1} for i < 15
        for i in 0..15 {
            assert_eq!(
                ranges[i].end.as_ref()[0],
                ranges[i + 1].start.as_ref()[0],
                "shards {i} and {} must meet",
                i + 1
            );
        }
    }

    #[test]
    fn shards_cover_full_range_at_1() {
        let range = shard_key_range(0, 1);
        assert_eq!(range.start.as_ref()[0], 0x00);
        assert_eq!(range.end.as_ref(), &[0xFFu8; KEY_SIZE]);
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
}
