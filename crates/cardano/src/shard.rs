//! How the per-credential leg of the epoch-boundary pipeline is split.
//!
//! The credential-keyed entities (`AccountState`, `PendingRewardState`,
//! `PendingMirState`) are keyed by [`crate::AccountKey`], whose
//! [`crate::AccountKey::shard_ranges`] cut the key space on the first byte of
//! the credential hash — the header before it carries no entropy. A shard
//! count must therefore divide 256, so each shard owns an equal whole number
//! of first-byte buckets.

pub const PREFIX_SPACE: u32 = 256;

/// Number of shards used to partition the per-credential leg of the
/// epoch-boundary pipeline (RUPD, Ewrap, Estart). Must divide 256 (so
/// shards are whole first-byte prefix buckets) and be >= 1.
pub const ACCOUNT_SHARDS: u32 = 32;

const _: () = assert!(
    ACCOUNT_SHARDS >= 1 && PREFIX_SPACE.is_multiple_of(ACCOUNT_SHARDS),
    "ACCOUNT_SHARDS must be >= 1 and divide PREFIX_SPACE (256)"
);

/// Return `Ok(())` if `total_shards` is a valid sharding factor (>= 1 and
/// divides 256).
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

#[cfg(test)]
mod tests {
    use super::*;

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
