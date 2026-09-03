//! The `digests` layer's codec, which lives in [`stelae_driver::digests`].
//!
//! Nothing about the kind is profile-shaped — its records name
//! immutable-database files by content and reach no store — so the codec moved
//! down to the driver crate. What stays here is the shim that keeps its old
//! path resolving, and the two free functions are spelled out rather than
//! re-exported because callers pass them where a `Result<_, crate::Error>` is
//! expected: the module's shape is unchanged, and so is what a failure of
//! either one is.

use stelae::frame::CanonicalCbor;

pub use stelae_driver::digests::{ImmutableDigests, OrderCheck, DIGESTS};

use crate::Error;

pub fn encode(record: &ImmutableDigests) -> Result<CanonicalCbor, Error> {
    Ok(stelae_driver::digests::encode(record)?)
}

pub fn decode(bytes: &[u8]) -> Result<ImmutableDigests, Error> {
    Ok(stelae_driver::digests::decode(bytes)?)
}
