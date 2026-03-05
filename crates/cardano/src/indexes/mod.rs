//! Cardano-specific index support.
//!
//! This module provides Cardano-specific extensions to the generic index store
//! defined in `dolos-core`. It includes:
//!
//! - Dimension constants for UTxO filter and archive indexes
//! - Extension traits for convenient Cardano-specific index queries
//! - Delta builders for constructing index deltas from Cardano block data

mod delta;
mod dimensions;
mod ext;
mod query;

pub use delta::{index_delta_from_utxo_delta, CardanoIndexDeltaBuilder};
pub use dimensions::{archive as archive_dimensions, utxo as utxo_dimensions};
pub use ext::CardanoIndexExt;
pub use query::{AsyncCardanoQueryExt, ScriptData, ScriptLanguage, SlotOrder};
