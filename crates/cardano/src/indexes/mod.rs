//! Cardano-specific index support.
//!
//! This module provides Cardano-specific extensions to the generic index
//! types defined in `dolos-core`. It includes:
//!
//! - Dimension constants for UTxO filter and archive indexes
//! - Extension traits for convenient Cardano-specific index queries
//! - Delta builders for constructing index deltas from Cardano block data
//! - The entity kinds Cardano declares for the archive's entity index

mod delta;
mod dimensions;
pub mod entities;
mod ext;
mod query;

pub use delta::{utxo_index_delta_from_utxo_delta, utxo_tags, CardanoIndexDeltaBuilder};
pub use dimensions::{archive as archive_dimensions, utxo as utxo_dimensions};
pub use ext::{pool_certs_in, CardanoArchiveIndexExt, CardanoStateIndexExt, PoolCert};
pub use query::{AsyncCardanoQueryExt, ScriptData, ScriptLanguage, SlotOrder};
