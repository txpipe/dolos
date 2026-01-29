//! Cardano-specific index dimension names.
//!
//! Dimensions are static strings that identify the type of index table.
//! Each dimension corresponds to a separate table in the storage backend.

/// UTxO filter index dimensions.
///
/// These dimensions are used for querying the current UTxO set by various criteria.
pub mod utxo {
    use dolos_core::TagDimension;

    /// Full address (any type: Shelley, Byron, Stake)
    pub const ADDRESS: TagDimension = "address";

    /// Payment credential (Shelley addresses only)
    pub const PAYMENT: TagDimension = "payment";

    /// Stake credential (Shelley addresses with delegation, or stake addresses)
    pub const STAKE: TagDimension = "stake";

    /// Native asset policy ID
    pub const POLICY: TagDimension = "policy";

    /// Native asset subject (policy ID + asset name)
    pub const ASSET: TagDimension = "asset";
}

/// Archive index dimensions.
///
/// These dimensions are used for querying historical blocks by various criteria.
pub mod archive {
    use dolos_core::TagDimension;

    /// Full address (any type: Shelley, Byron, Stake)
    pub const ADDRESS: TagDimension = "address";

    /// Payment credential (Shelley addresses only)
    pub const PAYMENT: TagDimension = "payment";

    /// Stake credential (Shelley addresses with delegation, or stake addresses)
    pub const STAKE: TagDimension = "stake";

    /// Native asset subject (policy ID + asset name)
    pub const ASSET: TagDimension = "asset";

    /// Native asset policy ID
    pub const POLICY: TagDimension = "policy";

    /// Plutus datum hash
    pub const DATUM: TagDimension = "datum";

    /// Script hash (Plutus or native)
    pub const SCRIPT: TagDimension = "script";

    /// Spent TxO reference (tx_hash + output_index)
    pub const SPENT_TXO: TagDimension = "spent_txo";

    /// Account certificates (stake registration, deregistration, delegation)
    pub const ACCOUNT_CERTS: TagDimension = "account_certs";

    /// Transaction metadata labels
    pub const METADATA: TagDimension = "metadata";
}
