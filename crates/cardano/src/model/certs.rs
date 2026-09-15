use dolos_core::{BlockSlot, CertIndex, TxOrder};
use serde::{Deserialize, Serialize};

/// Position of a certificate on the chain: block slot, transaction order
/// inside the block, certificate index inside the transaction.
///
/// Orders the way the ledger applies certificates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CertPosition {
    pub slot: BlockSlot,
    pub tx_order: TxOrder,
    pub cert_index: CertIndex,
}

impl CertPosition {
    pub fn new(slot: BlockSlot, tx_order: TxOrder, cert_index: CertIndex) -> Self {
        Self {
            slot,
            tx_order,
            cert_index,
        }
    }

    /// Joins the stored `(slot, tx order)` pair with its stored certificate
    /// index.
    pub fn from_parts(at: (BlockSlot, TxOrder), cert_index: CertIndex) -> Self {
        Self::new(at.0, at.1, cert_index)
    }

    /// The `(slot, tx order)` pair the entities store.
    pub fn tx_at(&self) -> (BlockSlot, TxOrder) {
        (self.slot, self.tx_order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_slot_then_tx_then_cert() {
        let base = CertPosition::new(10, 2, 3);

        assert!(base < CertPosition::new(11, 0, 0));
        assert!(base < CertPosition::new(10, 3, 0));
        assert!(base < CertPosition::new(10, 2, 4));
        assert!(base > CertPosition::new(10, 2, 2));
        assert_eq!(base, CertPosition::from_parts(base.tx_at(), 3));
    }
}
