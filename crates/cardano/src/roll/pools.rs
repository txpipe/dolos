use std::collections::HashMap;
use std::ops::Deref;

use dolos_core::{ChainError, Genesis, TxOrder, TxoRef};
use pallas::crypto::hash::{Hash, Hasher};
use pallas::ledger::primitives::Epoch;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraCert, MultiEraTx};

use super::WorkDeltas;
use crate::owned::OwnedMultiEraOutput;
use crate::roll::BlockVisitor;
use crate::{cip151, pallas_extras};
use crate::{
    CalidusRegistration, MintedBlocksInc, PParamsSet, PoolCalidusUpdate, PoolDeRegistration,
    PoolRegistration,
};

#[derive(Default, Clone)]
pub struct PoolStateVisitor {
    epoch: Option<Epoch>,
    deposit: Option<u64>,

    /// The current transaction's CIP-151 registration, emitted after its
    /// certificates: it finds the row its own transaction creates, and none
    /// a later transaction creates.
    calidus: Option<PoolCalidusUpdate>,
}

impl BlockVisitor for PoolStateVisitor {
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        _: &Genesis,
        pparams: &PParamsSet,
        epoch: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.epoch = Some(epoch);
        self.deposit = pparams.ensure_pool_deposit().ok();

        if let Some(key) = block.header().issuer_vkey() {
            let operator: Hash<28> = Hasher::<224>::hash(key);
            deltas.add_for_entity(MintedBlocksInc {
                operator,
                count: 1,
                epoch,
            });
        }

        Ok(())
    }

    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        _: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        self.flush(deltas)?;

        if !tx.is_valid() {
            return Ok(());
        }

        let Some(metadata) = cip151::cip151_metadata_for_tx(tx) else {
            return Ok(());
        };

        let Ok(registration) = cip151::parse_cip151_pool_registration(&metadata) else {
            return Ok(());
        };

        let pub_key = registration.calidus_pub_key;
        let calidus = CalidusRegistration {
            pub_key: (!cip151::calidus_key_is_revoked(&pub_key)).then_some(pub_key),
            nonce: registration.nonce,
            tx_hash: tx.hash(),
            slot: block.slot(),
        };

        self.calidus = Some(PoolCalidusUpdate::new(registration.pool_id, calidus));

        Ok(())
    }

    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        _: &TxOrder,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        if let Some(cert) = pallas_extras::cert_as_pool_registration(cert) {
            let epoch = self.epoch.expect("value set in root");
            let deposit = self
                .deposit
                .ok_or(ChainError::StartingPParamMissing("pool_deposit"))?;

            deltas.add_for_entity(PoolRegistration::new(
                cert.clone(),
                block.slot(),
                epoch,
                deposit,
            ));
        }

        match cert {
            MultiEraCert::AlonzoCompatible(cow) => {
                if let pallas::ledger::primitives::alonzo::Certificate::PoolRetirement(
                    operator,
                    epoch,
                ) = cow.deref().deref()
                {
                    deltas.add_for_entity(PoolDeRegistration::new(*operator, *epoch));
                }
            }
            MultiEraCert::Conway(cow) => {
                if let pallas::ledger::primitives::conway::Certificate::PoolRetirement(
                    operator,
                    epoch,
                ) = cow.deref().deref()
                {
                    deltas.add_for_entity(PoolDeRegistration::new(*operator, *epoch));
                }
            }
            _ => {}
        };

        Ok(())
    }

    fn flush(&mut self, deltas: &mut WorkDeltas) -> Result<(), ChainError> {
        if let Some(update) = self.calidus.take() {
            deltas.add_for_entity(update);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::{EntityDelta as _, RawBlock};
    use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
    use pallas::codec::utils::Bytes;
    use pallas::ledger::primitives::{alonzo::Metadatum, Int};

    use super::*;
    use crate::model::{PParamValue as Val, PoolState};
    use crate::{CardanoDelta, OwnedMultiEraBlock};

    /// The pool `build_synthetic_blocks` registers when its `pool_id` is
    /// left at the default.
    const POOL: [u8; 28] = [9; 28];

    fn md_int(value: i64) -> Metadatum {
        Metadatum::Int(Int::from(value))
    }

    /// A CIP-151 version 2 pool registration of `key` for `pool`.
    fn registration(pool: [u8; 28], nonce: i64, key: [u8; 32]) -> Vec<(u64, Metadatum)> {
        let payload = Metadatum::Map(
            vec![
                (
                    md_int(1),
                    Metadatum::Array(vec![
                        md_int(1),
                        Metadatum::Bytes(Bytes::from(pool.to_vec())),
                    ]),
                ),
                (md_int(2), Metadatum::Array(vec![])),
                (md_int(3), Metadatum::Array(vec![md_int(2)])),
                (md_int(4), md_int(nonce)),
                (md_int(7), Metadatum::Bytes(Bytes::from(key.to_vec()))),
            ]
            .into(),
        );

        vec![(
            cip151::CIP151_METADATA_LABEL,
            Metadatum::Map(vec![(md_int(0), md_int(2)), (md_int(1), payload)].into()),
        )]
    }

    fn metadata_block(slot: u64, metadata: Vec<(u64, Metadatum)>, valid: bool) -> RawBlock {
        dolos_testing::blocks::make_conway_block_with_metadata(slot, metadata, valid).1
    }

    fn pool_state() -> PoolState {
        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            ..Default::default()
        });
        let mut entity = None;
        roll(&blocks[0], &mut entity);
        let entity = entity.expect("the synthetic chain registers the pool");
        assert_eq!(entity.calidus, None);
        entity
    }

    /// Visit the transactions of `raws` as one block, the way the crawl
    /// does: the first block's root, then each transaction and the
    /// certificates of the valid ones, then the flush.
    fn deltas_of(raws: &[&RawBlock]) -> WorkDeltas {
        let blocks: Vec<_> = raws
            .iter()
            .map(|raw| OwnedMultiEraBlock::decode((*raw).clone()).unwrap())
            .collect();
        let genesis = crate::include::devnet::load();
        let pparams = PParamsSet::default().with(Val::PoolDeposit(500_000_000));
        let mut visitor = PoolStateVisitor::default();
        let mut deltas = WorkDeltas::default();

        visitor
            .visit_root(&mut deltas, blocks[0].view(), &genesis, &pparams, 0, 0, 0)
            .unwrap();

        for block in &blocks {
            let block = block.view();

            for (order, tx) in block.txs().iter().enumerate() {
                visitor
                    .visit_tx(&mut deltas, block, tx, &HashMap::new())
                    .unwrap();

                if tx.is_valid() {
                    for cert in tx.certs() {
                        visitor
                            .visit_cert(&mut deltas, block, tx, &order, &cert)
                            .unwrap();
                    }
                }
            }
        }

        visitor.flush(&mut deltas).unwrap();

        deltas
    }

    /// Apply the deltas the transactions of `raws`, as one block, make for
    /// [`POOL`] to `entity`, in order, and return them for undo.
    fn roll_txs(raws: &[&RawBlock], entity: &mut Option<PoolState>) -> Vec<CardanoDelta> {
        let mut deltas = deltas_of(raws);
        let key = dolos_core::NsKey::of::<PoolState>(&crate::PoolKey(POOL.into()));
        let mut group = deltas.entities.remove(&key).unwrap_or_default();

        for delta in group.iter_mut() {
            match delta {
                CardanoDelta::PoolRegistration(x) => x.apply(entity),
                CardanoDelta::PoolCalidusUpdate(x) => x.apply(entity),
                other => panic!("unexpected pool delta: {other:?}"),
            }
        }

        group
    }

    fn roll(raw: &RawBlock, entity: &mut Option<PoolState>) -> Vec<CardanoDelta> {
        roll_txs(&[raw], entity)
    }

    fn undo(deltas: &[CardanoDelta], entity: &mut Option<PoolState>) {
        for delta in deltas.iter().rev() {
            match delta {
                CardanoDelta::PoolRegistration(x) => x.undo(entity),
                CardanoDelta::PoolCalidusUpdate(x) => x.undo(entity),
                other => panic!("unexpected pool delta: {other:?}"),
            }
        }
    }

    fn calidus(entity: &Option<PoolState>) -> Option<&CalidusRegistration> {
        entity.as_ref().unwrap().calidus.as_ref()
    }

    fn pub_key(entity: &Option<PoolState>) -> Option<[u8; 32]> {
        calidus(entity).and_then(|x| x.pub_key)
    }

    #[test]
    fn a_valid_registration_sets_the_key() {
        let mut entity = Some(pool_state());
        let raw = metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true);
        roll(&raw, &mut entity);

        let block = OwnedMultiEraBlock::decode(raw).unwrap();
        let tx_hash = block.view().txs()[0].hash();

        assert_eq!(
            entity.unwrap().calidus,
            Some(CalidusRegistration {
                pub_key: Some([0x57; 32]),
                nonce: 7,
                tx_hash,
                slot: 1_000,
            })
        );
    }

    #[test]
    fn an_invalid_transaction_registers_nothing() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(POOL, 7, [0x57; 32]), false),
            &mut entity,
        );

        assert_eq!(calidus(&entity), None);
    }

    #[test]
    fn a_higher_nonce_replaces_the_key() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(2_000, registration(POOL, 8, [0x58; 32]), true),
            &mut entity,
        );

        assert_eq!(pub_key(&entity), Some([0x58; 32]));
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    #[test]
    fn a_lower_nonce_in_a_later_block_is_ignored() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true),
            &mut entity,
        );
        let before = entity.clone();

        roll(
            &metadata_block(2_000, registration(POOL, 3, [0x58; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(3_000, registration(POOL, 7, [0x59; 32]), true),
            &mut entity,
        );

        assert_eq!(entity, before);
    }

    #[test]
    fn a_higher_nonce_earlier_in_a_block_wins() {
        let higher = metadata_block(1_000, registration(POOL, 8, [0x58; 32]), true);
        let lower = metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true);
        let mut entity = Some(pool_state());
        roll_txs(&[&higher, &lower], &mut entity);

        assert_eq!(pub_key(&entity), Some([0x58; 32]));
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    #[test]
    fn a_zero_key_revokes() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(2_000, registration(POOL, 8, [0; 32]), true),
            &mut entity,
        );

        assert_eq!(pub_key(&entity), None);
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    #[test]
    fn a_lower_nonce_after_a_revocation_is_ignored() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(POOL, 8, [0; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(2_000, registration(POOL, 7, [0x57; 32]), true),
            &mut entity,
        );

        assert_eq!(pub_key(&entity), None);
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    #[test]
    fn a_registration_in_the_pools_own_registration_transaction_is_kept() {
        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            metadata_entries: registration(POOL, 7, [0x57; 32]),
            ..Default::default()
        });
        let mut entity = None;
        let deltas = roll(&blocks[0], &mut entity);

        assert!(matches!(
            deltas.as_slice(),
            [
                CardanoDelta::PoolRegistration(_),
                ..,
                CardanoDelta::PoolCalidusUpdate(_)
            ]
        ));
        assert_eq!(pub_key(&entity), Some([0x57; 32]));
    }

    #[test]
    fn a_registration_before_the_pools_first_certificate_is_ignored() {
        let early = metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true);
        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            ..Default::default()
        });
        let mut entity = None;
        roll_txs(&[&early, &blocks[0]], &mut entity);

        assert!(entity.is_some(), "the later transaction registers the pool");
        assert_eq!(calidus(&entity), None);
    }

    #[test]
    fn a_registration_for_an_unknown_pool_is_ignored() {
        let mut entity = None;
        roll(
            &metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true),
            &mut entity,
        );

        assert_eq!(entity, None);
    }

    #[test]
    fn undo_restores_the_previous_key() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true),
            &mut entity,
        );
        let before = entity.clone();

        let deltas = roll(
            &metadata_block(2_000, registration(POOL, 8, [0x58; 32]), true),
            &mut entity,
        );
        assert_eq!(pub_key(&entity), Some([0x58; 32]));

        undo(&deltas, &mut entity);
        assert_eq!(entity, before);
    }

    #[test]
    fn undo_of_an_ignored_registration_keeps_the_key() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(POOL, 7, [0x57; 32]), true),
            &mut entity,
        );
        let before = entity.clone();

        let deltas = roll(
            &metadata_block(2_000, registration(POOL, 3, [0x58; 32]), true),
            &mut entity,
        );
        undo(&deltas, &mut entity);

        assert_eq!(entity, before);
    }

    /// A chain that opens on Byron's pparams carries no pool deposit, so a
    /// pool registration reached before the Shelley fork is a typed error
    /// rather than a panic.
    #[test]
    fn pool_registration_without_pool_deposit_is_a_typed_error() {
        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            ..Default::default()
        });
        let block = MultiEraBlock::decode(&blocks[0]).unwrap();

        let genesis = crate::include::devnet::load();
        let pparams = crate::forks::from_byron_genesis(&genesis.byron);

        let mut visitor = PoolStateVisitor::default();
        let mut deltas = WorkDeltas::default();

        visitor
            .visit_root(&mut deltas, &block, &genesis, &pparams, 0, 0, 0)
            .unwrap();

        let txs = block.txs();
        let cert = txs[0]
            .certs()
            .into_iter()
            .find(|cert| pallas_extras::cert_as_pool_registration(cert).is_some())
            .unwrap();

        let err = visitor
            .visit_cert(&mut deltas, &block, &txs[0], &0, &cert)
            .unwrap_err();

        assert!(matches!(
            err,
            ChainError::StartingPParamMissing("pool_deposit")
        ));
    }
}
