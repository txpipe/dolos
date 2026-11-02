use std::collections::{HashMap, HashSet};
use std::ops::Deref;

use dolos_core::{
    keys, ArchiveStore, BlockSlot, ChainError, Entity as _, Genesis, KeyRange, StateStore, TxOrder,
    TxoRef,
};
use pallas::crypto::hash::{Hash, Hasher};
use pallas::ledger::primitives::Epoch;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraCert, MultiEraTx};

use super::WorkDeltas;
use crate::owned::OwnedMultiEraOutput;
use crate::roll::BlockVisitor;
use crate::{cip151, pallas_extras};
use crate::{
    CalidusRegistration, FixedNamespace as _, MintedBlocksInc, PParamsSet, PoolCalidusUpdate,
    PoolDeRegistration, PoolHash, PoolKey, PoolRegistration, PoolState,
};

/// The pools a block registers, from its valid transactions.
pub fn block_pool_registrations(block: &MultiEraBlock) -> Vec<PoolHash> {
    block
        .txs()
        .iter()
        .filter(|tx| tx.is_valid())
        .flat_map(|tx| tx.certs())
        .filter_map(|cert| pallas_extras::cert_as_pool_registration(&cert))
        .map(|cert| cert.operator)
        .collect()
}

/// The CIP-151 nonce floors of the pools a batch registers while only the
/// final-row log holds them.
///
/// A moved pool that registers again gets a fresh row without the calidus key
/// its final row carried (founder ruling, 2026-10-25), but the nonce stays: on
/// a node that holds the final row, a signed registration replayed with a
/// lower nonce is still rejected. A node that has pruned the final row has no
/// floor and accepts the replay. The floor is the final row's registration
/// with no key, seeded right after the certificate that makes the fresh row,
/// so the transaction's own registration lands on top of it. A visitor cannot
/// read the stores, so the floors are read once per batch, ahead of the crawl,
/// and each is spent by the registration that seeds it.
#[derive(Debug, Default, Clone)]
pub struct CalidusFloors {
    floors: HashMap<PoolHash, CalidusRegistration>,
}

impl CalidusFloors {
    /// Read the floors for a batch starting at `first_slot` whose blocks
    /// register `registered`.
    ///
    /// Only pools without a state row read the log, in one pass over it. The
    /// slot bound keeps a re-roll over an archive that already holds later
    /// moves (`data rebuild`) from reading a final row the pool has
    /// not reached yet: a row is keyed after the registration that precedes
    /// its retirement.
    pub fn load<S: StateStore, A: ArchiveStore>(
        state: &S,
        archive: &A,
        registered: &[Vec<PoolHash>],
        first_slot: BlockSlot,
    ) -> Result<Self, ChainError> {
        let pools: Vec<PoolHash> = registered
            .iter()
            .flatten()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let keys: Vec<PoolKey> = pools.into_iter().map(PoolKey).collect();
        let rows = state.read_entities_typed::<PoolState>(&keys.iter().collect::<Vec<_>>())?;

        let wanted: HashMap<Vec<u8>, PoolHash> = keys
            .iter()
            .zip(rows)
            .filter(|(_, row)| row.is_none())
            .map(|(key, _)| (keys::encode(key).to_vec(), key.0))
            .collect();

        if wanted.is_empty() {
            return Ok(Self::default());
        }

        // the log is keyed slot first, so a later row of a pool replaces an
        // earlier one
        let mut newest = HashMap::new();

        for row in archive.iter_logs(PoolState::NS, KeyRange::slots(0..first_slot))? {
            let (log_key, value) = row?;

            if let Some(pool) = wanted.get(log_key.entity()) {
                newest.insert(*pool, value);
            }
        }

        let rows = newest
            .into_iter()
            .map(|(pool, value)| Ok((pool, PoolState::decode_entity(PoolState::NS, &value)?)))
            .collect::<Result<Vec<_>, ChainError>>()?;

        Ok(Self::from_final_rows(rows))
    }

    /// The floors of moved pools, from each one's newest final row.
    pub fn from_final_rows(rows: impl IntoIterator<Item = (PoolHash, PoolState)>) -> Self {
        let floors = rows
            .into_iter()
            .filter_map(|(pool, row)| {
                let calidus = row.calidus?;

                Some((
                    pool,
                    CalidusRegistration {
                        pub_key: None,
                        ..calidus
                    },
                ))
            })
            .collect();

        Self { floors }
    }

    /// The floor a registration of `pool` would seed.
    pub fn floor(&self, pool: &PoolHash) -> Option<&CalidusRegistration> {
        self.floors.get(pool)
    }
}

#[derive(Default, Clone)]
pub struct PoolStateVisitor {
    epoch: Option<Epoch>,
    deposit: Option<u64>,
    floors: CalidusFloors,

    /// The current transaction's CIP-151 registration, emitted after its
    /// certificates: it finds the row its own transaction creates, and none
    /// a later transaction creates.
    calidus: Option<PoolCalidusUpdate>,
}

impl PoolStateVisitor {
    pub fn new(floors: CalidusFloors) -> Self {
        Self {
            floors,
            ..Default::default()
        }
    }

    /// The floors this block's registrations have not spent.
    pub fn take_floors(&mut self) -> CalidusFloors {
        std::mem::take(&mut self.floors)
    }
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

        if !cip151::cip151_registration_is_signed(&metadata, &registration) {
            return Ok(());
        }

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

            if let Some(floor) = self.floors.floors.remove(&cert.operator) {
                deltas.add_for_entity(PoolCalidusUpdate::new(cert.operator, floor));
            }
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
    use dolos_testing::cip151 as signer;
    use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
    use pallas::crypto::key::ed25519::SecretKey;
    use pallas::ledger::primitives::alonzo::Metadatum;

    use super::*;
    use crate::model::{PParamValue as Val, PoolState};
    use crate::{CardanoDelta, OwnedMultiEraBlock};

    /// The cold key of the pool the synthetic chain registers.
    fn cold_key() -> SecretKey {
        signer::cold_key(9)
    }

    fn pool() -> Hash<28> {
        signer::pool_id(&cold_key())
    }

    /// A CIP-151 registration of `key` for [`pool`], signed by its cold key.
    fn registration(nonce: u64, key: [u8; 32]) -> Vec<(u64, Metadatum)> {
        signer::signed_registration(&cold_key(), nonce, key)
    }

    /// A one-block synthetic chain that registers [`pool`], carrying
    /// `metadata`.
    fn synthetic_chain(metadata: Vec<(u64, Metadatum)>) -> Vec<RawBlock> {
        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            pool_id: signer::pool_bech32(&cold_key()),
            metadata_entries: metadata,
            ..Default::default()
        });
        blocks
    }

    fn metadata_block(slot: u64, metadata: Vec<(u64, Metadatum)>, valid: bool) -> RawBlock {
        dolos_testing::blocks::make_conway_block_with_metadata(slot, metadata, valid).1
    }

    fn pool_state() -> PoolState {
        let blocks = synthetic_chain(vec![]);
        let mut entity = None;
        roll(&blocks[0], &mut entity);
        let entity = entity.expect("the synthetic chain registers the pool");
        assert_eq!(entity.calidus, None);
        entity
    }

    /// Visit the transactions of `raws` as one block, the way the crawl
    /// does: the first block's root, then each transaction and the
    /// certificates of the valid ones, then the flush, with the batch's
    /// calidus floors.
    fn deltas_of(raws: &[&RawBlock], floors: CalidusFloors) -> WorkDeltas {
        let blocks: Vec<_> = raws
            .iter()
            .map(|raw| OwnedMultiEraBlock::decode((*raw).clone()).unwrap())
            .collect();
        let genesis = crate::include::devnet::load();
        let pparams = PParamsSet::default().with(Val::PoolDeposit(500_000_000));
        let mut visitor = PoolStateVisitor::new(floors);
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
    /// [`pool`] to `entity`, in order, and return them for undo.
    fn roll_txs(raws: &[&RawBlock], entity: &mut Option<PoolState>) -> Vec<CardanoDelta> {
        roll_with_floors(raws, CalidusFloors::default(), entity)
    }

    /// [`roll_txs`], with the batch's calidus floors.
    fn roll_with_floors(
        raws: &[&RawBlock],
        floors: CalidusFloors,
        entity: &mut Option<PoolState>,
    ) -> Vec<CardanoDelta> {
        let mut deltas = deltas_of(raws, floors);
        let key = dolos_core::NsKey::of::<PoolState>(&crate::PoolKey(pool()));
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
        let raw = metadata_block(1_000, registration(7, [0x57; 32]), true);
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
            &metadata_block(1_000, registration(7, [0x57; 32]), false),
            &mut entity,
        );

        assert_eq!(calidus(&entity), None);
    }

    #[test]
    fn an_unsigned_registration_registers_nothing() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(
                1_000,
                signer::unsigned_registration(pool(), 7, [0x57; 32]),
                true,
            ),
            &mut entity,
        );

        assert_eq!(calidus(&entity), None);
    }

    #[test]
    fn a_registration_signed_by_another_key_registers_nothing() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(
                1_000,
                signer::registration_signed_by(pool(), &signer::cold_key(2), 7, [0x57; 32]),
                true,
            ),
            &mut entity,
        );

        assert_eq!(calidus(&entity), None);
    }

    #[test]
    fn an_unsigned_higher_nonce_does_not_displace_a_signed_key() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );
        let before = entity.clone();

        roll(
            &metadata_block(
                2_000,
                signer::unsigned_registration(pool(), u64::MAX, [0x58; 32]),
                true,
            ),
            &mut entity,
        );

        assert_eq!(entity, before);
    }

    #[test]
    fn an_unsigned_revocation_keeps_the_key() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );
        let before = entity.clone();

        roll(
            &metadata_block(
                2_000,
                signer::unsigned_registration(pool(), 8, [0; 32]),
                true,
            ),
            &mut entity,
        );

        assert_eq!(entity, before);
    }

    #[test]
    fn a_higher_nonce_replaces_the_key() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(2_000, registration(8, [0x58; 32]), true),
            &mut entity,
        );

        assert_eq!(pub_key(&entity), Some([0x58; 32]));
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    #[test]
    fn a_lower_nonce_in_a_later_block_is_ignored() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );
        let before = entity.clone();

        roll(
            &metadata_block(2_000, registration(3, [0x58; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(3_000, registration(7, [0x59; 32]), true),
            &mut entity,
        );

        assert_eq!(entity, before);
    }

    #[test]
    fn a_higher_nonce_earlier_in_a_block_wins() {
        let higher = metadata_block(1_000, registration(8, [0x58; 32]), true);
        let lower = metadata_block(1_000, registration(7, [0x57; 32]), true);
        let mut entity = Some(pool_state());
        roll_txs(&[&higher, &lower], &mut entity);

        assert_eq!(pub_key(&entity), Some([0x58; 32]));
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    #[test]
    fn a_zero_key_revokes() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(2_000, registration(8, [0; 32]), true),
            &mut entity,
        );

        assert_eq!(pub_key(&entity), None);
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    #[test]
    fn a_lower_nonce_after_a_revocation_is_ignored() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(8, [0; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(2_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );

        assert_eq!(pub_key(&entity), None);
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    #[test]
    fn a_registration_in_the_pools_own_registration_transaction_is_kept() {
        let blocks = synthetic_chain(registration(7, [0x57; 32]));
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
        let early = metadata_block(1_000, registration(7, [0x57; 32]), true);
        let blocks = synthetic_chain(vec![]);
        let mut entity = None;
        roll_txs(&[&early, &blocks[0]], &mut entity);

        assert!(entity.is_some(), "the later transaction registers the pool");
        assert_eq!(calidus(&entity), None);
    }

    #[test]
    fn a_registration_for_an_unknown_pool_is_ignored() {
        let mut entity = None;
        roll(
            &metadata_block(1_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );

        assert_eq!(entity, None);
    }

    #[test]
    fn undo_restores_the_previous_key() {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );
        let before = entity.clone();

        let deltas = roll(
            &metadata_block(2_000, registration(8, [0x58; 32]), true),
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
            &metadata_block(1_000, registration(7, [0x57; 32]), true),
            &mut entity,
        );
        let before = entity.clone();

        let deltas = roll(
            &metadata_block(2_000, registration(3, [0x58; 32]), true),
            &mut entity,
        );
        undo(&deltas, &mut entity);

        assert_eq!(entity, before);
    }

    /// The pool's row once its registrations of `first` and `revoked` have
    /// landed, as ESTART logs it when it moves the pool: the key revoked.
    fn revoked_final_row(first: u64, revoked: u64) -> PoolState {
        let mut entity = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(first, [0x57; 32]), true),
            &mut entity,
        );
        roll(
            &metadata_block(2_000, registration(revoked, [0; 32]), true),
            &mut entity,
        );

        assert_eq!(pub_key(&entity), None);
        entity.unwrap()
    }

    /// Registered again after the move, the fresh row gets no key, but keeps
    /// the final row's nonce: the replayed registration of the revoked key is
    /// ignored, and a higher nonce registers a key.
    #[test]
    fn a_registration_after_a_move_keeps_the_nonce_floor() {
        let floors = CalidusFloors::from_final_rows([(pool(), revoked_final_row(100, 200))]);
        assert_eq!(floors.floor(&pool()).unwrap().pub_key, None);

        let blocks = synthetic_chain(vec![]);
        let mut entity = None;
        roll_with_floors(&[&blocks[0]], floors, &mut entity);

        let fresh = entity.as_ref().expect("the registration makes a fresh row");
        assert_eq!(fresh.deposit, 500_000_000);
        assert_eq!(pub_key(&entity), None);
        assert_eq!(calidus(&entity).unwrap().nonce, 200);

        roll(
            &metadata_block(3_000, registration(100, [0x57; 32]), true),
            &mut entity,
        );
        assert_eq!(pub_key(&entity), None);
        assert_eq!(calidus(&entity).unwrap().nonce, 200);

        roll(
            &metadata_block(4_000, registration(201, [0x58; 32]), true),
            &mut entity,
        );
        assert_eq!(pub_key(&entity), Some([0x58; 32]));
    }

    /// The floor lands before the registering transaction's own CIP-151
    /// registration: a lower nonce there is ignored, a higher one kept.
    #[test]
    fn the_floor_lands_before_the_registering_transactions_key() {
        let floors = || CalidusFloors::from_final_rows([(pool(), revoked_final_row(7, 8))]);

        let lower = synthetic_chain(registration(5, [0x57; 32]));
        let mut entity = None;
        roll_with_floors(&[&lower[0]], floors(), &mut entity);
        assert_eq!(pub_key(&entity), None);
        assert_eq!(calidus(&entity).unwrap().nonce, 8);

        let higher = synthetic_chain(registration(9, [0x57; 32]));
        let mut entity = None;
        roll_with_floors(&[&higher[0]], floors(), &mut entity);
        assert_eq!(pub_key(&entity), Some([0x57; 32]));
        assert_eq!(calidus(&entity).unwrap().nonce, 9);
    }

    /// A pool moved with its key live, registering again in a transaction
    /// that replays that key's registration: the nonce equals the floor's, so
    /// only the floor landing first keeps the key off the fresh row.
    #[test]
    fn an_equal_nonce_replay_of_a_live_key_is_ignored() {
        let mut live = Some(pool_state());
        roll(
            &metadata_block(1_000, registration(8, [0x57; 32]), true),
            &mut live,
        );
        let floors = CalidusFloors::from_final_rows([(pool(), live.unwrap())]);

        let replay = synthetic_chain(registration(8, [0x57; 32]));
        let mut entity = None;
        roll_with_floors(&[&replay[0]], floors, &mut entity);
        assert_eq!(pub_key(&entity), None);
        assert_eq!(calidus(&entity).unwrap().nonce, 8);
    }

    /// Undoing the registration undoes its floor with it.
    #[test]
    fn undo_of_a_registration_after_a_move_leaves_no_row() {
        let floors = CalidusFloors::from_final_rows([(pool(), revoked_final_row(7, 8))]);

        let blocks = synthetic_chain(vec![]);
        let mut entity = None;
        let deltas = roll_with_floors(&[&blocks[0]], floors, &mut entity);
        assert!(calidus(&entity).is_some());

        undo(&deltas, &mut entity);
        assert_eq!(entity, None);
    }

    /// A final row without a registration seeds nothing.
    #[test]
    fn a_final_row_without_a_registration_has_no_floor() {
        let floors = CalidusFloors::from_final_rows([(pool(), pool_state())]);
        assert_eq!(floors.floor(&pool()), None);
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
