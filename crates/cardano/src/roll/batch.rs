//! Batch processing types for block roll work units.
//!
//! This module contains the types used to batch and process blocks during
//! the "roll" phase of chain synchronization.

use std::{collections::HashMap, ops::RangeInclusive};

use itertools::Itertools as _;
use rayon::prelude::*;

use dolos_core::{
    ArchiveStore, ArchiveWriter as _, Block as _, BlockSlot, ChainLogic, ChainPoint, Domain,
    DomainError, EntityDelta, EntityMap, IndexStore as _, IndexWriter as _, LogValue, NsKey,
    RawBlock, RawUtxoMap, SlotTags, StateError, StateStore as _, StateWriter as _, TxoRef,
    UtxoSetDelta, WalStore as _,
};

use crate::{CardanoDelta, CardanoEntity, CardanoLogic, OwnedMultiEraBlock, OwnedMultiEraOutput};

/// Container for entity deltas computed during block processing.
#[derive(Debug)]
pub struct WorkDeltas {
    pub entities: HashMap<NsKey, Vec<CardanoDelta>>,
    pub slot: SlotTags,
}

impl Default for WorkDeltas {
    fn default() -> Self {
        Self {
            entities: HashMap::new(),
            slot: SlotTags::default(),
        }
    }
}

impl WorkDeltas {
    pub fn add_for_entity(&mut self, delta: impl Into<CardanoDelta>) {
        let delta = delta.into();
        let key = delta.key();
        let group = self.entities.entry(key).or_default();
        group.push(delta);
    }
}

/// A single block with its computed deltas.
pub struct WorkBlock {
    pub block: OwnedMultiEraBlock,

    // computed afterwards
    pub deltas: WorkDeltas,
    pub utxo_delta: Option<UtxoSetDelta>,
}

impl WorkBlock {
    pub fn new(block: OwnedMultiEraBlock) -> Self {
        Self {
            block,
            deltas: WorkDeltas::default(),
            utxo_delta: None,
        }
    }

    pub fn slot(&self) -> BlockSlot {
        self.block.slot()
    }

    pub fn decoded(&self) -> &OwnedMultiEraBlock {
        &self.block
    }

    pub fn raw(&self) -> RawBlock {
        self.block.raw()
    }

    pub fn depends_on(&self, loaded: &mut RawUtxoMap) -> Vec<TxoRef> {
        self.block.depends_on(loaded)
    }

    pub fn point(&self) -> ChainPoint {
        let decoded = self.decoded();
        let slot = decoded.slot();
        let hash = decoded.hash();
        ChainPoint::Specific(slot, hash)
    }
}

/// A batch of blocks to be processed together.
pub struct WorkBatch {
    pub blocks: Vec<WorkBlock>,
    pub utxos: RawUtxoMap,
    pub utxos_decoded: HashMap<TxoRef, OwnedMultiEraOutput>,

    entities: EntityMap<CardanoEntity>,

    // internal checks
    is_sorted: bool,
}

impl Default for WorkBatch {
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            entities: HashMap::new(),
            utxos: HashMap::new(),
            utxos_decoded: HashMap::new(),
            is_sorted: false,
        }
    }
}

impl WorkBatch {
    pub fn for_single_block(block: WorkBlock) -> Self {
        let mut batch = Self::default();
        batch.add_work(block);
        batch.is_sorted = true;
        batch
    }

    pub fn sort_by_slot(&mut self) {
        self.blocks.sort_by_key(|x| x.block.slot());
        self.is_sorted = true;
    }

    pub fn add_work(&mut self, work: WorkBlock) {
        self.blocks.push(work);
        self.is_sorted = false;
    }

    pub fn iter_blocks(&self) -> impl Iterator<Item = &OwnedMultiEraBlock> {
        self.blocks.iter().map(|x| &x.block)
    }

    fn compile_all_entity_keys(&self) -> impl Iterator<Item = &NsKey> {
        self.blocks
            .iter()
            .flat_map(|x| x.deltas.entities.keys())
            .unique()
    }

    pub fn first_point(&self) -> ChainPoint {
        self.blocks.first().unwrap().point()
    }

    pub fn first_slot(&self) -> BlockSlot {
        let point = self.first_point();
        point.slot()
    }

    pub fn last_slot(&self) -> BlockSlot {
        self.blocks.last().unwrap().slot()
    }

    pub fn last_point(&self) -> ChainPoint {
        self.blocks.last().unwrap().point()
    }

    #[allow(dead_code)]
    fn range(&self) -> RangeInclusive<BlockSlot> {
        debug_assert!(!self.blocks.is_empty());

        let start = self.first_slot();
        let end = self.last_slot();

        start..=end
    }

    pub fn load_utxos<D>(&mut self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = CardanoLogic>,
    {
        // TODO: paralelize in chunks

        let all_refs: Vec<_> = self
            .blocks
            .iter()
            .flat_map(|x| x.depends_on(&mut self.utxos))
            .unique()
            .collect();

        let inputs: HashMap<_, _> = domain.state().get_utxos(all_refs)?.into_iter().collect();

        self.utxos.extend(inputs);

        Ok(())
    }

    pub fn decode_utxos(&mut self, chain: &CardanoLogic) -> Result<(), DomainError> {
        let pairs: Vec<_> = self
            .utxos
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let decoded: HashMap<_, _> = pairs
            .par_chunks(100)
            .flatten_iter()
            .map(|(k, v)| chain.decode_utxo(v.clone()).map(|x| (k.clone(), x)))
            .collect::<Result<_, _>>()?;

        self.utxos_decoded = decoded;

        Ok(())
    }

    pub fn commit_wal<D>(&self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = CardanoLogic, EntityDelta = CardanoDelta>,
    {
        debug_assert!(self.is_sorted);

        let mut entries = Vec::new();

        for block in self.blocks.iter() {
            let point = block.point();
            let raw = block.raw();
            let delta = block.deltas.entities.values().flatten().cloned().collect();

            let value = LogValue {
                block: (*raw).clone(),
                delta,
                inputs: self.utxos.clone(),
            };

            entries.push((point.clone(), value));
        }

        domain.wal().append_entries(entries)?;

        Ok(())
    }

    const LOAD_CHUNK_SIZE: usize = 100;

    /// Loads the entities involved in a batch of deltas
    ///
    /// This methods is a fancy way of loading required entities for a batch of
    /// deltas. It optimizes the process by organizing read operations in chunks
    /// that execute in parallel using Rayon. The assumption is that the storage
    /// backend supports concurrent reads (eg: Redb).
    ///
    /// Chunks are defined by sorting the entity keys grouping by namespace. The
    /// assumption is that the storage backend will benefit from loading keys
    /// that are close to each other (eg: disk block reads)
    pub fn load_entities<D>(&mut self, domain: &D) -> Result<(), StateError>
    where
        D: Domain<Chain = CardanoLogic, Entity = CardanoEntity>,
    {
        // TODO: semantics for starting a read transaction

        let mut keys: Vec<_> = self.compile_all_entity_keys().cloned().collect();

        keys.sort();

        let result = keys
            .par_chunks(Self::LOAD_CHUNK_SIZE)
            .map(|chunk| dolos_core::state::load_entity_chunk::<D>(chunk, domain.state()))
            .try_reduce(EntityMap::new, |mut acc, x| {
                acc.extend(x);
                Ok(acc)
            })?;

        self.entities.extend(result);

        Ok(())
    }

    pub fn apply_entities(&mut self) -> Result<(), StateError> {
        for block in self.blocks.iter_mut() {
            for (key, entity) in self.entities.iter_mut() {
                let to_apply = block.deltas.entities.get_mut(key);

                if let Some(to_apply) = to_apply {
                    for delta in to_apply {
                        delta.apply(entity);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn commit_state<D>(&mut self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = CardanoLogic>,
    {
        let writer = domain.state().start_writer()?;

        for (key, entity) in self.entities.iter_mut() {
            let NsKey(ns, key) = key;
            writer.save_entity_typed(ns, key, entity.as_ref())?;
        }

        // TODO: we treat the UTxO set differently due to tech-debt. We should migrate
        // this into the entity system.
        for block in self.blocks.iter() {
            if let Some(utxo_delta) = &block.utxo_delta {
                writer.apply_utxoset(utxo_delta)?;
            }
        }

        writer.set_cursor(self.last_point())?;

        writer.commit()?;

        Ok(())
    }

    pub fn commit_archive<D>(&mut self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = CardanoLogic>,
    {
        let writer = domain.archive().start_writer()?;

        for block in self.blocks.iter() {
            let point = block.point();
            let raw = block.raw();
            let tags = &block.deltas.slot;

            writer.apply(&point, &raw, tags)?;
        }

        writer.commit()?;

        Ok(())
    }

    pub fn commit_indexes<D>(&mut self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = CardanoLogic>,
    {
        let writer = domain.indexes().start_writer()?;

        // UTxO filter indexes
        for block in self.blocks.iter() {
            if let Some(utxo_delta) = &block.utxo_delta {
                writer.apply_utxoset(utxo_delta)?;
            }
        }

        // Archive indexes
        for block in self.blocks.iter() {
            let point = block.point();
            let tags = &block.deltas.slot;

            writer.apply_archive(&point, tags)?;
        }

        // Set cursor to track last indexed point
        writer.set_cursor(self.last_point())?;

        writer.commit()?;

        Ok(())
    }
}
