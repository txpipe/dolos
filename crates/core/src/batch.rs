use std::{collections::HashMap, ops::RangeInclusive};

use itertools::Itertools as _;
use rayon::prelude::*;

use crate::{
    ArchiveStore, ArchiveWriter as _, Block as _, BlockSlot, ChainLogic, ChainPoint, Domain,
    DomainError, EntityDelta, EntityMap, LogValue, NsKey, RawBlock, RawUtxoMap, SlotTags,
    StateError, StateStore as _, StateWriter as _, TxoRef, WalStore as _,
};

pub struct WorkDeltas<C: ChainLogic> {
    pub entities: HashMap<NsKey, Vec<C::Delta>>,
    pub slot: SlotTags,
}

impl<C: ChainLogic> Default for WorkDeltas<C> {
    fn default() -> Self {
        Self {
            entities: HashMap::new(),
            slot: SlotTags::default(),
        }
    }
}

impl<C: ChainLogic> WorkDeltas<C> {
    pub fn add_for_entity(&mut self, delta: impl Into<C::Delta>) {
        let delta = delta.into();
        let key = delta.key();
        let group = self.entities.entry(key.into_owned()).or_default();
        group.push(delta);
    }
}

pub struct WorkBlock<C: ChainLogic> {
    pub raw: RawBlock,
    pub decoded: Option<C::Block>,
    pub deltas: WorkDeltas<C>,
}

impl<C: ChainLogic> WorkBlock<C> {
    pub fn unwrap_slot(&self) -> BlockSlot {
        self.decoded.as_ref().unwrap().slot()
    }

    pub fn unwrap_decoded(&self) -> &C::Block {
        self.decoded.as_ref().unwrap()
    }

    pub fn unwrap_deps(&self, loaded: &mut RawUtxoMap) -> Vec<TxoRef> {
        self.decoded.as_ref().unwrap().depends_on(loaded)
    }

    pub fn unwrap_point(&self) -> ChainPoint {
        let decoded = self.unwrap_decoded();
        let slot = decoded.slot();
        let hash = decoded.hash();
        ChainPoint::Specific(slot, hash)
    }

    pub fn decode(&mut self, chain: &C) -> Result<(), DomainError> {
        let decoded = chain.decode_block(self.raw.clone())?;
        self.decoded = Some(decoded);

        Ok(())
    }
}

pub struct WorkBatch<C: ChainLogic> {
    blocks: Vec<WorkBlock<C>>,

    entities: EntityMap<C::Entity>,
    utxos: RawUtxoMap,
    utxos_decoded: HashMap<TxoRef, C::Utxo>,

    // internal checks
    is_sorted: bool,
}

// impl<C: ChainLogic> std::ops::AddAssign<Self> for WorkBatch<C> {
//     fn add_assign(&mut self, rhs: Self) {
//         for (key, deltas) in rhs.deltas {
//             let entry = self.deltas.entry(key).or_default();
//             entry.extend(deltas);
//         }

//         self.entities.extend(rhs.entities);

//         self.new_cursor = self.new_cursor.max(rhs.new_cursor);
//     }
// }

impl<C: ChainLogic> WorkBatch<C> {
    fn new(blocks: Vec<WorkBlock<C>>, is_sorted: bool) -> Self {
        Self {
            blocks,
            entities: HashMap::new(),
            utxos: HashMap::new(),
            utxos_decoded: HashMap::new(),
            is_sorted,
        }
    }

    pub fn from_raw_batch(raw: Vec<RawBlock>) -> Self {
        let blocks = raw
            .into_iter()
            .map(|raw| WorkBlock {
                raw,
                decoded: None,
                deltas: WorkDeltas::default(),
            })
            .collect();

        Self::new(blocks, false)
    }

    pub fn iter_raw(&self) -> impl Iterator<Item = (ChainPoint, &RawBlock)> {
        self.blocks.iter().map(|x| (x.unwrap_point(), &x.raw))
    }

    fn compile_all_entity_keys(&self) -> impl Iterator<Item = &NsKey> {
        self.blocks
            .iter()
            .flat_map(|x| x.deltas.entities.keys())
            .unique()
    }

    fn sort_by_slot(&mut self) {
        self.blocks.sort_by_key(|x| x.unwrap_slot());
        self.is_sorted = true;
    }

    pub fn first_slot(&self) -> BlockSlot {
        debug_assert!(self.is_sorted);

        self.blocks.first().unwrap().unwrap_slot()
    }

    pub fn last_slot(&self) -> BlockSlot {
        debug_assert!(self.is_sorted);

        self.blocks.last().unwrap().unwrap_slot()
    }

    pub fn last_point(&self) -> ChainPoint {
        debug_assert!(self.is_sorted);

        self.blocks.last().unwrap().unwrap_point()
    }

    fn range(&self) -> RangeInclusive<BlockSlot> {
        debug_assert!(!self.blocks.is_empty());

        let start = self.first_slot();
        let end = self.last_slot();

        start..=end
    }

    fn split_at(mut self, slot: BlockSlot) -> (Self, Self) {
        debug_assert!(self.entities.is_empty());
        debug_assert!(self.is_sorted);
        debug_assert!(self.blocks.len() > 1);

        let mut before = Vec::new();
        let mut after = Vec::new();

        for block in self.blocks.drain(..) {
            if block.unwrap_slot() <= slot {
                before.push(block);
            } else {
                after.push(block);
            }
        }

        let before = Self::new(before, true);
        let after = Self::new(after, true);

        (before, after)
    }

    pub fn decode_blocks(&mut self, chain: &C) -> Result<(), DomainError> {
        for block in self.blocks.iter_mut() {
            block.decode(chain)?;
        }

        self.sort_by_slot();

        Ok(())
    }

    pub fn split_by_sweep<D>(
        self,
        domain: &D,
    ) -> Result<(Self, Option<(BlockSlot, Self)>), DomainError>
    where
        D: Domain<Chain = C>,
    {
        let range = self.range();
        let next_sweep = domain.chain().next_sweep(domain, *range.start())?;

        if !range.contains(&next_sweep) {
            Ok((self, None))
        } else {
            let (before, after) = self.split_at(next_sweep);

            Ok((before, Some((next_sweep, after))))
        }
    }

    pub fn load_utxos<D>(&mut self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = C>,
    {
        // TODO: paralelize in chunks

        let all_refs: Vec<_> = self
            .blocks
            .iter()
            .flat_map(|x| x.unwrap_deps(&mut self.utxos))
            .unique()
            .collect();

        let inputs: HashMap<_, _> = domain.state().get_utxos(all_refs)?.into_iter().collect();

        self.utxos.extend(inputs);

        Ok(())
    }

    pub fn decode_utxos(&mut self, chain: &C) -> Result<(), DomainError> {
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

    pub fn compute_delta<D>(&mut self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = C>,
    {
        for block in self.blocks.iter_mut() {
            domain.chain().compute_delta(block, &self.utxos_decoded)?;
        }

        Ok(())
    }

    pub fn commit_utxo_set<D>(&self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = C>,
    {
        let deltas: Vec<_> = self
            .blocks
            .iter()
            .map(|b| {
                domain
                    .chain()
                    .compute_block_utxo_delta(b.unwrap_decoded(), &self.utxos)
            })
            .collect::<Result<_, _>>()?;

        domain.state().apply_utxoset(&deltas)?;

        Ok(())
    }

    pub fn commit_wal<D>(&self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = C, EntityDelta = C::Delta>,
    {
        debug_assert!(self.is_sorted);

        let mut entries = Vec::new();

        for block in self.blocks.iter() {
            let point = block.unwrap_point();
            let delta = block.deltas.entities.values().flatten().cloned().collect();

            let value = LogValue {
                block: (*block.raw).clone(),
                delta,
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
        D: Domain<Chain = C, Entity = C::Entity>,
    {
        // TODO: semantics for starting a read transaction

        let mut keys: Vec<_> = self.compile_all_entity_keys().cloned().collect();

        keys.sort();

        let result = keys
            .par_chunks(Self::LOAD_CHUNK_SIZE)
            .map(|chunk| crate::state::load_entity_chunk::<D>(chunk, domain.state()))
            .try_reduce(
                || EntityMap::new(),
                |mut acc, x| {
                    acc.extend(x);
                    Ok(acc)
                },
            )?;

        self.entities.extend(result);

        Ok(())
    }

    pub fn apply_entities(&mut self) -> Result<(), StateError> {
        for (key, entity) in self.entities.iter_mut() {
            for block in self.blocks.iter_mut() {
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

    pub fn commit_state<D: Domain>(&mut self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = C>,
    {
        let writer = domain.state().start_writer()?;

        for (key, entity) in self.entities.iter_mut() {
            let NsKey(ns, key) = key;

            writer.save_entity_typed(ns, &key, entity.as_ref())?;
        }

        writer.set_cursor(self.last_point())?;

        writer.commit()?;

        Ok(())
    }

    pub fn commit_archive<D: Domain>(&mut self, domain: &D) -> Result<(), DomainError>
    where
        D: Domain<Chain = C>,
    {
        let writer = domain.archive().start_writer()?;

        for block in self.blocks.iter() {
            let point = block.unwrap_point();
            let raw = block.raw.clone();
            let tags = &block.deltas.slot;

            writer.apply(&point, &raw, tags)?;
        }

        writer.commit()?;

        Ok(())
    }
}
