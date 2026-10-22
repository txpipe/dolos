//! Live-UTxO tags through the real pipeline and its WAL: after every roll
//! forward and every rollback, each tag holds exactly the entries the UTxO set
//! implies, each at its UTxO's creation position.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use dolos_cardano::indexes::utxo_tags;
use dolos_core::{
    sync::SyncExt as _, ChainPoint, Domain as _, RawBlock, StateStore as _, TagDimension, TagEntry,
    TagOrder, TagStart, TxoRef, UtxoPosition, UtxoValue, WalStore as _,
};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};

type TagKey = (TagDimension, Vec<u8>);

fn chain(cfg: SyntheticBlockConfig) -> (Vec<RawBlock>, ToyDomain) {
    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    (blocks, domain)
}

fn point(block: &RawBlock) -> ChainPoint {
    let block = MultiEraBlock::decode(block).unwrap();
    ChainPoint::Specific(block.slot(), block.hash())
}

/// Every tag the chain's outputs carry.
fn chain_tags(blocks: &[RawBlock]) -> BTreeSet<TagKey> {
    let mut tags = BTreeSet::new();

    for block in blocks {
        let block = MultiEraBlock::decode(block).unwrap();

        for tx in block.txs() {
            for (_, output) in tx.produces() {
                tags.extend(
                    utxo_tags(&output)
                        .into_iter()
                        .map(|tag| (tag.dimension, tag.key)),
                );
            }
        }
    }

    tags
}

/// The UTxO set, positions included.
fn utxo_set(domain: &ToyDomain) -> Vec<(TxoRef, UtxoValue)> {
    domain
        .state()
        .iter_utxos()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Under each of `tags`, the entries the store holds.
fn held(domain: &ToyDomain, tags: &BTreeSet<TagKey>) -> BTreeMap<TagKey, Vec<TagEntry>> {
    tags.iter()
        .map(|(dimension, key)| {
            let entries = domain
                .state()
                .utxos_by_tag_page(dimension, key, TagOrder::Asc, TagStart::Skip(0), usize::MAX)
                .unwrap();

            ((*dimension, key.clone()), entries)
        })
        .collect()
}

/// Under each of `tags`, the entries the UTxO set implies: every UTxO whose
/// output carries the tag, at its creation position, in tag order.
fn implied(domain: &ToyDomain, tags: &BTreeSet<TagKey>) -> BTreeMap<TagKey, Vec<TagEntry>> {
    let mut implied: BTreeMap<TagKey, Vec<TagEntry>> =
        tags.iter().map(|tag| (tag.clone(), Vec::new())).collect();

    for (txo, value) in utxo_set(domain) {
        let output = MultiEraOutput::try_from(&value).unwrap();

        for tag in utxo_tags(&output) {
            if let Some(entries) = implied.get_mut(&(tag.dimension, tag.key)) {
                entries.push(TagEntry::new(value.position(), txo.clone()));
            }
        }
    }

    for entries in implied.values_mut() {
        entries.sort();
    }

    implied
}

/// Rolls `blocks` forward one at a time, checking the tags against the set
/// after each, and returns the set and the tags after each block.
fn roll_forward(
    domain: &ToyDomain,
    blocks: &[RawBlock],
    tags: &BTreeSet<TagKey>,
) -> Vec<(Vec<(TxoRef, UtxoValue)>, BTreeMap<TagKey, Vec<TagEntry>>)> {
    blocks
        .iter()
        .enumerate()
        .map(|(i, block)| {
            domain.roll_forward(block.clone()).unwrap();

            let held = held(domain, tags);
            assert_eq!(held, implied(domain, tags), "after block {i}");

            (utxo_set(domain), held)
        })
        .collect()
}

/// Rolls back one block at a time, checking that the set and the tags are
/// what they were after each block still applied.
fn roll_back(
    domain: &ToyDomain,
    blocks: &[RawBlock],
    tags: &BTreeSet<TagKey>,
    after: &[(Vec<(TxoRef, UtxoValue)>, BTreeMap<TagKey, Vec<TagEntry>>)],
) {
    for i in (0..blocks.len() - 1).rev() {
        domain.rollback(&point(&blocks[i])).unwrap();

        let held = held(domain, tags);
        assert_eq!(held, implied(domain, tags), "rolled back to block {i}");
        assert_eq!(
            (utxo_set(domain), held),
            after[i],
            "rolled back to block {i}"
        );
    }
}

/// Each tx after the first of a block spends the output before it, so each
/// block produces and spends the same outputs. Undoing such a block leaves
/// them no tag: the WAL inputs hold them, so the undo both removes and
/// recovers them, and the tags must end as the block found them.
#[test]
fn rolling_back_a_block_that_spends_its_own_outputs_leaves_no_tag_behind() {
    let (blocks, domain) = chain(SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 3,
        spend_within_block: true,
        ..Default::default()
    });

    for block in &blocks {
        let block = MultiEraBlock::decode(block).unwrap();

        let produced: HashSet<TxoRef> = block
            .txs()
            .iter()
            .flat_map(|tx| {
                let hash = tx.hash();
                tx.produces()
                    .into_iter()
                    .map(move |(index, _)| TxoRef(hash, index as u32))
            })
            .collect();

        assert!(
            block
                .txs()
                .iter()
                .flat_map(|tx| tx.consumes())
                .any(|input| produced.contains(&TxoRef::from(&input))),
            "the block spends none of its own outputs"
        );
    }

    let tags = chain_tags(&blocks);
    let after = roll_forward(&domain, &blocks, &tags);

    roll_back(&domain, &blocks, &tags, &after);
}

/// Each tx spends the output the previous block's tx at its index produced,
/// so undoing a block recovers UTxOs another block created. The WAL keeps them
/// with their position, and they come back at it, in the set and in every
/// tag.
#[test]
fn a_rollback_recovers_spent_outputs_at_their_creation_position() {
    let (blocks, domain) = chain(SyntheticBlockConfig {
        block_count: 3,
        spend_previous_outputs: true,
        ..Default::default()
    });

    let tags = chain_tags(&blocks);
    let after = roll_forward(&domain, &blocks, &tags);

    let first = MultiEraBlock::decode(&blocks[0]).unwrap();
    let entry = domain
        .wal()
        .read_entry(&point(&blocks[1]))
        .unwrap()
        .unwrap();

    for (tx_index, tx) in first.txs().iter().enumerate() {
        let input = &entry.inputs[&TxoRef(tx.hash(), 0)];

        assert_eq!(
            input.position(),
            Some(UtxoPosition::of_tx(first.slot(), tx_index)),
            "the WAL input of tx {tx_index}"
        );
    }

    roll_back(&domain, &blocks, &tags, &after);
}
