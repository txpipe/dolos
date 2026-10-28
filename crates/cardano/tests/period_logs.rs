//! Period logs through the real pipeline: staging rolls back with its block,
//! and ESTART seals short epochs.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use dolos_cardano::{
    indexes::{
        archive_dimensions::{SCRIPT, STAKE},
        entities, AsyncCardanoQueryExt as _,
    },
    model::{
        AccountAddressChunk, AccountAddressSet, AssetMintChunk, AssetMintSet, FirstMintChunk,
        FirstMintSet, FirstScriptChunk, FirstScriptSet,
    },
    periods::{
        account_addresses,
        assets::{self, AssetEntry, WalkBudget, ASSETS},
        is_sealable,
    },
    AccountKey, AssetKey, CardanoDelta, ChainSummary,
};
use dolos_core::{
    import::ImportExt as _, sync::SyncExt as _, ArchiveIndexDelta, ArchiveStore as _,
    ArchiveWriter as _, AsyncQueryFacade, BlockSlot, CacheStore as _, ChainPoint, Domain as _,
    Genesis, RangeOf, StateStore as _, StateWriter as _, Tag, WalStore as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    synthetic::{build_synthetic_blocks, sample_alonzo_block, SyntheticBlockConfig},
    toy_domain::ToyDomain,
    MIN_UTXO_AMOUNT,
};
use pallas::crypto::hash::Hash;
use pallas::ledger::{
    addresses::{Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart},
    primitives::{alonzo, StakeCredential, TransactionInput},
    traverse::MultiEraBlock,
};

/// Plain, balanced txs: no certificates for a pre-Conway chain to choke on at
/// a boundary, and a seed paying exactly the fee of 7, so the pots hold.
fn plain(cfg: SyntheticBlockConfig) -> SyntheticBlockConfig {
    SyntheticBlockConfig {
        plain_txs: true,
        seed_amount: MIN_UTXO_AMOUNT + 7,
        ..cfg
    }
}

fn staged(domain: &ToyDomain) -> BTreeSet<(u64, Vec<u8>, u64)> {
    domain
        .state()
        .iter_entities_typed::<FirstMintSet>(None)
        .unwrap()
        .map(|row| {
            let (_, row) = row.unwrap();
            (row.epoch, row.subject(), row.slot)
        })
        .collect()
}

/// The synthetic chain's own account, which every tx's first output pays.
fn primary() -> StakeCredential {
    let address = Address::from_bech32(&SyntheticBlockConfig::default().address).unwrap();
    dolos_cardano::pallas_extras::address_as_stake_cred(&address)
        .unwrap()
        .0
}

fn base(payment: u8) -> Vec<u8> {
    let StakeCredential::AddrKeyhash(stake) = primary() else {
        unreachable!("the synthetic account is a key")
    };

    ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::Key([payment; 28].into()),
        ShelleyDelegationPart::Key(stake),
    )
    .to_vec()
}

fn staged_addresses(domain: &ToyDomain) -> BTreeSet<(u64, Vec<u8>, u64)> {
    domain
        .state()
        .iter_entities_typed::<AccountAddressSet>(None)
        .unwrap()
        .map(|row| {
            let (_, row) = row.unwrap();
            (row.epoch, row.address, row.slot)
        })
        .collect()
}

/// Rollback for account addresses: undoing a block removes exactly the staged
/// rows it created, and a repeat within a block never reaches the WAL.
#[test]
fn rollback_unstages_exactly_the_addresses_its_block_created() {
    let cfg = plain(SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 2,
        ada_only_by_block: vec![true; 3],
        extra_outputs_by_block: vec![
            // 0x90 three times across one block's txs: staged once
            vec![vec![base(0x90), base(0x90)], vec![base(0x90)]],
            // 0x90 again in a later block, same epoch: still the first row
            vec![vec![base(0x90), base(0x91)], vec![]],
            vec![vec![base(0x92)], vec![]],
        ],
        ..Default::default()
    });

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    let mut points = Vec::new();
    let mut after = Vec::new();

    for block in &blocks {
        domain.roll_forward(block.clone()).unwrap();

        let decoded = MultiEraBlock::decode(block).unwrap();
        points.push(ChainPoint::Specific(decoded.slot(), decoded.hash()));
        after.push(staged_addresses(&domain));
    }

    let slot_of = |set: &BTreeSet<(u64, Vec<u8>, u64)>, address: &[u8]| {
        set.iter()
            .find(|(_, x, _)| x == address)
            .map(|(_, _, slot)| *slot)
    };

    let first = slot_of(&after[0], &base(0x90)).expect("staged by the first block");
    assert_eq!(slot_of(&after[1], &base(0x90)), Some(first));
    assert!(slot_of(&after[0], &base(0x91)).is_none());
    assert!(after[1].len() > after[0].len());
    assert!(after[2].len() > after[1].len());

    // the repeats inside the first block never reached the WAL: one row per
    // distinct address its outputs pay
    let entry = domain.wal().read_entry(&points[0]).unwrap().unwrap();
    let stagings = entry
        .delta
        .iter()
        .filter(|delta| matches!(delta, CardanoDelta::StageAccountAddress(_)))
        .count();
    assert_eq!(stagings, after[0].len());

    domain.rollback(&points[1]).unwrap();
    assert_eq!(staged_addresses(&domain), after[1]);

    domain.rollback(&points[0]).unwrap();
    assert_eq!(staged_addresses(&domain), after[0]);
}

/// Done criterion 1, rollback: undoing a block removes exactly the staged rows
/// it created, and nothing a later or earlier block staged.
#[test]
fn rollback_unstages_exactly_the_rows_its_block_created() {
    let policy = [0x44u8; 28];
    let mint = |name: &str| (policy, name.to_string(), 1);

    let cfg = plain(SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 2,
        mints_by_block: vec![
            // "A" twice in one block: the second stages nothing
            vec![vec![mint("A"), mint("B")], vec![mint("A")]],
            // "A" again in a later block, same epoch: still the first row
            vec![vec![mint("A")], vec![mint("C")]],
            vec![vec![mint("D")], vec![]],
        ],
        ..Default::default()
    });

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    let mut points = Vec::new();
    let mut after = Vec::new();

    for block in &blocks {
        domain.roll_forward(block.clone()).unwrap();

        let decoded = MultiEraBlock::decode(block).unwrap();
        points.push(ChainPoint::Specific(decoded.slot(), decoded.hash()));
        after.push(staged(&domain));
    }

    let subjects = |set: &BTreeSet<(u64, Vec<u8>, u64)>| -> Vec<Vec<u8>> {
        set.iter()
            .map(|(_, subject, _)| subject[28..].to_vec())
            .collect()
    };
    assert_eq!(subjects(&after[0]), [b"A".to_vec(), b"B".to_vec()]);
    assert_eq!(subjects(&after[2]).len(), 4);

    // the repeat inside the first block never reached the WAL
    let entry = domain.wal().read_entry(&points[0]).unwrap().unwrap();
    let stagings = entry
        .delta
        .iter()
        .filter(|delta| matches!(delta, CardanoDelta::StageFirstMint(_)))
        .count();
    assert_eq!(stagings, 2);

    domain.rollback(&points[1]).unwrap();
    assert_eq!(staged(&domain), after[1]);

    domain.rollback(&points[0]).unwrap();
    assert_eq!(staged(&domain), after[0]);
}

fn staged_mints(domain: &ToyDomain) -> BTreeSet<(u64, u64, u32, i64)> {
    domain
        .state()
        .iter_entities_typed::<AssetMintSet>(None)
        .unwrap()
        .map(|row| {
            let (key, row) = row.unwrap();
            (key.epoch, key.member.0, key.member.1, row.amount)
        })
        .collect()
}

/// Undoing a block removes exactly the asset-mint rows it staged, one per
/// (valid tx, asset), and nothing another block staged.
#[test]
fn rollback_unstages_exactly_the_mints_its_block_created() {
    let policy = [0x44u8; 28];
    let mint = |name: &str, amount| (policy, name.to_string(), amount);

    let cfg = plain(SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 2,
        mints_by_block: vec![
            vec![vec![mint("A", 5), mint("B", 1)], vec![mint("A", -2)]],
            vec![vec![mint("A", 3)], vec![mint("C", 1)]],
            vec![vec![mint("A", -1)], vec![]],
        ],
        ..Default::default()
    });

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    let mut points = Vec::new();
    let mut after = Vec::new();

    for block in &blocks {
        domain.roll_forward(block.clone()).unwrap();

        let decoded = MultiEraBlock::decode(block).unwrap();
        points.push(ChainPoint::Specific(decoded.slot(), decoded.hash()));
        after.push(staged_mints(&domain));
    }

    assert_eq!(after[0].len(), 3);
    assert_eq!(after[1].len(), 5);
    assert_eq!(after[2].len(), 6);

    let entry = domain.wal().read_entry(&points[0]).unwrap().unwrap();
    let stagings = entry
        .delta
        .iter()
        .filter(|delta| matches!(delta, CardanoDelta::StageAssetMint(_)))
        .count();
    assert_eq!(stagings, 3);

    domain.rollback(&points[1]).unwrap();
    assert_eq!(staged_mints(&domain), after[1]);

    domain.rollback(&points[0]).unwrap();
    assert_eq!(staged_mints(&domain), after[0]);
}

/// A zero-quantity entry stages no first mint and no mint row, beside a real
/// mint in the same tx that does.
#[test]
fn zero_mint_stages_no_first_mint() {
    let (blocks, vectors, chain_config) = build_synthetic_blocks(plain(SyntheticBlockConfig {
        block_count: 1,
        txs_per_block: 1,
        ..Default::default()
    }));
    let last = vectors.blocks.last().unwrap();

    let mut assets = std::collections::BTreeMap::new();
    assets.insert(b"A".to_vec().into(), 1);
    assets.insert(b"ZERO".to_vec().into(), 0);

    let mut mint: alonzo::Mint = std::collections::BTreeMap::new();
    mint.insert([0x44u8; 28].into(), assets);

    let input = TransactionInput {
        transaction_id: last.tx_hashes[0].parse().unwrap(),
        index: 0,
    };

    let (block, _, _) = sample_alonzo_block(
        last.block_number + 1,
        last.slot + 1,
        Some(last.block_hash.parse().unwrap()),
        input,
        vec![],
        Some(mint),
    );

    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    domain.roll_forward(blocks[0].clone()).unwrap();
    domain.roll_forward(block.clone()).unwrap();

    let decoded = MultiEraBlock::decode(&block).unwrap();

    let subjects: Vec<Vec<u8>> = staged(&domain)
        .into_iter()
        .filter(|(_, _, slot)| *slot == decoded.slot())
        .map(|(_, subject, _)| subject[28..].to_vec())
        .collect();
    assert_eq!(subjects, [b"A".to_vec()]);

    let point = ChainPoint::Specific(decoded.slot(), decoded.hash());
    let entry = domain.wal().read_entry(&point).unwrap().unwrap();
    let count = |f: fn(&CardanoDelta) -> bool| entry.delta.iter().filter(|x| f(x)).count();

    assert_eq!(count(|x| matches!(x, CardanoDelta::StageFirstMint(_))), 1);
    assert_eq!(count(|x| matches!(x, CardanoDelta::StageAssetMint(_))), 1);
}

/// The `(epoch, script hash, slot)` of each script staged as first seen.
fn scripts(domain: &ToyDomain) -> BTreeSet<(u64, [u8; 28], u64)> {
    domain
        .state()
        .iter_entities_typed::<FirstScriptSet>(None)
        .unwrap()
        .map(|row| {
            let (key, row) = row.unwrap();
            (key.epoch, *key.member.0, row.slot)
        })
        .collect()
}

/// Rollback for first scripts: undoing the block that first carried a script
/// removes its set rows, and a block that only repeats scripts leaves them
/// alone. The first block's tx failed phase-2 validation, so its witnesses
/// do not count, get no `script` tag, and the scripts are staged once, at the
/// second block, the first valid carrier. Each undone block's `script` tags go
/// with it.
#[test]
fn rollback_removes_the_script_rows_of_the_block_that_first_carried_them() {
    let cfg = SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 1,
        invalid_txs_by_block: vec![vec![0], vec![], vec![]],
        ..Default::default()
    };

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    let mut points = Vec::new();
    let mut after = Vec::new();

    for block in &blocks {
        domain.roll_forward(block.clone()).unwrap();

        let decoded = MultiEraBlock::decode(block).unwrap();
        points.push(ChainPoint::Specific(decoded.slot(), decoded.hash()));
        after.push(scripts(&domain));
    }

    let none = BTreeSet::new();
    assert_eq!(after[0], none, "an invalid tx's witnesses were recorded");
    assert_eq!(after[1].len(), 3);
    assert!(after[1]
        .iter()
        .all(|(_, _, slot)| *slot == points[1].slot()));
    assert_eq!(after[2], after[1]);

    let tagged = |domain: &ToyDomain| -> BTreeSet<u64> {
        after[1]
            .iter()
            .flat_map(|(_, hash, _)| {
                domain
                    .archive()
                    .slots_by_tag(SCRIPT, hash, 0, u64::MAX)
                    .unwrap()
                    .map(Result::unwrap)
                    .collect::<Vec<_>>()
            })
            .collect()
    };

    assert_eq!(
        tagged(&domain),
        BTreeSet::from([points[1].slot(), points[2].slot()])
    );

    domain.rollback(&points[1]).unwrap();
    assert_eq!(scripts(&domain), after[1]);
    assert_eq!(tagged(&domain), BTreeSet::from([points[1].slot()]));

    domain.rollback(&points[0]).unwrap();
    assert_eq!(scripts(&domain), none);
    assert!(tagged(&domain).is_empty());
}

/// `script_by_hash` finds a script at its staged row's block, which is also
/// its first tagged one, the block that counts it: the invalid tx that
/// carried it earlier left no tag.
#[tokio::test]
async fn script_by_hash_finds_a_script_at_its_counting_block() {
    let cfg = SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 1,
        invalid_txs_by_block: vec![vec![0], vec![], vec![]],
        ..Default::default()
    };

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    let mut slots = Vec::new();

    for block in &blocks {
        domain.roll_forward(block.clone()).unwrap();
        slots.push(MultiEraBlock::decode(block).unwrap().slot());
    }

    let staged = scripts(&domain);
    assert_eq!(staged.len(), 3);

    let query = AsyncQueryFacade::new(domain.clone());

    for (_, hash, slot) in staged {
        let first = domain
            .archive()
            .slots_by_tag(SCRIPT, &hash, 0, slots[2])
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(first, slots[1], "first tag is not the counting block");
        assert_eq!(slot, slots[1]);

        let found = query.script_by_hash(&Hash::from(hash)).await.unwrap();
        assert!(found.is_some_and(|x| !x.script.is_empty()));
    }
}

/// The roll's witness across sealed epochs: every block carries the same
/// scripts, the seal moves their first carrier's epoch into the entity index,
/// and no later epoch stages them again. `script_by_hash` then finds each
/// through the index.
#[tokio::test]
async fn scripts_are_staged_once_across_sealed_epochs() {
    let (blocks, step) = (16, 350);

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: step,
        slot_step: step,
        asset_names_by_block: (0..blocks).map(|i| format!("M{i:03}")).collect(),
        ..Default::default()
    });

    let genesis = genesis_with(700, 10, 6);
    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis.clone(), chain_config, None, None);
    let tip = domain.import_blocks(raw).unwrap();

    let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let mutable = dolos_cardano::mutable_slots(&genesis);
    let (tip_epoch, _) = chain.slot_epoch(tip);
    let (first_epoch, _) = chain.slot_epoch(step);

    assert!(is_sealable(&chain, mutable, first_epoch, tip_epoch));
    assert!(
        scripts(&domain).is_empty(),
        "staged again: {:?}",
        scripts(&domain)
    );

    let indexed: Vec<_> = domain
        .archive()
        .iter_entity_records(&[entities::SCRIPT], 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(indexed.len(), 3);
    assert!(indexed.iter().all(|record| record.slot == step));

    let listed = domain
        .archive()
        .read_log_typed::<FirstScriptChunk>(chain.epoch_start(first_epoch), &0)
        .unwrap()
        .expect("the first carrier's epoch is not sealed");
    assert_eq!(listed.count, 3);

    let query = AsyncQueryFacade::new(domain.clone());

    for record in indexed {
        let hash = Hash::<28>::from(record.key());
        let found = query.script_by_hash(&hash).await.unwrap();
        assert!(found.is_some_and(|x| !x.script.is_empty()));
    }
}

/// `script_by_hash` reads the staged rows of every unsealed epoch, not only
/// the tip's: the tip has crossed into epoch 1 while the scripts' first
/// carrier's epoch 0 is still staged, and each script is found there.
#[tokio::test]
async fn script_by_hash_finds_a_script_staged_in_an_earlier_unsealed_epoch() {
    let step = 350;

    let cfg = plain(SyntheticBlockConfig {
        block_count: 3,
        txs_per_block: 1,
        slot: step,
        slot_step: step,
        asset_names_by_block: (0..3).map(|i| format!("M{i:03}")).collect(),
        ..Default::default()
    });

    let genesis = genesis_with(700, 10, 6);
    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
    let tip = domain.import_blocks(raw).unwrap();

    let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let (tip_epoch, _) = chain.slot_epoch(tip);
    let (first_epoch, _) = chain.slot_epoch(step);

    assert!(tip_epoch > first_epoch);

    let staged = scripts(&domain);
    assert_eq!(staged.len(), 3);
    assert!(staged.iter().all(|(epoch, _, _)| *epoch == first_epoch));

    let query = AsyncQueryFacade::new(domain.clone());

    for (_, hash, _) in staged {
        let found = query.script_by_hash(&Hash::from(hash)).await.unwrap();
        assert!(found.is_some_and(|x| !x.script.is_empty()));
    }
}

/// A seal that lands between `script_by_hash`'s first index read and its
/// staged-row read moves the script out of the staged rows and into the
/// index; the second index read finds it there.
#[tokio::test]
async fn script_by_hash_finds_a_script_sealed_during_the_lookup() {
    let cfg = SyntheticBlockConfig {
        block_count: 2,
        txs_per_block: 1,
        ..Default::default()
    };

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    for block in blocks {
        domain.roll_forward(block).unwrap();
    }

    let (key, row) = domain
        .state()
        .iter_entities_typed::<FirstScriptSet>(None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    let hash = key.member.0;
    let faulty = FaultyToyDomain::new(domain.clone(), TestFault::None);

    let sealer = domain.clone();
    faulty.after_entity_slots(move || {
        let writer = sealer.archive().start_writer().unwrap();
        writer
            .write_entity(entities::SCRIPT, hash.as_slice(), row.slot)
            .unwrap();
        writer.commit().unwrap();

        let writer = sealer.state().start_writer().unwrap();
        writer
            .save_entity_typed::<FirstScriptSet>(&key, None)
            .unwrap();
        writer.commit().unwrap();
    });

    let found = AsyncQueryFacade::new(faulty)
        .script_by_hash(&hash)
        .await
        .unwrap();

    assert!(found.is_some_and(|x| !x.script.is_empty()));
    assert!(scripts(&domain)
        .iter()
        .all(|(_, staged, _)| *staged != *hash));
}

/// The stake address the `stake` tag files `address` under, if any.
fn stake_of(address: &[u8]) -> Option<Vec<u8>> {
    match Address::from_bytes(address).unwrap() {
        Address::Shelley(x) => {
            dolos_cardano::pallas_extras::shelley_address_to_stake_address(&x).map(|x| x.to_vec())
        }
        Address::Stake(x) => Some(x.to_vec()),
        Address::Byron(_) => None,
    }
}

/// The stake address of the synthetic chain's own account.
fn primary_stake() -> Vec<u8> {
    let address = Address::from_bech32(&SyntheticBlockConfig::default().address).unwrap();
    stake_of(&address.to_vec()).unwrap()
}

/// The slot of the first block the `stake` tag files under `stake`.
fn first_tag(domain: &ToyDomain, stake: &[u8]) -> Option<BlockSlot> {
    domain
        .archive()
        .slots_by_tag(STAKE, stake, 0, BlockSlot::MAX)
        .unwrap()
        .next()
        .transpose()
        .unwrap()
}

/// File `stake` under the `stake` tag at `slot`, as a block there would.
fn tag_stake(domain: &ToyDomain, stake: &[u8], slot: BlockSlot) {
    let writer = domain.archive().start_writer().unwrap();
    writer
        .apply_index(&[ArchiveIndexDelta {
            slot,
            block_hash: vec![0xab; 32],
            block_number: None,
            tx_hashes: vec![],
            tags: vec![Tag::new(STAKE, stake.to_vec())],
        }])
        .unwrap();
    writer.commit().unwrap();
}

/// Sixteen blocks, 350 slots apart, over 700-slot epochs: the first epochs
/// seal, the last stay staged. Returns the domain, its tip and the genesis.
fn sealed_chain() -> (ToyDomain, BlockSlot, Arc<Genesis>) {
    let (blocks, step) = (16, 350);

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: step,
        slot_step: step,
        ..Default::default()
    });

    let genesis = genesis_with(700, 10, 6);
    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis.clone(), chain_config, None, None);
    let tip = domain.import_blocks(raw).unwrap();

    (domain, tip, genesis)
}

/// The seal indexes every stake address the sealed account addresses file
/// under, at the slot of its first `stake` tag, and nothing an unsealed epoch
/// saw first.
#[test]
fn the_seal_indexes_each_stake_address_at_its_first_tag() {
    let (domain, tip, genesis) = sealed_chain();

    let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let mutable = dolos_cardano::mutable_slots(&genesis);
    let (tip_epoch, _) = chain.slot_epoch(tip);

    let indexed: HashMap<Vec<u8>, BlockSlot> = domain
        .archive()
        .iter_entity_records(&[entities::STAKE], 0..u64::MAX)
        .unwrap()
        .map(|record| {
            let record = record.unwrap();
            (record.key().to_vec(), record.slot)
        })
        .collect();

    let primary = primary_stake();
    assert_eq!(indexed.get(&primary), Some(&350));

    for (stake, slot) in &indexed {
        assert_eq!(
            first_tag(&domain, stake),
            Some(*slot),
            "{}",
            hex::encode(stake)
        );

        let (epoch, _) = chain.slot_epoch(*slot);
        assert!(
            is_sealable(&chain, mutable, epoch, tip_epoch),
            "epoch {epoch}"
        );
    }

    let sealed: Vec<(BlockSlot, AccountKey, AccountAddressChunk)> = domain
        .archive()
        .iter_logs_typed::<AccountAddressChunk>(None)
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert!(!sealed.is_empty());

    for (_, _, chunk) in sealed {
        for address in chunk.addresses {
            let stake = stake_of(&address).unwrap();
            assert!(indexed.contains_key(&stake), "{}", hex::encode(stake));
        }
    }
}

/// `stake_seen` takes the index for the sealed epochs and the `stake` tag for
/// the staged ones: a tag below the staged epochs that the index lacks is not
/// walked, and a tag in them is found.
#[test]
fn stake_seen_reads_the_index_then_the_staged_tags() {
    let (domain, tip, _) = sealed_chain();

    let seen = |stake: &[u8]| account_addresses::stake_seen(&domain, stake).unwrap();

    let primary = primary_stake();
    assert!(seen(&primary));

    let early = [0xe0; 29];
    tag_stake(&domain, &early, 350);
    assert!(!seen(&early));

    let late = [0xe1; 29];
    tag_stake(&domain, &late, tip);
    assert!(seen(&late));

    assert!(!seen(&[0xe2; 29]));
}

/// A seal that lands between `stake_seen`'s first index read and its tag read
/// moves the sighting's epoch out of the staged ones; the second index read
/// finds the entry the seal committed.
#[test]
fn stake_seen_finds_a_stake_address_sealed_during_the_lookup() {
    let (domain, _, _) = sealed_chain();

    let stake = [0xe3; 29];
    tag_stake(&domain, &stake, 350);

    let faulty = FaultyToyDomain::new(domain.clone(), TestFault::None);

    let sealer = domain.clone();
    faulty.after_entity_slots(move || {
        let writer = sealer.archive().start_writer().unwrap();
        writer.write_entity(entities::STAKE, &stake, 350).unwrap();
        writer.commit().unwrap();
    });

    assert!(account_addresses::stake_seen(&faulty, &stake).unwrap());
}

/// A copy of the devnet genesis with `epoch_length`-slot epochs, security
/// parameter `k`, starting at `protocol`.
fn genesis_with(epoch_length: u32, k: usize, protocol: usize) -> Arc<Genesis> {
    let mut genesis = dolos_cardano::include::devnet::load();
    genesis.force_protocol = Some(protocol);
    genesis.shelley.epoch_length = Some(epoch_length);
    genesis.shelley.security_param = Some(k as u32);
    genesis.byron.protocol_consts.k = k;
    Arc::new(genesis)
}

/// Mint a fresh asset in each of `blocks` blocks spaced `step` slots apart, and
/// check that every epoch ESTART could seal is sealed, and every other one is
/// still staged.
fn assert_epochs_seal(genesis: Arc<Genesis>, blocks: usize, step: u64) {
    let names: Vec<String> = (0..blocks).map(|i| format!("M{i:03}")).collect();

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: step,
        slot_step: step,
        asset_names_by_block: names,
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis.clone(), chain_config, None, None);
    let tip = domain.import_blocks(raw).unwrap();

    let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let mutable = dolos_cardano::mutable_slots(&genesis);
    let (tip_epoch, _) = chain.slot_epoch(tip);

    let minted: BTreeSet<u64> = (1..=blocks as u64)
        .map(|i| chain.slot_epoch(i * step).0)
        .collect();

    let staged_epochs: BTreeSet<u64> = staged(&domain).into_iter().map(|(e, _, _)| e).collect();
    let staged_mint_epochs: BTreeSet<u64> = staged_mints(&domain)
        .into_iter()
        .map(|(e, _, _, _)| e)
        .collect();
    let sealed_mint_slots: BTreeSet<u64> = domain
        .archive()
        .iter_logs_typed::<AssetMintChunk>(None)
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();
    let account = AccountKey(primary());
    let staged_account_epochs: BTreeSet<u64> = staged_addresses(&domain)
        .into_iter()
        .map(|(e, _, _)| e)
        .collect();

    let mut sealed = 0;

    for epoch in minted {
        let row = domain
            .archive()
            .read_log_typed::<FirstMintChunk>(chain.epoch_start(epoch), &0)
            .unwrap();

        let addresses = domain
            .archive()
            .read_log_typed::<AccountAddressChunk>(chain.epoch_start(epoch), &account)
            .unwrap();

        if is_sealable(&chain, mutable, epoch, tip_epoch) {
            let addresses = addresses
                .unwrap_or_else(|| panic!("epoch {epoch}'s addresses not sealed by {tip_epoch}"));
            assert_eq!(addresses.addresses.len(), 1);
            assert!(!staged_account_epochs.contains(&epoch));

            let row = row.unwrap_or_else(|| panic!("epoch {epoch} not sealed by {tip_epoch}"));
            assert_eq!(row.count as usize, row.subjects.len());
            assert!(
                !staged_epochs.contains(&epoch),
                "epoch {epoch} still staged"
            );
            assert!(sealed_mint_slots.contains(&chain.epoch_start(epoch)));
            assert!(!staged_mint_epochs.contains(&epoch));
            sealed += 1;
        } else {
            assert!(
                addresses.is_none(),
                "epoch {epoch}'s addresses sealed early"
            );
            assert!(staged_account_epochs.contains(&epoch));
            assert!(row.is_none(), "epoch {epoch} sealed early");
            assert!(staged_epochs.contains(&epoch), "epoch {epoch} not staged");
            assert!(!sealed_mint_slots.contains(&chain.epoch_start(epoch)));
            assert!(staged_mint_epochs.contains(&epoch));
        }
    }

    assert!(sealed > 0, "nothing sealed by epoch {tip_epoch}");
}

/// Done criterion 1, short epochs: 700-slot Alonzo epochs end before RUPD's
/// trigger (`4k/f` = 800 slots in), so RUPD never runs, and ESTART still seals.
#[test]
fn epochs_too_short_for_rupd_seal() {
    assert_epochs_seal(genesis_with(700, 10, 6), 16, 350);
}

/// Done criterion 1, short epochs: a Conway devnet with 500-slot epochs.
#[test]
fn short_devnet_epochs_seal() {
    assert_epochs_seal(genesis_with(500, 5, 9), 14, 250);
}

/// A policy whose CIP-25 entry uses v1 text keys: the policy id in hex and
/// the asset name as UTF-8.
const TEXT_KEYS: [u8; 28] = [0x51; 28];

/// A policy whose CIP-25 entry uses v2 byte-string keys.
const BYTE_KEYS: [u8; 28] = [0x52; 28];

/// Label 721 naming `A` under [`TEXT_KEYS`] and `C` under [`BYTE_KEYS`], and
/// no other asset.
fn label_721() -> (u64, alonzo::Metadatum) {
    use alonzo::Metadatum;
    use pallas::codec::utils::KeyValuePairs;

    let nft = || {
        Metadatum::Map(KeyValuePairs::from(vec![
            (Metadatum::Text("name".into()), Metadatum::Text("x".into())),
            (
                Metadatum::Text("image".into()),
                Metadatum::Text("ipfs://x".into()),
            ),
        ]))
    };

    let assets = |key: Metadatum| Metadatum::Map(KeyValuePairs::from(vec![(key, nft())]));

    let policies = KeyValuePairs::from(vec![
        (
            Metadatum::Text(hex::encode(TEXT_KEYS)),
            assets(Metadatum::Text("A".into())),
        ),
        (
            Metadatum::Bytes(BYTE_KEYS.to_vec().into()),
            assets(Metadatum::Bytes(b"C".to_vec().into())),
        ),
    ]);

    (721, Metadatum::Map(policies))
}

fn asset(policy: [u8; 28], name: &str) -> AssetKey {
    AssetKey::of_parts(&policy, name.as_bytes())
}

/// The staged `(amount, names_asset)` of each asset, in chain order.
fn staged_flags(domain: &ToyDomain) -> HashMap<AssetKey, Vec<(i64, bool)>> {
    let mut out: HashMap<_, Vec<_>> = HashMap::new();

    for row in domain
        .state()
        .iter_entities_typed::<AssetMintSet>(None)
        .unwrap()
    {
        let (key, row) = row.unwrap();
        out.entry(key.group)
            .or_default()
            .push((row.amount, row.names_asset));
    }

    out
}

/// A mint is flagged exactly when it sets `metadata_tx`: its tx's label 721
/// names the asset, by text or byte-string keys, and it is not a burn.
#[test]
fn mint_flags_follow_the_cip25_predicate() {
    let mint = |policy, name: &str, amount| (policy, name.to_string(), amount);

    let mints_by_block = vec![
        vec![
            vec![
                mint(TEXT_KEYS, "A", 1),
                // the same policy's entry, which names A alone
                mint(TEXT_KEYS, "B", 1),
                mint(BYTE_KEYS, "C", 1),
            ],
            // a burn of a named asset
            vec![mint(TEXT_KEYS, "A", -1)],
        ],
        vec![vec![mint(TEXT_KEYS, "A", 2)], vec![]],
        // a burn by a tx whose label 721 names the asset
        vec![vec![mint(TEXT_KEYS, "A", -1)], vec![]],
    ];

    let build = |metadata_entries| {
        let cfg = plain(SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 2,
            mints_by_block: mints_by_block.clone(),
            metadata_entries,
            ..Default::default()
        });

        let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
        let genesis = Arc::new(dolos_cardano::include::preview::load());
        let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

        for block in blocks {
            domain.roll_forward(block).unwrap();
        }

        staged_flags(&domain)
    };

    let named = build(vec![label_721()]);

    assert_eq!(
        named[&asset(TEXT_KEYS, "A")],
        [(1, true), (-1, false), (2, true), (-1, false)]
    );
    assert_eq!(named[&asset(TEXT_KEYS, "B")], [(1, false)]);
    assert_eq!(named[&asset(BYTE_KEYS, "C")], [(1, true)]);

    // the default chain's txs carry label 1990 and no 721
    let unnamed = build(vec![]);

    assert_eq!(
        unnamed[&asset(TEXT_KEYS, "A")],
        [(1, false), (-1, false), (2, false), (-1, false)]
    );
    assert_eq!(unnamed[&asset(BYTE_KEYS, "C")], [(1, false)]);
}

/// Across sealed chunks and staged rows, an asset's newest flagged mint is
/// the `metadata_tx` its read answers, and an asset never flagged has none.
#[tokio::test]
async fn the_newest_flagged_mint_is_the_metadata_tx() {
    let genesis = genesis_with(500, 5, 9);
    let mint = |policy, name: &str, amount| (policy, name.to_string(), amount);

    let blocks = 14;
    let mints_by_block = (0..blocks)
        .map(|i| {
            // A ends on a burn, which never moves its metadata tx
            let a = if i % 3 == 1 { -1 } else { 1 };
            let mut mints = vec![mint(TEXT_KEYS, "A", a), mint(TEXT_KEYS, "B", 1)];

            if i % 2 == 0 {
                mints.push(mint(BYTE_KEYS, "C", 1));
            }

            vec![mints]
        })
        .collect();

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        mints_by_block,
        metadata_entries: vec![label_721()],
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
    let tip = domain.import_blocks(raw).unwrap();

    let sealed: Vec<(AssetKey, AssetMintSet)> = domain
        .archive()
        .iter_logs_typed::<AssetMintChunk>(None)
        .unwrap()
        .flat_map(|row| {
            let (_, (key, _), chunk) = row.unwrap();
            chunk.mints.into_iter().map(move |x| (key, x))
        })
        .collect();

    assert!(!sealed.is_empty(), "nothing sealed");

    for (policy, name, flagged) in [
        (TEXT_KEYS, "A", true),
        (TEXT_KEYS, "B", false),
        (BYTE_KEYS, "C", true),
    ] {
        let key = asset(policy, name);

        let staged = domain
            .state()
            .iter_entities_typed::<AssetMintSet>(Some(RangeOf::prefix(&key)))
            .unwrap()
            .map(|row| row.unwrap().1);

        let mints: Vec<AssetMintSet> = sealed
            .iter()
            .filter(|(x, _)| *x == key)
            .map(|(_, row)| row.clone())
            .chain(staged)
            .collect();

        let newest = mints
            .iter()
            .rev()
            .find(|x| x.names_asset)
            .map(|x| x.tx_hash);

        let read = read_asset(&domain, tip, &subject(policy, name)).await;

        assert_eq!(newest.is_some(), flagged);
        assert_eq!(newest, read.unwrap().fold.metadata_tx);
        assert!(mints.last().is_some_and(|x| x.amount < 0) || key != asset(TEXT_KEYS, "A"));
    }
}

fn subject(policy: [u8; 28], name: &str) -> Vec<u8> {
    [policy.as_slice(), name.as_bytes()].concat()
}

/// The blocks a read walks for a label-100 token's metadata tx: minibf's
/// default `max_scan_items`.
const WALK: u64 = 3_000;

/// What `/assets/{asset}` reads of `subject` with the tip at `tip`.
async fn read_asset(domain: &ToyDomain, tip: u64, subject: &[u8]) -> Option<Facts> {
    let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
    let (epoch, _) = chain.slot_epoch(tip);

    assets::asset_summary(
        domain,
        &chain,
        epoch,
        subject.to_vec(),
        &WalkBudget::new(WALK),
    )
    .await
    .unwrap()
    .map(|x| Facts {
        quantity: x.fold.quantity(),
        mint_tx_count: x.fold.mint_tx_count,
        initial_tx: x.fold.initial_tx,
        initial_slot: x.initial_slot,
        fold: x.fold,
    })
}

/// An asset's facts, with the fold they came from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Facts {
    quantity: i128,
    mint_tx_count: u64,
    initial_tx: Option<Hash<32>>,
    initial_slot: u64,
    fold: assets::AssetFold,
}

/// `subject`'s facts as `AssetState` kept them, from every block the archive
/// holds: each non-zero mint or burn of a valid tx counts, the first sets the
/// initial tx and slot, and the newest output carrying a label-100 token with
/// a datum, or valid mint whose CIP-25 metadata names the asset, sets the
/// metadata tx. `None` for an asset never minted.
fn state_kept(
    domain: &ToyDomain,
    subject: &[u8],
) -> Option<(i128, u64, Hash<32>, u64, Option<Hash<32>>)> {
    use dolos_cardano::{
        cip25::{cip25_metadata_for_tx, cip25_mint_names_asset},
        cip68::{has_cip25_metadata, parse_cip67_label_from_asset_name},
    };

    let (policy, name) = subject.split_at(28);
    let policy = Hash::<28>::from(policy);
    let reference = parse_cip67_label_from_asset_name(name) == Some(100);

    let (mut quantity, mut count, mut initial, mut metadata) = (0i128, 0u64, None, None);

    for (_, body) in domain.archive().get_range(None, None).unwrap() {
        let block = MultiEraBlock::decode(&body).unwrap();

        for tx in block.txs() {
            if reference {
                for (_, output) in tx.produces() {
                    let holds = output.value().assets().iter().any(|x| {
                        *x.policy() == policy && x.assets().iter().any(|a| a.name() == name)
                    });

                    if holds && output.datum().is_some() {
                        metadata = Some(tx.hash());
                    }
                }
            }

            if !tx.is_valid() {
                continue;
            }

            let cip25 = has_cip25_metadata(&tx)
                .then(|| cip25_metadata_for_tx(&tx))
                .flatten();

            for mint in tx.mints().iter().filter(|x| *x.policy() == policy) {
                for asset in mint.assets().iter().filter(|x| x.name() == name) {
                    let amount = asset.mint_coin().unwrap_or_default();

                    if amount == 0 {
                        continue;
                    }

                    quantity += i128::from(amount);
                    count += 1;
                    initial.get_or_insert((tx.hash(), block.slot()));

                    if cip25_mint_names_asset(cip25.as_ref(), &policy, name, amount) {
                        metadata = Some(tx.hash());
                    }
                }
            }
        }
    }

    initial.map(|(tx, slot)| (quantity, count, tx, slot, metadata))
}

fn assert_read_as_kept(
    facts: &Option<Facts>,
    kept: Option<(i128, u64, Hash<32>, u64, Option<Hash<32>>)>,
    what: &str,
) {
    let got = facts.as_ref().map(|x| {
        (
            x.quantity,
            x.mint_tx_count,
            x.initial_tx.unwrap(),
            x.initial_slot,
            x.fold.metadata_tx,
        )
    });

    assert_eq!(got, kept, "{what}");
}

fn chain_of(domain: &ToyDomain) -> ChainSummary {
    dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap()
}

fn cached_entry(domain: &ToyDomain, subject: &[u8]) -> Option<AssetEntry> {
    let value = domain.cache().get(ASSETS.name, subject)?;
    Some(AssetEntry::decode(&value).unwrap())
}

/// The merge of the cached epochs with the tail, against the facts
/// `AssetState` kept: an asset minted only in sealed epochs, only in the
/// unsealed tail, and in both; a full burn; a CIP-25 metadata tx in the tail;
/// and an entry behind the watermark, read before the background extension
/// and after it.
#[tokio::test]
async fn the_asset_read_merges_the_cached_epochs_with_the_tail() {
    const P: [u8; 28] = [0x61; 28];

    let genesis = genesis_with(500, 5, 9);
    let mint = |policy, name: &str, amount| (policy, name.to_string(), amount);

    let blocks = 14;
    let mints_by_block: Vec<_> = (0..blocks)
        .map(|i| {
            let mut mints = vec![];

            match i {
                0 => {
                    mints.push(mint(P, "SEAL", 5));
                    mints.push(mint(P, "BOTH", 7));
                    mints.push(mint(TEXT_KEYS, "A", 1));
                }
                1 => {
                    mints.push(mint(P, "SEAL", 2));
                    mints.push(mint(P, "BURN", 3));
                    mints.push(mint(BYTE_KEYS, "C", 1));
                }
                2 => mints.push(mint(P, "SEAL", -4)),
                3 => mints.push(mint(P, "BURN", -3)),
                5 => mints.push(mint(TEXT_KEYS, "A", -1)),
                12 => mints.push(mint(P, "BOTH", -2)),
                13 => {
                    mints.push(mint(P, "BOTH", 4));
                    mints.push(mint(P, "TAIL", 9));
                    mints.push(mint(TEXT_KEYS, "A", 2));
                }
                _ => mints.push(mint(P, "FILL", 1)),
            }

            vec![mints]
        })
        .collect();

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        mints_by_block,
        metadata_entries: vec![label_721()],
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    let subjects: Vec<(&str, Vec<u8>)> = vec![
        ("sealed only", subject(P, "SEAL")),
        ("both", subject(P, "BOTH")),
        ("tail only", subject(P, "TAIL")),
        ("full burn", subject(P, "BURN")),
        ("CIP-25 in the tail", subject(TEXT_KEYS, "A")),
        ("CIP-25 sealed", subject(BYTE_KEYS, "C")),
        ("never minted", subject(P, "NONE")),
    ];

    // the first ten blocks: the entries fill at the watermark then
    let early = domain.import_blocks(raw[..10].to_vec()).unwrap();
    let chain = chain_of(&domain);
    let early_watermark =
        assets::assets_watermark(&domain, &chain, chain.slot_epoch(early).0).unwrap();

    for (what, subject) in &subjects {
        let cold = read_asset(&domain, early, subject).await;
        assert_read_as_kept(&cold, state_kept(&domain, subject), what);

        let warm = read_asset(&domain, early, subject).await;
        assert_eq!(warm, cold, "{what} warm");
    }

    let entry = cached_entry(&domain, &subject(P, "SEAL")).expect("an entry for SEAL");
    assert_eq!(entry.watermark, early_watermark);
    assert!(cached_entry(&domain, &subject(P, "NONE")).is_none());

    // the rest: every entry is now behind the watermark
    let tip = domain.import_blocks(raw[10..].to_vec()).unwrap();
    let watermark = assets::assets_watermark(&domain, &chain, chain.slot_epoch(tip).0).unwrap();
    assert!(
        watermark > early_watermark,
        "{watermark} after {early_watermark}"
    );

    for (what, subject) in &subjects {
        let lagging = read_asset(&domain, tip, subject).await;
        assert_read_as_kept(&lagging, state_kept(&domain, subject), what);
    }

    // the background extension catches the entries up
    for _ in 0..100 {
        let entry = cached_entry(&domain, &subject(P, "SEAL")).unwrap();

        if entry.watermark == watermark {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    for (what, subject) in &subjects {
        let warm = read_asset(&domain, tip, subject).await;
        assert_read_as_kept(&warm, state_kept(&domain, subject), what);
    }

    let entry = cached_entry(&domain, &subject(P, "SEAL")).unwrap();
    assert_eq!(entry.watermark, watermark);

    // first minted past the watermark: the tail alone, nothing cached
    let (first, _) = chain.slot_epoch(
        read_asset(&domain, tip, &subject(P, "TAIL"))
            .await
            .unwrap()
            .initial_slot,
    );
    assert!(first > watermark);
    assert!(cached_entry(&domain, &subject(P, "TAIL")).is_none());

    // the full burn
    let burn = read_asset(&domain, tip, &subject(P, "BURN")).await.unwrap();
    assert_eq!((burn.quantity, burn.mint_tx_count), (0, 2));
}

/// A Conway block at `slot` with one valid tx spending `input`, if any, and
/// minting `minted` of `subject`, whose one output carries one `subject`,
/// with a datum or without. No lovelace moves, so the pots hold.
fn reference_block(
    slot: u64,
    subject: &[u8],
    input: Option<Hash<32>>,
    minted: Option<i64>,
    datum: bool,
) -> (dolos_core::RawBlock, Hash<32>) {
    reference_block_with(slot, subject, input, minted, datum, vec![])
}

/// [`reference_block`] whose tx carries `metadata`.
fn reference_block_with(
    slot: u64,
    subject: &[u8],
    input: Option<Hash<32>>,
    minted: Option<i64>,
    datum: bool,
    metadata: Vec<(u64, alonzo::Metadatum)>,
) -> (dolos_core::RawBlock, Hash<32>) {
    use pallas::{
        codec::utils::{Bytes, KeepRaw, NonZeroInt, PositiveCoin, Set},
        ledger::primitives::conway::{
            DatumOption, PostAlonzoTransactionOutput, TransactionBody, TransactionOutput, Value,
        },
    };

    let (policy, name) = subject.split_at(28);
    let policy = Hash::<28>::from(policy);
    let name = Bytes::from(name.to_vec());

    let address = Address::from_bech32(&SyntheticBlockConfig::default().address).unwrap();

    let held = std::collections::BTreeMap::from([(
        policy,
        std::collections::BTreeMap::from([(name.clone(), PositiveCoin::try_from(1).unwrap())]),
    )]);

    let output = PostAlonzoTransactionOutput {
        address: Bytes::from(address.to_vec()),
        value: Value::Multiasset(0, held),
        datum_option: datum.then(|| KeepRaw::from(DatumOption::Hash(Hash::from([0x42; 32])))),
        script_ref: None,
    };

    let mint = minted.map(|amount| {
        std::collections::BTreeMap::from([(
            policy,
            std::collections::BTreeMap::from([(
                name.clone(),
                NonZeroInt::try_from(amount).unwrap(),
            )]),
        )])
    });

    let inputs: Vec<_> = input
        .map(|transaction_id| TransactionInput {
            transaction_id,
            index: 0,
        })
        .into_iter()
        .collect();

    let aux = (!metadata.is_empty()).then(|| {
        alonzo::AuxiliaryData::ShelleyMa(alonzo::ShelleyMaAuxiliaryData {
            transaction_metadata: metadata.into_iter().collect(),
            auxiliary_scripts: None,
        })
    });

    let body = TransactionBody {
        inputs: Set::from(inputs),
        outputs: vec![TransactionOutput::PostAlonzo(KeepRaw::from(output))],
        fee: 0,
        ttl: None,
        certificates: None,
        withdrawals: None,
        auxiliary_data_hash: aux
            .as_ref()
            .map(pallas::ledger::traverse::ComputeHash::compute_hash),
        validity_interval_start: None,
        mint,
        script_data_hash: None,
        collateral: None,
        required_signers: None,
        network_id: None,
        collateral_return: None,
        total_collateral: None,
        reference_inputs: None,
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    };

    let (_, raw) = dolos_testing::blocks::make_conway_block_with_txs_at(
        slot,
        slot,
        None,
        vec![(body, true)],
        aux,
    );

    let tx = MultiEraBlock::decode(&raw).unwrap().txs()[0].hash();

    (raw, tx)
}

/// A CIP-68 reference token (label 100) is updated by an output carrying it
/// with a datum, which is not a mint: its read walks the token's tagged
/// blocks newest first, past one that carries it without a datum, and the
/// entry's walk and the tail's agree with what `AssetState` kept.
#[tokio::test]
async fn a_label_100_token_takes_its_newest_datum_output() {
    let genesis = genesis_with(500, 5, 9);

    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    // label 100's prefix, then a name
    let reference = [[0x71; 28].as_slice(), &[0x00, 0x06, 0x43, 0xb0], b"REF"].concat();

    // minted in epoch 0, with a datum
    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);
    raw.insert(1, minted);

    let early = domain.import_blocks(raw).unwrap();

    let read = read_asset(&domain, early, &reference).await;
    assert_read_as_kept(&read, state_kept(&domain, &reference), "minted");
    assert_eq!(read.unwrap().fold.metadata_tx, Some(mint_tx));

    let entry = cached_entry(&domain, &reference).expect("an entry for the token");
    assert_eq!(entry.fold.metadata_tx, Some(mint_tx));

    // in the tail: moved with a new datum, then moved again without one
    let (updated, update_tx) = reference_block(3510, &reference, Some(mint_tx), None, true);
    let (moved, move_tx) = reference_block(3520, &reference, Some(update_tx), None, false);

    let tip = domain.import_blocks(vec![updated, moved]).unwrap();

    let read = read_asset(&domain, tip, &reference).await;
    assert_read_as_kept(
        &read,
        state_kept(&domain, &reference),
        "updated in the tail",
    );

    let read = read.unwrap();
    assert_eq!(read.fold.metadata_tx, Some(update_tx));
    assert_ne!(read.fold.metadata_tx, Some(move_tx));
    assert_eq!((read.quantity, read.initial_tx), (1, Some(mint_tx)));
}

/// Label 100's prefix, then `REF`, under `policy`.
fn reference_subject(policy: [u8; 28]) -> Vec<u8> {
    [policy.as_slice(), &[0x00, 0x06, 0x43, 0xb0], b"REF"].concat()
}

/// Label 721 metadata naming `reference` by its bytes, under its policy's
/// bytes.
fn naming(policy: [u8; 28], reference: &[u8]) -> Vec<(u64, alonzo::Metadatum)> {
    use alonzo::Metadatum;
    use pallas::codec::utils::KeyValuePairs;

    let nft = Metadatum::Map(KeyValuePairs::from(vec![(
        Metadatum::Text("name".into()),
        Metadatum::Text("x".into()),
    )]));
    let assets = Metadatum::Map(KeyValuePairs::from(vec![(
        Metadatum::Bytes(reference[28..].to_vec().into()),
        nft,
    )]));
    let label_721 = Metadatum::Map(KeyValuePairs::from(vec![(
        Metadatum::Bytes(policy.to_vec().into()),
        assets,
    )]));

    vec![(721, label_721)]
}

/// A label-100 token updated twice in the tail answers the newer datum: the
/// walk goes newest first.
#[tokio::test]
async fn a_label_100_token_updated_twice_takes_the_newer_datum() {
    let genesis = genesis_with(500, 5, 9);

    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    let reference = reference_subject([0x72; 28]);

    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);
    raw.insert(1, minted);
    domain.import_blocks(raw).unwrap();

    let (first, first_tx) = reference_block(3510, &reference, Some(mint_tx), None, true);
    let (second, second_tx) = reference_block(3520, &reference, Some(first_tx), None, true);
    let tip = domain.import_blocks(vec![first, second]).unwrap();

    let read = read_asset(&domain, tip, &reference).await;
    assert_read_as_kept(&read, state_kept(&domain, &reference), "updated twice");
    assert_eq!(read.unwrap().fold.metadata_tx, Some(second_tx));
}

/// A label-100 token's newest metadata tx can be a mint whose CIP-25 metadata
/// names it, with no datum on its output: the walk takes that tx, as the roll
/// set `AssetState.metadata_tx` from it.
#[tokio::test]
async fn a_label_100_token_takes_a_newer_cip25_mint() {
    let genesis = genesis_with(500, 5, 9);

    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    let policy = [0x73; 28];
    let reference = reference_subject(policy);

    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);
    raw.insert(1, minted);
    domain.import_blocks(raw).unwrap();

    let (named, named_tx) = reference_block_with(
        3510,
        &reference,
        Some(mint_tx),
        Some(1),
        false,
        naming(policy, &reference),
    );
    let tip = domain.import_blocks(vec![named]).unwrap();

    let read = read_asset(&domain, tip, &reference).await;
    assert_read_as_kept(&read, state_kept(&domain, &reference), "a CIP-25 mint");

    let read = read.unwrap();
    assert_eq!(read.fold.metadata_tx, Some(named_tx));
    assert_eq!((read.quantity, read.mint_tx_count), (2, 2));
}

/// The `assets` watermark is the newest epoch no rollback can reach: with
/// 500-slot epochs and `k` = 5, two epochs behind the tip's.
#[test]
fn the_assets_watermark_trails_the_tip_by_two_epochs() {
    let cfg = plain(SyntheticBlockConfig {
        block_count: 2,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain =
        ToyDomain::new_with_genesis_and_config(genesis_with(500, 5, 9), chain_config, None, None);
    domain.import_blocks(raw).unwrap();

    let chain = chain_of(&domain);

    assert_eq!(assets::assets_watermark(&domain, &chain, 1), None);
    assert_eq!(assets::assets_watermark(&domain, &chain, 5), Some(3));
    assert_eq!(assets::assets_watermark(&domain, &chain, 7), Some(5));
}

/// A seal that lands between `first_mint_slot`'s first index read and its
/// staged-row read moves the first mint out of the staged rows and into the
/// index; the second index read finds it there.
#[test]
fn first_mint_slot_finds_an_asset_sealed_during_the_lookup() {
    let cfg = SyntheticBlockConfig {
        block_count: 2,
        txs_per_block: 1,
        ..Default::default()
    };

    let (blocks, _, chain_config) = build_synthetic_blocks(cfg);
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);

    for block in blocks {
        domain.roll_forward(block).unwrap();
    }

    let (key, row) = domain
        .state()
        .iter_entities_typed::<FirstMintSet>(None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    let subject = row.subject();
    let faulty = FaultyToyDomain::new(domain.clone(), TestFault::None);

    let sealer = domain.clone();
    faulty.after_entity_slots(move || {
        let writer = sealer.archive().start_writer().unwrap();
        writer
            .write_entity(entities::ASSET, key.member.0.as_slice(), row.slot)
            .unwrap();
        writer.commit().unwrap();

        let writer = sealer.state().start_writer().unwrap();
        writer
            .save_entity_typed::<FirstMintSet>(&key, None)
            .unwrap();
        writer.commit().unwrap();
    });

    let chain = chain_of(&domain);
    let tip = domain.state().read_cursor().unwrap().unwrap().slot();
    let (tip, _) = chain.slot_epoch(tip);

    let found = assets::first_mint_slot(&faulty, &chain, tip, &subject).unwrap();

    assert_eq!(found, Some(row.slot));
    assert!(staged(&domain)
        .iter()
        .all(|(_, staged, _)| *staged != subject));
}

/// A seal that lands between an entry's staged read and its sealed read moves
/// an epoch's mints from the first to the second. The build reads the staged
/// rows first, so it counts that epoch from them, as a build after the seal
/// counts it from the chunks.
#[test]
fn an_entry_built_across_a_seal_counts_every_epoch() {
    const P: [u8; 28] = [0x63; 28];

    let blocks = 14;
    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        mints_by_block: (0..blocks)
            .map(|_| vec![vec![(P, "X".to_string(), 1)]])
            .collect(),
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain =
        ToyDomain::new_with_genesis_and_config(genesis_with(500, 5, 9), chain_config, None, None);

    // up to epoch 5, which sealed epochs 0 to 3 and left 4 staged
    let tip = domain.import_blocks(raw[..11].to_vec()).unwrap();
    let chain = chain_of(&domain);
    assert_eq!(chain.slot_epoch(tip).0, 5);

    let in_epoch_4 = |domain: &ToyDomain| staged_mints(domain).iter().any(|(e, ..)| *e == 4);
    assert!(in_epoch_4(&domain));

    let x = subject(P, "X");
    let faulty = FaultyToyDomain::new(domain.clone(), TestFault::None);

    // the next boundary seals epoch 4 right after the build's first sealed read
    let sealer = domain.clone();
    let rest = raw[11..].to_vec();
    faulty.after_read_logs(move || {
        sealer.import_blocks(rest).unwrap();
    });

    let built = assets::build_asset_entry(&faulty, &chain, &x, 0, 4).unwrap();
    assert!(!in_epoch_4(&domain), "epoch 4 not sealed by the hook");

    let sealed = assets::build_asset_entry(&domain, &chain, &x, 0, 4).unwrap();

    let built = AssetEntry::decode(&built.value).unwrap();
    let sealed = AssetEntry::decode(&sealed.value).unwrap();

    // one mint at slot 250, then two per 500-slot epoch up to epoch 4
    assert_eq!(sealed.fold.mint_tx_count, 9);
    assert_eq!(built.fold, sealed.fold);
}

/// An entry that started at another epoch than the asset's first mint, as one
/// a read filled before a rebuild corrected the first mint, is not served:
/// the read folds from the first mint, and the entry is built again from
/// there.
#[tokio::test]
async fn an_entry_from_another_first_mint_is_built_again() {
    const P: [u8; 28] = [0x64; 28];

    let blocks = 14;
    let mints_by_block = (0..blocks)
        .map(|i| match i {
            0 => vec![vec![(P, "X".to_string(), 5)]],
            6 => vec![vec![(P, "X".to_string(), 3)]],
            _ => vec![vec![(P, "FILL".to_string(), 1)]],
        })
        .collect();

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        mints_by_block,
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain =
        ToyDomain::new_with_genesis_and_config(genesis_with(500, 5, 9), chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    let tip = domain.import_blocks(raw).unwrap();
    let chain = chain_of(&domain);
    let watermark = assets::assets_watermark(&domain, &chain, chain.slot_epoch(tip).0).unwrap();

    // the re-mint's epoch, 3, taken for the first mint's
    let x = subject(P, "X");
    let stale = assets::build_asset_entry(&domain, &chain, &x, 3, watermark).unwrap();
    domain
        .cache()
        .get_or_fill(ASSETS.name, &x, move || {
            Ok::<_, dolos_core::ChainError>(stale)
        })
        .await
        .unwrap();

    let entry = cached_entry(&domain, &x).unwrap();
    assert_eq!((entry.start, entry.fold.quantity()), (3, 3));

    let read = read_asset(&domain, tip, &x).await;
    assert_read_as_kept(&read, state_kept(&domain, &x), "past a stale entry");
    assert_eq!(read.unwrap().quantity, 8);

    for _ in 0..100 {
        if cached_entry(&domain, &x).unwrap().start == 0 {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let entry = cached_entry(&domain, &x).unwrap();
    assert_eq!((entry.start, entry.watermark), (0, watermark));

    let warm = read_asset(&domain, tip, &x).await;
    assert_read_as_kept(&warm, state_kept(&domain, &x), "built again");
}

/// A read walks at most its limit of a label-100 token's tagged blocks past
/// the cached epochs, newest first. One that finds no metadata tx within it
/// keeps the one the cached epochs found; the lists read the supply alone.
#[tokio::test]
async fn a_bounded_walk_keeps_the_cached_metadata_tx() {
    let genesis = genesis_with(500, 5, 9);

    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    let reference = reference_subject([0x74; 28]);

    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);
    raw.insert(1, minted);
    domain.import_blocks(raw).unwrap();

    // in the tail: a new datum, then a move without one
    let (updated, update_tx) = reference_block(3510, &reference, Some(mint_tx), None, true);
    let (moved, _) = reference_block(3520, &reference, Some(update_tx), None, false);
    let tip = domain.import_blocks(vec![updated, moved]).unwrap();

    let chain = chain_of(&domain);
    let (epoch, _) = chain.slot_epoch(tip);

    let read = |walk| {
        let (domain, chain, reference) = (domain.clone(), chain.clone(), reference.clone());

        async move {
            assets::asset_summary(&domain, &chain, epoch, reference, &WalkBudget::new(walk))
                .await
                .unwrap()
                .unwrap()
                .fold
        }
    };

    // the move alone: the cached epochs' mint
    assert_eq!(read(1).await.metadata_tx, Some(mint_tx));

    // the move, then the update
    assert_eq!(read(2).await.metadata_tx, Some(update_tx));
    assert_eq!(read(WALK).await.metadata_tx, Some(update_tx));

    let quantity = assets::asset_quantity(&domain, &chain, epoch, reference.clone())
        .await
        .unwrap();
    assert_eq!(quantity, Some(1));
}

/// An entry extended across a label-100 update answers what one built afresh
/// answers, though it counts the blocks of both walks in its cost: `verify`
/// compares the entries without it.
#[tokio::test]
async fn an_entry_extended_across_a_label_100_update_answers_as_built_afresh() {
    let genesis = genesis_with(500, 5, 9);

    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    let reference = reference_subject([0x75; 28]);

    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);
    raw.insert(1, minted);

    // up to slot 2250, epoch 4: the entry fills at watermark 2
    let early = domain.import_blocks(raw[..10].to_vec()).unwrap();
    read_asset(&domain, early, &reference).await.unwrap();
    assert_eq!(cached_entry(&domain, &reference).unwrap().watermark, 2);

    // updated in epoch 4, which the next watermark covers
    let (updated, update_tx) = reference_block(2260, &reference, Some(mint_tx), None, true);
    let mut rest = vec![updated];
    rest.extend(raw[10..].iter().cloned());
    let tip = domain.import_blocks(rest).unwrap();

    let chain = chain_of(&domain);
    let watermark = assets::assets_watermark(&domain, &chain, chain.slot_epoch(tip).0).unwrap();
    assert!(watermark >= 4);

    let read = read_asset(&domain, tip, &reference).await.unwrap();
    assert_eq!(read.fold.metadata_tx, Some(update_tx));

    for _ in 0..100 {
        if cached_entry(&domain, &reference).unwrap().watermark == watermark {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let stored = domain.cache().get(ASSETS.name, &reference).unwrap();
    let afresh = assets::build_asset_entry(&domain, &chain, &reference, 0, watermark).unwrap();

    let (extended, built) = (
        AssetEntry::decode(&stored).unwrap(),
        AssetEntry::decode(&afresh.value).unwrap(),
    );
    assert_eq!(extended.watermark, watermark);
    assert_eq!(extended.fold.metadata_tx, Some(update_tx));

    // the mint block the first walk read is counted once more
    assert_eq!(extended.cost, built.cost + 1);
    assert_eq!(
        AssetEntry::without_cost(&stored),
        AssetEntry::without_cost(&afresh.value)
    );
}

/// The 14-block chain of the label-100 tests: slots 250 to 3500 in 500-slot
/// epochs with `k` = 5, so its tip is in epoch 7 and the watermark is 5, with
/// `insert` as its second block. Registers the `assets` namespace.
fn label_100_chain(insert: Option<dolos_core::RawBlock>) -> (ToyDomain, Vec<dolos_core::RawBlock>) {
    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain =
        ToyDomain::new_with_genesis_and_config(genesis_with(500, 5, 9), chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    if let Some(block) = insert {
        raw.insert(1, block);
    }

    (domain, raw)
}

/// `n` moves of `subject` without a datum, one block each from slot `from`,
/// spending `input` first.
fn moves(subject: &[u8], mut input: Hash<32>, from: u64, n: u64) -> Vec<dolos_core::RawBlock> {
    let mut out = vec![];

    for i in 0..n {
        let (block, tx) = reference_block(from + 10 * i, subject, Some(input), None, false);
        out.push(block);
        input = tx;
    }

    out
}

/// The metadata tx a read of `subject` answers within `budget`.
async fn metadata_within(
    domain: &ToyDomain,
    tip: u64,
    subject: &[u8],
    budget: &WalkBudget,
) -> Option<Hash<32>> {
    let chain = chain_of(domain);
    let (epoch, _) = chain.slot_epoch(tip);

    assets::asset_summary(domain, &chain, epoch, subject.to_vec(), budget)
        .await
        .unwrap()
        .expect("minted")
        .fold
        .metadata_tx
}

/// The moves without a datum after a token's last metadata tx.
const MOVES: u64 = 4;

/// With the cache off the `assets` namespace is not registered, as a miss
/// would walk a label-100 token's whole history and keep nothing: a list read
/// walks no block, and a detail read no more than its budget.
#[tokio::test]
async fn with_the_cache_off_a_read_walks_within_its_budget() {
    use dolos_core::config::CacheConfig;
    use dolos_testing::{
        measured::MeasuredStores,
        toy_domain::{MemoryStores, ToyStores as _},
    };
    use std::sync::atomic::Ordering;

    type Measured = ToyDomain<MeasuredStores<MemoryStores>>;

    const MANY: u64 = 20;

    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);

    let domain = ToyDomain::with_stores(
        genesis_with(500, 5, 9),
        chain_config,
        None,
        None,
        MeasuredStores::new(MemoryStores::open()),
    )
    .with_cache_config(CacheConfig {
        enabled: false,
        ..CacheConfig::default()
    });

    assert!(!assets::register_assets_cache(&domain).unwrap());

    // minted with a datum, then moved without one, all in epoch 0
    let reference = reference_subject([0x76; 28]);
    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);
    raw.insert(1, minted);

    for (i, moved) in moves(&reference, mint_tx, 261, MANY)
        .into_iter()
        .enumerate()
    {
        raw.insert(2 + i, moved);
    }

    let tip = domain.import_blocks(raw).unwrap();

    fn decoded(domain: &Measured) -> u64 {
        domain
            .archive()
            .counters
            .block_reads
            .load(Ordering::Relaxed)
    }

    let chain = dolos_cardano::eras::load_era_summary::<Measured>(domain.state()).unwrap();
    let (epoch, _) = chain.slot_epoch(tip);

    let before = decoded(&domain);
    let quantity = assets::asset_quantity(&domain, &chain, epoch, reference.clone())
        .await
        .unwrap();
    assert_eq!((quantity, decoded(&domain) - before), (Some(1), 0));

    let read = |blocks| {
        let (domain, chain, reference) = (domain.clone(), chain.clone(), reference.clone());

        async move {
            let before = decoded(&domain);
            let summary =
                assets::asset_summary(&domain, &chain, epoch, reference, &WalkBudget::new(blocks))
                    .await
                    .unwrap()
                    .unwrap();

            (summary.fold.metadata_tx, decoded(&domain) - before)
        }
    };

    // no entry stands behind the read, so a walk short of the mint finds none
    assert_eq!(read(1).await, (None, 1));
    assert_eq!(read(MANY + 1).await, (Some(mint_tx), MANY + 1));
}

/// One request's walks share its budget: a token whose walk spends it leaves
/// none for the next, which keeps its cached metadata tx, where a budget of
/// its own finds its update.
#[tokio::test]
async fn the_walks_of_one_request_share_its_budget() {
    let (a, b) = (reference_subject([0x77; 28]), reference_subject([0x78; 28]));

    let (minted_a, mint_a) = reference_block(260, &a, None, Some(1), true);
    let (minted_b, mint_b) = reference_block(270, &b, None, Some(1), true);

    let (domain, mut raw) = label_100_chain(Some(minted_a));
    raw.insert(2, minted_b);
    domain.import_blocks(raw).unwrap();

    // in the tail: b updated, then a moved without a datum, past b's update
    let (updated_b, update_b) = reference_block(3510, &b, Some(mint_b), None, true);
    let mut tail = vec![updated_b];
    tail.extend(moves(&a, mint_a, 3520, MOVES));
    let tip = domain.import_blocks(tail).unwrap();

    let shared = WalkBudget::new(MOVES);
    assert_eq!(
        metadata_within(&domain, tip, &a, &shared).await,
        Some(mint_a)
    );
    assert_eq!(
        metadata_within(&domain, tip, &b, &shared).await,
        Some(mint_b)
    );

    let own = WalkBudget::new(MOVES);
    assert_eq!(
        metadata_within(&domain, tip, &b, &own).await,
        Some(update_b)
    );
}

/// A walk stopped by its budget keeps the newest mint whose CIP-25 metadata
/// names the token, though older than the blocks it read: in the tail, over
/// the entry's older tx, and with no entry behind the read.
#[tokio::test]
async fn a_stopped_walk_keeps_a_cip25_mint_of_the_walked_epochs() {
    let policy = [0x79; 28];
    let reference = reference_subject(policy);
    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);

    let (domain, raw) = label_100_chain(Some(minted));
    domain.import_blocks(raw).unwrap();

    let (named, named_tx) = reference_block_with(
        3510,
        &reference,
        Some(mint_tx),
        Some(1),
        false,
        naming(policy, &reference),
    );
    let mut tail = vec![named];
    tail.extend(moves(&reference, named_tx, 3520, MOVES));
    let tip = domain.import_blocks(tail).unwrap();

    assert_eq!(state_kept(&domain, &reference).unwrap().4, Some(named_tx));

    let stopped = metadata_within(&domain, tip, &reference, &WalkBudget::new(MOVES)).await;
    assert_eq!(stopped, Some(named_tx), "over the entry's mint");

    let entry = cached_entry(&domain, &reference).expect("filled by the read");
    assert_eq!(entry.fold.metadata_tx, Some(mint_tx));

    // a token first minted in the tail, named by its mint, without an entry
    let young_policy = [0x7a; 28];
    let young = reference_subject(young_policy);
    let (named, young_tx) = reference_block_with(
        3570,
        &young,
        None,
        Some(1),
        false,
        naming(young_policy, &young),
    );
    let mut tail = vec![named];
    tail.extend(moves(&young, young_tx, 3580, MOVES));
    let tip = domain.import_blocks(tail).unwrap();

    let stopped = metadata_within(&domain, tip, &young, &WalkBudget::new(MOVES)).await;
    assert_eq!(stopped, Some(young_tx), "without an entry");
    assert!(cached_entry(&domain, &young).is_none());
}

/// A token first minted in the tail, with a datum, then moved more times than
/// the budget allows: with no entry behind the read and no CIP-25 mint, the
/// read answers no metadata tx, as documented.
#[tokio::test]
async fn a_young_token_past_its_budget_answers_no_metadata_tx() {
    let (domain, raw) = label_100_chain(None);
    domain.import_blocks(raw).unwrap();

    let reference = reference_subject([0x7b; 28]);
    let (minted, mint_tx) = reference_block(3510, &reference, None, Some(1), true);
    let mut tail = vec![minted];
    tail.extend(moves(&reference, mint_tx, 3520, MOVES));
    let tip = domain.import_blocks(tail).unwrap();

    let short = metadata_within(&domain, tip, &reference, &WalkBudget::new(MOVES)).await;
    let enough = metadata_within(&domain, tip, &reference, &WalkBudget::new(MOVES + 1)).await;

    assert_eq!((short, enough), (None, Some(mint_tx)));
}

/// An entry from another first mint sends the read past the cache, folding
/// from the first mint within the request's budget: the tail spends it, so the
/// epochs before the tail are not walked, and the read answers no metadata tx.
#[tokio::test]
async fn an_entry_from_another_first_mint_reads_within_the_budget() {
    let reference = reference_subject([0x7c; 28]);
    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);

    let (domain, raw) = label_100_chain(Some(minted));
    domain.import_blocks(raw).unwrap();
    let tip = domain
        .import_blocks(moves(&reference, mint_tx, 3510, MOVES))
        .unwrap();

    let chain = chain_of(&domain);
    let watermark = assets::assets_watermark(&domain, &chain, chain.slot_epoch(tip).0).unwrap();

    let stale = assets::build_asset_entry(&domain, &chain, &reference, 3, watermark).unwrap();
    domain
        .cache()
        .get_or_fill(ASSETS.name, &reference, move || {
            Ok::<_, dolos_core::ChainError>(stale)
        })
        .await
        .unwrap();
    assert_eq!(cached_entry(&domain, &reference).unwrap().start, 3);

    let short = metadata_within(&domain, tip, &reference, &WalkBudget::new(MOVES)).await;
    let enough = metadata_within(&domain, tip, &reference, &WalkBudget::new(MOVES + 1)).await;

    assert_eq!((short, enough), (None, Some(mint_tx)));
}

/// An entry behind the watermark folds the epochs between it and the tail
/// within the request's budget: the tail's moves spend it, so the update
/// between them is not read and the entry's tx stands.
#[tokio::test]
async fn a_lagging_entry_reads_within_the_budget() {
    let reference = reference_subject([0x7d; 28]);
    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);

    let (domain, raw) = label_100_chain(Some(minted));

    // up to slot 2250, epoch 4: the entry fills at watermark 2
    let early = domain.import_blocks(raw[..10].to_vec()).unwrap();
    read_asset(&domain, early, &reference).await.unwrap();
    assert_eq!(cached_entry(&domain, &reference).unwrap().watermark, 2);

    let (updated, update_tx) = reference_block(2260, &reference, Some(mint_tx), None, true);
    let mut rest = vec![updated];
    rest.extend(raw[10..].iter().cloned());
    rest.extend(moves(&reference, update_tx, 3510, MOVES));
    let tip = domain.import_blocks(rest).unwrap();

    assert_eq!(cached_entry(&domain, &reference).unwrap().watermark, 2);

    let short = metadata_within(&domain, tip, &reference, &WalkBudget::new(MOVES)).await;
    assert_eq!(short, Some(mint_tx));

    let enough = metadata_within(&domain, tip, &reference, &WalkBudget::new(MOVES + 1)).await;
    assert_eq!(enough, Some(update_tx));
}

/// A read of a lagging entry decodes each tagged block once: the tail's
/// moves, then the epochs between the entry and the tail back to the update,
/// beside the background extension's own walk of those epochs.
#[tokio::test]
async fn a_lagging_read_decodes_each_block_once() {
    use dolos_testing::{
        measured::MeasuredStores,
        toy_domain::{MemoryStores, ToyStores as _},
    };
    use std::sync::atomic::Ordering;

    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);

    let domain = ToyDomain::with_stores(
        genesis_with(500, 5, 9),
        chain_config,
        None,
        None,
        MeasuredStores::new(MemoryStores::open()),
    );
    assert!(assets::register_assets_cache(&domain).unwrap());

    let reference = reference_subject([0x7e; 28]);
    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);
    raw.insert(1, minted);

    type Measured = ToyDomain<MeasuredStores<MemoryStores>>;

    let chain_now = |domain: &Measured| {
        dolos_cardano::eras::load_era_summary::<Measured>(domain.state()).unwrap()
    };

    let early = domain.import_blocks(raw[..10].to_vec()).unwrap();
    let chain = chain_now(&domain);
    assets::asset_summary(
        &domain,
        &chain,
        chain.slot_epoch(early).0,
        reference.clone(),
        &WalkBudget::new(WALK),
    )
    .await
    .unwrap();

    let (updated, update_tx) = reference_block(2260, &reference, Some(mint_tx), None, true);
    let mut rest = vec![updated];
    rest.extend(raw[10..].iter().cloned());
    rest.extend(moves(&reference, update_tx, 3510, MOVES));
    let tip = domain.import_blocks(rest).unwrap();

    let chain = chain_now(&domain);
    let (epoch, _) = chain.slot_epoch(tip);
    let watermark = assets::assets_watermark(&domain, &chain, epoch).unwrap();
    let decoded = || {
        domain
            .archive()
            .counters
            .block_reads
            .load(Ordering::Relaxed)
    };

    let before = decoded();
    let read = assets::asset_summary(
        &domain,
        &chain,
        epoch,
        reference.clone(),
        &WalkBudget::new(WALK),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(read.fold.metadata_tx, Some(update_tx));

    for _ in 0..100 {
        let value = domain.cache().get(ASSETS.name, &reference).unwrap();

        if AssetEntry::decode(&value).unwrap().watermark == watermark {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // the read: the moves, then the update; the extension: the update
    assert_eq!(decoded() - before, MOVES + 1 + 1);
}

/// Finding TA3 of the third QA review of dolos#1524: a read past a stale entry
/// (another first mint) adds the tail's mints to the epochs it folds up to the
/// watermark.
#[tokio::test]
async fn a_stale_entry_read_adds_the_tail() {
    const P: [u8; 28] = [0x91; 28];

    let blocks = 14;
    let mints_by_block = (0..blocks)
        .map(|i| match i {
            0 => vec![vec![(P, "X".to_string(), 5)]],
            6 => vec![vec![(P, "X".to_string(), 3)]],
            // slot 3500, epoch 7: past the watermark, 5
            13 => vec![vec![(P, "X".to_string(), 2)]],
            _ => vec![vec![(P, "FILL".to_string(), 1)]],
        })
        .collect();

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        mints_by_block,
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain =
        ToyDomain::new_with_genesis_and_config(genesis_with(500, 5, 9), chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    let tip = domain.import_blocks(raw).unwrap();
    let chain = chain_of(&domain);
    let watermark = assets::assets_watermark(&domain, &chain, chain.slot_epoch(tip).0).unwrap();
    assert_eq!(watermark, 5);

    let x = subject(P, "X");
    let stale = assets::build_asset_entry(&domain, &chain, &x, 3, watermark).unwrap();
    domain
        .cache()
        .get_or_fill(ASSETS.name, &x, move || {
            Ok::<_, dolos_core::ChainError>(stale)
        })
        .await
        .unwrap();
    assert_eq!(cached_entry(&domain, &x).unwrap().start, 3);

    let read = read_asset(&domain, tip, &x).await;
    assert_read_as_kept(&read, state_kept(&domain, &x), "stale entry, tail mint");
    assert_eq!(read.unwrap().quantity, 10);
}

/// Finding TA4 of the third QA review of dolos#1524: a lagging entry's read
/// answers the tail's naming mint over one in the epochs between the entry and
/// the tail.
#[tokio::test]
async fn a_lagging_read_takes_the_tail_naming_mint() {
    let mint = |policy, name: &str, amount| (policy, name.to_string(), amount);

    let blocks = 14;
    let mints_by_block = (0..blocks)
        .map(|i| match i {
            // epochs 0 (the entry), 4 (between), 7 (the tail)
            0 | 8 | 13 => vec![vec![mint(TEXT_KEYS, "A", 1)]],
            _ => vec![vec![mint([0x92; 28], "FILL", 1)]],
        })
        .collect();

    let cfg = plain(SyntheticBlockConfig {
        block_count: blocks,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        mints_by_block,
        metadata_entries: vec![label_721()],
        ..Default::default()
    });

    let (raw, _, chain_config) = build_synthetic_blocks(cfg);
    let domain =
        ToyDomain::new_with_genesis_and_config(genesis_with(500, 5, 9), chain_config, None, None);
    assert!(assets::register_assets_cache(&domain).unwrap());

    let a = subject(TEXT_KEYS, "A");

    // tip in epoch 5: the entry fills at watermark 3
    let early = domain.import_blocks(raw[..10].to_vec()).unwrap();
    read_asset(&domain, early, &a).await.unwrap();
    assert_eq!(cached_entry(&domain, &a).unwrap().watermark, 3);

    // tip in epoch 7, watermark 5: between is 4..=5, the tail 6..=7
    let tip = domain.import_blocks(raw[10..].to_vec()).unwrap();
    assert_eq!(cached_entry(&domain, &a).unwrap().watermark, 3);

    let lagging = read_asset(&domain, tip, &a).await;
    assert_read_as_kept(
        &lagging,
        state_kept(&domain, &a),
        "lagging, between and tail",
    );
}

/// Finding TA5 of the third QA review of dolos#1524: a label-100 token named by
/// a CIP-25 mint without a datum, then given a datum, answers the datum's tx,
/// as AssetState kept it: in the tail, and inside an entry's range.
#[tokio::test]
async fn a_datum_after_a_naming_mint_wins() {
    // inside the entry's range: epoch 0
    let old_policy = [0x94; 28];
    let old = reference_subject(old_policy);
    let (named, named_tx) =
        reference_block_with(260, &old, None, Some(1), false, naming(old_policy, &old));
    let (updated, _) = reference_block(270, &old, Some(named_tx), None, true);

    let (domain, mut raw) = label_100_chain(Some(named));
    raw.insert(2, updated);
    domain.import_blocks(raw).unwrap();

    // in the tail: epoch 7, past the watermark
    let young_policy = [0x93; 28];
    let young = reference_subject(young_policy);
    let (named, named_tx) = reference_block_with(
        3510,
        &young,
        None,
        Some(1),
        false,
        naming(young_policy, &young),
    );
    let (updated, update_tx) = reference_block(3520, &young, Some(named_tx), None, true);
    let tip = domain.import_blocks(vec![named, updated]).unwrap();

    assert_eq!(state_kept(&domain, &young).unwrap().4, Some(update_tx));

    for (what, subject) in [("in the entry", &old), ("in the tail", &young)] {
        let read = read_asset(&domain, tip, subject).await;
        assert_read_as_kept(&read, state_kept(&domain, subject), what);
    }
}

/// Finding TA2 of the third QA review of dolos#1524: with the cache off, a read
/// whose tail walk finds the metadata tx walks no block before the tail.
#[tokio::test]
async fn a_tail_hit_walks_nothing_before_the_tail() {
    use dolos_core::config::CacheConfig;
    use dolos_testing::{
        measured::MeasuredStores,
        toy_domain::{MemoryStores, ToyStores as _},
    };
    use std::sync::atomic::Ordering;

    type Measured = ToyDomain<MeasuredStores<MemoryStores>>;

    const MANY: u64 = 20;

    let cfg = plain(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        ..Default::default()
    });

    let (mut raw, _, chain_config) = build_synthetic_blocks(cfg);

    let domain = ToyDomain::with_stores(
        genesis_with(500, 5, 9),
        chain_config,
        None,
        None,
        MeasuredStores::new(MemoryStores::open()),
    )
    .with_cache_config(CacheConfig {
        enabled: false,
        ..CacheConfig::default()
    });

    assert!(!assets::register_assets_cache(&domain).unwrap());

    // minted with a datum, then moved MANY times without one, in epoch 0
    let reference = reference_subject([0x95; 28]);
    let (minted, mint_tx) = reference_block(260, &reference, None, Some(1), true);
    raw.insert(1, minted);

    let mut input = mint_tx;

    for i in 0..MANY {
        let (moved, tx) = reference_block(261 + 10 * i, &reference, Some(input), None, false);
        raw.insert(2 + i as usize, moved);
        input = tx;
    }

    domain.import_blocks(raw).unwrap();

    // updated with a datum in the tail, epoch 7
    let (updated, update_tx) = reference_block(3510, &reference, Some(input), None, true);
    let tip = domain.import_blocks(vec![updated]).unwrap();

    let chain = dolos_cardano::eras::load_era_summary::<Measured>(domain.state()).unwrap();
    let (epoch, _) = chain.slot_epoch(tip);

    let decoded = || {
        domain
            .archive()
            .counters
            .block_reads
            .load(Ordering::Relaxed)
    };

    let before = decoded();
    let summary = assets::asset_summary(&domain, &chain, epoch, reference, &WalkBudget::new(WALK))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        (summary.fold.metadata_tx, decoded() - before),
        (Some(update_tx), 1)
    );
}

/// Finding W1 of the third QA review of dolos#1524: the units of one request
/// walk on shares of its budget, so each answers by its share whether the
/// walks run one after another, in either order, or at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn walks_on_shares_answer_by_their_share_alone() {
    let (domain, raw) = label_100_chain(None);
    domain.import_blocks(raw).unwrap();

    // two young tokens, each minted with a datum, then moved without one, so
    // that a walk reads MOVES + 1 blocks to find its mint, interleaved
    let (a, b) = (reference_subject([0x7d; 28]), reference_subject([0x7e; 28]));
    let (minted_a, mint_a) = reference_block(3510, &a, None, Some(1), true);
    let (minted_b, mint_b) = reference_block(3515, &b, None, Some(1), true);

    let mut tail = vec![minted_a, minted_b];

    for (moved_a, moved_b) in moves(&a, mint_a, 3520, MOVES)
        .into_iter()
        .zip(moves(&b, mint_b, 3525, MOVES))
    {
        tail.extend([moved_a, moved_b]);
    }

    let tip = domain.import_blocks(tail).unwrap();
    let chain = chain_of(&domain);
    let (epoch, _) = chain.slot_epoch(tip);

    // a walk for a, a block short of one for b
    let shares = || WalkBudget::split(2 * (MOVES + 1) - 1, 2);
    let expected = (Some(mint_a), None);

    let [for_a, for_b] = <[WalkBudget; 2]>::try_from(shares()).unwrap();
    let b_first = metadata_within(&domain, tip, &b, &for_b).await;
    assert_eq!(
        (metadata_within(&domain, tip, &a, &for_a).await, b_first),
        expected
    );

    for round in 0..20 {
        let [for_a, for_b] = <[WalkBudget; 2]>::try_from(shares()).unwrap();

        let (x, y) = tokio::join!(
            assets::asset_summary(&domain, &chain, epoch, a.clone(), &for_a),
            assets::asset_summary(&domain, &chain, epoch, b.clone(), &for_b),
        );

        let answers = (
            x.unwrap().unwrap().fold.metadata_tx,
            y.unwrap().unwrap().fold.metadata_tx,
        );
        assert_eq!(answers, expected, "round {round}");
    }
}
