//! Period logs through the real pipeline: staging rolls back with its block,
//! and ESTART seals short epochs.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use dolos_cardano::{
    model::{
        AccountAddressChunk, AccountAddressSet, AssetMintChunk, AssetMintSet, AssetState,
        FirstMintChunk, FirstMintSet, FirstScriptSet,
    },
    periods::is_sealable,
    AccountKey, AssetKey, CardanoDelta,
};
use dolos_core::{
    import::ImportExt as _, sync::SyncExt as _, ArchiveStore as _, ChainPoint, Domain as _,
    Genesis, RangeOf, StateStore as _, WalStore as _,
};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, sample_alonzo_block, SyntheticBlockConfig},
    toy_domain::ToyDomain,
    MIN_UTXO_AMOUNT,
};
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

/// A zero-quantity entry stages no first mint and no mint stats, beside a real
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
    assert_eq!(count(|x| matches!(x, CardanoDelta::MintStatsUpdate(_))), 1);
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
/// get no `script` tag and the scripts are staged once, at the second block,
/// the first valid carrier.
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

    domain.rollback(&points[1]).unwrap();
    assert_eq!(scripts(&domain), after[1]);

    domain.rollback(&points[0]).unwrap();
    assert_eq!(scripts(&domain), none);
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
/// the `metadata_tx` its state holds, and an asset never flagged holds none.
#[test]
fn the_newest_flagged_mint_is_the_metadata_tx() {
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
    domain.import_blocks(raw).unwrap();

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

    for (key, flagged) in [
        (asset(TEXT_KEYS, "A"), true),
        (asset(TEXT_KEYS, "B"), false),
        (asset(BYTE_KEYS, "C"), true),
    ] {
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

        let state = domain
            .state()
            .read_entity_typed::<AssetState>(&key)
            .unwrap()
            .unwrap();

        assert_eq!(newest.is_some(), flagged);
        assert_eq!(newest, state.metadata_tx);
        assert!(mints.last().is_some_and(|x| x.amount < 0) || key != asset(TEXT_KEYS, "A"));
    }
}
