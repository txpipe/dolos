use dolos_core::{ArchiveStore, Domain, StateStore};
use dolos_testing::{
    performance::{ApiFixture, FixtureShape},
    toy_domain::{FjallStores, ToyStores},
};
use pallas::ledger::{addresses::Address, traverse::MultiEraBlock};

fn fixture(blocks: usize) -> ApiFixture<FjallStores> {
    ApiFixture::new(
        FjallStores::open(),
        FixtureShape {
            blocks,
            ..Default::default()
        },
    )
    .unwrap()
}

#[divan::bench(args = [16, 64], sample_count = 30)]
fn archive_address_tags(bencher: divan::Bencher, blocks: usize) {
    let fixture = fixture(blocks);
    let key = Address::from_bech32(&fixture.vectors.address)
        .unwrap()
        .to_vec();
    let expected: Vec<_> = fixture
        .vectors
        .blocks
        .iter()
        .take(blocks)
        .map(|block| block.slot)
        .collect();
    bencher.bench_local(|| {
        let actual = fixture
            .domain
            .archive()
            .slots_by_tag("address", &key, 0, u64::MAX)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(divan::black_box(actual), expected);
    });
}

#[divan::bench(args = [16, 64], sample_count = 30)]
fn exact_transaction_lookup(bencher: divan::Bencher, blocks: usize) {
    let fixture = fixture(blocks);
    let keys: Vec<_> = fixture
        .vectors
        .blocks
        .iter()
        .take(blocks)
        .map(|block| (hex::decode(&block.tx_hashes[0]).unwrap(), block.slot))
        .collect();
    bencher.bench_local(|| {
        for (hash, slot) in &keys {
            assert_eq!(
                fixture
                    .domain
                    .archive()
                    .slot_by_tx_hash(divan::black_box(hash))
                    .unwrap(),
                Some(*slot)
            );
        }
        assert_eq!(
            fixture
                .domain
                .archive()
                .slot_by_tx_hash(&[0xff; 32])
                .unwrap(),
            None
        );
    });
}

#[divan::bench(args = [16, 64], sample_count = 30)]
fn state_address_tags(bencher: divan::Bencher, blocks: usize) {
    let fixture = fixture(blocks);
    let key = Address::from_bech32(&fixture.vectors.address)
        .unwrap()
        .to_vec();
    bencher.bench_local(|| {
        let actual = fixture
            .domain
            .state()
            .utxos_by_tag("address", divan::black_box(&key))
            .unwrap();
        assert_eq!(actual.len(), blocks);
    });
}

#[divan::bench(args = [16, 64], sample_count = 30)]
fn compressed_valid_block_reads(bencher: divan::Bencher, blocks: usize) {
    let fixture = fixture(blocks);
    bencher.bench_local(|| {
        for (index, expected) in fixture.blocks.iter().enumerate().step_by(3) {
            let slot = fixture.vectors.blocks[index].slot;
            let body = fixture
                .domain
                .archive()
                .get_block_by_slot(&slot)
                .unwrap()
                .unwrap();
            assert_eq!(divan::black_box(body.as_slice()), expected.as_slice());
        }
    });
}

#[divan::bench(args = [16, 64], sample_count = 30)]
fn compressed_reverse_block_scan(bencher: divan::Bencher, blocks: usize) {
    let fixture = fixture(blocks);
    bencher.bench_local(|| {
        let mut count = 0;
        for (slot, body) in fixture
            .domain
            .archive()
            .get_range(Some(1), Some(blocks as u64 + 1))
            .unwrap()
            .rev()
        {
            let block = MultiEraBlock::decode(&body).unwrap();
            assert_eq!(block.slot(), slot);
            divan::black_box(block.header().hash());
            count += 1;
        }
        assert_eq!(count, blocks);
    });
}

#[divan::bench(args = [1000, 10000], sample_count = 30)]
fn logs_only_scale(bencher: divan::Bencher, rows: u64) {
    use dolos_testing::archive::{populate_archive, ArchiveShape};
    let shape = ArchiveShape {
        epochs: 4,
        blocks_per_epoch: 0,
        log_rows_per_epoch: rows,
        slots_per_epoch: 432_000,
        seed: 0,
    };
    let directory = tempfile::tempdir().unwrap();
    let store = dolos_fjall::archive::ArchiveStore::open(
        dolos_cardano::model::build_schema(),
        directory.path(),
        &Default::default(),
    )
    .unwrap();
    populate_archive(&store, "account-epochs", &shape).unwrap();
    assert!(store.get_tip().unwrap().is_none());
    let range = shape.log_key(2, 0)..shape.log_key(3, 0);
    bencher.bench_local(|| {
        let mut count = 0;
        for entry in store.iter_logs("account-epochs", range.clone()).unwrap() {
            divan::black_box(entry.unwrap());
            count += 1;
        }
        assert_eq!(count, rows);
    });
}

#[divan::bench(sample_count = 50)]
fn compressed_real_alonzo_block(bencher: divan::Bencher) {
    use dolos_core::{ArchiveWriter, ChainPoint};
    let body = std::sync::Arc::new(
        hex::decode(include_str!("../../crates/cardano/test_data/alonzo27.block").trim()).unwrap(),
    );
    let block = MultiEraBlock::decode(&body).unwrap();
    let point = ChainPoint::Specific(block.slot(), block.hash());
    let directory = tempfile::tempdir().unwrap();
    let store = dolos_fjall::archive::ArchiveStore::open(
        dolos_cardano::model::build_schema(),
        directory.path(),
        &Default::default(),
    )
    .unwrap();
    let writer = store.start_writer().unwrap();
    writer.apply(&point, &body).unwrap();
    writer.commit().unwrap();
    bencher.bench_local(|| {
        let actual = store.get_block_by_slot(&point.slot()).unwrap().unwrap();
        assert_eq!(divan::black_box(actual.as_slice()), body.as_slice());
    });
}
