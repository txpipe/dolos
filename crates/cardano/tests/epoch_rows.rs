//! The `EpochState` rows EWRAP archives, through the real pipeline.

use std::sync::Arc;

use dolos_cardano::{EpochState, EpochValue, PParamsSet, RollingStats};
use dolos_core::{import::ImportExt as _, ArchiveStore as _, Domain as _};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
    MIN_UTXO_AMOUNT,
};
use pallas::codec::minicbor;

/// `value` read back as a full `EpochValue`, so its `set` and `go` slots show.
fn deep<T, V>(value: &V) -> EpochValue<T>
where
    V: minicbor::Encode<()>,
    T: for<'b> minicbor::Decode<'b, ()>,
{
    minicbor::decode(&minicbor::to_vec(value).unwrap()).unwrap()
}

/// The `EpochState` rows EWRAP archives over a synthetic chain crossing several
/// boundaries.
fn archived_rows() -> Vec<EpochState> {
    let mut genesis = dolos_cardano::include::devnet::load();
    genesis.force_protocol = Some(9);
    genesis.shelley.epoch_length = Some(500);
    genesis.shelley.security_param = Some(5);
    genesis.byron.protocol_consts.k = 5;

    // plain txs, each seed paying exactly the fee of 7, so the pots hold
    let (raw, _, chain_config) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 14,
        txs_per_block: 1,
        slot: 250,
        slot_step: 250,
        plain_txs: true,
        seed_amount: MIN_UTXO_AMOUNT + 7,
        ..Default::default()
    });

    let domain =
        ToyDomain::new_with_genesis_and_config(Arc::new(genesis), chain_config, None, None);
    domain.import_blocks(raw).unwrap();

    domain
        .archive()
        .iter_logs_typed::<EpochState>(None)
        .unwrap()
        .map(|row| row.unwrap().2)
        .collect()
}

/// An archived row past epoch 3 carries `next`, `live` and `mark` of `rolling`
/// and `pparams`, and no `set` or `go` copy of either.
#[test]
fn archived_epoch_rows_keep_no_set_or_go_copies() {
    let rows = archived_rows();

    assert!(
        rows.iter().any(|row| row.number >= 3),
        "no archived row old enough to have held a `go` copy"
    );

    for row in &rows {
        let rolling: EpochValue<RollingStats> = deep(&row.rolling);
        let pparams: EpochValue<PParamsSet> = deep(&row.pparams);

        if row.number >= 1 {
            assert!(
                rolling.mark().is_some(),
                "epoch {}: no rolling mark",
                row.number
            );
            assert!(
                pparams.mark().is_some(),
                "epoch {}: no pparams mark",
                row.number
            );
        }

        assert_eq!(rolling.set(), None, "epoch {}: rolling set", row.number);
        assert_eq!(rolling.go(), None, "epoch {}: rolling go", row.number);
        assert_eq!(pparams.set(), None, "epoch {}: pparams set", row.number);
        assert_eq!(pparams.go(), None, "epoch {}: pparams go", row.number);
    }
}

/// EWRAP writes each row from its finalize state, where its own cursor stands
/// at the last shard and `ewrap_finalized` is set. The archived row keeps
/// none of the sync cursors.
#[test]
fn archived_epoch_rows_carry_no_sync_cursors() {
    let rows = archived_rows();

    assert!(rows.len() >= 3, "too few boundaries: {}", rows.len());

    for row in &rows {
        assert_eq!(
            row.set_sync_cursors(),
            Vec::<&str>::new(),
            "epoch {}",
            row.number
        );
    }
}
