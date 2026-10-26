use dolos_core::{ChainError, ChainPoint, Domain, Genesis, StateStore as _, StateWriter as _};

use crate::{
    gov_from_conway_genesis, indexes::utxo_index_delta_from_utxo_delta, pots::Pots,
    utils::nonce_stability_window, EndStats, EpochState, EpochValue, EraBoundary, EraKey, EraState,
    GovState, Lovelace, Nonces, PParamsSet, RollingStats,
};

mod staking;
pub mod work_unit;

pub use work_unit::GenesisWorkUnit;

fn get_utxo_amount(genesis: &Genesis) -> Lovelace {
    let byron_utxo = pallas::interop::hardano::configs::byron::genesis_utxos(&genesis.byron)
        .iter()
        .fold(0, |acc, (_, _, amount)| acc + amount);

    let shelley_utxo = pallas::interop::hardano::configs::shelley::shelley_utxos(&genesis.shelley)
        .iter()
        .fold(0, |acc, (_, _, amount)| acc + amount);

    byron_utxo + shelley_utxo
}

fn bootstrap_pots(pparams: &PParamsSet, genesis: &Genesis) -> Result<Pots, ChainError> {
    let utxos = get_utxo_amount(genesis);

    let max_supply = genesis
        .shelley
        .max_lovelace_supply
        .ok_or(ChainError::GenesisFieldMissing(
            "max_lovelace_supply".to_string(),
        ))?;

    let reserves = max_supply - utxos;

    Ok(Pots {
        reserves,
        utxos,
        deposit_per_pool: pparams.pool_deposit_or_default(),
        deposit_per_account: pparams.key_deposit_or_default(),
        ..Default::default()
    })
}

/// The pparams the chain opens with: Byron's, fast-forwarded to the
/// genesis's starting protocol when it has one.
fn starting_pparams(genesis: &Genesis) -> Result<PParamsSet, ChainError> {
    let pparams = crate::forks::from_byron_genesis(&genesis.byron);

    match genesis.starting_protocol()? {
        Some(protocol) if protocol > crate::forks::LATEST_PROTOCOL => {
            Err(ChainError::UnsupportedStartingProtocol(protocol.into()))
        }
        Some(protocol) => Ok(crate::forks::force_pparams_version(
            &pparams, genesis, 0, protocol,
        )?),
        None => Ok(pparams),
    }
}

pub fn bootstrap_epoch<D: Domain>(
    state: &D::State,
    genesis: &Genesis,
) -> Result<EpochState, ChainError> {
    let pparams = starting_pparams(genesis)?;

    // When we start on Shelley or beyond, we need to bootstrap the nonces.
    let nonces = genesis
        .starting_protocol()?
        .is_some_and(|protocol| protocol >= 2)
        .then(|| Nonces::bootstrap(genesis.shelley_hash));

    let pots = bootstrap_pots(&pparams, genesis)?;
    let protocol = pparams.ensure_protocol_major()?;

    let pparams = EpochValue::with_genesis(pparams);

    let epoch = EpochState {
        pparams,
        initial_pots: pots,
        largest_stable_slot: genesis.shelley.epoch_length.unwrap() as u64
            - nonce_stability_window(protocol, genesis),
        nonces,
        previous_nonce_tail: None,
        number: 0,
        rolling: EpochValue::with_live(0, RollingStats::default()),
        // Seed `end` with defaults so the first boundary's per-shard
        // Ewrap pass (which runs before the global Ewrap finalize) finds
        // a populated slot to accumulate into. ESTART's `EpochTransition`
        // re-seeds it on every subsequent epoch.
        end: Some(EndStats::default()),
        incentives: None,
        ewrap_progress: None,
        estart_progress: None,
        rupd_progress: None,
        active_stake: None,
        estart_tally: None,
        ewrap_finalized: false,
    };

    let writer = state.start_writer()?;
    writer.write_entity_typed(&(), &epoch)?;
    writer.commit()?;

    Ok(epoch)
}

pub fn bootstrap_eras<D: Domain>(state: &D::State, epoch: &EpochState) -> Result<(), ChainError> {
    let pparams = epoch.pparams.unwrap_live();

    let system_start = pparams.ensure_system_start()?;
    let epoch_length = pparams.ensure_epoch_length()?;
    let slot_length = pparams.ensure_slot_length()?;
    let protocol_major = pparams.protocol_major_or_default();

    let era = EraState {
        start: EraBoundary {
            epoch: 0,
            slot: 0,
            timestamp: system_start,
        },
        end: None,
        epoch_length,
        slot_length,
        protocol: protocol_major,
    };

    let writer = state.start_writer()?;
    writer.write_entity_typed(&EraKey::from(protocol_major), &era)?;
    writer.commit()?;

    Ok(())
}

/// Create the governance singleton. Its existence is an invariant that
/// starts here: every store carries the row regardless of era. Networks
/// that start at Conway (protocol >= 9, e.g. devnets) get it
/// activated with the genesis enact-state; everyone else gets the
/// inactive row, activated later at the Chang boundary (`GovGenesisInit`).
pub fn bootstrap_gov<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<(), ChainError> {
    let mut gov = GovState::default();

    if genesis
        .starting_protocol()?
        .is_some_and(|protocol| protocol >= 9)
    {
        let (constitution, committee) = gov_from_conway_genesis(&genesis.conway)?;
        gov.seed_genesis(constitution, committee, 0);
    }

    let writer = state.start_writer()?;
    writer.write_entity_typed(&(), &gov)?;
    writer.commit()?;

    Ok(())
}

pub fn bootstrap_utxos<D: Domain>(
    state: &D::State,
    genesis: &Genesis,
    config: &crate::CardanoLogicConfig,
) -> Result<(), ChainError> {
    let state_writer = state.start_writer()?;

    // Genesis UTxOs from Byron and Shelley genesis files
    let origin_delta = crate::utxoset::compute_origin_delta(genesis);
    state_writer.apply_utxoset(&origin_delta)?;
    state_writer.apply_utxo_tags(&utxo_index_delta_from_utxo_delta(&origin_delta))?;

    // Custom UTxOs from config (e.g., for testing)
    let custom_delta = crate::utxoset::build_custom_utxos_delta(config)?;
    state_writer.apply_utxoset(&custom_delta)?;
    state_writer.apply_utxo_tags(&utxo_index_delta_from_utxo_delta(&custom_delta))?;

    state_writer.commit()?;

    Ok(())
}

pub fn execute<D: Domain>(
    state: &D::State,
    genesis: &Genesis,
    config: &crate::CardanoLogicConfig,
) -> Result<(), ChainError> {
    let epoch = bootstrap_epoch::<D>(state, genesis)?;

    bootstrap_eras::<D>(state, &epoch)?;

    bootstrap_gov::<D>(state, genesis)?;

    bootstrap_utxos::<D>(state, genesis, config)?;

    staking::bootstrap::<D>(state, genesis)?;

    // Mark genesis state as applied by setting the cursor to Origin. This
    // lets crash recovery distinguish "genesis done, no blocks yet" (cursor
    // at Origin) from "genesis never ran" (no cursor), so a restart doesn't
    // re-run genesis and reset a WAL that already holds blocks. Set last so
    // a crash mid-genesis leaves no cursor and genesis re-runs cleanly.
    let writer = state.start_writer()?;
    writer.set_cursor(ChainPoint::Origin)?;
    writer.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use dolos_core::{Domain as _, StateStore as _};
    use dolos_testing::toy_domain::ToyDomain;
    use std::sync::Arc;

    use crate::GovState;

    use super::*;

    fn read_gov(domain: &ToyDomain) -> Option<GovState> {
        domain.state().read_entity_typed::<GovState>(&()).unwrap()
    }

    #[test]
    fn bootstrap_seeds_gov_singleton_for_forced_conway() {
        // the devnet genesis force-starts at protocol 9
        let domain = ToyDomain::new(None, None);

        let (constitution, committee) = gov_from_conway_genesis(&domain.genesis().conway).unwrap();

        let gov = read_gov(&domain).expect("gov singleton seeded at bootstrap");

        assert_eq!(gov.constitution, Some(constitution));
        assert_eq!(gov.committee, Some(committee));
        assert_eq!(gov.num_dormant_epochs, 0);
        assert_eq!(gov.active_since, Some(0));
    }

    /// The pparams a start opened with before it was derived: Byron's,
    /// forced only by an explicit protocol.
    fn forced_only(genesis: &Genesis, forced: Option<u16>) -> PParamsSet {
        let byron = crate::forks::from_byron_genesis(&genesis.byron);

        match forced {
            Some(to) => crate::forks::force_pparams_version(&byron, genesis, 0, to).unwrap(),
            None => byron,
        }
    }

    #[test]
    fn starting_pparams_match_the_forced_ladder() {
        use crate::include::{devnet, mainnet, preprod, preview};
        use pallas::codec::minicbor;

        // preview carried `force_protocol = 6` before the start was derived
        let cases = [
            ("mainnet", mainnet::load(), None),
            ("preprod", preprod::load(), None),
            ("devnet", devnet::load(), Some(9)),
            ("preview", preview::load(), Some(6)),
        ];

        for (name, genesis, forced) in cases {
            let derived = minicbor::to_vec(starting_pparams(&genesis).unwrap()).unwrap();
            let expected = minicbor::to_vec(forced_only(&genesis, forced)).unwrap();

            assert_eq!(derived, expected, "{name}");
        }

        assert_eq!(
            starting_pparams(&preview::load()).unwrap().protocol_major(),
            Some(6)
        );
    }

    #[test]
    fn derived_preview_start_bootstraps_nonces() {
        let genesis = crate::include::preview::load();
        let shelley_hash = genesis.shelley_hash;

        let domain = ToyDomain::new_with_genesis(Arc::new(genesis), None, None);

        let epoch = domain
            .state()
            .read_entity_typed::<EpochState>(&())
            .unwrap()
            .expect("epoch state written at bootstrap");

        assert_eq!(epoch.nonces, Some(Nonces::bootstrap(shelley_hash)));
    }

    #[test]
    fn byron_start_bootstraps_without_nonces() {
        let genesis = crate::include::preprod::load();

        let domain = ToyDomain::new_with_genesis(Arc::new(genesis), None, None);

        let epoch = domain
            .state()
            .read_entity_typed::<EpochState>(&())
            .unwrap()
            .expect("epoch state written at bootstrap");

        assert_eq!(epoch.nonces, None);
    }

    #[test]
    fn derived_conway_start_activates_gov() {
        let mut genesis = crate::include::devnet::load();
        genesis.force_protocol = None;
        genesis.shelley.protocol_params.protocol_version.major = 9;

        let domain = ToyDomain::new_with_genesis(Arc::new(genesis), None, None);

        let gov = read_gov(&domain).expect("gov singleton seeded at bootstrap");
        assert_eq!(gov.active_since, Some(0));
    }

    #[test]
    fn start_past_the_ladder_is_a_typed_error() {
        let mut genesis = crate::include::preview::load();
        genesis.shelley.protocol_params.protocol_version.major =
            (crate::forks::LATEST_PROTOCOL + 1).into();

        let err = starting_pparams(&genesis).unwrap_err();

        assert!(matches!(
            err,
            ChainError::UnsupportedStartingProtocol(major)
                if major == u64::from(crate::forks::LATEST_PROTOCOL) + 1
        ));
    }

    #[test]
    fn start_at_the_top_of_the_ladder_is_supported() {
        let mut genesis = crate::include::preview::load();
        genesis.shelley.protocol_params.protocol_version.major =
            crate::forks::LATEST_PROTOCOL.into();

        let pparams = starting_pparams(&genesis).unwrap();

        assert_eq!(
            pparams.protocol_major(),
            Some(crate::forks::LATEST_PROTOCOL)
        );
    }

    #[test]
    fn bootstrap_seeds_inactive_gov_singleton_before_conway() {
        let mut genesis = crate::include::devnet::load();
        genesis.force_protocol = Some(8);

        let domain = ToyDomain::new_with_genesis(Arc::new(genesis), None, None);

        // the row exists on every store — governance just isn't active yet
        let gov = read_gov(&domain).expect("gov singleton exists pre-Conway");
        assert_eq!(gov, GovState::default());
        assert_eq!(gov.active_since, None);
    }
}
