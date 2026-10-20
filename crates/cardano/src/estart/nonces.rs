use dolos_core::{BlockSlot, ChainError};
use pallas::{crypto::hash::Hash, ledger::primitives::NonceVariant};

use crate::{sub, utils::nonce_stability_window, EraKey, NonceTransition, Nonces};

/// The protocol of the epoch being entered: the transition's new version at a
/// hard fork, otherwise the active one.
fn entering_protocol(ctx: &super::WorkContext) -> EraKey {
    ctx.ended_state
        .pparams
        .era_transition()
        .map(|transition| transition.new_version)
        .unwrap_or(ctx.active_protocol)
}

/// The stable slot of the epoch being entered. At a hard fork the new era is
/// written only at commit, so its epoch length and protocol come from the
/// transition rather than the chain summary.
fn next_largest_stable_slot(ctx: &super::WorkContext) -> BlockSlot {
    let next_epoch = ctx.starting_epoch_no() + 1;

    let epoch_finish_slot = match ctx.ended_state.pparams.era_transition() {
        Some(transition) => {
            let (_, entering) = super::commit::split_era(
                ctx.chain_summary.edge().clone(),
                ctx.starting_epoch_no(),
                transition.new_version,
                &ctx.genesis,
            );

            entering.epoch_start(next_epoch)
        }
        None => ctx.chain_summary.epoch_start(next_epoch),
    };

    let stability_window = nonce_stability_window(entering_protocol(ctx).into(), &ctx.genesis);

    sub!(epoch_finish_slot, stability_window)
}

/// The entering epoch's `extra_entropy`. The transition has not rotated the
/// parameters yet, so they are the scheduled set when there is one. Only
/// TPraos mixes it in: Praos (protocol 7 onward) has no entropy term.
fn extra_entropy(ctx: &super::WorkContext) -> Option<Hash<32>> {
    if entering_protocol(ctx) >= 7 {
        return None;
    }

    let pparams = &ctx.ended_state.pparams;

    let nonce = pparams
        .next()
        .and_then(|p| p.extra_entropy())
        .or_else(|| pparams.live().and_then(|p| p.extra_entropy()))?;

    match nonce.variant {
        NonceVariant::Nonce => nonce.hash,
        NonceVariant::NeutralNonce => None,
    }
}

fn initial_nonces(ctx: &super::WorkContext) -> Option<Nonces> {
    let era_transition = ctx.ended_state.pparams.era_transition()?;

    if era_transition.entering_shelley() {
        Some(Nonces::bootstrap(ctx.genesis.shelley_hash))
    } else {
        None
    }
}

fn next_nonce(ctx: &super::WorkContext) -> Option<Nonces> {
    let Some(current) = &ctx.ended_state.nonces else {
        return initial_nonces(ctx);
    };

    let tail = ctx.ended_state.previous_nonce_tail;
    let entropy = extra_entropy(ctx);

    Some(current.sweep(tail, entropy.as_ref().map(|x| x.as_ref())))
}

#[derive(Default)]
pub struct BoundaryVisitor;

impl super::BoundaryVisitor for BoundaryVisitor {
    fn flush(&mut self, ctx: &mut super::WorkContext) -> Result<(), ChainError> {
        let next_slot = next_largest_stable_slot(ctx);
        let next_nonce = next_nonce(ctx);

        ctx.deltas
            .add_for_entity(NonceTransition::new(next_nonce, next_slot));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr, sync::Arc};

    use dolos_core::Genesis;
    use pallas::{
        crypto::hash::Hasher,
        ledger::primitives::{Nonce, ProtocolVersion},
    };

    use super::*;
    use crate::{
        estart::WorkContext, utils::load_genesis, ChainSummary, EpochState, EpochValue,
        EraBoundary, EraKey, EraState, PParamValue, PParamsSet,
    };

    const BYRON_EPOCH_LENGTH: u64 = 21_600;
    const SHELLEY_EPOCH_LENGTH: u64 = 432_000;

    /// 3k/f and 4k/f with mainnet's k = 2160 and f = 0.05.
    const STABILITY_WINDOW: u64 = 129_600;
    const RANDOMNESS_STABILITY_WINDOW: u64 = 172_800;

    /// The first Shelley epoch on mainnet.
    const SHELLEY_START: EraBoundary = EraBoundary {
        epoch: 208,
        slot: 4_492_800,
        timestamp: 0,
    };

    fn hash(hex: &str) -> Hash<32> {
        Hash::from_str(hex).unwrap()
    }

    fn mainnet_genesis() -> Arc<Genesis> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("mainnet")
            .join("genesis");

        Arc::new(load_genesis(&path))
    }

    fn pparams(major: u16) -> PParamsSet {
        let version: ProtocolVersion = (major as u64, 0);
        PParamsSet::default().with(PParamValue::ProtocolVersion(version))
    }

    fn summary(protocol: u16, start: EraBoundary, epoch_length: u64) -> ChainSummary {
        let mut summary = ChainSummary::default();

        summary.append_era(
            protocol,
            EraState {
                start,
                end: None,
                epoch_length,
                slot_length: 1,
                protocol,
            },
        );

        summary
    }

    /// The context at the boundary closing `ended`, whose live parameters
    /// run `live` and whose scheduled ones `next`.
    fn context(
        ended: u64,
        live: PParamsSet,
        next: PParamsSet,
        summary: ChainSummary,
    ) -> WorkContext {
        let mut pparams = EpochValue::with_live(ended, live);
        pparams.schedule(ended, Some(next));

        let ended_state = EpochState {
            number: ended,
            pparams,
            ..Default::default()
        };

        let active_protocol = EraKey::from(summary.edge().protocol);

        WorkContext {
            ended_state,
            active_protocol,
            chain_summary: summary,
            genesis: mainnet_genesis(),
            avvm_reclamation: Default::default(),
            owners: Default::default(),
            tally_protocol: active_protocol,
            deltas: Default::default(),
            active_stake: None,
        }
    }

    fn with_nonces(mut ctx: WorkContext) -> WorkContext {
        ctx.ended_state.previous_nonce_tail = Some(hash(&"11".repeat(32)));
        ctx.ended_state.nonces = Some(Nonces {
            active: hash(&"22".repeat(32)),
            evolving: hash(&"33".repeat(32)),
            candidate: hash(&"44".repeat(32)),
            tail: Some(hash(&"55".repeat(32))),
        });
        ctx
    }

    fn with_entropy(set: PParamsSet, nonce: Nonce) -> PParamsSet {
        set.with(PParamValue::ExtraEntropy(nonce))
    }

    fn entropy_nonce(hex: &str) -> Nonce {
        Nonce {
            variant: NonceVariant::Nonce,
            hash: Some(hash(hex)),
        }
    }

    fn neutral_nonce() -> Nonce {
        Nonce {
            variant: NonceVariant::NeutralNonce,
            hash: None,
        }
    }

    fn mix(nonce: Hash<32>, entropy: Hash<32>) -> Hash<32> {
        let mut hasher = Hasher::<256>::new();
        hasher.input(nonce.as_ref());
        hasher.input(entropy.as_ref());
        hasher.finalize()
    }

    /// Entering Shelley, the stable slot falls inside the first Shelley
    /// epoch, sized with Shelley's epoch length (mainnet 207 → 208).
    #[test]
    fn byron_to_shelley_sizes_with_the_entering_epoch_length() {
        let byron = EraBoundary {
            epoch: 0,
            slot: 0,
            timestamp: 0,
        };

        let ctx = context(
            207,
            pparams(1),
            pparams(2),
            summary(1, byron, BYRON_EPOCH_LENGTH),
        );

        let slot = next_largest_stable_slot(&ctx);

        assert_eq!(
            slot,
            SHELLEY_START.slot + SHELLEY_EPOCH_LENGTH - STABILITY_WINDOW
        );
        assert!((SHELLEY_START.slot..SHELLEY_START.slot + SHELLEY_EPOCH_LENGTH).contains(&slot));
    }

    /// Entering Conway, the window is 4k/f from its first epoch (mainnet
    /// 506 → 507).
    #[test]
    fn protocol_8_to_9_uses_the_randomness_window() {
        let ctx = context(
            506,
            pparams(8),
            pparams(9),
            summary(8, SHELLEY_START, SHELLEY_EPOCH_LENGTH),
        );

        let epoch_finish = SHELLEY_START.slot + (508 - 208) * SHELLEY_EPOCH_LENGTH;

        assert_eq!(
            next_largest_stable_slot(&ctx),
            epoch_finish - RANDOMNESS_STABILITY_WINDOW
        );
        assert_eq!(next_largest_stable_slot(&ctx), 133_920_000);
    }

    /// Without a transition the active protocol's window and the summary's
    /// epoch length apply (mainnet 507 → 508).
    #[test]
    fn steady_boundary_uses_the_active_protocol() {
        let conway = EraBoundary {
            epoch: 507,
            slot: SHELLEY_START.slot + (507 - 208) * SHELLEY_EPOCH_LENGTH,
            timestamp: 0,
        };

        let ctx = context(
            507,
            pparams(9),
            pparams(9),
            summary(9, conway, SHELLEY_EPOCH_LENGTH),
        );

        assert_eq!(next_largest_stable_slot(&ctx), 134_352_000);
    }

    /// The entering epoch's extra entropy is mixed into the nonce under every
    /// TPraos protocol, across a TPraos hard fork too:
    /// η = blake2b-256(η_plain ‖ entropy).
    #[test]
    fn extra_entropy_is_mixed_into_the_nonce() {
        let entropy = hash(&"66".repeat(32));

        let cases = [
            (2, 2),
            (3, 3),
            (4, 4),
            (5, 5),
            (6, 6),
            (2, 3),
            (3, 4),
            (4, 5),
            (5, 6),
        ];

        for (live, next) in cases {
            let plain = with_nonces(context(
                258,
                pparams(live),
                pparams(next),
                summary(live, SHELLEY_START, SHELLEY_EPOCH_LENGTH),
            ));
            let plain = next_nonce(&plain).unwrap();

            let mixed = with_nonces(context(
                258,
                pparams(live),
                with_entropy(pparams(next), entropy_nonce(&entropy.to_string())),
                summary(live, SHELLEY_START, SHELLEY_EPOCH_LENGTH),
            ));
            let mixed = next_nonce(&mixed).unwrap();

            let label = format!("protocol {live} → {next}");
            assert_eq!(mixed.active, mix(plain.active, entropy), "{label}");
            assert_eq!(mixed.candidate, plain.candidate, "{label}");
            assert_eq!(mixed.evolving, plain.evolving, "{label}");
        }
    }

    /// A neutral entropy, or none at all, gives the plain sweep.
    #[test]
    fn neutral_entropy_is_the_plain_sweep() {
        let neutral = with_nonces(context(
            259,
            with_entropy(pparams(3), entropy_nonce(&"66".repeat(32))),
            with_entropy(pparams(3), neutral_nonce()),
            summary(3, SHELLEY_START, SHELLEY_EPOCH_LENGTH),
        ));

        let current = neutral.ended_state.nonces.clone().unwrap();
        let expected = current.sweep(neutral.ended_state.previous_nonce_tail, None);

        assert_eq!(next_nonce(&neutral), Some(expected.clone()));

        let unset = with_nonces(context(
            259,
            pparams(3),
            pparams(3),
            summary(3, SHELLEY_START, SHELLEY_EPOCH_LENGTH),
        ));

        assert_eq!(next_nonce(&unset), Some(expected));
    }

    /// Praos has no entropy term, so a set entropy gives the plain sweep from
    /// protocol 7 onward, including at the boundary entering it.
    #[test]
    fn praos_ignores_extra_entropy() {
        let entropy = entropy_nonce(&"66".repeat(32));

        for (live, next) in [(6, 7), (7, 7), (9, 9)] {
            let ctx = with_nonces(context(
                300,
                with_entropy(pparams(live), entropy.clone()),
                with_entropy(pparams(next), entropy.clone()),
                summary(live, SHELLEY_START, SHELLEY_EPOCH_LENGTH),
            ));

            let current = ctx.ended_state.nonces.clone().unwrap();
            let expected = current.sweep(ctx.ended_state.previous_nonce_tail, None);

            assert_eq!(next_nonce(&ctx), Some(expected), "protocol {live} → {next}");
        }
    }

    /// Without a previous tail the active nonce is the candidate, mixed with
    /// the entropy under TPraos and left alone under Praos.
    #[test]
    fn first_boundary_mixes_extra_entropy() {
        let entropy = hash(&"66".repeat(32));

        let first = |protocol| {
            let mut ctx = with_nonces(context(
                208,
                pparams(protocol),
                with_entropy(pparams(protocol), entropy_nonce(&entropy.to_string())),
                summary(protocol, SHELLEY_START, SHELLEY_EPOCH_LENGTH),
            ));
            ctx.ended_state.previous_nonce_tail = None;
            ctx
        };

        let tpraos = first(2);
        let candidate = tpraos.ended_state.nonces.as_ref().unwrap().candidate;
        assert_eq!(next_nonce(&tpraos).unwrap().active, mix(candidate, entropy));

        let praos = first(7);
        assert_eq!(next_nonce(&praos).unwrap().active, candidate);
    }

    /// Mainnet 259, entered at protocol 4: Dolos's nonce without the entropy,
    /// mixed with the 2021 injection, gives the ledger's nonce.
    #[test]
    fn mainnet_epoch_259_vector() {
        let ctx = context(
            258,
            pparams(4),
            with_entropy(
                pparams(4),
                entropy_nonce("d982e06fd33e7440b43cefad529b7ecafbaa255e38178ad4189a37e4ce9bf1fa"),
            ),
            summary(4, SHELLEY_START, SHELLEY_EPOCH_LENGTH),
        );

        let entropy = extra_entropy(&ctx).unwrap();
        let unmixed = hash("c3668a56d87a6964d649720d4370321838be5fd941bf23597a98b9a8647edfa5");

        assert_eq!(
            mix(unmixed, entropy),
            hash("0022cfa563a5328c4fb5c8017121329e964c26ade5d167b1bd9b2ec967772b60")
        );
    }
}
