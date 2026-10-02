use dolos_cardano::{
    CardanoDelta, CardanoEntity, GovPurpose, NewProposal, NewProposalV2, NewProposalV3,
    ProposalAction, ProposalState,
};
use dolos_core::EntityDelta as _;
use pallas::ledger::primitives::{
    conway::{Anchor, GovActionId, Vote},
    StakeCredential,
};

fn proposal(entity: &Option<CardanoEntity>) -> Option<&ProposalState> {
    entity.as_ref().map(|entity| match entity {
        CardanoEntity::ProposalState(state) => state.as_ref(),
        _ => panic!("expected proposal state"),
    })
}

fn v1() -> NewProposal {
    NewProposal::new(
        123,
        [1; 32].into(),
        2,
        ProposalAction::HardFork((10, 0)),
        Some(500),
        Some(StakeCredential::AddrKeyhash([2; 28].into())),
        Some(10),
        100,
        2,
        10,
    )
}

fn v2() -> NewProposalV2 {
    NewProposalV2::new(
        123,
        [1; 32].into(),
        2,
        ProposalAction::HardFork((10, 0)),
        Some(500),
        Some(StakeCredential::AddrKeyhash([2; 28].into())),
        Some(10),
        100,
        2,
        10,
        Some(GovActionId {
            transaction_id: [3; 32].into(),
            action_index: 4,
        }),
        Some(GovPurpose::HardFork),
        Some(Anchor {
            url: "ipfs://creation".into(),
            content_hash: [4; 32].into(),
        }),
    )
}

fn preimage() -> ProposalState {
    let mut state = None;
    v2().apply(&mut state);
    let mut state = state.unwrap();
    state.ratified_epoch = Some(105);
    state.canceled_epoch = Some(106);
    state.cc_votes.insert(
        StakeCredential::AddrKeyhash([5; 28].into()),
        vec![(124, Vote::Yes), (125, Vote::No)],
    );
    state.drep_votes.insert(
        StakeCredential::ScriptHash([6; 28].into()),
        vec![(126, Vote::Abstain)],
    );
    state
        .spo_votes
        .insert([7; 28].into(), vec![(127, Vote::Yes)]);
    state
}

// Captured from the pre-change binary, including the outer CardanoDelta tag.
// Keep these literals: regenerating them would erase the compatibility check.
#[test]
fn legacy_creation_wal_bytes_replay_and_undo() {
    use bincode::Options as _;

    for (populated, first_hex, second_hex) in [
        (
            false,
            include_str!("fixtures/proposal_creation/v1-empty.hex"),
            include_str!("fixtures/proposal_creation/v2-empty.hex"),
        ),
        (
            true,
            include_str!("fixtures/proposal_creation/v1-populated.hex"),
            include_str!("fixtures/proposal_creation/v2-populated.hex"),
        ),
    ] {
        for (first, encoded) in [(true, first_hex), (false, second_hex)] {
            let original = populated.then(preimage);
            let mut state = original.clone().map(Into::into);
            let mut delta = if first {
                CardanoDelta::from(v1())
            } else {
                CardanoDelta::from(v2())
            };
            delta.apply(&mut state);
            let bytes = hex::decode(encoded.trim()).unwrap();
            assert_eq!(bincode::serialize(&delta).unwrap(), bytes);
            assert_eq!(
                &bytes[..4],
                &(if first { 19u32 } else { 46u32 }).to_le_bytes()
            );
            let mut restored: CardanoDelta = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .reject_trailing_bytes()
                .deserialize(&bytes)
                .unwrap();
            assert_eq!(restored.key(), delta.key());
            restored.undo(&mut state);
            assert_eq!(proposal(&state), original.as_ref());
            // Replay must capture the same preimage and reconstruct the same row.
            let mut replayed = original.clone().map(Into::into);
            restored.apply(&mut replayed);
            delta.apply(&mut state);
            assert_eq!(proposal(&replayed), proposal(&state));
            restored.undo(&mut replayed);
            assert_eq!(proposal(&replayed), original.as_ref());
        }
    }
}

#[test]
fn new_creation_wal_preserves_complete_preimage_and_metadata() {
    let mut positioned = preimage();
    positioned.drep_vote_positions = Some(std::collections::BTreeMap::from([(
        StakeCredential::ScriptHash([6; 28].into()),
        vec![Some(4)],
    )]));
    for original in [None, Some(preimage()), Some(positioned)] {
        let mut delta: CardanoDelta = NewProposalV3::new(
            123,
            [1; 32].into(),
            2,
            ProposalAction::HardFork((10, 0)),
            Some(500),
            Some(StakeCredential::AddrKeyhash([2; 28].into())),
            Some(10),
            100,
            2,
            10,
            Some(GovActionId {
                transaction_id: [3; 32].into(),
                action_index: 4,
            }),
            Some(GovPurpose::HardFork),
            Some(Anchor {
                url: "ipfs://creation".into(),
                content_hash: [4; 32].into(),
            }),
        )
        .into();
        let mut expected = None;
        v2().apply(&mut expected);
        let mut state = original.clone().map(Into::into);
        delta.apply(&mut state);
        assert_eq!(proposal(&state), expected.as_ref());
        let bytes = bincode::serialize(&delta).unwrap();
        assert_eq!(&bytes[..4], &65u32.to_le_bytes());
        let restored: CardanoDelta = bincode::deserialize(&bytes).unwrap();
        restored.undo(&mut state);
        assert_eq!(proposal(&state), original.as_ref());
    }
}
