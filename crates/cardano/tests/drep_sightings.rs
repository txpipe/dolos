//! DRep sightings through the real pipeline: a rollback restores the rows of
//! the blocks it undoes.

use std::{collections::BTreeMap, sync::Arc};

use dolos_cardano::{model::DRepSightingState, DRepKey};
use dolos_core::{sync::SyncExt as _, ChainPoint, Domain as _, StateStore as _};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
    toy_domain::ToyDomain,
};
use pallas::ledger::{
    addresses::Address,
    primitives::{
        conway::{Anchor, Certificate, DRep},
        StakeCredential,
    },
    traverse::MultiEraBlock,
};

/// The synthetic chain's own account, which every tx registers.
fn primary() -> StakeCredential {
    let address = Address::from_bech32(&SyntheticBlockConfig::default().address).unwrap();
    dolos_cardano::pallas_extras::address_as_stake_cred(&address)
        .unwrap()
        .0
}

fn delegation(target: u8) -> Certificate {
    Certificate::VoteDeleg(primary(), DRep::Key([target; 28].into()))
}

fn anchor(byte: u8) -> Anchor {
    Anchor {
        url: format!("https://drep.invalid/{byte}"),
        content_hash: [byte; 32].into(),
    }
}

fn registration(drep: u8, anchor: Option<Anchor>) -> Certificate {
    Certificate::RegDRepCert(
        StakeCredential::AddrKeyhash([drep; 28].into()),
        1000,
        anchor,
    )
}

fn update(drep: u8, anchor: Option<Anchor>) -> Certificate {
    Certificate::UpdateDRepCert(StakeCredential::AddrKeyhash([drep; 28].into()), anchor)
}

fn sightings(domain: &ToyDomain) -> BTreeMap<DRepKey, DRepSightingState> {
    domain
        .state()
        .iter_entities_typed::<DRepSightingState>(None)
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
}

/// Each block first sights a DRep and moves the anchor of one sighted
/// earlier; undoing a block takes both back.
#[test]
fn rollback_restores_the_sightings_of_its_block() {
    let cfg = SyntheticBlockConfig {
        block_count: 3,
        extra_certs_by_block: vec![
            vec![vec![delegation(0xa1), registration(0xb2, Some(anchor(1)))]],
            vec![vec![update(0xb2, Some(anchor(2))), delegation(0xc3)]],
            vec![vec![registration(0xa1, Some(anchor(3))), delegation(0xd4)]],
        ],
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
        after.push(sightings(&domain));
    }

    let key = |byte: u8| DRepKey::from(DRep::Key([byte; 28].into()));

    // each block adds a DRep, and a later certificate never moves the first
    // sighting
    assert!(!after[0].contains_key(&key(0xc3)));
    assert!(after[1].contains_key(&key(0xc3)));
    assert!(!after[1].contains_key(&key(0xd4)));
    assert!(after[2].contains_key(&key(0xd4)));
    assert_eq!(
        after[2][&key(0xa1)].first_seen,
        after[0][&key(0xa1)].first_seen
    );

    // the anchor follows the latest certificate that carried one
    assert_eq!(after[0][&key(0xb2)].anchor, Some(anchor(1)));
    assert_eq!(after[1][&key(0xb2)].anchor, Some(anchor(2)));
    assert_eq!(after[1][&key(0xa1)].anchor, None);
    assert_eq!(after[2][&key(0xa1)].anchor, Some(anchor(3)));

    domain.rollback(&points[1]).unwrap();
    assert_eq!(sightings(&domain), after[1]);

    domain.rollback(&points[0]).unwrap();
    assert_eq!(sightings(&domain), after[0]);
}
