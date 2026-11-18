//! The era rows a store synced from genesis holds on each public network, with
//! the boundaries those networks really took.

use std::sync::Arc;

use dolos_cardano::{EraBoundary, EraKey, EraState};
use dolos_core::{
    BlockSlot, ChainPoint, Domain, EntityKey, FixedNamespace as _, Genesis, StateStore as _,
    StateWriter as _,
};

pub struct PublicNetwork {
    pub genesis: Arc<Genesis>,
    pub eras: Vec<(u16, EraState)>,
    pub tip: BlockSlot,
}

impl PublicNetwork {
    /// Byron then every protocol bump, up to protocol 10.
    pub fn mainnet() -> Self {
        Self {
            genesis: Arc::new(dolos_cardano::include::mainnet::load()),
            eras: shelley_eras(
                1506203091 + 89856000,
                (208, 4492800),
                432000,
                &[
                    (2, 208),
                    (3, 236),
                    (4, 251),
                    (5, 290),
                    (6, 298),
                    (7, 365),
                    (8, 394),
                    (9, 507),
                    (10, 537),
                ],
            ),
            tip: 202492107,
        }
    }

    /// Byron then every protocol bump, up to protocol 10.
    pub fn preprod() -> Self {
        Self {
            genesis: Arc::new(dolos_cardano::include::preprod::load()),
            eras: shelley_eras(
                1654041600 + 1728000,
                (4, 86400),
                432000,
                &[
                    (2, 4),
                    (3, 5),
                    (4, 6),
                    (5, 7),
                    (6, 9),
                    (7, 12),
                    (8, 20),
                    (9, 163),
                    (10, 180),
                ],
            ),
            tip: 138375191,
        }
    }

    /// Opens at protocol 6, so no store holds Byron, Shelley, Allegra or Mary.
    pub fn preview() -> Self {
        Self {
            genesis: Arc::new(dolos_cardano::include::preview::load()),
            eras: shelley_eras(
                1666656000,
                (0, 0),
                86400,
                &[(6, 0), (7, 3), (8, 12), (9, 646), (10, 700)],
            ),
            tip: 127402390,
        }
    }

    pub fn all() -> [Self; 3] {
        [Self::mainnet(), Self::preprod(), Self::preview()]
    }

    /// Writes the era rows and the tip into `domain`'s state.
    pub fn seed<D: Domain>(&self, domain: &D) {
        let writer = domain.state().start_writer().unwrap();

        for (protocol, era) in &self.eras {
            writer
                .write_entity_typed(&EraKey::from(*protocol), era)
                .unwrap();
        }

        writer.set_cursor(ChainPoint::Slot(self.tip)).unwrap();
        writer.commit().unwrap();
    }
}

/// Deletes every era row from `domain`'s state.
pub fn clear_eras<D: Domain>(domain: &D) {
    let keys: Vec<_> = domain
        .state()
        .iter_entities_typed::<EraState>(None)
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();

    let writer = domain.state().start_writer().unwrap();
    for key in keys {
        writer
            .delete_entity(EraState::NS, &EntityKey::of(&key))
            .unwrap();
    }
    writer.commit().unwrap();
}

fn shelley_eras(
    first_timestamp: u64,
    (first_epoch, first_slot): (u64, u64),
    epoch_length: u64,
    bumps: &[(u16, u64)],
) -> Vec<(u16, EraState)> {
    let boundary = |epoch: u64| {
        let slots = (epoch - first_epoch) * epoch_length;

        EraBoundary {
            epoch,
            slot: first_slot + slots,
            timestamp: first_timestamp + slots,
        }
    };

    let mut eras = Vec::with_capacity(bumps.len());

    for (index, (protocol, epoch)) in bumps.iter().enumerate() {
        eras.push((
            *protocol,
            EraState {
                start: boundary(*epoch),
                end: bumps.get(index + 1).map(|(_, next)| boundary(*next)),
                epoch_length,
                slot_length: 1,
                protocol: *protocol,
            },
        ));
    }

    eras
}
