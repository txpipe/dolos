use std::{collections::VecDeque, sync::Arc};

use pallas::ledger::traverse::MultiEraBlock;
use tracing::warn;

use crate::{
    ArchiveError, ArchiveStore as _, ChainPoint, Domain, DomainError, RawBlock, TipEvent,
    TipSubscription, WalStore,
};

pub enum Batch<D: Domain> {
    Tip(ChainPoint, D::TipSubscription),
    WalPage(ChainPoint, VecDeque<(ChainPoint, RawBlock)>),
    ArchivePage(ChainPoint, VecDeque<(ChainPoint, RawBlock)>),
}

impl<D: Domain> Batch<D> {
    fn from_tip(point: ChainPoint, domain: &D) -> Result<Self, DomainError> {
        let subscription = domain.watch_tip(Some(point.clone()))?;
        Ok(Self::Tip(point, subscription))
    }

    fn from_wal(point: ChainPoint, domain: &D) -> Result<Self, DomainError> {
        let page = domain
            .wal()
            .iter_blocks(Some(point.clone()), None)?
            .skip(1)
            .take(LOAD_BATCH_SIZE)
            .collect::<VecDeque<_>>();

        // if the wal page is empty, this means we reached the end and we need to
        // await the next tip change.
        if page.is_empty() {
            return Self::from_tip(point, domain);
        }

        Ok(Self::WalPage(point, page))
    }

    fn from_archive(last_point: ChainPoint, domain: &D) -> Result<Self, DomainError> {
        let mut rows = domain
            .archive()
            .get_range(Some(last_point.slot()), None)?
            .peekable();

        // A slot can hold more than one block, so the cursor is found by its hash and
        // the crawl continues after it. Origin has seen nothing, so it skips nothing.
        if let ChainPoint::Specific(slot, hash) = &last_point {
            let mut found = false;
            let mut seen_other = false;

            while let Some((_, body)) = rows.next_if(|(row_slot, _)| row_slot == slot) {
                let block = MultiEraBlock::decode(&body).map_err(ArchiveError::from)?;

                if block.hash() == *hash {
                    found = true;
                    break;
                }

                seen_other = true;
            }

            // Later rows without the cursor mean it was rolled back or pruned, and carrying
            // on would hand out a fork or a gap.
            if !found && (seen_other || rows.peek().is_some()) {
                return Err(no_overlap(&last_point));
            }
        }

        let page = rows
            .take(LOAD_BATCH_SIZE)
            // Both stores intersect a point by its hash, so a cursor that only
            // knew its slot could never cross into the WAL or seek back here.
            .map(|(slot, body)| {
                let block = MultiEraBlock::decode(&body).map_err(ArchiveError::from)?;
                Ok((ChainPoint::Specific(slot, block.hash()), Arc::new(body)))
            })
            .collect::<Result<VecDeque<_>, DomainError>>()?;

        // if the archive page is empty, this means we reached the end of the archive
        // store and we need to transition to the wal.
        if page.is_empty() {
            let intersect = domain
                .wal()
                .find_intersect(std::slice::from_ref(&last_point))?;

            // No overlap means the WAL no longer covers the point we were
            // crawling from. Returning an error lets the caller re-seek instead
            // of unwinding the task it is streaming from.
            let Some((point, _)) = intersect else {
                return Err(no_overlap(&last_point));
            };

            return Self::from_wal(point, domain);
        }

        Ok(Self::ArchivePage(last_point, page))
    }

    fn is_drained(&self) -> bool {
        match self {
            Self::Tip(_, _) => false,
            Self::WalPage(_, ref pending) => pending.is_empty(),
            Self::ArchivePage(_, ref pending) => pending.is_empty(),
        }
    }

    fn is_tip(&self) -> bool {
        matches!(self, Self::Tip(_, _))
    }

    fn pop(&mut self) -> Option<(ChainPoint, RawBlock)> {
        match self {
            Self::Tip(_, _) => unreachable!("can't pop from tip"),
            Self::WalPage(point, pending) => {
                if let Some((new_point, value)) = pending.pop_front() {
                    *point = new_point.clone();
                    Some((new_point, value))
                } else {
                    unreachable!("can't pop from wal page if it's empty")
                }
            }
            Self::ArchivePage(point, pending) => {
                if let Some((new_point, value)) = pending.pop_front() {
                    *point = new_point.clone();
                    Some((new_point, value))
                } else {
                    unreachable!("can't pop from archive page if it's empty")
                }
            }
        }
    }
}

const LOAD_BATCH_SIZE: usize = 10;

fn no_overlap(point: &ChainPoint) -> DomainError {
    warn!(
        slot = point.slot(),
        "crawl cursor has no overlap with the stores"
    );

    DomainError::NoOverlap(point.slot())
}

pub struct ChainCrawler<D: Domain> {
    domain: D,
    batch: Batch<D>,
}

impl<D: Domain> ChainCrawler<D> {
    pub fn start(
        domain: &D,
        intersect: &[ChainPoint],
    ) -> Result<Option<(Self, ChainPoint)>, DomainError> {
        let domain = domain.clone();

        if intersect.is_empty() {
            // An empty WAL has no tip to start from. Report it rather than
            // unwinding, so the caller gets an error it can retry.
            let (point, _) = domain.wal().find_tip()?.ok_or(DomainError::WalIsEmpty)?;
            let batch = Batch::from_tip(point.clone(), &domain)?;

            let iter = Self { domain, batch };

            return Ok(Some((iter, point)));
        }

        if let Some((point, _)) = domain.wal().find_intersect(intersect)? {
            let batch = Batch::from_wal(point.clone(), &domain)?;

            let iter = Self { domain, batch };

            return Ok(Some((iter, point)));
        }

        if let Some(point) = domain.archive().find_intersect(intersect)? {
            let batch = Batch::from_archive(point.clone(), &domain)?;

            let iter = Self { domain, batch };

            return Ok(Some((iter, point)));
        }

        Ok(None)
    }

    fn load_next_batch(&mut self) -> Result<(), DomainError> {
        let next = match &self.batch {
            Batch::WalPage(point, _) => Batch::from_wal(point.clone(), &self.domain),
            Batch::ArchivePage(x, _) => Batch::from_archive(x.clone(), &self.domain),
            _ => unreachable!("can't load batch for this type of cursor"),
        }?;

        self.batch = next;

        Ok(())
    }

    /// The point the cursor currently sits on. Used by callers that need to
    /// re-seek after a loading error.
    pub fn last_point(&self) -> &ChainPoint {
        match &self.batch {
            Batch::Tip(point, _) | Batch::WalPage(point, _) | Batch::ArchivePage(point, _) => point,
        }
    }

    /// Advance the cursor, reporting a failure to load the next batch.
    ///
    /// A drained batch stays in place when loading fails, so the caller can
    /// re-seek from [`Self::last_point`] and call again. Returning `None` means
    /// the cursor genuinely reached the tip and has nothing more to hand out.
    pub fn next_block(&mut self) -> Result<Option<(ChainPoint, RawBlock)>, DomainError> {
        if self.batch.is_drained() {
            self.load_next_batch()?;
        }

        if self.batch.is_tip() {
            return Ok(None);
        }

        Ok(self.batch.pop())
    }

    pub async fn next_tip(&mut self) -> TipEvent {
        match &mut self.batch {
            Batch::Tip(_, subscription) => subscription.next_tip().await,
            _ => unreachable!("can't next tip for this type of batch"),
        }
    }

    pub fn find_tip(&self) -> Result<Option<ChainPoint>, DomainError> {
        let point = self.domain.wal().find_tip()?;
        Ok(point.map(|(x, _)| x))
    }
}

impl<D: Domain> Iterator for ChainCrawler<D> {
    type Item = (ChainPoint, RawBlock);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_block().ok().flatten()
    }
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use dolos_core::{StorageConfig, TipEvent};
//     use dolos_testing::toy_domain::ToyDomain;

//     use super::*;

//     #[test]
//     fn test_iterator() {
//         let domain = ToyDomain::new(
//             None,
//             Some(StorageConfig {
//                 max_wal_history: Some(50),
//                 max_chain_history: Some(1000),
//                 ..Default::default()
//             }),
//         );

//         let (genesis_point, genesis_block) =
// dolos_testing::blocks::make_conway_block(0);

//         dolos_core::follow::roll_forward(&domain, &genesis_block).unwrap();

//         for i in 1..=50 {
//             let (_, block) = dolos_testing::blocks::make_conway_block(i);

//             dolos_core::follow::roll_forward(&domain, &block).unwrap();
//         }

//         for i in 51..=100 {
//             let (_, block) = dolos_testing::blocks::make_conway_block(i);

//             dolos_core::follow::roll_forward(&domain, &block).unwrap();
//         }

//         for i in 101..=150 {
//             let (_, block) = dolos_testing::blocks::make_conway_block(i);

//             dolos_core::follow::roll_forward(&domain, &block).unwrap();
//         }

//         let (iter, intersected) = ChainIterator::<ToyDomain>::new(
//             domain.wal().clone(),
//             domain.archive().clone(),
//             std::slice::from_ref(&genesis_point),
//         )
//         .unwrap();

//         assert_eq!(intersected.slot(), genesis_point.slot());

//         for (i, (point, _)) in iter.enumerate() {
//             assert_eq!(point.slot(), i as u64 + 1);
//         }
//     }
// }
