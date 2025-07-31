use std::collections::VecDeque;

use dolos_core::{
    ArchiveStore as _, ChainPoint, Domain, DomainError, LogEntry, LogSeq, LogValue, RawBlock,
    WalStore as _,
};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::warn;

pub enum Batch {
    Tip(LogSeq),
    WalPage(LogSeq, VecDeque<LogEntry>),
    ArchivePage(ChainPoint, VecDeque<RawBlock>),
}

impl Batch {
    fn from_wal<D: Domain>(seq: LogSeq, wal: &D::Wal) -> Result<Self, DomainError> {
        let page = wal
            .crawl_from(Some(seq))?
            .skip(1)
            .take(LOAD_BATCH_SIZE)
            .collect::<VecDeque<_>>();

        // if the wal page is empty, this means we reached the end and we need to
        // await the next tip change.
        if page.is_empty() {
            return Ok(Self::Tip(seq));
        }

        Ok(Self::WalPage(seq, page))
    }

    fn from_archive<D: Domain>(
        last_point: ChainPoint,
        archive: &D::Archive,
        wal: &D::Wal,
    ) -> Result<Self, DomainError> {
        let page = archive
            .get_range(Some(last_point.slot()), None)?
            // skip the last point, we already seen it
            .skip(1)
            .take(LOAD_BATCH_SIZE)
            .map(|(_, block)| {
                let parsed = MultiEraBlock::decode(&block).unwrap();

                RawBlock {
                    slot: parsed.slot(),
                    hash: parsed.hash(),
                    era: parsed.era(),
                    body: block,
                }
            })
            .collect::<VecDeque<_>>();

        // if the archive page is empty, this means we reached the end of the archive
        // store and we need to transition to the wal.
        if page.is_empty() {
            let intersect = wal.find_intersect(&[last_point])?;

            let Some((seq, _)) = intersect else {
                warn!("no overlap between archive and wal");
                panic!("no overlap between archive and wal");
            };

            return Self::from_wal::<D>(seq, wal);
        }

        Ok(Self::ArchivePage(last_point, page))
    }

    fn is_drained(&self) -> bool {
        match self {
            Self::Tip(_) => false,
            Self::WalPage(_, ref pending) => pending.is_empty(),
            Self::ArchivePage(_, ref pending) => pending.is_empty(),
        }
    }

    fn is_tip(&self) -> bool {
        matches!(self, Self::Tip(_))
    }

    fn pop(&mut self) -> Result<Option<LogValue>, ()> {
        match self {
            Self::Tip(_) => unreachable!("can't pop from tip"),
            Self::WalPage(seq, pending) => {
                if let Some((new_seq, value)) = pending.pop_front() {
                    *seq = new_seq;
                    Ok(Some(value))
                } else {
                    unreachable!("can't pop from wal page if it's empty")
                }
            }
            Self::ArchivePage(point, pending) => {
                if let Some(block) = pending.pop_front() {
                    *point = ChainPoint::Specific(block.slot, block.hash);
                    Ok(Some(LogValue::Apply(block)))
                } else {
                    unreachable!("can't pop from archive page if it's empty")
                }
            }
        }
    }
}

const LOAD_BATCH_SIZE: usize = 10;

pub struct ChainIterator<D: Domain> {
    pub wal: D::Wal,
    archive: D::Archive,
    batch: Batch,
}

impl<D: Domain> ChainIterator<D> {
    pub fn at_tip(wal: D::Wal, archive: D::Archive, seq: LogSeq) -> Result<Self, DomainError> {
        let iter = Self {
            wal,
            archive,
            batch: Batch::Tip(seq),
        };

        Ok(iter)
    }

    pub fn at_wal_seq(wal: D::Wal, archive: D::Archive, seq: LogSeq) -> Result<Self, DomainError> {
        Ok(Self {
            wal,
            archive,
            batch: Batch::WalPage(seq, VecDeque::new()),
        })
    }

    pub fn at_archive_point(
        wal: D::Wal,
        archive: D::Archive,
        point: ChainPoint,
    ) -> Result<Self, DomainError> {
        Ok(Self {
            wal,
            archive,
            batch: Batch::ArchivePage(point, VecDeque::new()),
        })
    }

    pub fn new(
        wal: D::Wal,
        archive: D::Archive,
        intersect: &[ChainPoint],
    ) -> Result<(Self, ChainPoint), DomainError> {
        if intersect.is_empty() {
            let (seq, point) = wal.find_tip()?.unwrap();
            let iter = Self::at_tip(wal, archive, seq)?;
            return Ok((iter, point));
        }

        if let Some((seq, point)) = wal.find_intersect(intersect)? {
            let iter = Self::at_wal_seq(wal, archive, seq)?;
            return Ok((iter, point));
        }

        if let Some(point) = archive.find_intersect(intersect)? {
            let iter = Self::at_archive_point(wal, archive, point.clone())?;
            return Ok((iter, point));
        }

        panic!("no intersection found");
    }

    fn load_next_batch(&mut self) -> Result<(), DomainError> {
        let next = match &self.batch {
            Batch::WalPage(seq, _) => Batch::from_wal::<D>(*seq, &self.wal),
            Batch::ArchivePage(x, _) => {
                Batch::from_archive::<D>(x.clone(), &self.archive, &self.wal)
            }
            _ => unreachable!("can't load batch for this type of cursor"),
        }?;

        self.batch = next;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn assume_tip_change(&mut self) -> Result<(), DomainError> {
        let next = match &self.batch {
            Batch::Tip(seq) => Batch::WalPage(*seq, VecDeque::new()),
            _ => unreachable!("can't assume tip change for this type of cursor"),
        };

        self.batch = next;

        Ok(())
    }
}

impl<D: Domain> Iterator for ChainIterator<D> {
    type Item = LogValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.batch.is_drained() {
            self.load_next_batch().ok()?;
        }

        if self.batch.is_tip() {
            return None;
        }

        self.batch.pop().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::StorageConfig;
    use dolos_testing::toy_domain::ToyDomain;

    use super::*;

    #[test]
    fn test_iterator() {
        let domain = ToyDomain::new(
            None,
            Some(StorageConfig {
                max_wal_history: Some(50),
                max_chain_history: Some(200),
                ..Default::default()
            }),
        );

        domain.wal().initialize_from_origin().unwrap();

        let genesis_block = dolos_testing::blocks::make_conway_block(0);
        domain.apply_blocks(&[genesis_block.clone()]).unwrap();

        let genesis_point = ChainPoint::Specific(genesis_block.slot, genesis_block.hash);

        for i in 1..=50 {
            let block = dolos_testing::blocks::make_conway_block(i);
            domain.apply_blocks(&[block]).unwrap();
        }

        for i in 51..=100 {
            let block = dolos_testing::blocks::make_conway_block(i);
            domain
                .wal()
                .roll_forward([block.clone()].into_iter())
                .unwrap();
            domain.apply_blocks(&[block]).unwrap();
        }

        for i in 101..=150 {
            let block = dolos_testing::blocks::make_conway_block(i);
            domain.wal().roll_forward([block].into_iter()).unwrap();
        }

        let (iter, intersected) = ChainIterator::<ToyDomain>::new(
            domain.wal().clone(),
            domain.archive().clone(),
            &[genesis_point.clone()],
        )
        .unwrap();

        assert_eq!(intersected.slot(), genesis_point.slot());

        for (i, log) in iter.enumerate() {
            match log {
                LogValue::Apply(RawBlock { slot, .. }) => assert_eq!(slot, i as u64 + 1),
                _ => panic!("unexpected log value variant"),
            }
        }
    }
}
