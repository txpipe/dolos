use crate::prelude::*;
use futures_core::Stream;

pub struct ChainStream;

impl ChainStream {
    pub fn start<D: Domain, C: CancelToken>(
        wal: D::Wal,
        archive: D::Archive,
        intersect: Vec<ChainPoint>,
        cancel: C,
    ) -> impl Stream<Item = LogValue> + 'static {
        async_stream::stream! {
            let (catchup, intersected) = super::iterator::ChainIterator::<D>::new(
                wal.clone(),
                archive.clone(),
                &intersect,
            ).unwrap();

            yield LogValue::Mark(intersected.clone());

            let mut last_point = intersected.clone();

            for value in catchup {
                last_point = ChainPoint::from(&value);
                yield value;
            }

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    }
                    _ = wal.tip_change() => {
                        let (updates, _) = super::iterator::ChainIterator::<D>::new(
                            wal.clone(),
                            archive.clone(),
                            &[last_point.clone()],
                        ).unwrap();

                        for value in updates {
                            last_point = ChainPoint::from(&value);
                            yield value;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use dolos_testing::toy_domain::ToyDomain;
    use futures_util::{pin_mut, StreamExt};
    use pallas::crypto::hash::Hash;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::serve::CancelTokenImpl;

    fn dummy_block(slot: u64) -> RawBlock {
        let hash = pallas::crypto::hash::Hasher::<256>::hash(slot.to_be_bytes().as_slice());

        RawBlock {
            slot,
            hash,
            era: pallas::ledger::traverse::Era::Byron,
            body: slot.to_be_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn test_stream_waiting() {
        let wal = dolos_redb::wal::RedbWalStore::memory().unwrap();
        let archive = dolos_redb::archive::ChainStore::in_memory_v1().unwrap();

        wal.initialize_from_origin().unwrap();

        let blocks = (0..=100).map(|i| dummy_block(i * 10));
        wal.roll_forward(blocks).unwrap();

        let wal2 = wal.clone();
        let background = tokio::spawn(async move {
            for i in 101..=200 {
                let block = dummy_block(i * 10);
                wal2.roll_forward([block].into_iter()).unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let s = ChainStream::start::<ToyDomain, CancelTokenImpl>(
            wal.clone(),
            archive.clone(),
            vec![ChainPoint::Specific(500, Hash::<32>::from([0; 32]))],
            CancelTokenImpl(CancellationToken::new()),
        );

        pin_mut!(s);

        let first = s.next().await.unwrap();

        assert_eq!(
            first,
            LogValue::Mark(ChainPoint::Specific(500, Hash::<32>::from([0; 32])))
        );

        for i in 51..=200 {
            let evt = s.next().await;
            let value = evt.unwrap();

            match value {
                LogValue::Apply(RawBlock { slot, .. }) => assert_eq!(slot, i * 10),
                _ => panic!("unexpected log value variant"),
            }
        }

        background.abort();
    }
}
