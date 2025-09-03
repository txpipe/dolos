use crate::prelude::*;
use dolos_core::crawl::ChainCrawler;
use futures_core::Stream;

pub struct ChainStream;

impl ChainStream {
    pub fn start<D: Domain, C: CancelToken>(
        domain: D,
        intersect: Vec<ChainPoint>,
        cancel: C,
    ) -> impl Stream<Item = TipEvent> + 'static {
        async_stream::stream! {
            let (mut crawler, intersected) = ChainCrawler::<D>::start(
                &domain,
                &intersect,
            ).unwrap().unwrap();

            yield TipEvent::Mark(intersected.clone());

            while let Some((point, block)) = crawler.next_block() {
                yield TipEvent::Apply(point, block);
            }

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    }
                    next = crawler.next_tip() => {
                        yield next;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use dolos_redb::testing::dummy_entry_from_slot;
    use dolos_testing::toy_domain::ToyDomain;
    use futures_util::{pin_mut, StreamExt};
    use pallas::crypto::hash::Hash;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::serve::CancelTokenImpl;

    #[tokio::test]
    async fn test_stream_waiting() {
        let wal = dolos_redb::wal::RedbWalStore::memory().unwrap();
        let archive = dolos_redb::archive::ChainStore::in_memory_v1().unwrap();

        let logs: Vec<_> = (0..=100).map(|i| dummy_entry_from_slot(i * 10)).collect();
        wal.roll_forward(logs).unwrap();

        let wal2 = wal.clone();
        let background = tokio::spawn(async move {
            for i in 101..=200 {
                let log = dummy_entry_from_slot(i * 10);
                wal2.roll_forward(vec![log]).unwrap();
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
                LogValue::Apply(p, _) => assert_eq!(p.slot(), i * 10),
                _ => panic!("unexpected log value variant"),
            }
        }

        background.abort();
    }
}
