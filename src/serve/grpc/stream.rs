use crate::prelude::*;
use dolos_core::crawl::ChainCrawler;
use futures_core::Stream;

pub struct ChainStream;

impl ChainStream {
    pub fn start<D: Domain, C: CancelToken>(
        domain: D,
        intersect: Vec<ChainPoint>,
        cancel: C,
    ) -> Result<Option<impl Stream<Item = TipEvent> + 'static>, DomainError> {
        let Some((mut crawler, intersected)) = ChainCrawler::<D>::start(&domain, &intersect)?
        else {
            return Ok(None);
        };

        Ok(Some(async_stream::stream! {
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
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dolos_testing::blocks::make_conway_block;
    use dolos_testing::toy_domain::ToyDomain;
    use futures_util::{pin_mut, StreamExt};
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::serve::CancelTokenImpl;

    #[tokio::test]
    async fn test_stream_waiting() {
        let domain = ToyDomain::new(None, None);

        for i in 0..=100 {
            let (_, block) = make_conway_block(i * 10);

            use dolos_core::SyncExt;
            domain.roll_forward(block).unwrap();
        }

        let domain2 = domain.clone();
        let background = tokio::spawn(async move {
            for i in 101..=200 {
                let (_, block) = make_conway_block(i * 10);

                use dolos_core::SyncExt;
                domain2.roll_forward(block).unwrap();

                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let chain_point = make_conway_block(500).0;
        let s = ChainStream::start::<ToyDomain, CancelTokenImpl>(
            domain,
            vec![chain_point.clone()],
            CancelTokenImpl(CancellationToken::new()),
        )
        .unwrap()
        .expect("intersect point should be found");

        pin_mut!(s);

        let first = s.next().await.unwrap();

        assert_eq!(first, TipEvent::Mark(chain_point));

        for i in 51..=200 {
            let evt = timeout(Duration::from_secs(5), s.next())
                .await
                .expect("took too long");
            let value = evt.unwrap();

            match value {
                TipEvent::Apply(p, _) => {
                    assert_eq!(p.slot(), i * 10)
                }
                _ => panic!("unexpected log value variant"),
            }
        }

        background.abort();
    }

    #[tokio::test]
    async fn test_stream_unknown_intersect() {
        let domain = ToyDomain::new(None, None);

        for i in 0..=10 {
            let (_, block) = make_conway_block(i * 10);

            use dolos_core::SyncExt;
            domain.roll_forward(block).unwrap();
        }

        // this point was never rolled forward, so the domain can't intersect
        // it.
        let unknown_point = make_conway_block(9999).0;

        let result = ChainStream::start::<ToyDomain, CancelTokenImpl>(
            domain,
            vec![unknown_point],
            CancelTokenImpl(CancellationToken::new()),
        );

        assert!(result.unwrap().is_none());
    }
}
