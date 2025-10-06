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
            let result = ChainCrawler::<D>::start(
                &domain,
                &intersect,
            );

            let start = result.expect("issue starting crawler");

            let (mut crawler, intersected) = start.expect("crawler can't find start point");

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
            dolos_core::facade::roll_forward(&domain, block).unwrap();
        }

        let domain2 = domain.clone();
        let background = tokio::spawn(async move {
            for i in 101..=200 {
                let (_, block) = make_conway_block(i * 10);
                dolos_core::facade::roll_forward(&domain2, block).unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let chain_point = make_conway_block(500).0;
        let s = ChainStream::start::<ToyDomain, CancelTokenImpl>(
            domain,
            vec![chain_point.clone()],
            CancelTokenImpl(CancellationToken::new()),
        );

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
}
