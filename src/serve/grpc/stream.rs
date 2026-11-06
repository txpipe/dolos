use crate::prelude::*;
use dolos_core::crawl::ChainCrawler;
use futures_core::Stream;
use tracing::warn;

pub struct ChainStream;

impl ChainStream {
    pub fn start<D: Domain, C: CancelToken>(
        domain: D,
        intersect: Vec<ChainPoint>,
        cancel: C,
    ) -> Result<Option<impl Stream<Item = Result<TipEvent, DomainError>> + 'static>, DomainError>
    {
        let Some((mut crawler, intersected)) = ChainCrawler::<D>::start(&domain, &intersect)?
        else {
            return Ok(None);
        };

        Ok(Some(async_stream::stream! {
            yield Ok(TipEvent::Mark(intersected.clone()));

            loop {
                match crawler.next_block() {
                    Ok(Some((point, block))) => yield Ok(TipEvent::Apply(point, block)),
                    Ok(None) => break,
                    Err(err) => {
                        // Lost the archive/WAL overlap mid-stream. Re-seek from
                        // where we stalled, and report the failure if that
                        // doesn't restore it so the client can tell it from an
                        // orderly end of stream.
                        warn!(%err, "crawler lost its overlap, re-seeking from the last point");

                        match ChainCrawler::<D>::start(&domain, std::slice::from_ref(crawler.last_point())) {
                            Ok(Some((next, _))) => crawler = next,
                            Ok(None) => {
                                yield Err(err);
                                return;
                            }
                            Err(reseek_err) => {
                                yield Err(reseek_err);
                                return;
                            }
                        }
                    }
                }
            }

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    }
                    next = crawler.next_tip() => {
                        yield Ok(next);
                    }
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dolos_core::{ArchiveStore as _, ArchiveWriter as _};
    use dolos_testing::blocks::make_conway_block;
    use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
    use dolos_testing::toy_domain::ToyDomain;
    use futures_util::{pin_mut, StreamExt};
    use pallas::ledger::traverse::MultiEraBlock;
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

        let first = s.next().await.unwrap().unwrap();

        assert_eq!(first, TipEvent::Mark(chain_point));

        for i in 51..=200 {
            let evt = timeout(Duration::from_secs(5), s.next())
                .await
                .expect("took too long");
            let value = evt.unwrap().unwrap();

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

        // this point was never rolled forward, so the domain can't intersect it.
        let unknown_point = make_conway_block(9999).0;

        let result = ChainStream::start::<ToyDomain, CancelTokenImpl>(
            domain,
            vec![unknown_point],
            CancelTokenImpl(CancellationToken::new()),
        );

        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_stream_reports_a_lost_overlap() {
        let domain = ToyDomain::new(None, None);

        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());

        let writer = domain.archive().start_writer().unwrap();
        for raw in &blocks {
            let block = MultiEraBlock::decode(raw).unwrap();
            let point = ChainPoint::Specific(block.slot(), block.hash());

            writer.apply(&point, raw).unwrap();
        }
        writer.commit().unwrap();

        let first = MultiEraBlock::decode(&blocks[0]).unwrap();
        let start = ChainPoint::Specific(first.slot(), first.hash());

        let s = ChainStream::start::<ToyDomain, CancelTokenImpl>(
            domain,
            vec![start.clone()],
            CancelTokenImpl(CancellationToken::new()),
        )
        .unwrap()
        .expect("the archive covers the start point");

        pin_mut!(s);

        assert_eq!(s.next().await.unwrap().unwrap(), TipEvent::Mark(start));

        // Every block after the start point arrives once, then the gap is an
        // error rather than a replay or an orderly end of stream.
        for raw in &blocks[1..] {
            match s.next().await.unwrap().unwrap() {
                TipEvent::Apply(_, block) => assert_eq!(&block, raw),
                other => panic!("unexpected event: {other:?}"),
            }
        }

        assert!(matches!(
            s.next().await,
            Some(Err(DomainError::Internal(_)))
        ));
        assert!(s.next().await.is_none());
    }
}
