use crate::prelude::*;
use futures_core::Stream;

pub struct WalStream;

impl WalStream {
    pub fn start<W, C>(wal: W, from: LogSeq, cancel: C) -> impl Stream<Item = LogEntry>
    where
        W: WalStore,
        C: CancelToken,
    {
        async_stream::stream! {
            let mut last_seq = from;

            let iter = wal.crawl_from(Some(last_seq)).unwrap();

            for entry in iter {
                last_seq = entry.0;
                yield entry;
            }

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    }
                    _ = wal.tip_change() => {
                        let iter = wal.crawl_from(Some(last_seq)).unwrap().skip(1);

                        for entry in iter {
                            last_seq = entry.0;
                            yield entry;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use dolos_redb::wal::RedbWalStore;
    use futures_util::{pin_mut, StreamExt};
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
        let mut db = RedbWalStore::memory().unwrap();

        db.initialize_from_origin().unwrap();

        let blocks = (0..=100).map(|i| dummy_block(i * 10));
        db.roll_forward(blocks).unwrap();

        let mut db2 = db.clone();
        let background = tokio::spawn(async move {
            for i in 101..=200 {
                let block = dummy_block(i * 10);
                db2.roll_forward([block].into_iter()).unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let s = WalStream::start(db.clone(), 50, CancelTokenImpl(CancellationToken::new()));

        pin_mut!(s);

        for i in 49..=200 {
            let evt = s.next().await;
            let (_, value) = evt.unwrap();

            match value {
                LogValue::Apply(RawBlock { slot, .. }) => assert_eq!(slot, i * 10),
                _ => panic!("unexpected log value variant"),
            }
        }

        background.abort();
    }
}
