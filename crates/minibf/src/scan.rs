//! Store scans in bounded chunks, each one blocking task under the domain's
//! query limiter.
//!
//! A scan holds a permit only while a chunk runs, so a long scan lets other
//! queries in between its chunks. A dropped request starts no further chunk.

use std::marker::PhantomData;

use axum::http::StatusCode;
use dolos_core::{
    ArchiveStore as _, BlockSlot, Domain, EntityIterTyped, EntityKey, KeyBytes, KeyRange,
    LogEntity, LogIterTyped, LogKey, StateEntity, StateStore as _, TagDimension, TxoRef,
};

use crate::{log_and_500, Facade};

/// Entities or log rows read per blocking task. One chunk of accounts holds
/// its permit for a few milliseconds.
pub const ENTITY_CHUNK: usize = 4096;

impl<D> Facade<D>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    /// Run `f` on the blocking pool under the domain's query limiter.
    pub async fn blocking<T, F>(&self, f: F) -> Result<T, StatusCode>
    where
        T: Send + 'static,
        F: FnOnce(Facade<D>) -> Result<T, StatusCode> + Send + 'static,
    {
        let facade = self.clone();

        self.query()
            .run_blocking(move |_| Ok(f(facade)))
            .await
            .map_err(log_and_500("blocking query task failed"))?
    }
}

/// The part of a key range a scan has not read yet.
struct Cursor {
    range: KeyRange,
    done: bool,
}

impl Cursor {
    fn new(range: KeyRange) -> Self {
        Self { range, done: false }
    }

    /// Move past `last`, the last key of a chunk. A chunk shorter than the
    /// chunk size ends the scan.
    fn advance(&mut self, last: Option<KeyBytes>, full: bool) {
        match last {
            Some(mut last) if full => {
                // the least key above `last`
                last.push(0);
                self.range.start = last;
            }
            _ => self.done = true,
        }
    }
}

/// A walk over the entities of kind `T`, in key order, `chunk` entities per
/// blocking task.
pub struct EntityScan<T> {
    cursor: Cursor,
    chunk: usize,
    _entity: PhantomData<fn() -> T>,
}

impl<T> EntityScan<T>
where
    T: StateEntity + Send + 'static,
    T::Key: Send + 'static,
{
    /// A scan of every entity of kind `T`.
    pub fn all(chunk: usize) -> Self {
        Self {
            cursor: Cursor::new(KeyRange::all()),
            chunk: chunk.max(1),
            _entity: PhantomData,
        }
    }

    /// Read the next chunk and pass it to `f` in the same blocking task.
    /// Returns `None` once the scan is exhausted.
    pub async fn next<D, R, F>(&mut self, facade: &Facade<D>, f: F) -> Result<Option<R>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        R: Send + 'static,
        F: FnOnce(Vec<(T::Key, T)>) -> R + Send + 'static,
    {
        if self.cursor.done {
            return Ok(None);
        }

        self.read_chunk(facade, f).await.map(Some)
    }

    /// Fold every remaining chunk into `acc`, which moves into each chunk's
    /// blocking task and back.
    pub async fn fold<D, A, F>(
        mut self,
        facade: &Facade<D>,
        mut acc: A,
        f: F,
    ) -> Result<A, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        A: Send + 'static,
        F: Fn(A, Vec<(T::Key, T)>) -> Result<A, StatusCode> + Clone + Send + 'static,
    {
        while !self.cursor.done {
            let f = f.clone();
            acc = self
                .read_chunk(facade, move |items| f(acc, items))
                .await??;
        }

        Ok(acc)
    }

    /// Every remaining entity that `f` keeps, mapped, in key order.
    pub async fn filter_map<D, R, F>(self, facade: &Facade<D>, f: F) -> Result<Vec<R>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(T::Key, T) -> Option<R> + Clone + Send + 'static,
    {
        self.fold(facade, Vec::new(), move |mut kept, items| {
            kept.extend(items.into_iter().filter_map(|(key, entity)| f(key, entity)));
            Ok(kept)
        })
        .await
    }

    async fn read_chunk<D, R, F>(&mut self, facade: &Facade<D>, f: F) -> Result<R, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        R: Send + 'static,
        F: FnOnce(Vec<(T::Key, T)>) -> R + Send + 'static,
    {
        let range = self.cursor.range.clone();
        let chunk = self.chunk;

        let (out, last, full) = facade
            .blocking(move |facade| {
                let iter = facade
                    .state()
                    .iter_entities(T::NS, range)
                    .map_err(log_and_500("failed to scan entities"))?;

                let items = EntityIterTyped::<D::State, T>::new(iter)
                    .take(chunk)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(log_and_500("failed to read a scanned entity"))?;

                let last = items
                    .last()
                    .map(|(key, _)| KeyBytes::from_slice(EntityKey::of(key).as_ref()));
                let full = items.len() == chunk;

                Ok((f(items), last, full))
            })
            .await?;

        self.cursor.advance(last, full);

        Ok(out)
    }
}

/// A walk over the log rows of kind `T` in a key range, in key order, `chunk`
/// rows per blocking task.
pub struct LogScan<T> {
    cursor: Cursor,
    chunk: usize,
    _entity: PhantomData<fn() -> T>,
}

impl<T> LogScan<T>
where
    T: LogEntity + Send + 'static,
    T::Key: Send + 'static,
{
    pub fn new(range: KeyRange, chunk: usize) -> Self {
        Self {
            cursor: Cursor::new(range),
            chunk: chunk.max(1),
            _entity: PhantomData,
        }
    }

    /// Read the next chunk and pass it to `f` in the same blocking task.
    /// Returns `None` once the scan is exhausted.
    pub async fn next<D, R, F>(&mut self, facade: &Facade<D>, f: F) -> Result<Option<R>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        R: Send + 'static,
        F: FnOnce(Vec<(BlockSlot, T::Key, T)>) -> R + Send + 'static,
    {
        if self.cursor.done {
            return Ok(None);
        }

        let range = self.cursor.range.clone();
        let chunk = self.chunk;

        let (out, last, full) = facade
            .blocking(move |facade| {
                let iter = facade
                    .archive()
                    .iter_logs(T::NS, range)
                    .map_err(log_and_500("failed to scan logs"))?;

                let rows = LogIterTyped::<D::Archive, T>::new(iter)
                    .take(chunk)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(log_and_500("failed to read a scanned log"))?;

                let last = rows
                    .last()
                    .map(|(slot, key, _)| KeyBytes::from_slice(LogKey::of(*slot, key).as_ref()));
                let full = rows.len() == chunk;

                Ok((f(rows), last, full))
            })
            .await?;

        self.cursor.advance(last, full);

        Ok(Some(out))
    }
}

/// Every live ref of one tag when the tag holds at most `budget` of them,
/// `None` when it holds more.
///
/// One bounded read under the domain's query limiter: the store is asked for
/// `budget + 1` refs and stops at the first one past the budget, so a tag of
/// any size costs at most that many refs and is never whole in memory. The
/// refs come in `TxoRef` order.
pub async fn tag_within_budget<D>(
    facade: &Facade<D>,
    dimension: TagDimension,
    key: Vec<u8>,
    budget: usize,
) -> Result<Option<Vec<TxoRef>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let limit = budget.saturating_add(1);

    let refs = facade
        .blocking(move |facade| {
            facade
                .state()
                .utxos_by_tag_limited(dimension, &key, limit)
                .map_err(log_and_500("failed to read a utxo tag"))
        })
        .await?;

    Ok((refs.len() <= budget).then_some(refs))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use dolos_cardano::{
        indexes::{utxo_dimensions, CardanoStateIndexExt as _},
        model::AccountState,
        AccountKey,
    };

    use super::*;
    use crate::test_support::{replicated_app, replicated_app_with_fault, TestApp, TestFault};

    fn asset_subject(app: &TestApp) -> Vec<u8> {
        hex::decode(&app.vectors().asset_unit).expect("asset unit hex")
    }

    /// A tag within the budget comes whole and in ref order, as
    /// `utxos_by_tag` has it; one past the budget is `None`, whether it is
    /// past by one ref or by a budget of zero.
    #[tokio::test]
    async fn tag_within_budget_reads_the_tag_or_gives_it_up() {
        let app = TestApp::new();
        let facade = app.facade();
        let subject = asset_subject(&app);

        let mut whole: Vec<TxoRef> = facade
            .state()
            .utxos_by_asset(&subject)
            .expect("asset refs")
            .into_iter()
            .collect();
        whole.sort();
        let n = whole.len();
        assert!(n > 1, "too few utxos for a budget to cut");

        let within = |budget| {
            let facade = facade.clone();
            let subject = subject.clone();
            async move {
                tag_within_budget(&facade, utxo_dimensions::ASSET, subject, budget)
                    .await
                    .expect("tag read")
            }
        };

        assert_eq!(within(n).await, Some(whole.clone()));
        assert_eq!(within(n + 1).await, Some(whole.clone()));
        assert_eq!(within(usize::MAX).await, Some(whole));
        assert_eq!(within(n - 1).await, None);
        assert_eq!(within(0).await, None);
    }

    async fn account_keys(app: &TestApp, chunk: usize) -> Vec<AccountKey> {
        let facade = app.facade();
        let mut scan = EntityScan::<AccountState>::all(chunk);
        let mut keys = Vec::new();

        while let Some(chunk) = scan
            .next(&facade, |accounts| {
                accounts.into_iter().map(|(key, _)| key).collect::<Vec<_>>()
            })
            .await
            .expect("account chunk")
        {
            keys.extend(chunk);
        }

        keys
    }

    #[tokio::test]
    async fn chunked_entity_scan_matches_unchunked() {
        let app = replicated_app();

        let whole = account_keys(&app, usize::MAX).await;
        let chunked = account_keys(&app, 2).await;

        assert!(whole.len() > 2, "too few accounts to split into chunks");
        assert_eq!(chunked, whole);
    }

    #[tokio::test]
    async fn dropped_account_scan_starts_no_further_chunk() {
        const SLOW_SCAN: Duration = Duration::from_millis(200);

        let app = replicated_app_with_fault(Some(TestFault::SlowStateScans(SLOW_SCAN)));
        let facade = app.facade();
        let chunks = Arc::new(AtomicUsize::new(0));

        let scan = {
            let chunks = chunks.clone();
            async move {
                let mut scan = EntityScan::<AccountState>::all(1);
                loop {
                    let chunks = chunks.clone();
                    let read = scan
                        .next(&facade, move |accounts| {
                            chunks.fetch_add(1, Ordering::SeqCst);
                            accounts.len()
                        })
                        .await
                        .expect("account chunk");

                    if read.is_none() {
                        break;
                    }
                }
            }
        };

        // the drop lands while the first chunk sleeps in its read
        let dropped = tokio::time::timeout(SLOW_SCAN / 2, scan).await;
        assert!(dropped.is_err(), "the scan ended before the drop");

        tokio::time::sleep(SLOW_SCAN * 4).await;

        assert_eq!(
            chunks.load(Ordering::SeqCst),
            1,
            "a chunk started after the scan was dropped"
        );
    }
}
