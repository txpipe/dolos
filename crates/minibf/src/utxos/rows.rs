use axum::http::StatusCode;
use pallas::ledger::traverse::MultiEraOutput;
use std::{collections::HashSet, sync::Arc};

use dolos_core::{Domain, StateStore as _, TagEntry, TxoRef, UtxoValue};

use crate::Facade;

use super::MAX_WINDOW;

/// A page's rows, gathered in walk order.
///
/// A ref counts once: a fork that re-creates a UTxO at a later position
/// between two reads puts its entry ahead of the walk a second time. It counts
/// once its value loads, so an entry whose UTxO a rollback removed before the
/// value read leaves the ref to the entry the fork block writes.
pub(super) struct Rows {
    pub(super) rows: Vec<(TxoRef, Arc<UtxoValue>)>,
    counted: HashSet<TxoRef>,
    count: usize,
}

impl Rows {
    pub(super) fn new(count: usize) -> Self {
        Self {
            rows: Vec::with_capacity(count),
            counted: HashSet::new(),
            count,
        }
    }

    pub(super) fn full(&self) -> bool {
        self.rows.len() >= self.count
    }

    pub(super) fn missing(&self) -> usize {
        self.count.saturating_sub(self.rows.len())
    }

    /// The entries whose ref the walk has not counted, each ref once.
    pub(super) fn uncounted(&self, entries: Vec<TagEntry>) -> Vec<TagEntry> {
        let mut refs = HashSet::new();

        entries
            .into_iter()
            .filter(|entry| !self.counted.contains(&entry.txo) && refs.insert(entry.txo.clone()))
            .collect()
    }

    /// Counts the refs of `entries`.
    pub(super) fn count_entries(&mut self, entries: &[TagEntry]) {
        self.counted
            .extend(entries.iter().map(|entry| entry.txo.clone()));
    }

    /// Loads the UTxOs behind the uncounted entries of `entries`, as
    /// [`load_values`] does, and counts the ones that load.
    pub(super) async fn load<D>(
        &mut self,
        domain: &Facade<D>,
        entries: Vec<TagEntry>,
        ada_only: bool,
    ) -> Result<Vec<(TxoRef, Arc<UtxoValue>)>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        let loaded = load_values(domain, self.uncounted(entries), ada_only).await?;

        self.counted
            .extend(loaded.iter().map(|(txo, _)| txo.clone()));

        Ok(loaded)
    }
}

/// The output holds no native asset.
fn holds_no_asset(output: &MultiEraOutput<'_>) -> bool {
    output
        .value()
        .assets()
        .iter()
        .all(|policy| policy.assets().is_empty())
}

/// Reads the UTxOs behind `entries` on the blocking pool, in the order of
/// `entries`, keeping only those whose output holds no native asset when
/// `ada_only` is set. A UTxO spent since its entry was read is skipped; every
/// output the filter reads must decode.
async fn load_values<D>(
    domain: &Facade<D>,
    entries: Vec<TagEntry>,
    ada_only: bool,
) -> Result<Vec<(TxoRef, Arc<UtxoValue>)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    debug_assert!(entries.len() <= MAX_WINDOW);

    domain
        .blocking(move |domain| {
            let refs: Vec<TxoRef> = entries.into_iter().map(|entry| entry.txo).collect();

            let mut utxos = domain
                .state()
                .get_utxos(refs.clone())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let mut kept = Vec::with_capacity(refs.len());
            for txo_ref in refs {
                let Some(value) = utxos.remove(&txo_ref) else {
                    continue;
                };

                if ada_only {
                    let output = MultiEraOutput::try_from(value.as_ref())
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    if !holds_no_asset(&output) {
                        continue;
                    }
                }

                kept.push((txo_ref, value));
            }

            Ok(kept)
        })
        .await
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::utxos::{testing::*, walk::Walk, MAX_WINDOW};
    use dolos_core::{Domain, TagOrder, TagStart, TxoRef, UtxoPosition, UtxoValue};
    use pallas::crypto::hash::Hash;

    /// A row whose UTxO is gone by the time its value is read gives its place
    /// to the next one, at either end of the tag, so a page that is not the
    /// last is full, on the plain listing and on an asset's.
    #[tokio::test]
    async fn pages_fill_past_rows_spent_since_the_tag_read() {
        let app = chain_with_configured_rows();
        let address = app.vectors().address.clone();
        let unit = app.vectors().asset_unit.clone();

        let asc = whole_set_rows(&app, &address, "asc").await;
        let desc = whole_set_rows(&app, &address, "desc").await;
        let with_asset = walk(&app, &format!("/addresses/{address}/utxos"), "asc", 100)
            .await
            .into_iter()
            .filter(|row| row.units.contains(&unit))
            .collect::<Vec<_>>();
        assert!(with_asset.len() > 3, "the fixture holds too few asset rows");

        // gone rows at the head of the tag in each order, holding the asset
        let output = value_of(&app, &with_asset[0].txo);
        let domain = app.facade().inner;
        tag_only(&domain, TxoRef(Hash::from([0x01; 32]), 9), None, &output);
        tag_only(
            &domain,
            TxoRef(Hash::from([0x02; 32]), 0),
            Some(UtxoPosition::new(u64::MAX, 0)),
            &output,
        );

        for count in [1, 3, 4] {
            let path = format!("/addresses/{address}/utxos?count={count}");

            let rows = without_units(page_rows(&app, &format!("{path}&order=asc")).await);
            assert_eq!(rows, asc[..count], "asc count={count}");

            let rows = without_units(page_rows(&app, &format!("{path}&order=desc")).await);
            assert_eq!(rows, desc[..count], "desc count={count}");

            let path = format!("/addresses/{address}/utxos/{unit}?count={count}&order=asc");
            assert_eq!(page_rows(&app, &path).await, with_asset[..count]);
        }
    }

    /// A ref the tag holds at two positions, as a fork that re-creates a UTxO
    /// later in the chain leaves it between two reads, is listed once.
    #[tokio::test]
    async fn a_ref_reached_twice_is_one_row() {
        let app = chain_with_configured_rows();
        let address = app.vectors().address.clone();

        let asc = whole_set_rows(&app, &address, "asc").await;
        let lovelace_path = format!("/addresses/{address}/utxos/lovelace?order=asc&count=100");
        let lovelace = page_rows(&app, &lovelace_path).await;
        assert!(
            lovelace.len() > 2,
            "the fixture lists too few lovelace rows"
        );

        let listed = &lovelace[1].txo;
        let domain = app.facade().inner;
        tag_only(
            &domain,
            listed.parse().expect("txo ref"),
            Some(UtxoPosition::new(u64::MAX, 0)),
            &value_of(&app, listed),
        );

        let path = format!("/addresses/{address}/utxos?order=asc&count=100");
        assert_eq!(without_units(page_rows(&app, &path).await), asc);
        assert_eq!(page_rows(&app, &lovelace_path).await, lovelace);
    }

    /// A ref counts once its value loads. A rollback that removes a UTxO
    /// between its tag read and its value read leaves the ref uncounted, so
    /// the entry the fork block writes for it further on is a row.
    #[tokio::test]
    async fn a_ref_counts_once_its_value_loads() {
        let app = chain_with_configured_rows();
        let facade = app.facade();

        let mut walk = Walk::tag(address_tag(&app), TagOrder::Asc, TagStart::Skip(0));
        let mut rows = Rows::new(100);

        let first = walk.next(&facade, MAX_WINDOW).await.expect("first read");
        let gone = first
            .iter()
            .find(|entry| entry.order != UtxoPosition::tag_order(None))
            .expect("a block row in the first read")
            .txo
            .clone();
        let value = value_of(&app, &gone.to_string());

        spend(facade.inner.state(), &gone);

        let loaded = rows
            .load(&facade, first, false)
            .await
            .expect("first values");
        assert!(loaded.iter().all(|(txo, _)| *txo != gone));

        let position = Some(UtxoPosition::new(u64::MAX, 0));
        produce(
            facade.inner.state(),
            vec![(gone.clone(), UtxoValue::new(value.output, position))],
        );

        let mut listed = false;

        loop {
            let next = walk.next(&facade, MAX_WINDOW).await.expect("next read");

            if next.is_empty() {
                break;
            }

            let loaded = rows.load(&facade, next, false).await.expect("next values");
            listed |= loaded.iter().any(|(txo, _)| *txo == gone);
        }

        assert!(listed, "the re-created UTxO is no row");
    }

    /// Through the routes, a rollback between the tag read and the value read
    /// of the window that holds a row, then a fork block that re-creates the
    /// row past the walk, moves the row to the end of the plain listing and of
    /// the `lovelace` walk: each counts a ref only once its value loads.
    #[tokio::test]
    async fn listings_count_a_ref_once_its_value_loads() {
        for listing in ["", "/lovelace"] {
            let app = chain_with_configured_rows();
            let address = app.vectors().address.clone();
            let path = format!("/addresses/{address}/utxos{listing}?order=asc&count=100");

            let before: Vec<String> = page_rows(&app, &path)
                .await
                .into_iter()
                .map(|row| row.txo)
                .collect();
            let moved = before
                .iter()
                .find(|txo| value_of(&app, txo).position().is_some())
                .expect("a block row")
                .clone();

            let txo: TxoRef = moved.parse().expect("txo ref");
            let position = Some(UtxoPosition::new(u64::MAX, 0));
            let mut fork = Some(UtxoValue::new(value_of(&app, &moved).output, position));
            let mut rolled_back = false;

            app.facade().inner.after_tag_page(move |entries, state| {
                if !rolled_back {
                    if entries.iter().any(|entry| entry.txo == txo) {
                        spend(state, &txo);
                        rolled_back = true;
                    }
                } else if let Some(value) = fork.take() {
                    produce(state, vec![(txo.clone(), value)]);
                }
            });

            let after: Vec<String> = page_rows(&app, &path)
                .await
                .into_iter()
                .map(|row| row.txo)
                .collect();

            let mut expected: Vec<String> =
                before.into_iter().filter(|txo| *txo != moved).collect();
            expected.push(moved);

            assert_eq!(after, expected, "listing {listing:?}");
        }
    }
}
