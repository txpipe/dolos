use axum::http::StatusCode;

use dolos_core::{Domain, StateStore as _, Tag, TagEntry, TagStart};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

use super::{models::into_models, rows::Rows, tag_order, walk::Walk, FIRST_WINDOW, MAX_WINDOW};

/// What a filtered listing walks, in creation order.
pub enum UtxoWalk {
    /// The UTxOs of a tag whose outputs hold no native asset, the rows that
    /// Blockfrost lists for the `lovelace` asset. Each row's value decides, so
    /// every walked row is read.
    AdaOnly(Tag),
    /// The UTxOs of `entries`, one tag read whole, that the tag `other` also
    /// holds. Each entry is probed in `other` by its full key, so a row that
    /// does not match costs a key read, not a value read.
    ///
    /// The probe looks for the position `entries` was read at. When a rollback
    /// and a fork block re-include a row's transaction at another position in
    /// between, the probe misses it and one response leaves the live row out,
    /// as a walk does for a row the fork moves behind it.
    Intersection { entries: Vec<TagEntry>, other: Tag },
}

/// Loads one page of the UTxOs `walk` keeps, in creation order, and whether
/// any of the walk's UTxOs passed its filter at all.
///
/// Rows are walked in windows until the page is past its offset and full, or
/// the walk ends, so the cost follows the rows walked, not the size of the
/// tag. Only the page's blocks are read.
pub async fn load_filtered_page<D, T>(
    domain: &Facade<D>,
    walk: UtxoWalk,
    pagination: &Pagination,
) -> Result<(Vec<T>, bool), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let order = tag_order(&pagination.order);

    let (mut source, other) = match walk {
        UtxoWalk::AdaOnly(tag) => (Walk::tag(tag, order, TagStart::Skip(0)), None),
        UtxoWalk::Intersection { entries, other } => (Walk::entries(entries, order), Some(other)),
    };

    let mut rows = Rows::new(pagination.count);
    let mut skip = pagination.skip();
    let mut matched = false;
    let mut window = FIRST_WINDOW;

    while !rows.full() {
        let entries = source.next(domain, window).await?;

        if entries.is_empty() {
            break;
        }

        window = window.saturating_mul(2).min(MAX_WINDOW);

        let Some(other) = &other else {
            for row in rows.load(domain, entries, true).await? {
                matched = true;

                if skip > 0 {
                    skip -= 1;
                } else {
                    rows.rows.push(row);

                    if rows.full() {
                        break;
                    }
                }
            }

            continue;
        };

        let held = probe(domain, other, entries).await?;
        matched |= !held.is_empty();

        // a row before the page counts on the probe's word, its value unread
        let mut held = rows.uncounted(held);
        let skipped = skip.min(held.len());
        skip -= skipped;

        let page = held.split_off(skipped);
        rows.count_entries(&held);

        // the page's values only, a UTxO spent since the probe giving its
        // place to the next entry
        let mut page = page.into_iter();

        while !rows.full() {
            let window: Vec<_> = page.by_ref().take(rows.missing()).collect();

            if window.is_empty() {
                break;
            }

            let loaded = rows.load(domain, window, false).await?;
            rows.rows.extend(loaded);
        }
    }

    Ok((into_models(domain, rows.rows, None).await?, matched))
}

/// The entries of `entries` that `tag` also holds, in order.
async fn probe<D>(
    domain: &Facade<D>,
    tag: &Tag,
    entries: Vec<TagEntry>,
) -> Result<Vec<TagEntry>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    debug_assert!(entries.len() <= MAX_WINDOW);

    let tag = tag.clone();

    domain
        .blocking(move |domain| {
            domain
                .state()
                .utxos_with_tag(tag.dimension, &tag.key, entries)
                .map_err(|err| {
                    tracing::error!(?err);
                    StatusCode::INTERNAL_SERVER_ERROR
                })
        })
        .await
}

/// Reads two tags in `order`, a window at a time, until one of them ends
/// before its window does. Returns that one's entries, read whole, then that
/// tag and the other.
///
/// Neither tag is read much past the smaller one's end, and every read takes
/// at most [`MAX_WINDOW`] entries. The entries are read before any probe, so a
/// fork in between can hide a row, as [`UtxoWalk::Intersection`] says.
pub async fn smaller_tag<D>(
    domain: &Facade<D>,
    first: Tag,
    second: Tag,
    order: &Order,
) -> Result<(Vec<TagEntry>, Tag, Tag), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let order = tag_order(order);
    let mut walks = [
        (
            Walk::tag(first.clone(), order, TagStart::Skip(0)),
            Vec::new(),
        ),
        (
            Walk::tag(second.clone(), order, TagStart::Skip(0)),
            Vec::new(),
        ),
    ];
    let mut window = FIRST_WINDOW;

    loop {
        for (i, (walk, entries)) in walks.iter_mut().enumerate() {
            let read = walk.next(domain, window).await?;
            let ended = read.len() < window;
            entries.extend(read);

            if ended {
                let entries = std::mem::take(entries);

                return Ok(match i {
                    0 => (entries, first, second),
                    _ => (entries, second, first),
                });
            }
        }

        window = window.saturating_mul(2).min(MAX_WINDOW);
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        test_support::TestApp,
        utxos::{testing::*, FIRST_WINDOW, MAX_WINDOW},
    };
    use dolos_core::TxoRef;
    use dolos_testing::{synthetic::SyntheticBlockConfig, TestAddress};
    use pallas::{
        crypto::hash::Hash,
        ledger::{addresses::Address, primitives::conway::Value},
    };

    /// An asset listing is the address listing narrowed to the rows that hold
    /// the asset, whichever of the two tags is the smaller one to walk.
    #[tokio::test]
    async fn asset_listings_narrow_the_address_listing() {
        let cfg = SyntheticBlockConfig {
            block_count: 6,
            txs_per_block: 1,
            asset_names_by_block: ["A", "B", "A", "B", "A", "B"].map(String::from).to_vec(),
            ..Default::default()
        };

        // Alice holds one UTxO of asset A, which three of the chain's outputs
        // hold too: her tag is the smaller one there, the asset's is on the
        // chain's address
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            configure(
                domain,
                vec![
                    (
                        TxoRef(Hash::from([0xa1; 32]), 0),
                        asset_output(TestAddress::Alice, &vectors.policy_id, "A"),
                    ),
                    (
                        TxoRef(Hash::from([0xa1; 32]), 1),
                        dolos_testing::utxo_with_value(TestAddress::Alice, Value::Coin(2_000_000)),
                    ),
                ],
            );
        });

        let unit = format!("{}{}", app.vectors().policy_id, hex::encode("A"));
        let alice = Address::from_bytes(&TestAddress::Alice.to_bytes())
            .expect("alice address")
            .to_string();

        for address in [app.vectors().address.clone(), alice] {
            for order in ["asc", "desc"] {
                let listed: Vec<Row> =
                    walk(&app, &format!("/addresses/{address}/utxos"), order, 100)
                        .await
                        .into_iter()
                        .filter(|row| row.units.contains(&unit))
                        .collect();

                assert!(!listed.is_empty(), "{address} holds none of the asset");

                for count in [1, 2, 100] {
                    let path = format!("/addresses/{address}/utxos/{unit}");
                    assert_eq!(
                        walk(&app, &path, order, count).await,
                        listed,
                        "{address} order={order} count={count}"
                    );
                }
            }
        }
    }

    /// Both of an asset listing's tags are read a window at a time, each read
    /// within [`MAX_WINDOW`], which `tag_page` asserts: these tags outlast the
    /// windows a schedule that doubled past it would read.
    #[tokio::test]
    async fn asset_listings_read_both_tags_within_the_window() {
        let app = chain_with_configured_rows_over(2 * (FIRST_WINDOW + MAX_WINDOW));
        let address = app.vectors().address.clone();
        let unit = app.vectors().asset_unit.clone();

        let rows = walk(&app, &format!("/addresses/{address}/utxos"), "asc", 100).await;
        let with_asset: Vec<Row> = rows
            .iter()
            .filter(|row| row.units.contains(&unit))
            .cloned()
            .collect();

        let past_the_cap = FIRST_WINDOW + MAX_WINDOW;
        assert!(
            with_asset.len() > past_the_cap,
            "the asset tag is too small"
        );
        assert!(rows.len() > past_the_cap, "the address tag is too small");

        for count in [1, 100] {
            let path = format!("/addresses/{address}/utxos/{unit}");
            assert_eq!(walk(&app, &path, "asc", count).await, with_asset);
        }
    }
}
