use axum::http::StatusCode;

use dolos_core::{Domain, StateStore as _, Tag, TagEntry, TagOrder, TagStart};

use crate::Facade;

use super::MAX_WINDOW;

/// Reads up to `count` entries of `tag`, in creation order, from `start`.
pub(super) async fn tag_page<D>(
    domain: &Facade<D>,
    tag: &Tag,
    order: TagOrder,
    start: TagStart,
    count: usize,
) -> Result<Vec<TagEntry>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    debug_assert!(count <= MAX_WINDOW);

    let tag = tag.clone();

    domain
        .blocking(move |domain| {
            domain
                .state()
                .utxos_by_tag_page(tag.dimension, &tag.key, order, start, count)
                .map_err(|err| {
                    tracing::error!(?err);
                    StatusCode::INTERNAL_SERVER_ERROR
                })
        })
        .await
}

/// Returns `true` when no live UTxO carries `tag`.
pub async fn tag_is_empty<D>(domain: &Facade<D>, tag: &Tag) -> Result<bool, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    Ok(tag_page(domain, tag, TagOrder::Asc, TagStart::Skip(0), 1)
        .await?
        .is_empty())
}

/// The entries of a listing, in creation order, read a window at a time.
///
/// Each read of a tag resumes just past the last entry the walk read, not at
/// a count of entries, so a UTxO spent or rolled back between two reads shifts
/// none of the entries still ahead.
///
/// Reads are not one snapshot, so a fork during the walk can hide a live row
/// from one response. When a rollback and a fork block re-include a
/// transaction at a position the walk has already passed, its UTxOs' entries
/// land behind the walk and no later read reaches them. The next request lists
/// them. A response never lists a UTxO that was spent when its value was read.
pub(super) struct Walk {
    source: Source,
    order: TagOrder,
}

enum Source {
    Tag {
        tag: Tag,
        start: TagStart,
        done: bool,
    },
    Entries(std::vec::IntoIter<TagEntry>),
}

impl Walk {
    /// A walk of `tag` that starts at `start`.
    pub(super) fn tag(tag: Tag, order: TagOrder, start: TagStart) -> Self {
        let source = Source::Tag {
            tag,
            start,
            done: false,
        };

        Self { source, order }
    }

    /// A walk of entries already read in the listing's order.
    pub(super) fn entries(entries: Vec<TagEntry>, order: TagOrder) -> Self {
        let source = Source::Entries(entries.into_iter());

        Self { source, order }
    }

    /// The next `count` entries, fewer when the walk ends: none once it has.
    pub(super) async fn next<D>(
        &mut self,
        domain: &Facade<D>,
        count: usize,
    ) -> Result<Vec<TagEntry>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        match &mut self.source {
            Source::Tag { done: true, .. } => Ok(Vec::new()),
            Source::Tag { tag, start, done } => {
                let entries = tag_page(domain, tag, self.order, start.clone(), count).await?;

                *done = entries.len() < count;

                if let Some(last) = entries.last() {
                    *start = TagStart::After(last.clone());
                }

                Ok(entries)
            }
            Source::Entries(entries) => Ok(entries.by_ref().take(count).collect()),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::utxos::testing::*;
    use dolos_core::{Domain, StateStore, TagOrder, TagStart};

    /// A walk resumes just past the last entry it read. A row spent behind it
    /// moves nothing ahead: a read at a count of entries would pass over the
    /// row the spend shifted into its place.
    #[tokio::test]
    async fn walks_resume_past_the_last_entry_read() {
        for order in [TagOrder::Asc, TagOrder::Desc] {
            let app = chain_with_configured_rows();
            let facade = app.facade();
            let tag = address_tag(&app);

            let all = facade
                .state()
                .utxos_by_tag_page(
                    tag.dimension,
                    &tag.key,
                    order,
                    TagStart::Skip(0),
                    usize::MAX,
                )
                .expect("whole tag");
            assert!(all.len() > 6, "the fixture lists too few rows");

            let mut walk = Walk::tag(tag, order, TagStart::Skip(0));

            let first = walk.next(&facade, 3).await.expect("first read");
            assert_eq!(first, all[..3], "{order:?}");

            spend(facade.inner.state(), &first[0].txo);

            let next = walk.next(&facade, 3).await.expect("second read");
            assert_eq!(next, all[3..6], "{order:?}: the walk skipped a row");
        }
    }
}
