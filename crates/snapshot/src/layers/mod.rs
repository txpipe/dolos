//! One codec per layer kind.
//!
//! Every module here exposes the same three things, so a caller meets one shape
//! five times:
//!
//! - `encode(&Record) -> Result<CanonicalCbor, Error>` — through
//!   [`stelae::frame::encode`], so the deterministic-encoding profile is
//!   enforced by construction rather than by review.
//! - `decode(&[u8]) -> Result<Record, Error>` — fail-closed, and total: a
//!   record that decodes is a record the restore path can write.
//! - `OrderCheck` — the kind's ordering contract as a streaming validator,
//!   because ordering *is* content here. A restore ingests these records
//!   append-only into sorted stores; records arriving out of order do not fail
//!   loudly, they land in a store whose own queries then miss them.
//!
//! The ordering rules are ADR-004's, and three of the five are exactly the
//! iteration order the source stores already promise
//! ([`dolos_core::IndexStore::iter_archive_tags`],
//! [`dolos_core::IndexStore::iter_exact_records`]). The validator is not a
//! substitute for that promise; it is what catches a driver that merges,
//! chunks or parallelizes those iterators and loses it.
//!
//! The shape checks every `decode` runs — the field count, each field's type
//! and width, and that nothing trails the record — are [`stelae::codec`]'s, and
//! the reason they are not re-validating canonical form is documented there.

pub mod blocks;
pub mod digests;
pub mod indexes;
pub mod logs;
pub mod state;
