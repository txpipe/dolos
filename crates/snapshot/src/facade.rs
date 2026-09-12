//! Supported headless snapshot assembly surface.
//!
//! This module gathers the profile's host-facing lifecycle without hiding the
//! narrower modules that implement it. A host can plan, encode, inspect,
//! verify and restore without importing a Dolos binary module or constructing
//! terminal feedback.

pub use crate::{
    publisher::{Next, Publisher, RepositoryPublish},
    registry::{Auth, Point, Repository, SnapshotRepository, Tuning},
    restore::{execute as restore, Input as RestoreInput, RestoreOutcome, Restoring, Target},
    source::{Selection, SnapshotSource, StoreSnapshot},
};
