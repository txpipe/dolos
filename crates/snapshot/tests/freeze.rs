//! The version numbers the profile was frozen at for Dolos v2 (decision 0063).
//!
//! `field_registry.rs` holds each namespace's revision to its canaries; this
//! suite holds the numbers themselves to the freeze, so that moving one is a
//! deliberate act recorded here rather than a side effect of another change.

use dolos_snapshot::{MEDIA_TYPE_VERSION, PROFILE_VERSION, SCHEMA_REVS};

const PROCESS: &str = "the v2 freeze (decision 0063) set this; changing it requires a \
     revision bump under decision 0026, with PROFILE.md and this test updated in the \
     same change";

#[test]
fn the_v2_profile_freeze_baseline_holds() {
    assert_eq!(PROFILE_VERSION, 1, "PROFILE_VERSION moved: {PROCESS}");
    assert_eq!(MEDIA_TYPE_VERSION, 1, "MEDIA_TYPE_VERSION moved: {PROCESS}");

    for (ns, rev) in SCHEMA_REVS {
        let expected = if ns == "proposals" { 2 } else { 1 };
        assert_eq!(rev, expected, "{ns}: SCHEMA_REVS moved: {PROCESS}");
    }
}
