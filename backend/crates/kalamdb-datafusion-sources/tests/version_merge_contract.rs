//! Version-merge contract: the shared substrate must expose the ordering rule
//! used by every MVCC-backed provider family — `(commit_seq, seq_id)` with
//! commit_seq as the primary key and seq_id as the tiebreaker.
//!
//! The real merge execution plan lands in US3. This contract test pins the
//! ordering rule so the implementation cannot silently drift.

use kalamdb_datafusion_sources::exec::{
    select_latest_versions, version_ordering, SelectedVersion, VersionCandidate,
};

#[test]
fn commit_seq_wins_over_seq_id() {
    use std::cmp::Ordering::*;
    assert_eq!(version_ordering(10, 1_u64, 9, 999_u64), Greater);
    assert_eq!(version_ordering(5, 100_u64, 5, 101_u64), Less);
    assert_eq!(version_ordering(5, 100_u64, 5, 100_u64), Equal);
}

#[test]
fn ordering_is_total_and_transitive() {
    let samples = [(1, 1), (1, 2), (2, 0), (2, 5), (3, 1)];
    for &a in &samples {
        for &b in &samples {
            for &c in &samples {
                if version_ordering(a.0, a.1, b.0, b.1).is_lt()
                    && version_ordering(b.0, b.1, c.0, c.1).is_lt()
                {
                    assert!(
                        version_ordering(a.0, a.1, c.0, c.1).is_lt(),
                        "transitivity broke for {a:?} < {b:?} < {c:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn latest_version_selection_is_metadata_first_and_filters_deleted_winners() {
    let winners = select_latest_versions(
        vec![
            VersionCandidate::new("a".to_string(), 1, 1_u64, false, "hot-a"),
            VersionCandidate::new("b".to_string(), 5, 1_u64, false, "hot-b"),
        ],
        vec![
            VersionCandidate::new("a".to_string(), 2, 0_u64, false, "cold-a"),
            VersionCandidate::new("b".to_string(), 6, 0_u64, true, "cold-b-delete"),
        ],
        None,
        false,
    );

    assert_eq!(winners.len(), 1);
    match &winners[0] {
        SelectedVersion::Cold(payload) => assert_eq!(*payload, "cold-a"),
        SelectedVersion::Hot(payload) => panic!("expected cold winner, got {payload}"),
    }
}
