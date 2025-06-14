use std::sync::Arc;
use crate::db::snap_shot::Snapshot;

struct ReadOptions {
    verify_checksums: bool,
    fill_cache: bool,
    snapshot: Option<Arc<dyn Snapshot>>,
}