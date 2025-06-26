use crate::db::snap_shot::Snapshot;
use std::sync::Arc;

struct ReadOptions {
    verify_checksums: bool,
    fill_cache: bool,
    snapshot: Option<Arc<dyn Snapshot>>,
}
