//! Direct dispatch: push batches through a tokio mpsc channel so the
//! in-process worker can drain them. Responses flow back via each request's
//! oneshot channel.

use tokio::sync::mpsc;
use tracing::error;

use crate::batcher::{Batch, PendingRequest};

pub(super) fn dispatch_direct_vec(
    tx: &mpsc::Sender<Batch>,
    requests: &mut Vec<PendingRequest>,
) -> usize {
    let count = requests.len();
    let cap = requests.capacity();
    let batch = Batch {
        requests: std::mem::replace(requests, Vec::with_capacity(cap)),
    };
    if tx.try_send(batch).is_err() {
        error!("direct dispatch channel full or closed, dropping batch");
        0
    } else {
        count
    }
}
