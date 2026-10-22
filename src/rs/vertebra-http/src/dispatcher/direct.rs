//! Direct dispatch: push batches through a tokio mpsc channel so the
//! in-process worker can drain them. Responses flow back via each request's
//! oneshot channel.

use tokio::sync::mpsc;
use tracing::error;

use crate::batcher::Batch;

pub(super) fn dispatch_direct(tx: &mpsc::Sender<Batch>, batch: Batch) -> usize {
    let count = batch.requests.len();
    if tx.try_send(batch).is_err() {
        error!("direct dispatch channel full or closed, dropping batch");
        0
    } else {
        count
    }
}
