//! Direct dispatch: push batches through a tokio mpsc channel so the
//! in-process worker can drain them. Responses flow back via each request's
//! oneshot channel.

use tracing::error;

use super::types::{DirectBatch, DirectRequest};
use super::Inner;
use crate::collector::Batch;

pub(super) fn dispatch_direct(inner: &Inner, batch: Batch) -> usize {
    use vertebra_core::protocol::flags;

    let Some(tx) = inner.direct_tx.as_ref() else {
        error!("direct dispatch channel not configured");
        return 0;
    };

    let count = batch.requests.len();
    let requests = batch
        .requests
        .into_iter()
        .map(|req| {
            let is_fnf = req.flags & flags::FIRE_AND_FORGET != 0;
            DirectRequest {
                handler_id: req.handler_id,
                method_tag: req.method_tag,
                http_version_tag: req.http_version_tag,
                flags: req.flags,
                path: req.path,
                query: req.query,
                headers_mp: req.headers_mp,
                body: req.body,
                reply: if is_fnf { None } else { Some(req.reply) },
            }
        })
        .collect();

    if tx.try_send(DirectBatch { requests }).is_err() {
        error!("direct dispatch channel full or closed, dropping batch");
        0
    } else {
        count
    }
}
