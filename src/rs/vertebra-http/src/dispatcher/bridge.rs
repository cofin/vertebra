//! Bridge dispatch: write the batch into the arena-backed BatchBridge and
//! track each request's correlation ID in a DashMap until the response
//! reader pairs it back.

use std::sync::atomic::Ordering;

use tracing::error;

use vertebra_core::bridge::{BatchBridge, WriteRequest};

use super::BridgeCorrelation;
use crate::batcher::Batch;

pub(super) fn dispatch_bridge(bridge_corr: &BridgeCorrelation, batch: Batch) -> usize {
    use vertebra_core::protocol::flags;

    let Some(bridge) = BatchBridge::get() else {
        error!("batch bridge not initialized, dropping batch");
        return 0;
    };
    let writer = bridge.batch_writer();
    let mut dispatched_count = 0;

    for req in batch.requests {
        let id = bridge_corr.next_id.fetch_add(1, Ordering::Relaxed);
        let is_fnf = req.flags & flags::FIRE_AND_FORGET != 0;

        let push_result = writer.push_request(&WriteRequest {
            method_tag: req.method_tag,
            http_version_tag: req.http_version_tag,
            handler_id: req.handler_id,
            correlation_id: id,
            path: req.path().as_bytes(),
            query: req.query().map(str::as_bytes),
            headers_raw: &req.headers_mp,
            body: &req.body,
        });

        if let Err(e) = push_result {
            error!(error = %e, "transport push failed, dropping request");
        } else {
            if !is_fnf {
                bridge_corr.pending.insert(id, req.reply);
            }
            dispatched_count += 1;
        }
    }

    if let Err(e) = writer.seal() {
        error!(error = %e, "transport seal failed");
    }

    dispatched_count
}
