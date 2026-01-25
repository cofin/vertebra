/**
 * Module-level registry of worker notifiers.
 *
 * This module is separate from ProcessManager to avoid circular dependencies
 * with IpcConnection.
 *
 * Supports two modes:
 * 1. ProcessManager mode: Each worker has its own notifier (registered via registerWorkerNotifier)
 * 2. Supervisor mode: All workers share a single inherited notifier (set via setSupervisorNotifier)
 */

import type { ShmNotifier } from "../broker/shm";

/**
 * Registry mapping worker IDs to their notification primitives.
 * Used in ProcessManager mode.
 */
const workerNotifiers = new Map<number, ShmNotifier>();

/**
 * Shared notifier inherited from Rust supervisor.
 * Used when VERTEBRA_SKIP_PROCESS_MANAGER=1.
 */
let supervisorNotifier: ShmNotifier | null = null;

/**
 * Register a notifier for a worker.
 * Called by ProcessManager when spawning workers.
 */
export function registerWorkerNotifier(workerId: number, notifier: ShmNotifier): void {
  workerNotifiers.set(workerId, notifier);
}

/**
 * Unregister a notifier for a worker.
 * Called by ProcessManager when workers exit.
 */
export function unregisterWorkerNotifier(workerId: number): void {
  workerNotifiers.delete(workerId);
}

/**
 * Get the notifier for a specific worker.
 * Falls back to supervisor notifier if no per-worker notifier is registered.
 */
export function getWorkerNotifier(workerId: number): ShmNotifier | undefined {
  return workerNotifiers.get(workerId) ?? supervisorNotifier ?? undefined;
}

/**
 * Set the shared supervisor notifier.
 * Called during startup when running under Rust supervisor (VERTEBRA_SKIP_PROCESS_MANAGER=1).
 */
export function setSupervisorNotifier(notifier: ShmNotifier): void {
  supervisorNotifier = notifier;
}

/**
 * Get the supervisor notifier (if set).
 */
export function getSupervisorNotifier(): ShmNotifier | null {
  return supervisorNotifier;
}

/**
 * Notify a worker that data is available.
 * Call this after sending data to a worker's control or request ring.
 *
 * This is a no-op if the worker doesn't have a registered notifier
 * (e.g., when running without ProcessManager).
 *
 * Uses coalesced notification: if a notification is already pending,
 * we skip the redundant signal since the worker will drain all messages
 * when it wakes up anyway.
 */
export function notifyWorker(workerId: number): void {
  // Try per-worker notifier first, fall back to supervisor notifier
  const notifier = workerNotifiers.get(workerId) ?? supervisorNotifier;
  if (notifier) {
    try {
      // Coalesced notification: skip if already pending
      // This reduces syscall overhead under high load
      if (!notifier.hasPending()) {
        notifier.notify();
      }
    } catch {
      // Ignore errors - worker may have died
    }
  }
}

/**
 * Batch notification context for sending multiple messages with a single notify.
 *
 * Usage:
 * ```typescript
 * const batch = beginBatch(workerId);
 * for (const msg of messages) {
 *   ring.send(msg);
 * }
 * batch.commit();  // Single notification for all messages
 * ```
 */
export interface NotificationBatch {
  /** Commit the batch - sends a single notification */
  commit(): void;
  /** Abort the batch without notifying */
  abort(): void;
}

/** Track active batches per worker */
const activeBatches = new Map<number, number>();

/**
 * Begin a notification batch for a worker.
 * Messages sent during the batch will be notified with a single signal on commit().
 */
export function beginBatch(workerId: number): NotificationBatch {
  const count = activeBatches.get(workerId) ?? 0;
  activeBatches.set(workerId, count + 1);

  return {
    commit() {
      const current = activeBatches.get(workerId) ?? 0;
      if (current <= 1) {
        activeBatches.delete(workerId);
        // Only notify if this is the outermost batch
        notifyWorker(workerId);
      } else {
        activeBatches.set(workerId, current - 1);
      }
    },
    abort() {
      const current = activeBatches.get(workerId) ?? 0;
      if (current <= 1) {
        activeBatches.delete(workerId);
      } else {
        activeBatches.set(workerId, current - 1);
      }
    },
  };
}

/**
 * Check if a worker has an active batch.
 * Used internally to skip individual notifications during batching.
 */
export function hasBatch(workerId: number): boolean {
  return (activeBatches.get(workerId) ?? 0) > 0;
}

/**
 * Notify a worker only if not in an active batch.
 * Use this for individual sends that should respect batch context.
 */
export function notifyWorkerIfNotBatched(workerId: number): void {
  if (!hasBatch(workerId)) {
    notifyWorker(workerId);
  }
}
