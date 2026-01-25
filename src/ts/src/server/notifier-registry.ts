/**
 * Module-level registry of worker notifiers.
 *
 * This module is separate from ProcessManager to avoid circular dependencies
 * with IpcConnection.
 */

import type { ShmNotifier } from "../broker/shm";

/**
 * Registry mapping worker IDs to their notification primitives.
 */
const workerNotifiers = new Map<number, ShmNotifier>();

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
 */
export function getWorkerNotifier(workerId: number): ShmNotifier | undefined {
  return workerNotifiers.get(workerId);
}

/**
 * Notify a worker that data is available.
 * Call this after sending data to a worker's control or request ring.
 *
 * This is a no-op if the worker doesn't have a registered notifier
 * (e.g., when running without ProcessManager).
 */
export function notifyWorker(workerId: number): void {
  const notifier = workerNotifiers.get(workerId);
  if (notifier) {
    try {
      notifier.notify();
    } catch {
      // Ignore errors - worker may have died
    }
  }
}
