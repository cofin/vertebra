/**
 * Compute exponential backoff delay with random jitter, capped at `maxDelayMs`.
 */
export function computeBackoffDelay(attempt: number, baseDelayMs: number, maxDelayMs = 30_000): number {
  const exponential = baseDelayMs * 2 ** attempt;
  const jitter = Math.random() * baseDelayMs * 0.5;
  return Math.min(exponential + jitter, maxDelayMs);
}
