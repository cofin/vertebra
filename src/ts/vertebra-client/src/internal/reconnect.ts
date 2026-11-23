import { computeBackoffDelay } from "../client/retry";

export class ReconnectScheduler {
  private attempts = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly maxAttempts: number,
    private readonly baseDelayMs: number,
    private readonly maxDelayMs: number,
  ) {}

  schedule(onRetry: () => void): boolean {
    if (this.attempts >= this.maxAttempts) {
      return false;
    }
    const delay = computeBackoffDelay(this.attempts, this.baseDelayMs, this.maxDelayMs);
    this.attempts += 1;
    this.timer = setTimeout(() => {
      this.timer = null;
      onRetry();
    }, delay);
    return true;
  }

  reset(): void {
    this.attempts = 0;
  }

  cancel(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.attempts = 0;
  }
}
