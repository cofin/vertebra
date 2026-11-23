export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

export interface AsyncStateSink<TOutput> {
  onStart: () => void;
  onSuccess: (data: TOutput) => void;
  onError: (err: Error) => void;
  onFinish?: () => void;
}

export function createSequencedRunner<TOutput>(
  fetchFn: () => Promise<TOutput>,
  sink: AsyncStateSink<TOutput>,
  isEnabled: () => boolean = () => true,
): () => Promise<void> {
  let requestSeq = 0;
  return async () => {
    if (!isEnabled()) return;
    const seq = ++requestSeq;
    sink.onStart();
    try {
      const result = await fetchFn();
      if (seq === requestSeq) {
        sink.onSuccess(result);
      }
    } catch (err: unknown) {
      if (seq === requestSeq) {
        sink.onError(toError(err));
      }
    } finally {
      if (seq === requestSeq) {
        sink.onFinish?.();
      }
    }
  };
}

export async function runTrackedMutation<TOutput>(
  execute: () => Promise<TOutput>,
  setState: (loading: boolean, error: Error | null) => void,
  onSuccess?: () => void,
): Promise<TOutput> {
  setState(true, null);
  try {
    const result = await execute();
    onSuccess?.();
    setState(false, null);
    return result;
  } catch (err: unknown) {
    setState(false, toError(err));
    throw err;
  }
}
