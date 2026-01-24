export type ProcedureMap = Record<string, (params: any) => Promise<any>>;

export interface RpcTransport {
  request: (procedureId: string, payload: unknown) => Promise<unknown>;
  subscribe?: (
    procedureId: string,
    payload: unknown,
    onData: (data: unknown) => void,
    handlers?: { onError?: (error: unknown) => void; onClose?: () => void },
  ) => () => void;
}

export interface Subscription<T> {
  subscribe(
    onData: (data: T) => void,
    handlers?: { onError?: (error: unknown) => void; onClose?: () => void },
  ): () => void;
  [Symbol.asyncIterator](): AsyncIterator<T>;
}

function createAsyncIterator<T>(
  subscribe: (
    onData: (data: T) => void,
    handlers?: { onError?: (error: unknown) => void; onClose?: () => void },
  ) => () => void,
): AsyncIterator<T> {
  const queue: T[] = [];
  let resolveNext: ((value: IteratorResult<T>) => void) | null = null;
  let rejectNext: ((error: unknown) => void) | null = null;
  let done = false;
  let failure: unknown = null;

  const onData = (data: T) => {
    queue.push(data);
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = null;
      rejectNext = null;
      resolve({ value: queue.shift() as T, done: false });
    }
  };

  const onError = (error: unknown) => {
    failure = error;
    if (rejectNext) {
      const reject = rejectNext;
      resolveNext = null;
      rejectNext = null;
      reject(error);
    }
  };

  const onClose = () => {
    done = true;
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = null;
      rejectNext = null;
      resolve({ value: undefined as T, done: true });
    }
  };

  const unsubscribe = subscribe(onData, { onError, onClose });

  return {
    async next() {
      if (queue.length > 0) {
        return { value: queue.shift() as T, done: false };
      }
      if (failure) {
        throw failure;
      }
      if (done) {
        return { value: undefined as T, done: true };
      }
      return await new Promise<IteratorResult<T>>((resolve, reject) => {
        resolveNext = resolve;
        rejectNext = reject;
      });
    },
    async return() {
      unsubscribe();
      done = true;
      return { value: undefined as T, done: true };
    },
    async throw(error) {
      unsubscribe();
      failure = error;
      throw error;
    },
  };
}

export function createClient<TProcedures extends ProcedureMap>(transport: RpcTransport) {
  return new Proxy({} as any, {
    get(_target, prop) {
      // Handle nested namespaces if needed (future)
      // For now, assuming flat procedure map or dot notation usage in proxy
      // Actually, standard usage is client.procedureName(params)
      // Wait, the current implementation exposes a .call() method.
      // The desired usage is proxy-based: client.todos.add(...)

      const procedureId = String(prop);
      return (params: any) => {
        let unaryPromise: Promise<unknown> | null = null;
        const requestUnary = () => {
          if (!unaryPromise) {
            unaryPromise = transport.request(procedureId, params);
          }
          return unaryPromise;
        };

        const subscribe = (
          onData: (data: any) => void,
          handlers?: { onError?: (error: unknown) => void; onClose?: () => void },
        ) => {
          if (!transport.subscribe) {
            throw new Error("Transport does not support subscriptions");
          }
          return transport.subscribe(procedureId, params, onData, handlers);
        };

        return {
          then(resolve: (value: any) => void, reject: (reason: any) => void) {
            return requestUnary().then(resolve, reject);
          },
          catch(reject: (reason: any) => void) {
            return requestUnary().catch(reject);
          },
          finally(callback: () => void) {
            return requestUnary().finally(callback);
          },
          subscribe,
          [Symbol.asyncIterator]() {
            return createAsyncIterator(subscribe);
          },
        };
      };
    },
  }) as {
    [K in keyof TProcedures]: (
      params: Parameters<TProcedures[K]>[0],
    ) => ReturnType<TProcedures[K]> extends Promise<Subscription<infer U>>
      ? Subscription<U>
      : ReturnType<TProcedures[K]>;
  };
}
