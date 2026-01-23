export type ProcedureMap = Record<string, (params: any) => Promise<any>>;

export interface RpcTransport {
  request: (procedureId: string, payload: unknown) => Promise<unknown>;
  subscribe?: (
    procedureId: string,
    payload: unknown,
    onData: (data: unknown) => void,
  ) => () => void;
}

export interface Subscription<T> {
  subscribe(onData: (data: T) => void): () => void;
  [Symbol.asyncIterator](): AsyncIterator<T>;
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
        // Return an object that looks like a Promise but also has subscribe capability if it's a stream?
        // Or check return type? We can't check return type at runtime easily without metadata.

        // However, if the return type is Subscription<T>, the user will call .subscribe() on the result.
        // So we can return a "Subscription-like" object that also functions as a Promise if it's a regular call?
        // No, Promise and Subscription are distinct.

        // Approach: Return a hybrid object or rely on how it's called.
        // If it's a subscription, the transport handles it.
        // But how do we know?

        // If we want `await client.procedure(params)` for unary
        // AND `client.stream(params).subscribe(...)` for streams.

        // We can return an object that has .then (Promise) AND .subscribe (Subscription).
        // If .then is called, we do a unary request.
        // If .subscribe is called, we do a subscription request.

        return {
          then(resolve: (value: any) => void, reject: (reason: any) => void) {
            transport.request(procedureId, params).then(resolve, reject);
          },
          subscribe(onData: (data: any) => void) {
            if (!transport.subscribe) {
              throw new Error("Transport does not support subscriptions");
            }
            return transport.subscribe(procedureId, params, onData);
          },
          [Symbol.asyncIterator]() {
            // Adapt subscription to async iterator
            if (!transport.subscribe) {
              throw new Error("Transport does not support subscriptions");
            }
            const queue: any[] = [];
            let resolveNext: (() => void) | null = null;
            let done = false;
            let error: any = null;

            const unsubscribe = transport.subscribe(procedureId, params, (data) => {
              queue.push(data);
              if (resolveNext) {
                resolveNext();
                resolveNext = null;
              }
            });

            // TODO: Handle close/error events from transport if available

            return {
              async next() {
                if (queue.length > 0) {
                  return { value: queue.shift(), done: false };
                }
                if (done) return { value: undefined, done: true };
                if (error) throw error;

                await new Promise<void>((resolve) => {
                  resolveNext = resolve;
                });
                return { value: queue.shift(), done: false };
              },
              async return() {
                unsubscribe();
                done = true;
                return { value: undefined, done: true };
              },
            };
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
