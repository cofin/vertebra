import { effect, Injectable, inject, signal, untracked } from "@angular/core";
import { HttpClient } from "../client/client";
import type { ClientConfig } from "../client/types";

@Injectable({
  providedIn: "root",
})
export class ClientService {
  private client: HttpClient | null = null;

  init(config: ClientConfig) {
    this.client = new HttpClient(config);
  }

  getClient(): HttpClient {
    if (!this.client) {
      throw new Error("ClientService must be initialized with init()");
    }
    return this.client;
  }
}

/**
 * Angular 21 Signal-based query.
 */
export function injectQuery<TInput = any, TOutput = any>(
  method: string,
  path: string,
  input: () => TInput,
  options: { enabled?: () => boolean } = {},
) {
  const service = inject(ClientService);
  const data = signal<TOutput | null>(null);
  const loading = signal(false);
  const error = signal<Error | null>(null);
  let requestSeq = 0;

  const fetch = async () => {
    if (options.enabled && !options.enabled()) return;

    const seq = ++requestSeq;
    loading.set(true);
    error.set(null);
    try {
      const result = await service.getClient().call<TInput, TOutput>(method, path, input());
      if (seq === requestSeq) {
        data.set(result);
      }
    } catch (err: any) {
      if (seq === requestSeq) {
        error.set(err);
      }
    } finally {
      if (seq === requestSeq) {
        loading.set(false);
      }
    }
  };

  // Run effect when input changes
  effect(() => {
    input();
    untracked(() => fetch());
  });

  return {
    data: data.asReadonly(),
    loading: loading.asReadonly(),
    error: error.asReadonly(),
    refresh: fetch,
  };
}

/**
 * Angular 21 Signal-based mutation.
 */
export function injectMutation<TInput = any, TOutput = any>(method: string, path: string) {
  const service = inject(ClientService);
  const loading = signal(false);
  const error = signal<Error | null>(null);

  const mutate = async (input?: TInput) => {
    loading.set(true);
    error.set(null);
    try {
      return await service.getClient().call<TInput, TOutput>(method, path, input);
    } catch (err: any) {
      error.set(err);
      throw err;
    } finally {
      loading.set(false);
    }
  };

  return {
    mutate,
    loading: loading.asReadonly(),
    error: error.asReadonly(),
  };
}
