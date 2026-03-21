import { effect, Injectable, inject, signal, untracked } from "@angular/core";
import { VertebraClient } from "../client/client";
import type { VertebraConfig } from "../client/types";

@Injectable({
  providedIn: "root",
})
export class VertebraClientService {
  private client: VertebraClient | null = null;

  init(config: VertebraConfig) {
    this.client = new VertebraClient(config);
  }

  getClient(): VertebraClient {
    if (!this.client) {
      throw new Error("VertebraClientService must be initialized with init()");
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
  const service = inject(VertebraClientService);
  const data = signal<TOutput | null>(null);
  const loading = signal(false);
  const error = signal<Error | null>(null);

  const fetch = async () => {
    if (options.enabled && !options.enabled()) return;

    loading.set(true);
    error.set(null);
    try {
      const result = await service.getClient().call<TInput, TOutput>(method, path, input());
      data.set(result);
    } catch (err: any) {
      error.set(err);
    } finally {
      loading.set(false);
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
  const service = inject(VertebraClientService);
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
