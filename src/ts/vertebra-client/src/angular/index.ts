import { effect, Injectable, inject, signal, untracked } from "@angular/core";
import { HttpClient } from "../client/client";
import type { ClientConfig } from "../client/types";
import { createSequencedRunner, runTrackedMutation } from "../internal/async_runner";

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
export function injectQuery<TInput = unknown, TOutput = unknown>(
  method: string,
  path: string,
  input: () => TInput,
  options: { enabled?: () => boolean } = {},
) {
  const service = inject(ClientService);
  const data = signal<TOutput | null>(null);
  const loading = signal(false);
  const error = signal<Error | null>(null);

  const fetch = createSequencedRunner<TOutput>(
    () => service.getClient().call<TInput, TOutput>(method, path, input()),
    {
      onStart: () => {
        loading.set(true);
        error.set(null);
      },
      onSuccess: (result) => data.set(result),
      onError: (err) => error.set(err),
      onFinish: () => loading.set(false),
    },
    () => !options.enabled || options.enabled(),
  );

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
export function injectMutation<TInput = unknown, TOutput = unknown>(method: string, path: string) {
  const service = inject(ClientService);
  const loading = signal(false);
  const error = signal<Error | null>(null);

  const mutate = (input?: TInput) =>
    runTrackedMutation(
      () => service.getClient().call<TInput, TOutput>(method, path, input),
      (isLoading, err) => {
        loading.set(isLoading);
        error.set(err);
      },
    );

  return {
    mutate,
    loading: loading.asReadonly(),
    error: error.asReadonly(),
  };
}
