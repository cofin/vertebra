import { getContext, setContext } from "svelte";
import { derived, type Readable, writable } from "svelte/store";
import { HttpClient } from "../client/client";
import type { ClientConfig } from "../client/types";
import { createSequencedRunner, runTrackedMutation } from "../internal/async_runner";

const VERTEBRA_KEY = Symbol("vertebra");

export interface SvelteQueryState<TOutput> {
  data: TOutput | null;
  loading: boolean;
  error: Error | null;
}

export interface SvelteQueryResult<TOutput> extends Readable<SvelteQueryState<TOutput>> {
  data: Readable<TOutput | null>;
  loading: Readable<boolean>;
  error: Readable<Error | null>;
  refresh: () => Promise<void>;
}

export interface SvelteMutationState {
  loading: boolean;
  error: Error | null;
}

export interface SvelteMutationResult<TInput, TOutput> extends Readable<SvelteMutationState> {
  loading: Readable<boolean>;
  error: Readable<Error | null>;
  mutate: (input?: TInput) => Promise<TOutput>;
}

export function setClient(config: ClientConfig): HttpClient {
  const client = new HttpClient(config);
  setContext(VERTEBRA_KEY, client);
  return client;
}

export function getClient(): HttpClient {
  const client = getContext<HttpClient>(VERTEBRA_KEY);
  if (!client) {
    throw new Error("getClient must be used after setClient");
  }
  return client;
}

/**
 * Svelte store-based query.
 */
export function createQuery<TInput = unknown, TOutput = unknown>(
  method: string,
  path: string,
  input?: TInput | (() => TInput),
  options: { enabled?: boolean | (() => boolean) } = {},
): SvelteQueryResult<TOutput> {
  const client = getClient();
  const state = writable<SvelteQueryState<TOutput>>({
    data: null,
    loading: false,
    error: null,
  });
  const resolveInput = (): TInput | undefined => (typeof input === "function" ? (input as () => TInput)() : input);
  const isEnabled = (): boolean =>
    options.enabled === undefined ? true : typeof options.enabled === "function" ? options.enabled() : options.enabled;

  const refresh = createSequencedRunner<TOutput>(
    () => client.call<TInput, TOutput>(method, path, resolveInput()),
    {
      onStart: () => state.update((prev) => ({ ...prev, loading: true, error: null })),
      onSuccess: (result) => state.set({ data: result, loading: false, error: null }),
      onError: (err) => state.update((prev) => ({ ...prev, loading: false, error: err })),
    },
    isEnabled,
  );

  void refresh();

  return {
    subscribe: state.subscribe,
    data: derived(state, (current) => current.data),
    loading: derived(state, (current) => current.loading),
    error: derived(state, (current) => current.error),
    refresh,
  };
}

/**
 * Svelte store-based mutation.
 */
export function createMutation<TInput = unknown, TOutput = unknown>(
  method: string,
  path: string,
): SvelteMutationResult<TInput, TOutput> {
  const client = getClient();
  const state = writable<SvelteMutationState>({
    loading: false,
    error: null,
  });

  const mutate = (input?: TInput): Promise<TOutput> =>
    runTrackedMutation(
      () => client.call<TInput, TOutput>(method, path, input),
      (loading, error) => state.set({ loading, error }),
    );

  return {
    subscribe: state.subscribe,
    loading: derived(state, (current) => current.loading),
    error: derived(state, (current) => current.error),
    mutate,
  };
}
