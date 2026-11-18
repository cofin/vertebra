import { getContext, setContext } from "svelte";
import { derived, type Readable, writable } from "svelte/store";
import { HttpClient } from "../client/client";
import type { ClientConfig } from "../client/types";

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
  let requestSeq = 0;

  const resolveInput = (): TInput | undefined => {
    if (typeof input === "function") {
      return (input as () => TInput)();
    }
    return input;
  };

  const isEnabled = (): boolean => {
    if (options.enabled === undefined) {
      return true;
    }
    return typeof options.enabled === "function" ? options.enabled() : options.enabled;
  };

  async function refresh(): Promise<void> {
    if (!isEnabled()) {
      return;
    }

    const seq = ++requestSeq;
    state.update((prev) => ({ ...prev, loading: true, error: null }));
    try {
      const result = await client.call<TInput, TOutput>(method, path, resolveInput());
      if (seq === requestSeq) {
        state.set({ data: result, loading: false, error: null });
      }
    } catch (err: unknown) {
      if (seq === requestSeq) {
        const normalizedError = err instanceof Error ? err : new Error(String(err));
        state.update((prev) => ({ ...prev, loading: false, error: normalizedError }));
      }
    }
  }

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

  async function mutate(input?: TInput): Promise<TOutput> {
    state.set({ loading: true, error: null });
    try {
      const result = await client.call<TInput, TOutput>(method, path, input);
      state.set({ loading: false, error: null });
      return result;
    } catch (err: unknown) {
      const normalizedError = err instanceof Error ? err : new Error(String(err));
      state.set({ loading: false, error: normalizedError });
      throw err;
    }
  }

  return {
    subscribe: state.subscribe,
    loading: derived(state, (current) => current.loading),
    error: derived(state, (current) => current.error),
    mutate,
  };
}
