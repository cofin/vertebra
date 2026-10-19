import { getContext, setContext } from "svelte";
import { HttpClient } from "../client/client";
import type { ClientConfig } from "../client/types";

const VERTEBRA_KEY = Symbol("vertebra");

export function setVertebraClient(config: ClientConfig): HttpClient {
  const client = new HttpClient(config);
  setContext(VERTEBRA_KEY, client);
  return client;
}

export function getVertebraClient(): HttpClient {
  const client = getContext<HttpClient>(VERTEBRA_KEY);
  if (!client) {
    throw new Error("getVertebraClient must be used after setVertebraClient");
  }
  return client;
}

/**
 * Svelte 5 Rune-based query.
 */
export function createQuery<TInput = any, TOutput = any>(
  method: string,
  path: string,
  getInput: () => TInput,
  options: { enabled?: () => boolean } = {},
) {
  const client = getVertebraClient();

  let data = $state<TOutput | null>(null);
  let loading = $state(false);
  let error = $state<Error | null>(null);

  async function fetch() {
    if (options.enabled && !options.enabled()) return;

    loading = true;
    error = null;
    try {
      data = await client.call<TInput, TOutput>(method, path, getInput());
    } catch (err: any) {
      error = err;
    } finally {
      loading = false;
    }
  }

  // Svelte 5 effect for reactive re-fetching
  $effect(() => {
    fetch();
  });

  return {
    get data() {
      return data;
    },
    get loading() {
      return loading;
    },
    get error() {
      return error;
    },
    refresh: fetch,
  };
}

/**
 * Svelte 5 mutation.
 */
export function createMutation<TInput = any, TOutput = any>(method: string, path: string) {
  const client = getVertebraClient();

  let loading = $state(false);
  let error = $state<Error | null>(null);

  async function mutate(input?: TInput) {
    loading = true;
    error = null;
    try {
      return await client.call<TInput, TOutput>(method, path, input);
    } catch (err: any) {
      error = err;
      throw err;
    } finally {
      loading = false;
    }
  }

  return {
    mutate,
    get loading() {
      return loading;
    },
    get error() {
      return error;
    },
  };
}
