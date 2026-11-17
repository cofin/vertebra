import { inject, type MaybeRefOrGetter, provide, ref, toValue, watch } from "vue";
import { HttpClient } from "../client/client";
import type { ClientConfig } from "../client/types";

const VERTEBRA_SYMBOL = Symbol("vertebra");

export function provideClient(config: ClientConfig): HttpClient {
  const client = new HttpClient(config);
  provide(VERTEBRA_SYMBOL, client);
  return client;
}

export function useClient(): HttpClient {
  const client = inject<HttpClient>(VERTEBRA_SYMBOL);
  if (!client) {
    throw new Error("useClient must be used after provideClient");
  }
  return client;
}

/**
 * Vue 3.5 composable for data fetching.
 * Support reactive input via Ref or Getter.
 */
export function useQuery<TInput = unknown, TOutput = unknown>(
  method: string,
  path: string,
  input?: MaybeRefOrGetter<TInput>,
  options: { enabled?: MaybeRefOrGetter<boolean> } = {},
) {
  const client = useClient();
  const data = ref<TOutput | null>(null);
  const loading = ref(false);
  const error = ref<Error | null>(null);
  let requestSeq = 0;

  const fetchData = async () => {
    if (!toValue(options.enabled ?? true)) return;

    const seq = ++requestSeq;
    loading.value = true;
    error.value = null;
    try {
      const result = await client.call<TInput, TOutput>(method, path, toValue(input));
      if (seq === requestSeq) {
        data.value = result;
      }
    } catch (err: unknown) {
      if (seq === requestSeq) {
        error.value = err instanceof Error ? err : new Error(String(err));
      }
    } finally {
      if (seq === requestSeq) {
        loading.value = false;
      }
    }
  };

  // Watch for input changes
  watch(() => toValue(input), fetchData, { immediate: true });
  watch(
    () => toValue(options.enabled),
    (enabled) => {
      if (enabled && !data.value) fetchData();
    },
  );

  return { data, loading, error, refresh: fetchData };
}

/**
 * Vue 3.5 mutation composable.
 */
export function useMutation<TInput = unknown, TOutput = unknown>(method: string, path: string) {
  const client = useClient();
  const loading = ref(false);
  const error = ref<Error | null>(null);

  const mutate = async (input?: TInput) => {
    loading.value = true;
    error.value = null;
    try {
      const result = await client.call<TInput, TOutput>(method, path, input);
      return result;
    } catch (err: unknown) {
      error.value = err instanceof Error ? err : new Error(String(err));
      throw err;
    } finally {
      loading.value = false;
    }
  };

  return { mutate, loading, error };
}
