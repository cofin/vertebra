import { inject, type MaybeRefOrGetter, provide, ref, toValue, watch } from "vue";
import { VertebraClient } from "../client/client";
import type { VertebraConfig } from "../client/types";

const VERTEBRA_SYMBOL = Symbol("vertebra");

export function provideVertebra(config: VertebraConfig) {
  const client = new VertebraClient(config);
  provide(VERTEBRA_SYMBOL, client);
  return client;
}

export function useVertebraClient(): VertebraClient {
  const client = inject<VertebraClient>(VERTEBRA_SYMBOL);
  if (!client) {
    throw new Error("useVertebraClient must be used after provideVertebra");
  }
  return client;
}

/**
 * Vue 3.5 composable for data fetching.
 * Support reactive input via Ref or Getter.
 */
export function useQuery<TInput = any, TOutput = any>(
  method: string,
  path: string,
  input?: MaybeRefOrGetter<TInput>,
  options: { enabled?: MaybeRefOrGetter<boolean> } = {},
) {
  const client = useVertebraClient();
  const data = ref<TOutput | null>(null);
  const loading = ref(false);
  const error = ref<Error | null>(null);

  const fetchData = async () => {
    if (!toValue(options.enabled ?? true)) return;

    loading.value = true;
    error.value = null;
    try {
      data.value = await client.call<TInput, TOutput>(method, path, toValue(input));
    } catch (err: any) {
      error.value = err;
    } finally {
      loading.value = false;
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
export function useMutation<TInput = any, TOutput = any>(method: string, path: string) {
  const client = useVertebraClient();
  const loading = ref(false);
  const error = ref<Error | null>(null);

  const mutate = async (input?: TInput) => {
    loading.value = true;
    error.value = null;
    try {
      const result = await client.call<TInput, TOutput>(method, path, input);
      return result;
    } catch (err: any) {
      error.value = err;
      throw err;
    } finally {
      loading.value = false;
    }
  };

  return { mutate, loading, error };
}
