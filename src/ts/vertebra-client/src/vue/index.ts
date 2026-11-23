import { inject, type MaybeRefOrGetter, provide, ref, toValue, watch } from "vue";
import { HttpClient } from "../client/client";
import type { ClientConfig } from "../client/types";
import { createSequencedRunner, runTrackedMutation } from "../internal/async_runner";

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

  const fetchData = createSequencedRunner<TOutput>(
    () => client.call<TInput, TOutput>(method, path, toValue(input)),
    {
      onStart: () => {
        loading.value = true;
        error.value = null;
      },
      onSuccess: (result) => {
        data.value = result;
      },
      onError: (err) => {
        error.value = err;
      },
      onFinish: () => {
        loading.value = false;
      },
    },
    () => Boolean(toValue(options.enabled ?? true)),
  );

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

  const mutate = (input?: TInput) =>
    runTrackedMutation(
      () => client.call<TInput, TOutput>(method, path, input),
      (isLoading, err) => {
        loading.value = isLoading;
        error.value = err;
      },
    );

  return { mutate, loading, error };
}
