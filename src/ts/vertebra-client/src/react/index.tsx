import type React from "react";
import { createContext, use, useCallback, useContext, useMemo, useState } from "react";
import { HttpClient } from "../client/client";
import type { ClientConfig } from "../client/types";
import { QueryCache, queryCache } from "./cache";

export const ClientContext = createContext<HttpClient | null>(null);

export interface ClientProviderProps {
  config: ClientConfig;
  children: React.ReactNode;
}

export function ClientProvider({ config, children }: ClientProviderProps) {
  const client = useMemo(() => new HttpClient(config), [config]);
  return <ClientContext.Provider value={client}>{children}</ClientContext.Provider>;
}

export function useClient(): HttpClient {
  const client = useContext(ClientContext);
  if (!client) {
    throw new Error("useClient must be used within a ClientProvider");
  }
  return client;
}

/**
 * React 19 compatible query hook with cache support.
 *
 * Uses Suspense-ready `use()` for data fetching. Cached results avoid
 * redundant fetches for the same key. In-flight requests are deduplicated.
 */
export function useQuery<TInput = any, TOutput = any>(
  method: string,
  path: string,
  input?: TInput,
  options?: { timeout?: number; enabled?: boolean; staleTime?: number },
) {
  const client = useClient();
  const enabled = options?.enabled ?? true;
  const staleTime = options?.staleTime ?? 0;
  const timeout = options?.timeout;

  const promise = useMemo(() => {
    if (!enabled) return Promise.resolve(null);
    const key = QueryCache.stableKey(method, path, input);
    const callOptions = timeout !== undefined ? { timeout } : undefined;
    return queryCache.getOrCreate(key, staleTime, () => client.call<TInput, TOutput>(method, path, input, callOptions));
  }, [client, method, path, enabled, staleTime, timeout, input]);

  const data = use(promise);

  return { data } as { data: TOutput | null };
}

/**
 * Mutation hook for data modifications.
 *
 * After a successful mutation, invalidates all query cache entries
 * to ensure fresh data on next render.
 */
export function useMutation<TInput = any, TOutput = any>(method: string, path: string) {
  const client = useClient();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const mutate = async (input?: TInput) => {
    setLoading(true);
    setError(null);
    try {
      const result = await client.call<TInput, TOutput>(method, path, input);
      queryCache.invalidate();
      return result;
    } catch (err: any) {
      setError(err);
      throw err;
    } finally {
      setLoading(false);
    }
  };

  return { mutate, loading, error };
}

/**
 * Hook to manually invalidate query cache entries.
 *
 * @param pattern Optional prefix pattern — only matching keys are invalidated.
 *                Omit to invalidate all entries.
 */
export function useInvalidateQueries() {
  return useCallback((pattern?: string) => {
    queryCache.invalidate(pattern);
  }, []);
}

export { QueryCache, queryCache };
