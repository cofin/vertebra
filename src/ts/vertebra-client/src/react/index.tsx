import type React from "react";
import { createContext, use, useCallback, useContext, useMemo, useState } from "react";
import { VertebraClient } from "../client/client";
import type { VertebraConfig } from "../client/types";
import { QueryCache, queryCache } from "./cache";

const VertebraContext = createContext<VertebraClient | null>(null);

export interface VertebraProviderProps {
  config: VertebraConfig;
  children: React.ReactNode;
}

export function VertebraProvider({ config, children }: VertebraProviderProps) {
  const client = useMemo(() => new VertebraClient(config), [config]);
  return <VertebraContext.Provider value={client}>{children}</VertebraContext.Provider>;
}

export function useVertebraClient() {
  const client = useContext(VertebraContext);
  if (!client) {
    throw new Error("useVertebraClient must be used within a VertebraProvider");
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
  options: { timeout?: number; enabled?: boolean; staleTime?: number } = {},
) {
  const client = useVertebraClient();
  const { enabled = true, staleTime = 0 } = options;

  const promise = useMemo(() => {
    if (!enabled) return Promise.resolve(null);
    const key = QueryCache.stableKey(method, path, input);
    return queryCache.getOrCreate(key, staleTime, () => client.call<TInput, TOutput>(method, path, input, options));
  }, [client, method, path, enabled, staleTime, input, options]);

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
  const client = useVertebraClient();
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
