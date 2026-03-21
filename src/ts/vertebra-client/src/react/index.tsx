import type React from "react";
import { createContext, use, useContext, useMemo, useState } from "react";
import { VertebraClient } from "../client/client";
import type { VertebraConfig } from "../client/types";

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
 * React 19 compatible query hook.
 * Uses Suspense-ready 'use()' for data fetching.
 */
export function useQuery<TInput = any, TOutput = any>(
  method: string,
  path: string,
  input?: TInput,
  options: { timeout?: number; enabled?: boolean } = {},
) {
  const client = useVertebraClient();
  const { enabled = true } = options;

  // In a real implementation, we'd use a cache/resource manager here
  // to avoid redundant fetches and handle invalidation.
  const promise = useMemo(() => {
    if (!enabled) return Promise.resolve(null);
    return client.call<TInput, TOutput>(method, path, input, options);
  }, [client, method, path, input, options, enabled]);

  // React 19 'use()' unwraps the promise and integrates with Suspense
  const data = use(promise);

  return { data };
}

/**
 * Mutation hook for data modifications.
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
