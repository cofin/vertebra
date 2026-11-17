import { get } from "svelte/store";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { nextTick, ref } from "vue";
import { type RouteRegistry, routeUrl } from "../../src/router";

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
}

function createDeferred<T>(): Deferred<T> {
  let resolveFn: (value: T) => void = () => {};
  let rejectFn: (reason?: unknown) => void = () => {};
  const promise = new Promise<T>((resolve, reject) => {
    resolveFn = resolve;
    rejectFn = reject;
  });
  return { promise, resolve: resolveFn, reject: rejectFn };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const routes = {
  "users.get": { method: "GET", path: "/users/{id}" },
  "users.create": { method: "POST", path: "/users" },
} as const satisfies RouteRegistry;

interface MockContext<T> {
  _currentValue: T;
  Provider: {
    _context: MockContext<T>;
  };
}

interface HookSlot {
  value: unknown;
  deps?: readonly unknown[];
  cleanup?: () => void;
}

interface PromiseState {
  status: "pending" | "fulfilled" | "rejected";
  value?: unknown;
  reason?: unknown;
}

interface HookInstance {
  slots: HookSlot[];
  reset(): void;
  run<T>(fn: () => T): T;
  renderWithSuspense<T>(fn: () => T): Promise<{ result: T; renderCount: number }>;
}

const reactRuntime = vi.hoisted(() => {
  const instances = new Set<HookInstance>();
  const promiseStates = new WeakMap<Promise<unknown>, PromiseState>();
  const syncListeners = new Set<() => void>();
  let activeSlots: HookSlot[] = [];
  let hookIndex = 0;

  function depsChanged(prev: readonly unknown[] | undefined, next: readonly unknown[] | undefined): boolean {
    if (!prev || !next || prev.length !== next.length) {
      return true;
    }
    for (let i = 0; i < prev.length; i++) {
      if (!Object.is(prev[i], next[i])) {
        return true;
      }
    }
    return false;
  }

  return {
    createInstance(): HookInstance {
      const slots: HookSlot[] = [];
      const instance: HookInstance = {
        slots,
        reset() {
          for (const slot of slots) {
            slot.cleanup?.();
          }
          slots.length = 0;
        },
        run<T>(fn: () => T): T {
          const prevSlots = activeSlots;
          const prevIndex = hookIndex;
          activeSlots = slots;
          hookIndex = 0;
          try {
            return fn();
          } finally {
            activeSlots = prevSlots;
            hookIndex = prevIndex;
          }
        },
        async renderWithSuspense<T>(fn: () => T): Promise<{ result: T; renderCount: number }> {
          let renderCount = 0;
          for (let attempt = 0; attempt < 15; attempt++) {
            renderCount++;
            try {
              const result = instance.run(fn);
              return { result, renderCount };
            } catch (thrown: unknown) {
              if (thrown instanceof Promise) {
                await thrown.catch(() => undefined);
                continue;
              }
              throw thrown;
            }
          }
          throw new Error("Exceeded maximum Suspense render attempts");
        },
      };
      instances.add(instance);
      return instance;
    },
    resetAll() {
      for (const instance of instances) {
        instance.reset();
      }
      instances.clear();
      syncListeners.clear();
      activeSlots = [];
      hookIndex = 0;
    },
    onSyncStoreChange(listener: () => void): () => void {
      syncListeners.add(listener);
      return () => {
        syncListeners.delete(listener);
      };
    },
    createContext<T>(defaultValue: T): MockContext<T> {
      const ctx: MockContext<T> = {
        _currentValue: defaultValue,
        Provider: null as unknown as MockContext<T>["Provider"],
      };
      ctx.Provider = { _context: ctx };
      return ctx;
    },
    useContext<T>(ctx: MockContext<T>): T {
      return ctx._currentValue;
    },
    useMemo<T>(factory: () => T, deps: readonly unknown[]): T {
      const idx = hookIndex++;
      const existing = activeSlots[idx];
      if (!existing || depsChanged(existing.deps, deps)) {
        const value = factory();
        activeSlots[idx] = { value, deps };
        return value;
      }
      return existing.value as T;
    },
    useCallback<T extends (...args: never[]) => unknown>(fn: T, deps: readonly unknown[]): T {
      const idx = hookIndex++;
      const existing = activeSlots[idx];
      if (!existing || depsChanged(existing.deps, deps)) {
        activeSlots[idx] = { value: fn, deps };
        return fn;
      }
      return existing.value as T;
    },
    useState<T>(initial: T | (() => T)): [T, (next: T | ((prev: T) => T)) => void] {
      const idx = hookIndex++;
      let existing = activeSlots[idx];
      if (!existing) {
        const initialValue = typeof initial === "function" ? (initial as () => T)() : initial;
        existing = { value: initialValue };
        activeSlots[idx] = existing;
      }
      const slot = existing;
      const setter = (next: T | ((prev: T) => T)) => {
        slot.value = typeof next === "function" ? (next as (prev: T) => T)(slot.value as T) : next;
      };
      return [slot.value as T, setter];
    },
    useSyncExternalStore<T>(
      subscribe: (onStoreChange: () => void) => () => void,
      getSnapshot: () => T,
      _getServerSnapshot?: () => T,
    ): T {
      const idx = hookIndex++;
      const existing = activeSlots[idx];
      if (!existing) {
        const unsub = subscribe(() => {
          for (const listener of syncListeners) {
            listener();
          }
        });
        activeSlots[idx] = { value: subscribe, cleanup: unsub };
      }
      return getSnapshot();
    },
    use<T>(promise: Promise<T>): T {
      let state = promiseStates.get(promise);
      if (!state) {
        const nextState: PromiseState = { status: "pending" };
        state = nextState;
        promiseStates.set(promise, nextState);
        promise.then(
          (val) => {
            nextState.status = "fulfilled";
            nextState.value = val;
          },
          (err: unknown) => {
            nextState.status = "rejected";
            nextState.reason = err;
          },
        );
      }
      if (state.status === "fulfilled") {
        return state.value as T;
      }
      if (state.status === "rejected") {
        throw state.reason;
      }
      throw promise;
    },
  };
});

vi.mock("react", () => ({
  createContext: reactRuntime.createContext,
  useContext: reactRuntime.useContext,
  useMemo: reactRuntime.useMemo,
  useCallback: reactRuntime.useCallback,
  useState: reactRuntime.useState,
  useSyncExternalStore: reactRuntime.useSyncExternalStore,
  use: reactRuntime.use,
}));

const vueContext = vi.hoisted(() => new Map<symbol, unknown>());

vi.mock("vue", async (importOriginal) => {
  const actual = await importOriginal<typeof import("vue")>();
  return {
    ...actual,
    provide: (key: symbol, value: unknown) => {
      vueContext.set(key, value);
    },
    inject: <T>(key: symbol): T | undefined => vueContext.get(key) as T | undefined,
  };
});

const svelteContext = vi.hoisted(() => new Map<symbol, unknown>());

vi.mock("svelte", () => ({
  setContext: (key: symbol, value: unknown) => {
    svelteContext.set(key, value);
  },
  getContext: <T>(key: symbol): T | undefined => svelteContext.get(key) as T | undefined,
}));

import * as reactAdapter from "../../src/react/index";
import * as svelteAdapter from "../../src/svelte/index";
import * as vueAdapter from "../../src/vue/index";

function withReactContext<T>(ctx: MockContext<T>, value: T, fn: () => Promise<void> | void): Promise<void> | void {
  const prev = ctx._currentValue;
  ctx._currentValue = value;
  try {
    const result = fn();
    if (result instanceof Promise) {
      return result.finally(() => {
        ctx._currentValue = prev;
      });
    }
    ctx._currentValue = prev;
    return result;
  } catch (err) {
    ctx._currentValue = prev;
    throw err;
  }
}

describe("React, Vue, and Svelte adapters with generated route registries", () => {
  beforeEach(() => {
    reactRuntime.resetAll();
    vueContext.clear();
    svelteContext.clear();
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    reactRuntime.resetAll();
    vueContext.clear();
    svelteContext.clear();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  describe("React adapter", () => {
    it("throws outside ClientProvider and fetches once at staleTime=0 without looping", async () => {
      expect(() => reactAdapter.useClient()).toThrow("useClient must be used within a ClientProvider");

      const mockFetch = vi.mocked(fetch);
      mockFetch.mockResolvedValue(jsonResponse({ id: 1, name: "Ada" }));

      const client = new (await import("../../src/client/client")).HttpClient({
        baseURL: "http://api.test",
        retry: { maxAttempts: 0 },
      });
      const cache = new reactAdapter.QueryCache();
      let syncNotifications = 0;
      const unsub = reactRuntime.onSyncStoreChange(() => {
        syncNotifications++;
      });

      const userPath = routeUrl(routes, "users.get", { id: 1 });
      const ctx = reactAdapter.ClientContext as unknown as MockContext<typeof client | null>;
      const cacheCtx = reactAdapter.QueryCacheContext as unknown as MockContext<reactAdapter.QueryCache>;
      const queryComponent = reactRuntime.createInstance();

      await withReactContext(ctx, client, async () => {
        await withReactContext(cacheCtx, cache, async () => {
          const first = await queryComponent.renderWithSuspense(() =>
            reactAdapter.useQuery<undefined, { id: number; name: string }>(
              routes["users.get"].method,
              userPath,
              undefined,
              { staleTime: 0 },
            ),
          );
          expect(first.result.data).toEqual({ id: 1, name: "Ada" });
          expect(first.renderCount).toBe(2);
          expect(mockFetch).toHaveBeenCalledTimes(1);
          expect(mockFetch).toHaveBeenCalledWith("http://api.test/users/1", expect.objectContaining({ method: "GET" }));

          const reRendered = await queryComponent.renderWithSuspense(() =>
            reactAdapter.useQuery<undefined, { id: number; name: string }>(
              routes["users.get"].method,
              userPath,
              undefined,
              { staleTime: 0 },
            ),
          );
          expect(reRendered.result.data).toEqual({ id: 1, name: "Ada" });
          expect(reRendered.renderCount).toBe(1);
          expect(mockFetch).toHaveBeenCalledTimes(1);
          expect(syncNotifications).toBe(1);
        });
      });

      unsub();
    });

    it("deduplicates concurrent QueryCache requests and invalidates on useMutation and useInvalidateQueries", async () => {
      const deferred = createDeferred<Response>();
      const mockFetch = vi.mocked(fetch);
      mockFetch.mockReturnValueOnce(deferred.promise);

      const client = new (await import("../../src/client/client")).HttpClient({
        baseURL: "http://api.test",
        retry: { maxAttempts: 0 },
      });
      const cache = new reactAdapter.QueryCache();
      const ctx = reactAdapter.ClientContext as unknown as MockContext<typeof client | null>;
      const cacheCtx = reactAdapter.QueryCacheContext as unknown as MockContext<reactAdapter.QueryCache>;

      const getUserPath = routeUrl(routes, "users.get", { id: 2 });
      const cacheKey = reactAdapter.QueryCache.stableKey(routes["users.get"].method, getUserPath, undefined);

      const p1 = cache.getOrCreate<{ id: number }>(cacheKey, 5000, () =>
        client.call(routes["users.get"].method, getUserPath),
      );
      const p2 = cache.getOrCreate<{ id: number }>(cacheKey, 5000, () =>
        client.call(routes["users.get"].method, getUserPath),
      );
      expect(mockFetch).toHaveBeenCalledTimes(1);

      deferred.resolve(jsonResponse({ id: 2 }));
      await expect(Promise.all([p1, p2])).resolves.toEqual([{ id: 2 }, { id: 2 }]);

      mockFetch
        .mockResolvedValueOnce(jsonResponse({ id: 3, created: true }, 201))
        .mockResolvedValueOnce(jsonResponse({ id: 2, updated: true }))
        .mockResolvedValueOnce(jsonResponse({ id: 2, refreshed: true }));

      const queryComponent = reactRuntime.createInstance();
      const mutationComponent = reactRuntime.createInstance();
      const invalidateComponent = reactRuntime.createInstance();

      await withReactContext(ctx, client, async () => {
        await withReactContext(cacheCtx, cache, async () => {
          const mutation = mutationComponent.run(() =>
            reactAdapter.useMutation<{ name: string }, { id: number; created: boolean }>(
              routes["users.create"].method,
              routeUrl(routes, "users.create"),
            ),
          );
          const created = await mutation.mutate({ name: "Grace" });
          expect(created).toEqual({ id: 3, created: true });
          expect(mockFetch).toHaveBeenNthCalledWith(
            2,
            "http://api.test/users",
            expect.objectContaining({
              method: "POST",
              body: JSON.stringify({ name: "Grace" }),
            }),
          );

          const afterMutation = await queryComponent.renderWithSuspense(() =>
            reactAdapter.useQuery<undefined, { id: number; updated: boolean }>(routes["users.get"].method, getUserPath),
          );
          expect(afterMutation.result.data).toEqual({ id: 2, updated: true });
          expect(mockFetch).toHaveBeenCalledTimes(3);

          const invalidate = invalidateComponent.run(() => reactAdapter.useInvalidateQueries());
          invalidate("GET:/users/2");

          const afterInvalidate = await queryComponent.renderWithSuspense(() =>
            reactAdapter.useQuery<undefined, { id: number; refreshed: boolean }>(
              routes["users.get"].method,
              getUserPath,
            ),
          );
          expect(afterInvalidate.result.data).toEqual({ id: 2, refreshed: true });
          expect(mockFetch).toHaveBeenCalledTimes(4);
        });
      });
    });
  });

  describe("Vue adapter", () => {
    it("manages client injection, useQuery lifecycle/refresh, and useMutation errors", async () => {
      expect(() => vueAdapter.useClient()).toThrow("useClient must be used after provideClient");

      const mockFetch = vi.mocked(fetch);
      mockFetch
        .mockResolvedValueOnce(jsonResponse({ id: 10, name: "Lin" }))
        .mockResolvedValueOnce(jsonResponse({ id: 10, name: "Lin v2" }))
        .mockResolvedValueOnce(jsonResponse({ id: 11, name: "Created" }, 201))
        .mockResolvedValueOnce(jsonResponse({ message: "Validation failed" }, 422));

      const client = vueAdapter.provideClient({
        baseURL: "http://api.test",
        retry: { maxAttempts: 0 },
      });
      expect(vueAdapter.useClient()).toBe(client);

      const enabled = ref(false);
      const userId = ref(10);
      const query = vueAdapter.useQuery<{ page: number }, { id: number; name: string }>(
        routes["users.get"].method,
        routeUrl(routes, "users.get", { id: userId.value }),
        () => ({ page: 1 }),
        { enabled },
      );

      expect(query.loading.value).toBe(false);
      expect(query.data.value).toBeNull();
      expect(mockFetch).not.toHaveBeenCalled();

      enabled.value = true;
      await nextTick();
      await vi.waitFor(() => expect(query.loading.value).toBe(false));
      expect(query.data.value).toEqual({ id: 10, name: "Lin" });
      expect(query.error.value).toBeNull();
      expect(mockFetch).toHaveBeenCalledWith(
        "http://api.test/users/10?page=1",
        expect.objectContaining({ method: "GET" }),
      );

      await query.refresh();
      expect(query.data.value).toEqual({ id: 10, name: "Lin v2" });
      expect(mockFetch).toHaveBeenCalledTimes(2);

      const mutation = vueAdapter.useMutation<{ name: string }, { id: number; name: string }>(
        routes["users.create"].method,
        routeUrl(routes, "users.create"),
      );
      const created = await mutation.mutate({ name: "Created" });
      expect(created).toEqual({ id: 11, name: "Created" });
      expect(mutation.loading.value).toBe(false);
      expect(mutation.error.value).toBeNull();

      await expect(mutation.mutate({ name: "" })).rejects.toThrow("Validation failed");
      expect(mutation.loading.value).toBe(false);
      expect(mutation.error.value).toBeInstanceOf(Error);
      expect(mutation.error.value?.message).toContain("Validation failed");
    });
  });

  describe("Svelte adapter", () => {
    it("drives svelte/store streams across loading -> data, error capture, and sequence cancellation without runes", async () => {
      expect(() => svelteAdapter.getClient()).toThrow("getClient must be used after setClient");

      const deferredInitial = createDeferred<Response>();
      const deferredFirst = createDeferred<Response>();
      const deferredSecond = createDeferred<Response>();
      const mockFetch = vi.mocked(fetch);
      mockFetch
        .mockReturnValueOnce(deferredInitial.promise)
        .mockReturnValueOnce(deferredFirst.promise)
        .mockReturnValueOnce(deferredSecond.promise)
        .mockResolvedValueOnce(
          new Response(JSON.stringify({ message: "Server down" }), {
            status: 500,
            headers: { "Content-Type": "application/json" },
          }),
        );

      const client = svelteAdapter.setClient({
        baseURL: "http://api.test",
        retry: { maxAttempts: 0 },
      });
      expect(svelteAdapter.getClient()).toBe(client);

      const query = svelteAdapter.createQuery<undefined, { id: number; version: string }>(
        routes["users.get"].method,
        routeUrl(routes, "users.get", { id: 7 }),
      );

      const emissions: Array<svelteAdapter.SvelteQueryState<{ id: number; version: string }>> = [];
      const unsub = query.subscribe((state) => {
        emissions.push({ ...state });
      });

      expect(get(query.loading)).toBe(true);
      expect(get(query.data)).toBeNull();

      deferredInitial.resolve(jsonResponse({ id: 7, version: "v1" }));
      await vi.waitFor(() => expect(get(query.loading)).toBe(false));
      expect(get(query.data)).toEqual({ id: 7, version: "v1" });
      expect(get(query.error)).toBeNull();
      expect(emissions).toEqual([
        { data: null, loading: true, error: null },
        { data: { id: 7, version: "v1" }, loading: false, error: null },
      ]);

      const firstRefresh = query.refresh();
      const secondRefresh = query.refresh();

      deferredSecond.resolve(jsonResponse({ id: 7, version: "v3-fresh" }));
      await secondRefresh;
      expect(get(query.data)).toEqual({ id: 7, version: "v3-fresh" });

      deferredFirst.resolve(jsonResponse({ id: 7, version: "v2-stale" }));
      await firstRefresh;
      expect(get(query.data)).toEqual({ id: 7, version: "v3-fresh" });

      await query.refresh();
      expect(get(query.loading)).toBe(false);
      expect(get(query.error)).toBeInstanceOf(Error);
      expect(get(query.error)?.message).toContain("Server down");

      unsub();
    });

    it("updates loading and error stores on createMutation", async () => {
      const mockFetch = vi.mocked(fetch);
      mockFetch
        .mockResolvedValueOnce(jsonResponse({ id: 9, name: "Svelte" }, 201))
        .mockResolvedValueOnce(jsonResponse({ message: "Forbidden" }, 403));

      svelteAdapter.setClient({
        baseURL: "http://api.test",
        retry: { maxAttempts: 0 },
      });

      const mutation = svelteAdapter.createMutation<{ name: string }, { id: number; name: string }>(
        routes["users.create"].method,
        routeUrl(routes, "users.create"),
      );
      const loadingStates: boolean[] = [];
      const unsub = mutation.loading.subscribe((val) => loadingStates.push(val));

      const created = await mutation.mutate({ name: "Svelte" });
      expect(created).toEqual({ id: 9, name: "Svelte" });
      expect(loadingStates).toEqual([false, true, false]);
      expect(get(mutation.error)).toBeNull();

      await expect(mutation.mutate({ name: "Denied" })).rejects.toThrow("Forbidden");
      expect(get(mutation.loading)).toBe(false);
      expect(get(mutation.error)).toBeInstanceOf(Error);

      unsub();
    });
  });
});
