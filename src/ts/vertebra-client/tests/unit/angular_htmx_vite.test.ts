import { EventEmitter } from "node:events";
import { readFileSync } from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";
import type { Plugin, ViteDevServer } from "vite";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as clientModule from "../../src/client/client";
import * as asyncRunnerModule from "../../src/internal/async_runner";
import { type RouteRegistry, routeUrl, wsUrl } from "../../src/router";

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

type MockReadonlySignal<T> = () => T;

interface MockWritableSignal<T> {
  (): T;
  set: (value: T) => void;
  update: (updater: (current: T) => T) => void;
  asReadonly: () => MockReadonlySignal<T>;
}

const angularRuntime = vi.hoisted(() => {
  let serviceInstance: unknown = null;
  let activeEffect: (() => void) | null = null;
  let trackingEnabled = true;

  return {
    reset() {
      serviceInstance = null;
      activeEffect = null;
      trackingEnabled = true;
    },
    setService(instance: unknown) {
      serviceInstance = instance;
    },
    Injectable() {
      return <T extends new (...args: never[]) => unknown>(ctor: T): T => ctor;
    },
    inject<T>(token: new () => T): T {
      if (!serviceInstance) {
        serviceInstance = new token();
      }
      return serviceInstance as T;
    },
    signal<T>(initialValue: T): MockWritableSignal<T> {
      let value = initialValue;
      const subscribers = new Set<() => void>();

      const read = (() => {
        if (trackingEnabled && activeEffect) {
          subscribers.add(activeEffect);
        }
        return value;
      }) as MockWritableSignal<T>;

      read.set = (nextValue: T) => {
        value = nextValue;
        for (const sub of [...subscribers]) {
          sub();
        }
      };

      read.update = (updater: (current: T) => T) => {
        read.set(updater(value));
      };

      read.asReadonly = (): MockReadonlySignal<T> => () => read();

      return read;
    },
    effect(fn: () => void) {
      const runner = () => {
        const prevEffect = activeEffect;
        activeEffect = runner;
        try {
          fn();
        } finally {
          activeEffect = prevEffect;
        }
      };
      runner();
      return { destroy: () => {} };
    },
    untracked<T>(fn: () => T): T {
      const prev = trackingEnabled;
      trackingEnabled = false;
      try {
        return fn();
      } finally {
        trackingEnabled = prev;
      }
    },
  };
});

vi.mock("@angular/core", () => ({
  Injectable: angularRuntime.Injectable,
  inject: angularRuntime.inject,
  signal: angularRuntime.signal,
  effect: angularRuntime.effect,
  untracked: angularRuntime.untracked,
}));

import * as angularCoreMock from "@angular/core";
import type * as AngularAdapterModule from "../../src/angular/index";
import { initHtmx } from "../../src/htmx/index";
import { vitePlugin } from "../../src/vite/index";

function loadAngularAdapter(): typeof AngularAdapterModule {
  const sourcePath = path.resolve(import.meta.dirname, "../../src/angular/index.ts");
  const source = readFileSync(sourcePath, "utf8");
  const transpiled = ts.transpileModule(source, {
    fileName: sourcePath,
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2022,
      experimentalDecorators: true,
    },
  });
  const moduleExports: Record<string, unknown> = {};
  const moduleObj = { exports: moduleExports };
  const localRequire = (specifier: string): unknown => {
    if (specifier === "@angular/core") {
      return angularCoreMock;
    }
    if (specifier === "../client/client") {
      return clientModule;
    }
    if (specifier === "../internal/async_runner") {
      return asyncRunnerModule;
    }
    throw new Error(`Unexpected import in angular adapter: ${specifier}`);
  };
  const executor = new Function("exports", "require", "module", transpiled.outputText) as (
    exportsObj: Record<string, unknown>,
    req: (specifier: string) => unknown,
    mod: { exports: Record<string, unknown> },
  ) => void;
  executor(moduleExports, localRequire, moduleObj);
  return moduleObj.exports as typeof AngularAdapterModule;
}

const { ClientService, injectMutation, injectQuery } = loadAngularAdapter();

function evaluateEmittedModule(code: string): {
  routes: RouteRegistry;
  websocketRoutes: RouteRegistry;
} {
  const transformed = `${code.replace(/export const /g, "const ")}\nresult = { routes, websocketRoutes };`;
  const context: { result?: { routes: RouteRegistry; websocketRoutes: RouteRegistry } } = {};
  vm.runInNewContext(transformed, context);
  if (!context.result) {
    throw new Error("Evaluated module did not produce route exports");
  }
  return context.result;
}

function callPluginHook<TArgs extends unknown[], TResult>(
  hook: ((...args: TArgs) => TResult) | { handler: (...args: TArgs) => TResult } | undefined,
  thisArg: unknown,
  ...args: TArgs
): TResult {
  if (!hook) {
    throw new Error("Expected Vite plugin hook to be defined");
  }
  const fn = typeof hook === "function" ? hook : hook.handler;
  return fn.apply(thisArg, args);
}

describe("Angular, HTMX, and Vite adapters", () => {
  beforeEach(() => {
    angularRuntime.reset();
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    angularRuntime.reset();
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  describe("Angular adapter", () => {
    it("manages ClientService lifecycle, injectQuery signals, reactive reloads, and injectMutation", async () => {
      const service = new ClientService();
      expect(() => service.getClient()).toThrow("ClientService must be initialized with init()");

      service.init({ baseURL: "http://api.test", retry: { maxAttempts: 0 } });
      angularRuntime.setService(service);
      expect(service.getClient()).toBeDefined();

      const mockFetch = vi.mocked(fetch);
      mockFetch
        .mockResolvedValueOnce(jsonResponse({ id: 1, name: "Alice" }))
        .mockResolvedValueOnce(jsonResponse({ id: 2, name: "Bob" }))
        .mockResolvedValueOnce(jsonResponse({ message: "Query failed" }, 500))
        .mockResolvedValueOnce(jsonResponse({ id: 3, name: "Created" }, 201))
        .mockResolvedValueOnce(jsonResponse({ message: "Bad payload" }, 400));

      const userId = angularRuntime.signal(1);
      const enabled = angularRuntime.signal(true);

      const query = injectQuery<{ id: number }, { id: number; name: string }>(
        routes["users.get"].method,
        routeUrl(routes, "users.get", { id: 1 }),
        () => ({ id: userId() }),
        { enabled },
      );

      await vi.waitFor(() => expect(query.loading()).toBe(false));
      expect(query.data()).toEqual({ id: 1, name: "Alice" });
      expect(query.error()).toBeNull();

      userId.set(2);
      await vi.waitFor(() => expect(query.loading()).toBe(false));
      expect(query.data()).toEqual({ id: 2, name: "Bob" });
      expect(mockFetch).toHaveBeenCalledTimes(2);

      await query.refresh();
      expect(query.loading()).toBe(false);
      expect(query.error()).toBeInstanceOf(Error);
      expect(query.error()?.message).toContain("Query failed");

      enabled.set(false);
      await query.refresh();
      expect(mockFetch).toHaveBeenCalledTimes(3);

      const mutation = injectMutation<{ name: string }, { id: number; name: string }>(
        routes["users.create"].method,
        routeUrl(routes, "users.create"),
      );
      const created = await mutation.mutate({ name: "Created" });
      expect(created).toEqual({ id: 3, name: "Created" });
      expect(mutation.loading()).toBe(false);
      expect(mutation.error()).toBeNull();
      expect(mockFetch).toHaveBeenLastCalledWith(
        "http://api.test/users",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({ name: "Created" }),
        }),
      );

      await expect(mutation.mutate({ name: "" })).rejects.toThrow("Bad payload");
      expect(mutation.loading()).toBe(false);
      expect(mutation.error()).toBeInstanceOf(Error);
    });
  });

  describe("HTMX adapter", () => {
    it("scopes headers and Content-Type for GET, JSON POST, multipart/form-data, and malformed events", () => {
      const doc = new EventTarget();
      vi.stubGlobal("document", doc);
      const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

      initHtmx({
        baseURL: "http://api.test",
        headers: { Authorization: "Bearer test-token-placeholder" },
      });

      const getHeaders: Record<string, string> = {};
      doc.dispatchEvent(
        new CustomEvent("htmx:configRequest", {
          detail: { verb: "GET", headers: getHeaders },
        }),
      );
      expect(getHeaders.Authorization).toBe("Bearer test-token-placeholder");
      expect(getHeaders["Content-Type"]).toBeUndefined();

      const postHeaders: Record<string, string> = {};
      doc.dispatchEvent(
        new CustomEvent("htmx:configRequest", {
          detail: { method: "post", headers: postHeaders },
        }),
      );
      expect(postHeaders.Authorization).toBe("Bearer test-token-placeholder");
      expect(postHeaders["Content-Type"]).toBe("application/json");

      const multipartDetailHeaders: Record<string, string> = {};
      doc.dispatchEvent(
        new CustomEvent("htmx:configRequest", {
          detail: { verb: "POST", encoding: "multipart/form-data", headers: multipartDetailHeaders },
        }),
      );
      expect(multipartDetailHeaders["Content-Type"]).toBeUndefined();

      const multipartHeaderHeaders: Record<string, string> = { "Content-Type": "multipart/form-data; boundary=abc" };
      doc.dispatchEvent(
        new CustomEvent("htmx:configRequest", {
          detail: { verb: "POST", headers: multipartHeaderHeaders },
        }),
      );
      expect(multipartHeaderHeaders["Content-Type"]).toBe("multipart/form-data; boundary=abc");

      const formAncestor = {
        getAttribute: (attr: string) => (attr === "hx-encoding" ? "multipart/form-data" : null),
      };
      const formChildElement = {
        getAttribute: () => null,
        closest: (selector: string) => (selector.includes("hx-encoding") ? formAncestor : null),
      };
      const eltHeaders: Record<string, string> = {};
      doc.dispatchEvent(
        new CustomEvent("htmx:configRequest", {
          detail: { verb: "POST", headers: eltHeaders, elt: formChildElement },
        }),
      );
      expect(eltHeaders.Authorization).toBe("Bearer test-token-placeholder");
      expect(eltHeaders["Content-Type"]).toBeUndefined();

      expect(() => doc.dispatchEvent(new Event("htmx:configRequest"))).not.toThrow();
      expect(() => doc.dispatchEvent(new CustomEvent("htmx:configRequest", { detail: null }))).not.toThrow();
      expect(() =>
        doc.dispatchEvent(new CustomEvent("htmx:configRequest", { detail: { verb: "POST", headers: null } })),
      ).not.toThrow();
      expect(() => doc.dispatchEvent(new Event("htmx:responseError"))).not.toThrow();
      expect(errorSpy).not.toHaveBeenCalled();

      const failureDetail = { xhr: { status: 502 }, pathInfo: { requestPath: routeUrl(routes, "users.create") } };
      doc.dispatchEvent(new CustomEvent("htmx:responseError", { detail: failureDetail }));
      expect(errorSpy).toHaveBeenCalledTimes(1);
      expect(errorSpy).toHaveBeenCalledWith("[vertebra:htmx] request failed:", failureDetail);
    });
  });

  describe("Vite plugin adapter", () => {
    it("resolves virtual module IDs and loads evaluable route exports from manifest", async () => {
      const mockFetch = vi.mocked(fetch);
      mockFetch.mockResolvedValueOnce(
        jsonResponse({
          routes: {
            "users.get": { method: "GET", route: "/users/{id}" },
            "users.create": { method: "POST", route: "/users" },
          },
          websocketRoutes: {
            chat: { route: "/ws/chat/{room}" },
          },
        }),
      );

      const plugin = vitePlugin({
        serverUrl: "http://localhost:8000/",
        virtualModuleName: "virtual:custom-routes",
      });

      const resolved = callPluginHook(plugin.resolveId as Plugin["resolveId"], {}, "virtual:custom-routes", undefined, {
        attributes: {},
        isEntry: false,
      });
      expect(resolved).toBe("\0virtual:custom-routes");
      expect(
        callPluginHook(plugin.resolveId as Plugin["resolveId"], {}, "./src/main.ts", undefined, {
          attributes: {},
          isEntry: false,
        }),
      ).toBeNull();

      const code = await callPluginHook(plugin.load as Plugin["load"], {}, "\0virtual:custom-routes");
      expect(typeof code).toBe("string");
      expect(mockFetch).toHaveBeenCalledWith("http://localhost:8000/__vertebra/manifest.json");

      const evaluated = evaluateEmittedModule(code as string);
      expect(Object.isFrozen(evaluated.routes)).toBe(true);
      expect(Object.isFrozen(evaluated.websocketRoutes)).toBe(true);
      expect(evaluated.routes["users.get"]).toEqual({ method: "GET", path: "/users/{id}" });
      expect(evaluated.routes["users.create"]).toEqual({ method: "POST", path: "/users" });
      expect(evaluated.websocketRoutes.chat).toEqual({ method: "WS", path: "/ws/chat/{room}" });

      expect(routeUrl(evaluated.routes, "users.get", { id: 42 })).toBe("/users/42");
      expect(wsUrl(evaluated.websocketRoutes, "chat", { room: "lobby" }, { baseUrl: "https://api.test" })).toBe(
        "wss://api.test/ws/chat/lobby",
      );

      expect(await callPluginHook(plugin.load as Plugin["load"], {}, "./src/main.ts")).toBeNull();
    });

    it("falls back to frozen empty route exports when manifest server is unreachable", async () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const mockFetch = vi.mocked(fetch);
      mockFetch.mockRejectedValueOnce(new TypeError("connect ECONNREFUSED"));

      const plugin = vitePlugin({ serverUrl: "http://localhost:9999" });
      const code = await callPluginHook(plugin.load as Plugin["load"], {}, "\0virtual:vertebra-types");

      expect(warnSpy).toHaveBeenCalledWith(
        "[vertebra] Failed to fetch manifest, using empty stubs:",
        expect.any(TypeError),
      );
      const evaluated = evaluateEmittedModule(code as string);
      expect(evaluated.routes).toEqual({});
      expect(evaluated.websocketRoutes).toEqual({});
      expect(Object.isFrozen(evaluated.routes)).toBe(true);
      expect(Object.isFrozen(evaluated.websocketRoutes)).toBe(true);
    });

    it("polls version on interval, cleans up timer on close, and filters handleHotUpdate to .py files", async () => {
      vi.useFakeTimers();
      vi.spyOn(console, "log").mockImplementation(() => {});

      const mockFetch = vi.mocked(fetch);
      mockFetch
        .mockResolvedValueOnce(new Response("v1", { status: 200 }))
        .mockResolvedValueOnce(new Response("v2", { status: 200 }));

      const fakeMod = { id: "\0virtual:vertebra-types" };
      const getModuleById = vi.fn().mockReturnValue(fakeMod);
      const invalidateModule = vi.fn();
      const wsSend = vi.fn();
      const httpServer = new EventEmitter();

      const fakeServer = {
        moduleGraph: { getModuleById, invalidateModule },
        ws: { send: wsSend },
        httpServer,
      } as unknown as ViteDevServer;

      const plugin = vitePlugin({ serverUrl: "http://localhost:8000", hmr: true });
      callPluginHook(plugin.configureServer as Plugin["configureServer"], {}, fakeServer);

      callPluginHook(
        plugin.handleHotUpdate as Plugin["handleHotUpdate"],
        {},
        {
          file: "/src/App.tsx",
          modules: [],
          read: async () => "",
          server: fakeServer,
          timestamp: 0,
          type: "update",
        },
      );
      expect(mockFetch).not.toHaveBeenCalled();

      callPluginHook(
        plugin.handleHotUpdate as Plugin["handleHotUpdate"],
        {},
        {
          file: "/app/routes.py",
          modules: [],
          read: async () => "",
          server: fakeServer,
          timestamp: 0,
          type: "update",
        },
      );
      await vi.waitFor(() => expect(mockFetch).toHaveBeenCalledTimes(1));
      expect(mockFetch).toHaveBeenCalledWith("http://localhost:8000/__vertebra/types/version");
      expect(wsSend).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1000);
      await vi.waitFor(() => expect(wsSend).toHaveBeenCalledTimes(1));
      expect(getModuleById).toHaveBeenCalledWith("\0virtual:vertebra-types");
      expect(invalidateModule).toHaveBeenCalledWith(fakeMod);
      expect(wsSend).toHaveBeenCalledWith({
        type: "custom",
        event: "vertebra:types-update",
        data: { version: "v2" },
      });

      httpServer.emit("close");
      await vi.advanceTimersByTimeAsync(5000);
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });
  });
});
