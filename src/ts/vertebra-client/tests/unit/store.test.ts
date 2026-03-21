import { beforeEach, describe, expect, it, vi } from "vitest";
import { VertebraStore } from "../../src/store/index";

describe("VertebraStore (ADK Pattern)", () => {
  let store: VertebraStore;

  beforeEach(() => {
    // Mock localStorage
    const localStorageMock = (() => {
      let store: Record<string, string> = {};
      return {
        getItem: (key: string) => store[key] || null,
        setItem: (key: string, value: string) => {
          store[key] = value.toString();
        },
        removeItem: (key: string) => {
          delete store[key];
        },
        clear: () => {
          store = {};
        },
        key: (index: number) => Object.keys(store)[index] || null,
        get length() {
          return Object.keys(store).length;
        },
      };
    })();

    vi.stubGlobal("localStorage", localStorageMock);
    store = new VertebraStore();
  });

  it("sets and gets memory-only (temp:) values", () => {
    store.set("temp:query", "test");
    expect(store.get("temp:query")).toBe("test");
    expect(localStorage.getItem("vtb:temp:query")).toBeNull();
  });

  it("persists user: values to localStorage", () => {
    store.set("user:theme", "dark");
    expect(store.get("user:theme")).toBe("dark");
    expect(localStorage.getItem("vtb:user:theme")).toBe(JSON.stringify("dark"));
  });

  it("loads persistent values on initialization", () => {
    localStorage.setItem("vtb:user:lang", JSON.stringify("en"));
    const newStore = new VertebraStore();
    expect(newStore.get("user:lang")).toBe("en");
  });

  it("notifies subscribers on change", () => {
    const callback = vi.fn();
    store.subscribe(callback);
    store.set("any:key", "value");
    expect(callback).toHaveBeenCalledWith("any:key", "value");
  });

  it("deletes values and removes from disk", () => {
    store.set("user:data", "save");
    store.delete("user:data");
    expect(store.get("user:data")).toBeUndefined();
    expect(localStorage.getItem("vtb:user:data")).toBeNull();
  });
});
