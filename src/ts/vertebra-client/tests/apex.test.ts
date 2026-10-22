import { describe, expect, it } from "vitest";
import { type ApexGlobalNamespace, ApexOfflineOutbox, ApexRegion } from "../src/apex/index";

describe("ApexRegion, reactive islands, and offline sync outbox", () => {
  it("reads and writes APEX page items and triggers Dynamic Actions", () => {
    const store = new Map<string, string>([["P12_ORDER_ID", "45001"]]);
    const events: Array<{ name: string; data: unknown }> = [];
    const fakeApex: ApexGlobalNamespace = {
      item: (name: string) => ({
        getValue: () => store.get(name) ?? "",
        setValue: (val: string) => {
          store.set(name, val);
        },
      }),
      event: {
        trigger: (_target: unknown, eventName: string, data?: unknown) => {
          events.push({ name: eventName, data });
        },
      },
    };

    const bridge = new ApexRegion({
      regionId: "order_fulfillment_region",
      appId: 105,
      pageId: 12,
      sessionId: 999888,
      apexGlobal: fakeApex,
    });

    expect(bridge.readPageItem("P12_ORDER_ID")).toBe("45001");
    bridge.writePageItem("P12_ORDER_ID", "45002", false, { recordOffline: true, etag: "v1" });
    expect(bridge.readPageItem("P12_ORDER_ID")).toBe("45002");
    expect(bridge.outbox.size).toBe(1);

    const triggered = bridge.triggerRegionEvent("vertebra:order-updated", {
      orderId: 45002,
    });
    expect(triggered).toBe(true);
    expect(events).toHaveLength(1);
    expect(events[0]?.name).toBe("vertebra:order-updated");
  });

  it("parses <vertebra-island> attributes and HTML markup cleanly", () => {
    const desc = ApexRegion.parseIslands({
      "data-component": "ApexOrderGrid",
      "data-region-id": "orders_ig",
      "data-live-channel": "apex:orders:105",
      "data-bound-items": "P12_ORDER_ID,P12_STATUS",
      "data-props": JSON.stringify({ warehouseId: 204 }),
    });
    expect(desc.componentName).toBe("ApexOrderGrid");
    expect(desc.regionId).toBe("orders_ig");
    expect(desc.liveChannel).toBe("apex:orders:105");
    expect(desc.boundItems).toEqual(["P12_ORDER_ID", "P12_STATUS"]);
    expect(desc.props).toEqual({ warehouseId: 204 });

    const islands = ApexRegion.parseIslands(
      `<vertebra-island data-component="ApexOrderGrid" data-props='{"warehouseId":204}'></vertebra-island>`,
    );
    expect(islands).toHaveLength(1);
    expect(islands[0]?.componentName).toBe("ApexOrderGrid");
    expect(islands[0]?.props).toEqual({ warehouseId: 204 });
  });

  it("deduplicates offline item mutations and preserves HLC ordering in ApexOfflineOutbox", () => {
    const outbox = new ApexOfflineOutbox(4);
    outbox.enqueueItemMutation("orders_ig", "P12_STATUS", "DRAFT", { nowMs: 1000 });
    outbox.enqueueItemMutation("orders_ig", "P12_STATUS", "APPROVED", { nowMs: 1000 });
    outbox.enqueueRowMutation(
      "orders_ig",
      "duality_patch",
      { orderId: 45001, qty: 10 },
      { etag: "etag-1", nowMs: 1001 },
    );

    expect(outbox.size).toBe(2);
    const batch = outbox.drain();
    expect(batch).toHaveLength(2);
    expect(batch[0]?.operation).toBe("set_item");
    expect(batch[0]?.value).toBe("APPROVED");
    expect(batch[1]?.operation).toBe("duality_patch");
    expect(batch[1]?.hlcTimestamp).toBeGreaterThan(batch[0]?.hlcTimestamp ?? 0);
  });
});
