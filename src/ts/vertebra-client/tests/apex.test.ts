import { describe, expect, it } from "vitest";
import { type ApexGlobalNamespace, parseVertebraIslandAttributes, VertebraApexRegionBridge } from "../src/apex/index";

describe("VertebraApexRegionBridge and APEX islands", () => {
  it("reads and writes APEX page items and triggers Dynamic Actions", () => {
    const store = new Map<string, string>([["P12_PO_ID", "45001"]]);
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

    const bridge = new VertebraApexRegionBridge({
      regionId: "po_approval_region",
      appId: 105,
      pageId: 12,
      sessionId: 999888,
      apexGlobal: fakeApex,
    });

    expect(bridge.readPageItem("P12_PO_ID")).toBe("45001");
    bridge.writePageItem("P12_PO_ID", "45002");
    expect(bridge.readPageItem("P12_PO_ID")).toBe("45002");

    const triggered = bridge.triggerRegionEvent("vertebra:approved", {
      poId: 45002,
    });
    expect(triggered).toBe(true);
    expect(events).toHaveLength(1);
    expect(events[0]?.name).toBe("vertebra:approved");
  });

  it("parses <vertebra-island> attributes cleanly", () => {
    const desc = parseVertebraIslandAttributes({
      "data-component": "EbsAgentChat",
      "data-props": JSON.stringify({ orgId: 204 }),
    });
    expect(desc.componentName).toBe("EbsAgentChat");
    expect(desc.props).toEqual({ orgId: 204 });
  });
});
