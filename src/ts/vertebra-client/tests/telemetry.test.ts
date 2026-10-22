import { describe, expect, it } from "vitest";
import { TraceContext } from "../src/telemetry";

describe("W3C Trace Context client propagation", () => {
  it("parses and formats valid traceparent headers", () => {
    const raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const parsed = TraceContext.parse(raw);
    expect(parsed).not.toBeNull();
    expect(parsed?.version).toBe("00");
    expect(parsed?.traceId).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
    expect(parsed?.spanId).toBe("00f067aa0ba902b7");
    if (parsed) {
      expect(parsed.format()).toBe(raw);
      expect(TraceContext.format(parsed)).toBe(raw);
    }
  });

  it("injects child traceparent headers preserving traceId", () => {
    const parent = TraceContext.parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    expect(parent).not.toBeNull();
    const headers = TraceContext.injectHeader({ Authorization: "Bearer tok" }, parent ?? undefined);
    const child = TraceContext.parse(headers.traceparent ?? "");
    expect(child?.traceId).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
    expect(child?.spanId).not.toBe("00f067aa0ba902b7");
  });
});
