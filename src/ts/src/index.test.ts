import { describe, expect, it } from "bun:test";
import { VERSION } from "./index";

describe("VERSION", () => {
  it("matches the package version", () => {
    expect(VERSION).toBe("0.1.0");
  });
});
