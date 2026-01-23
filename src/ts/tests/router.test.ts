import { expect, test, describe } from "bun:test";
import { ShmRouter } from "../src/broker/shm";

describe("ShmRouter", () => {
  test("basic routing", () => {
    const router = new ShmRouter();
    try {
      router.insert("/hello", 1);
      router.insert("/echo", 2);
      router.insert("/user/:id", 3);

      expect(router.matchPath("/hello")).toBe(1);
      expect(router.matchPath("/echo")).toBe(2);
      expect(router.matchPath("/user/123")).toBe(3);
      expect(router.matchPath("/notfound")).toBeNull();
    } finally {
      router.close();
    }
  });
});