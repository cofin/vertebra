import { expect, test, describe } from "bun:test";
import { ShmRouter } from "../src/broker/shm";

describe("ShmRouter", () => {
  test("basic routing", () => {
    const router = new ShmRouter();
    try {
      router.insert("/hello", 1);
      router.insert("/echo", 2);
      router.insert("/user/:id", 3);

      expect(router.matchPath("/hello")).toEqual({ id: 1, params: {} });
      expect(router.matchPath("/echo")).toEqual({ id: 2, params: {} });
      expect(router.matchPath("/user/123")).toEqual({ id: 3, params: { id: "123" } });
      expect(router.matchPath("/notfound")).toBeNull();
    } finally {
      router.close();
    }
  });
});
