import { describe, expect, test } from "bun:test";
import { createRouter } from "../src/server/router";

describe("Router Static Mounts", () => {
  test("serves file from static mount", async () => {
    // Create a temporary file
    const tmpDir = `/tmp/vertebra-test-${Math.random()}`;
    const filePath = `${tmpDir}/test.txt`;
    await Bun.write(filePath, "content");

    const router = createRouter({
      staticMounts: {
        "/assets": tmpDir,
      },
    });

    // Request matching mount
    const req = new Request("http://localhost/assets/test.txt");
    const res = await router(req);

    expect(res).toBeDefined();
    expect(res?.status).toBe(200);
    expect(await res?.text()).toBe("content");
  });

  test("returns 404 for missing file in mount", async () => {
    const tmpDir = `/tmp/vertebra-test-404-${Math.random()}`;
    await Bun.write(`${tmpDir}/exists.txt`, "content");

    const router = createRouter({
      staticMounts: {
        "/assets": tmpDir,
      },
    });

    const req = new Request("http://localhost/assets/missing.txt");
    const res = await router(req);

    // Should fall through to API handler or return 404 if no handler
    // Since no apiHandler provided, it returns 404
    expect(res).toBeDefined();
    expect(res?.status).toBe(404);
  });

  test("supports dynamic mount updates", async () => {
    const tmpDir = `/tmp/vertebra-test-dyn-${Math.random()}`;
    await Bun.write(`${tmpDir}/dynamic.txt`, "dynamic");

    const router = createRouter({
      staticMounts: {},
    });

    // Initial request fails
    const req1 = new Request("http://localhost/dyn/dynamic.txt");
    const res1 = await router(req1);
    expect(res1?.status).toBe(404);

    // Update mounts
    router.updateMounts({ "/dyn": tmpDir });

    // Retry request
    const req2 = new Request("http://localhost/dyn/dynamic.txt");
    const res2 = await router(req2);
    expect(res2?.status).toBe(200);
    expect(await res2?.text()).toBe("dynamic");
  });
});
