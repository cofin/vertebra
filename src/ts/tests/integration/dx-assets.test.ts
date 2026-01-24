import { describe, expect, it } from "bun:test";
import { createRouter } from "../../src/server/router";

describe("DX Asset Serving", () => {
  it("should serve static assets in production mode", async () => {
    // Create a dummy file
    const staticDir = "/tmp/vertebra-test-assets";
    await Bun.write(`${staticDir}/test.txt`, "hello world");

    const router = createRouter({
      staticDir,
      assets: {}
    });

    const req = new Request("http://localhost/test.txt");
    const res = await router(req);

    expect(res).toBeDefined();
    if (res) {
        expect(res.status).toBe(200);
        expect(await res.text()).toBe("hello world");
    }
  });

  it("should proxy to dev server in dev mode", async () => {
    // Mock dev server
    const devServer = Bun.serve({
      port: 0,
      fetch(req) {
        return new Response("dev server content");
      }
    });

    const router = createRouter({
      assets: { devUrl: `http://localhost:${devServer.port}` }
    });

    const req = new Request("http://localhost/assets/main.js");
    const res = await router(req);

    expect(res).toBeDefined();
    if (res) {
        expect(res.status).toBe(200);
        expect(await res.text()).toBe("dev server content");
    }

    devServer.stop();
  });
});
