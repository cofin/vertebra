import { McpServer } from "../src/ts/src/mcp/server";
import { StdioTransport } from "../src/ts/src/mcp/transports/stdio";
import { Readable, Writable } from "node:stream";

async function runBenchmark() {
  const server = new McpServer();
  server.registerTool({
    name: "noop",
    handler: async () => ({})
  });

  const iterations = 10000;
  const latencies: number[] = [];

  const input = new Readable({ read() {} });
  const output = new Writable({ write(c, e, cb) { cb(); } });
  const transport = new StdioTransport(input, output);

  // Wire up manually for direct bench
  transport.onmessage = async (msg) => {
    const start = performance.now();
    await server.handle(msg);
    latencies.push(performance.now() - start);
  };
  await transport.start();

  console.log(`Running ${iterations} iterations...`);

  for (let i = 0; i < iterations; i++) {
    input.push(JSON.stringify({ jsonrpc: "2.0", id: i, method: "tools/call", params: { name: "noop" } }) + "\n");
    // Yield to allow processing
    await new Promise(r => setTimeout(r, 0));
  }

  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(iterations * 0.5)];
  const p99 = latencies[Math.floor(iterations * 0.99)];

  console.log(`P50: ${p50.toFixed(3)}ms`);
  console.log(`P99: ${p99.toFixed(3)}ms`);
}

runBenchmark();
