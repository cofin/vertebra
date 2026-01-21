import { type JsonRpcRequest, type McpServer } from "../server";

/**
 * Starts the MCP server using Stdio transport.
 * Reads newline-delimited JSON from stdin and writes JSON to stdout.
 */
export async function startStdioServer(server: McpServer) {
  // Bun's stdin iterator reads chunks, but we need line-delimited JSON.
  // We'll buffer chunks and split by newline.
  let buffer = "";

  for await (const chunk of Bun.stdin.stream()) {
    const text = new TextDecoder().decode(chunk);
    buffer += text;

    let newlineIndex: number;
    // biome-ignore lint/suspicious/noAssignInExpressions: loop condition
    while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);

      if (!line) continue;

      try {
        const request = JSON.parse(line) as JsonRpcRequest;
        const response = await server.handle(request);
        
        // Write response followed by newline
        const output = JSON.stringify(response) + "\n";
        process.stdout.write(output);
      } catch (error) {
        console.error("Failed to parse/handle request:", error);
        // Write parse error
        const errorResponse = {
          jsonrpc: "2.0",
          id: null,
          error: { code: -32700, message: "Parse error" },
        };
        process.stdout.write(JSON.stringify(errorResponse) + "\n");
      }
    }
  }
}