import { type McpServer, type JsonRpcRequest } from "../server";

export class SseTransport {
  private sessions = new Map<string, ReadableStreamDefaultController>();

  constructor(private server: McpServer) {}

  handleGet(req: Request): Response {
    const id = crypto.randomUUID();
    
    const stream = new ReadableStream({
        start: (controller) => {
            this.sessions.set(id, controller);
            // Send endpoint event with session ID relative URL
            // Assuming the client will use this to post messages
            const msg = `event: endpoint\ndata: /mcp/message?sessionId=${id}\n\n`;
            controller.enqueue(new TextEncoder().encode(msg));
        },
        cancel: () => {
            this.sessions.delete(id);
        }
    });
    
    return new Response(stream, {
        headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*", // For dev
        }
    });
  }

  async handlePost(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const sessionId = url.searchParams.get("sessionId");
    
    if (!sessionId || !this.sessions.has(sessionId)) {
        return new Response("Session not found", { status: 404 });
    }
    
    try {
        const body = await req.json();
        const response = await this.server.handle(body as JsonRpcRequest);
        
        const controller = this.sessions.get(sessionId);
        if (controller) {
            const msg = `event: message\ndata: ${JSON.stringify(response)}\n\n`;
            controller.enqueue(new TextEncoder().encode(msg));
        }
        
        return new Response("Accepted", { status: 202 });
    } catch (e) {
        return new Response(String(e), { status: 500 });
    }
  }
}
