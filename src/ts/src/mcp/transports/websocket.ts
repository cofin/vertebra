import { type McpServer, type JsonRpcRequest } from "../server";
import { type ServerWebSocket } from "bun";

export class WebSocketTransport {
    constructor(private server: McpServer) {}

    async handleMessage(ws: ServerWebSocket<unknown>, message: string | Buffer) {
        try {
            const text = typeof message === "string" ? message : new TextDecoder().decode(message);
            const request = JSON.parse(text) as JsonRpcRequest;
            
            const response = await this.server.handle(request);
            ws.send(JSON.stringify(response));
        } catch (e) {
            ws.send(JSON.stringify({
                jsonrpc: "2.0",
                id: null,
                error: { code: -32700, message: "Parse error" }
            }));
        }
    }
}
