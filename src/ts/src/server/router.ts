import type { Server } from "bun";

export type RequestHandler = (
  request: Request,
  server?: Server<unknown>,
) => Response | Promise<Response | undefined> | void;

export interface RouterConfig {
  staticDir?: string;
  apiHandler?: RequestHandler;
}

function normalizePath(pathname: string): string {
  if (pathname.startsWith("/")) {
    return pathname.slice(1);
  }
  return pathname;
}

export function createRouter(config: RouterConfig): RequestHandler {
  const staticDir = config.staticDir;
  const apiHandler = config.apiHandler;

  return async (request: Request, server?: Server<unknown>) => {
    const url = new URL(request.url);
    if (staticDir && (request.method === "GET" || request.method === "HEAD")) {
      const path = normalizePath(url.pathname);
      const file = Bun.file(`${staticDir}/${path}`);
      if (await file.exists()) {
        return new Response(file, {
          headers: {
            "cache-control": "public, max-age=60",
          },
        });
      }
    }
    if (apiHandler) {
      const response = await apiHandler(request, server);
      if (response !== undefined) {
        return response;
      }
      return;
    }
    return new Response("Not Found", { status: 404 });
  };
}
