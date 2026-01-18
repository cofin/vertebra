export type RequestHandler = (request: Request) => Response | Promise<Response>;

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

  return async (request: Request) => {
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
      return apiHandler(request);
    }
    return new Response("Not Found", { status: 404 });
  };
}
