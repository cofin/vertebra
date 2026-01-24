export interface ToolSchema {
  name: string;
  description?: string;
  inputSchema?: unknown;
}

export interface ResourceSchema {
  uri: string;
  name: string;
  description?: string;
  mimeType?: string;
}

export interface RegistryData {
  tools: ToolSchema[];
  resources?: ResourceSchema[];
}

import { type FSWatcher, watch } from "node:fs";
import { readFile } from "node:fs/promises";

export class SchemaRegistry {
  private tools = new Map<string, ToolSchema>();
  private resources = new Map<string, ResourceSchema>();
  private watcher?: FSWatcher;

  async loadFromFile(path: string): Promise<void> {
    try {
      const content = await readFile(path, "utf-8");
      const data = JSON.parse(content) as RegistryData;

      this.tools.clear();
      if (Array.isArray(data.tools)) {
        for (const tool of data.tools) {
          this.tools.set(tool.name, tool);
        }
      }

      this.resources.clear();
      if (Array.isArray(data.resources)) {
        for (const res of data.resources) {
          this.resources.set(res.uri, res);
        }
      }
    } catch (error) {
      if (process.env.NODE_ENV !== "test") {
        console.error(`Failed to load schema from ${path}:`, error);
      }
    }
  }

  async watch(path: string): Promise<void> {
    await this.loadFromFile(path);
    try {
      this.watcher = watch(path, async (eventType) => {
        if (eventType === "change") {
          await this.loadFromFile(path);
        }
      });
    } catch (e) {}
  }

  getTool(name: string): ToolSchema | undefined {
    return this.tools.get(name);
  }

  getTools(): ToolSchema[] {
    return Array.from(this.tools.values());
  }

  getResource(uri: string): ResourceSchema | undefined {
    return this.resources.get(uri);
  }

  getResources(): ResourceSchema[] {
    return Array.from(this.resources.values());
  }

  close(): void {
    this.watcher?.close();
    this.watcher = undefined;
  }
}
