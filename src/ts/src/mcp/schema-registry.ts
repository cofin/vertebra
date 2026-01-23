import { readFile } from "node:fs/promises";
import { watch, type FSWatcher } from "node:fs";

export interface ToolSchema {
  name: string;
  description?: string;
  inputSchema?: unknown;
}

export interface RegistryData {
  tools: ToolSchema[];
}

export class SchemaRegistry {
  private tools = new Map<string, ToolSchema>();
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
    } catch (error) {
      // Gracefully handle errors (invalid JSON, file not found/locked)
      // In a real app we might want to emit an event
      if (process.env.NODE_ENV !== 'test') {
        console.error(`Failed to load schema from ${path}:`, error);
      }
    }
  }

  async watch(path: string): Promise<void> {
    // Initial load
    await this.loadFromFile(path);

    try {
        this.watcher = watch(path, async (eventType) => {
          if (eventType === "change") {
            // Simple debounce/retry could go here, but relying on eventual consistency for now
            await this.loadFromFile(path);
          }
        });
    } catch (e) {
        // Watch might fail if file doesn't exist
    }
  }

  getTool(name: string): ToolSchema | undefined {
    return this.tools.get(name);
  }

  getTools(): ToolSchema[] {
    return Array.from(this.tools.values());
  }

  close(): void {
    this.watcher?.close();
    this.watcher = undefined;
  }
}