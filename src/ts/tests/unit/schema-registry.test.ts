import { describe, expect, it, afterEach } from "bun:test";
import { SchemaRegistry } from "../../src/mcp/schema-registry";
import { writeFileSync, unlinkSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

const TEMP_FILE = join(tmpdir(), `mcp-schema-${Date.now()}.json`);

describe("SchemaRegistry", () => {
  afterEach(() => {
    if (existsSync(TEMP_FILE)) {
      unlinkSync(TEMP_FILE);
    }
  });

  it("should load schemas from file", async () => {
    const schemas = {
      tools: [
        {
          name: "test_tool",
          description: "Test description",
          inputSchema: { type: "object" }
        }
      ]
    };
    writeFileSync(TEMP_FILE, JSON.stringify(schemas));

    const registry = new SchemaRegistry();
    await registry.loadFromFile(TEMP_FILE);

    const tool = registry.getTool("test_tool");
    expect(tool).toBeDefined();
    expect(tool?.name).toBe("test_tool");
    expect(tool?.description).toBe("Test description");
  });

  it("should handle invalid JSON gracefully", async () => {
    writeFileSync(TEMP_FILE, "{ invalid json }");

    const registry = new SchemaRegistry();
    // Should not throw
    await registry.loadFromFile(TEMP_FILE);

    expect(registry.getTools()).toHaveLength(0);
  });

  // Flaky test warning: fs.watch is timing dependent
  it("should reload on file change", async () => {
    const initialSchemas = {
      tools: [{ name: "tool1", inputSchema: {} }]
    };
    writeFileSync(TEMP_FILE, JSON.stringify(initialSchemas));

    const registry = new SchemaRegistry();
    await registry.watch(TEMP_FILE);

    // Initial load check
    expect(registry.getTool("tool1")).toBeDefined();

    // Update file
    const newSchemas = {
      tools: [
        { name: "tool1", inputSchema: {} },
        { name: "tool2", inputSchema: {} }
      ]
    };

    // Add delay to ensure mtime differs enough for some FS watchers
    await new Promise(resolve => setTimeout(resolve, 100));

    writeFileSync(TEMP_FILE, JSON.stringify(newSchemas));

    // Wait for watcher to trigger
    await new Promise(resolve => setTimeout(resolve, 500));

    expect(registry.getTool("tool2")).toBeDefined();
    expect(registry.getTools()).toHaveLength(2);

    registry.close();
  });
});
