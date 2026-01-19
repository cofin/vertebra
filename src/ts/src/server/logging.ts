export type LogLevel = "debug" | "info" | "warn" | "error";

type LogFields = Record<string, unknown>;

export function log(level: LogLevel, message: string, fields: LogFields = {}): void {
  const payload = {
    timestamp: new Date().toISOString(),
    level,
    component: "bun",
    message,
    ...fields,
  };
  if (level === "error") {
    console.error(JSON.stringify(payload));
  } else {
    console.log(JSON.stringify(payload));
  }
}
