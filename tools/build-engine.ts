import { join } from "node:path";

export interface BuildOptions {
  target: string;
  outdir: string;
  profile: "modern" | "baseline" | "native";
  minify: boolean;
  bytecode: boolean;
}

export function getBunTarget(target: string, profile: string): string {
  const baseTarget = {
    "linux-x64": "bun-linux-x64",
    "linux-arm64": "bun-linux-arm64",
    "darwin-x64": "bun-darwin-x64",
    "darwin-arm64": "bun-darwin-arm64",
    "windows-x64": "bun-windows-x64",
  }[target];

  if (!baseTarget) {
    if (target === "native") return "bun";
    throw new Error(`Unknown target: ${target}`);
  }

  if (profile === "baseline" && target.includes("x64")) {
    return `${baseTarget}-baseline`;
  }

  return baseTarget;
}

export async function buildEngine(options: BuildOptions): Promise<string> {
  const bunTarget = getBunTarget(options.target, options.profile);
  const entry = join(import.meta.dir, "..", "src", "ts", "src", "server", "index.ts");
  
  // Extension handling for Windows
  const ext = options.target.includes("windows") ? ".exe" : "";
  const outfile = join(options.outdir, `vertebra-engine${ext}`);

  console.log(`Building engine for ${options.target} (${options.profile})...`);
  console.log(`Entry: ${entry}`);
  console.log(`Outfile: ${outfile}`);
  console.log(`Target: ${bunTarget}`);

  const result = await Bun.build({
    entrypoints: [entry],
    compile: true,
    outfile,
    target: bunTarget as any,
    minify: options.minify,
    bytecode: options.bytecode,
    define: {
      "process.env.NODE_ENV": '"production"',
    },
  });

  console.log(`Success: ${result.success}`);
  console.log(`Outputs: ${result.outputs.map(o => o.path).join(", ")}`);

  if (!result.success) {
    throw new Error(`Build failed: ${result.logs.join("\n")}`);
  }

  return result.outputs[0].path;
}