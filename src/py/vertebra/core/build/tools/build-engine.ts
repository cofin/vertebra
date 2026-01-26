import { existsSync } from "node:fs";
import { join } from "node:path";

type EngineSource = { root: string; entry: string };

function resolveEngineSource(): EngineSource {
  const override = process.env.VERTEBRA_ENGINE_SOURCE_DIR;
  if (override) {
    const entry = join(override, "src", "server", "index.ts");
    if (existsSync(entry)) {
      return { root: override, entry };
    }
  }

  const repoRoot = process.cwd();
  const repoEntry = join(repoRoot, "src", "ts", "src", "server", "index.ts");
  if (existsSync(repoEntry)) {
    return { root: join(repoRoot, "src", "ts"), entry: repoEntry };
  }

  throw new Error("Unable to locate Vertebra engine source entrypoint");
}

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
  const source = resolveEngineSource();

  const ext = options.target.includes("windows") ? ".exe" : "";
  const outfile = join(options.outdir, `vertebra-engine${ext}`);

  console.log(`Building engine for ${options.target} (${options.profile})...`);
  console.log(`Entry: ${source.entry}`);
  console.log(`Outfile: ${outfile}`);
  console.log(`Target: ${bunTarget}`);

  if (!existsSync(join(source.root, "node_modules"))) {
    console.log("Installing engine dependencies...");
    const install = Bun.spawn({
      cmd: ["bun", "install"],
      cwd: source.root,
      stdout: "inherit",
      stderr: "inherit",
    });
    const exitCode = await install.exited;
    if (exitCode !== 0) {
      throw new Error(`bun install failed with exit code ${exitCode}`);
    }
  }

  const result = await Bun.build({
    entrypoints: [source.entry],
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
  console.log(`Outputs: ${result.outputs.map((o) => o.path).join(", ")}`);

  if (!result.success) {
    throw new Error(`Build failed: ${result.logs.join("\n")}`);
  }

  return result.outputs[0].path;
}
