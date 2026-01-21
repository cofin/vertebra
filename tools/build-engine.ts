import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

const targets = [
  "bun-linux-x64",
  "bun-linux-arm64",
  "bun-darwin-x64",
  "bun-darwin-arm64",
  "bun-windows-x64",
];

async function build() {
  const outDir = join(import.meta.dir, "..", "dist", "engine");
  if (!existsSync(outDir)) {
    mkdirSync(outDir, { recursive: true });
  }

  const entry = join(import.meta.dir, "..", "src", "ts", "src", "broker", "serve.ts");

  for (const target of targets) {
    console.log(`Building engine for ${target}...`);
    const outFile = join(outDir, `vertebra-engine-${target}${target.includes("windows") ? ".exe" : ""}`);
    
    const result = Bun.build({
      entrypoints: [entry],
      outdir: outDir,
      target: target as any,
      naming: `vertebra-engine-${target}.[ext]`,
      minify: true,
    });
    
    // Bun.build doesn't yet support standalone binary creation directly for all targets easily via API 
    // in the same way `bun build --compile` does for the current platform.
    // We'll use `bun build --compile` via spawn for now to get true standalone binaries.
    
    const compileResult = spawnSync("bun", [
      "build",
      "--compile",
      "--minify",
      "--target", target,
      entry,
      "--outfile", outFile
    ], { stdio: "inherit" });

    if (compileResult.status !== 0) {
      console.error(`Failed to build for ${target}`);
    } else {
      console.log(`Successfully built ${outFile}`);
    }
  }
}

build().catch(console.error);
