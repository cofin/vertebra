import { parseArgs } from "util";
import { mkdirSync } from "fs";
import { resolve } from "path";
import { buildEngine } from "./build-engine";

const { values } = parseArgs({
  args: Bun.argv,
  options: {
    target: { type: "string", default: "native" },
    outdir: { type: "string", default: "dist/engine" },
    profile: { type: "string", default: "modern" },
    minify: { type: "boolean", default: true },
    bytecode: { type: "boolean", default: true },
  },
  strict: true,
  allowPositionals: true,
});

// Ensure outdir exists
const outdir = resolve(process.cwd(), values.outdir as string);
mkdirSync(outdir, { recursive: true });

try {
  const path = await buildEngine({
    target: values.target as string,
    outdir,
    profile: values.profile as any,
    minify: values.minify as boolean,
    bytecode: values.bytecode as boolean,
  });
  console.log(`Built engine at: ${path}`);
} catch (error) {
  console.error(error);
  process.exit(1);
}
