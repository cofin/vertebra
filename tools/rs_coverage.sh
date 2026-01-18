#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not installed"
  exit 1
fi

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov not installed"
  exit 1
fi

cd "$(dirname "$0")/../src/rs"

cargo llvm-cov --workspace --lcov --output-path ../../coverage/rust.lcov
