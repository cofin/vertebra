#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not installed"
  exit 1
fi

if ! rustup toolchain list | grep -q nightly; then
  echo "nightly toolchain not installed"
  exit 1
fi

cd "$(dirname "$0")/../src/rs"

export RUSTFLAGS="-Zsanitizer=thread"
export RUSTDOCFLAGS="-Zsanitizer=thread"

cargo +nightly test -Zbuild-std --target x86_64-unknown-linux-gnu
