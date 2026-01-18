#!/usr/bin/env bash
set -euo pipefail

if ! command -v valgrind >/dev/null 2>&1; then
  echo "valgrind not installed"
  exit 1
fi

cd "$(dirname "$0")/../src/rs"

cargo test --no-run

for testbin in target/debug/deps/*-*; do
  if file "$testbin" | grep -q "ELF"; then
    echo "Running valgrind on $testbin"
    valgrind --leak-check=full --error-exitcode=1 "$testbin"
  fi
done
