# Litecore

**The "Batteries-Included" Python Web Server & Bundler.**

Litecore is a high-performance web server implementation that fuses the raw speed of a Rust "Hyper-Engine" with the developer experience of a modern frontend toolchain.

## Features

*   **Hyper-Engine:** Built on `hyper` and `tokio`. Native HTTP/1, HTTP/2, and HTTP/3.
*   **RSGI Support:** Direct-dispatch protocol for maximum Python performance (bypassing ASGI overhead).
*   **Zero-Node Bundling:** Embeds `rolldown` (Rust Bundler) to build JS/TS assets without requiring a Node.js installation.
*   **Polyglot DX:** `litecore serve` automatically manages your Python app AND your frontend dev server (Vite/Rspack), proxying requests transparently.
*   **One-File Dist:** `litecore build` produces a single binary containing your App + Server + Bundled Assets.

## Architecture

*   **Runtime:** In-process Python embedding via `PyO3`.
*   **Concurrency:** Rust handles I/O; Python handles Business Logic.
*   **Plugins:** Native `StatePlugin` system for zero-cost integration with tools like `vertebral`.

## Roadmap

See `.agent/specs/litecore/plan.md` for the implementation plan.
