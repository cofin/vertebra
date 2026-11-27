# vertebra-bundler

Oxc/Rolldown frontend bundler and QuickJS SSR extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._bundler` module. It powers `vtb js build` and Vertebra's server-side rendering. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[bundler]"
uv add "vertebra-core[bundler]"
```

## Features

- Bundles TypeScript, JSX/TSX, and CSS with Oxc and Rolldown. No Node.js build server required.
- Runs server-side rendering in an in-process QuickJS pool.
- Drives Vertebra's Inertia v3 and Vite integrations.

## Usage

```python
from vertebra.build import BundleConfig, Bundler

bundler = Bundler(
    BundleConfig(
        entry="frontend/src/main.tsx",
        outdir="static/dist",
        minify=True,
        sourcemap=True,
    )
)
result = bundler.build()
```

Or from the command line:

```bash
vtb js build
```

## Compatibility

- Python 3.12 to 3.15, including free-threaded builds.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Frontend & SSR guide](https://cofin.github.io/vertebra/usage/frontend-and-ssr/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-bundler)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
