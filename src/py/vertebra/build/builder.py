"""Build orchestrator."""

import asyncio
from pathlib import Path

from vertebra.build.bundler import Bundler
from vertebra.build.config import BuildConfig


class Builder:
    """Orchestrates Vertebra build process."""

    def __init__(self, project_dir: Path) -> None:
        self.project_dir = project_dir
        self.config = BuildConfig.load(project_dir)

    async def generate_types(self) -> Path:
        """Generate TypeScript types from Python procedures."""
        # TODO: Implement type generation logic
        # This requires loading the application code
        return self.project_dir / "src" / "ts" / "client" / "types.ts"

    async def compile_engine(self, target: str) -> Path:
        """Compile Bun engine binary for target platform."""
        # TODO: Implement engine compilation
        # This requires invoking the build-cli.ts script
        return self.project_dir / "dist" / "engine" / "vertebra-engine"

    async def build_wheel(self) -> Path:
        """Build Python wheel with embedded engine."""
        # TODO: Implement wheel building
        return self.project_dir / "dist" / "vertebra-0.1.0-py3-none-any.whl"

    async def build_one_file(
        self, output: Path, target: str, profile: str, clean: bool = False
    ) -> None:
        """Build standalone single-file executable."""
        bundler = Bundler(
            project_dir=self.project_dir,
            output_dir=output,
            target=target,
            profile=profile,
            clean=clean,
        )
        await bundler.build()
