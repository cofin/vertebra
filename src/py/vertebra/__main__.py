"""Entrypoint for ``python -m vertebra``."""

from vertebra.cli import cli

__all__ = ("cli",)

if __name__ == "__main__":
    cli()
