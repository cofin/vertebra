from __future__ import annotations

import asyncio

import pytest

import vertebra.di as di
from vertebra.di import DependencyError, ProviderRegistry


class Config:
    pass


class Repo:
    def __init__(self, config: Config) -> None:
        self.config = config


class Service:
    def __init__(self, repo: Repo) -> None:
        self.repo = repo


class CycleA:
    def __init__(self, b: "CycleB") -> None:
        self.b = b


class CycleB:
    def __init__(self, a: CycleA) -> None:
        self.a = a


def test_compiler_resolves_dependencies_in_order() -> None:
    registry = ProviderRegistry()

    @registry.provide
    def config() -> Config:
        return Config()

    registry.provide(Repo)
    registry.provide(Service)

    compiler = registry.compile()
    runner = compiler.compile(Service)

    service = asyncio.run(runner())
    assert isinstance(service, Service)
    assert isinstance(service.repo, Repo)
    assert isinstance(service.repo.config, Config)


def test_compiler_handles_async_factories() -> None:
    registry = ProviderRegistry()

    @registry.provide
    async def config() -> Config:
        return Config()

    registry.provide(Repo)
    compiler = registry.compile()

    runner = compiler.compile(Repo)
    repo = asyncio.run(runner())
    assert isinstance(repo.config, Config)


def test_compiler_caches_runners() -> None:
    registry = ProviderRegistry()

    @registry.provide
    def config() -> Config:
        return Config()

    registry.provide(Repo)
    compiler = registry.compile()

    runner_a = compiler.compile(Repo)
    runner_b = compiler.compile(Repo)

    assert runner_a is runner_b


def test_compiler_detects_missing_dependency() -> None:
    registry = ProviderRegistry()
    registry.provide(Service)

    compiler = registry.compile()
    with pytest.raises(DependencyError):
        compiler.compile(Service)


def test_compiler_detects_cycles() -> None:
    registry = ProviderRegistry()
    registry.provide(CycleA)
    registry.provide(CycleB)

    compiler = registry.compile()
    with pytest.raises(DependencyError):
        compiler.compile(CycleA)


def test_compiled_runner_avoids_runtime_introspection(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ProviderRegistry()

    @registry.provide
    def config() -> Config:
        return Config()

    compiler = registry.compile()
    runner = compiler.compile(Config)

    def boom(*_: object, **__: object) -> None:
        raise AssertionError("runtime introspection should not occur")

    monkeypatch.setattr(di.inspect, "signature", boom)
    monkeypatch.setattr(di.inspect, "iscoroutinefunction", boom)

    result = asyncio.run(runner())
    assert isinstance(result, Config)
