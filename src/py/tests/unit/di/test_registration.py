from __future__ import annotations

import inspect

from vertebra.di import ProviderRegistry


class ExampleConfig:
    pass


class ExampleRepo:
    pass


class ExampleService:
    def __init__(self, repo: ExampleRepo) -> None:
        self.repo = repo


class ExampleClock:
    pass


def test_class_provider_auto_discovery() -> None:
    registry = ProviderRegistry()
    spec = registry.provide(ExampleService)

    assert spec.provides is ExampleService
    assert spec.factory is ExampleService
    assert spec.scope == "singleton"
    assert [dep.dependency for dep in spec.dependencies] == [ExampleRepo]
    assert registry.providers[ExampleService] is spec


def test_factory_provider_decorator_sync() -> None:
    registry = ProviderRegistry()

    @registry.provide
    def config() -> ExampleConfig:
        return ExampleConfig()

    spec = registry.providers[ExampleConfig]
    assert spec.factory is config
    assert spec.dependencies == ()


def test_factory_provider_decorator_async() -> None:
    registry = ProviderRegistry()

    @registry.provide(scope="request")
    async def clock() -> ExampleClock:
        return ExampleClock()

    spec = registry.providers[ExampleClock]
    assert spec.scope == "request"
    assert inspect.iscoroutinefunction(spec.factory)


def test_factory_dependencies_are_inferred() -> None:
    registry = ProviderRegistry()

    @registry.provide
    def service(repo: ExampleRepo) -> ExampleService:
        return ExampleService(repo)

    spec = registry.providers[ExampleService]
    assert [dep.dependency for dep in spec.dependencies] == [ExampleRepo]


def test_class_provider_scope_override() -> None:
    registry = ProviderRegistry()
    spec = registry.provide(ExampleService, scope="request")

    assert spec.scope == "request"
