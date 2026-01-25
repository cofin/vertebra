from __future__ import annotations

from typing import get_args, get_origin

from vertebra.di import Inject, Provider, ProviderSpec, SCOPES, Scope


class ExampleService:
    pass


class ExampleProvider:
    provides = ExampleService
    scope: Scope = "singleton"

    def __call__(self) -> ExampleService:
        return ExampleService()


def test_inject_preserves_type_metadata() -> None:
    annotation = Inject[ExampleService]
    assert get_origin(annotation) is Inject
    assert get_args(annotation) == (ExampleService,)


def test_scope_values_are_expected() -> None:
    assert set(SCOPES) == {"singleton", "request", "transient"}


def test_provider_protocol_runtime_check() -> None:
    provider = ExampleProvider()
    assert isinstance(provider, Provider)


def test_provider_spec_implements_provider() -> None:
    provider = ProviderSpec(ExampleService, ExampleService)
    assert isinstance(provider, Provider)
    assert provider.scope == "singleton"
