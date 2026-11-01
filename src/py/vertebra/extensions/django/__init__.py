"""First-class Django extension for Vertebra."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

default_app_config = "vertebra.extensions.django.apps.DjangoAppConfig"

if TYPE_CHECKING:
    from vertebra.extensions.django.apps import DjangoAppConfig
    from vertebra.extensions.django.cache import CacheBackend
    from vertebra.extensions.django.channels import ChannelLayer
    from vertebra.extensions.django.config import DjangoConfig
    from vertebra.extensions.django.db import (
        ArrowManager,
        ArrowModel,
        ArrowQuerySet,
        ArrowQuerySetMixin,
        BackendPoolerConfig,
        DatabaseWrapperMixin,
    )
    from vertebra.extensions.django.fastpath import (
        CompiledRoute,
        FastPath,
        MsgspecView,
        compile_fastpath_routes,
        dispatch_django_direct,
        vertebra_view,
    )
    from vertebra.extensions.django.mail import EmailBackend
    from vertebra.extensions.django.mcp import (
        MCPConfig,
        MCPRegistry,
        PromptController,
        SkillController,
        WebMCPConfig,
        WebMCPManifest,
        WebMCPToolDescriptor,
        attach_mcp_routes,
        build_webmcp_manifest,
        dispatch_mcp_request,
        mcp_prompt,
        mcp_resource,
        mcp_tool,
        render_webmcp_manifest_json,
    )
    from vertebra.extensions.django.offload import offload_middlewares
    from vertebra.extensions.django.openapi import (
        AsyncAPIDocsConfig,
        OpenAPIConfig,
        ScalarDocsConfig,
        ScalarSpecSource,
        SwaggerDocsConfig,
        attach_openapi_docs,
        extract_openapi_schema,
    )
    from vertebra.extensions.django.orm import (
        ORMBridge,
        model_to_struct,
        models_to_structs,
        rows_to_structs,
    )
    from vertebra.extensions.django.plugin import DjangoPlugin
    from vertebra.extensions.django.queues import QueuesBackend, TaskEnvelope
    from vertebra.extensions.django.router import (
        RouteCompiler,
        RouteEntry,
        convert_django_pattern,
    )
    from vertebra.extensions.django.security import JWTAuthGuard, RateLimitGuard, SecurityBridge
    from vertebra.extensions.django.storage import (
        DjangoStorage,
        GCSConfig,
        LocalConfig,
        MemoryConfig,
        ObjectStore,
        S3Config,
        StorageRegistry,
    )
    from vertebra.extensions.django.templates import TemplateBridge
    from vertebra.extensions.django.vite import ViteBridge, ViteConfig, attach_vite

__all__ = (
    "ArrowManager",
    "ArrowModel",
    "ArrowQuerySet",
    "ArrowQuerySetMixin",
    "AsyncAPIDocsConfig",
    "BackendPoolerConfig",
    "CacheBackend",
    "ChannelLayer",
    "CompiledRoute",
    "DatabaseWrapperMixin",
    "DjangoAppConfig",
    "DjangoConfig",
    "DjangoPlugin",
    "DjangoStorage",
    "EmailBackend",
    "FastPath",
    "GCSConfig",
    "JWTAuthGuard",
    "LocalConfig",
    "MCPConfig",
    "MCPRegistry",
    "MemoryConfig",
    "MsgspecView",
    "ORMBridge",
    "ObjectStore",
    "OpenAPIConfig",
    "PromptController",
    "QueuesBackend",
    "RateLimitGuard",
    "RouteCompiler",
    "RouteEntry",
    "S3Config",
    "ScalarDocsConfig",
    "ScalarSpecSource",
    "SecurityBridge",
    "SkillController",
    "StorageRegistry",
    "SwaggerDocsConfig",
    "TaskEnvelope",
    "TemplateBridge",
    "ViteBridge",
    "ViteConfig",
    "WebMCPConfig",
    "WebMCPManifest",
    "WebMCPToolDescriptor",
    "attach_mcp_routes",
    "attach_openapi_docs",
    "attach_vite",
    "build_webmcp_manifest",
    "compile_fastpath_routes",
    "convert_django_pattern",
    "default_app_config",
    "dispatch_django_direct",
    "dispatch_mcp_request",
    "extract_openapi_schema",
    "mcp_prompt",
    "mcp_resource",
    "mcp_tool",
    "model_to_struct",
    "models_to_structs",
    "offload_middlewares",
    "render_webmcp_manifest_json",
    "rows_to_structs",
    "vertebra_view",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "ArrowManager": ("vertebra.extensions.django.db", "ArrowManager"),
    "ArrowModel": ("vertebra.extensions.django.db", "ArrowModel"),
    "ArrowQuerySet": ("vertebra.extensions.django.db", "ArrowQuerySet"),
    "ArrowQuerySetMixin": ("vertebra.extensions.django.db", "ArrowQuerySetMixin"),
    "AsyncAPIDocsConfig": ("vertebra.extensions.django.openapi", "AsyncAPIDocsConfig"),
    "BackendPoolerConfig": ("vertebra.extensions.django.db", "BackendPoolerConfig"),
    "CacheBackend": ("vertebra.extensions.django.cache", "CacheBackend"),
    "ChannelLayer": ("vertebra.extensions.django.channels", "ChannelLayer"),
    "CompiledRoute": ("vertebra.extensions.django.fastpath", "CompiledRoute"),
    "DatabaseWrapperMixin": ("vertebra.extensions.django.db", "DatabaseWrapperMixin"),
    "DjangoAppConfig": ("vertebra.extensions.django.apps", "DjangoAppConfig"),
    "DjangoConfig": ("vertebra.extensions.django.config", "DjangoConfig"),
    "DjangoPlugin": ("vertebra.extensions.django.plugin", "DjangoPlugin"),
    "DjangoStorage": ("vertebra.extensions.django.storage", "DjangoStorage"),
    "EmailBackend": ("vertebra.extensions.django.mail", "EmailBackend"),
    "FastPath": ("vertebra.extensions.django.fastpath", "FastPath"),
    "GCSConfig": ("vertebra.extensions.django.storage", "GCSConfig"),
    "JWTAuthGuard": ("vertebra.extensions.django.security", "JWTAuthGuard"),
    "LocalConfig": ("vertebra.extensions.django.storage", "LocalConfig"),
    "MCPConfig": ("vertebra.extensions.django.mcp", "MCPConfig"),
    "MCPRegistry": ("vertebra.extensions.django.mcp", "MCPRegistry"),
    "MemoryConfig": ("vertebra.extensions.django.storage", "MemoryConfig"),
    "MsgspecView": ("vertebra.extensions.django.fastpath", "MsgspecView"),
    "ORMBridge": ("vertebra.extensions.django.orm", "ORMBridge"),
    "ObjectStore": ("vertebra.extensions.django.storage", "ObjectStore"),
    "OpenAPIConfig": ("vertebra.extensions.django.openapi", "OpenAPIConfig"),
    "PromptController": ("vertebra.extensions.django.mcp", "PromptController"),
    "QueuesBackend": ("vertebra.extensions.django.queues", "QueuesBackend"),
    "RateLimitGuard": ("vertebra.extensions.django.security", "RateLimitGuard"),
    "RouteCompiler": ("vertebra.extensions.django.router", "RouteCompiler"),
    "RouteEntry": ("vertebra.extensions.django.router", "RouteEntry"),
    "S3Config": ("vertebra.extensions.django.storage", "S3Config"),
    "ScalarDocsConfig": ("vertebra.extensions.django.openapi", "ScalarDocsConfig"),
    "ScalarSpecSource": ("vertebra.extensions.django.openapi", "ScalarSpecSource"),
    "SecurityBridge": ("vertebra.extensions.django.security", "SecurityBridge"),
    "SkillController": ("vertebra.extensions.django.mcp", "SkillController"),
    "StorageRegistry": ("vertebra.extensions.django.storage", "StorageRegistry"),
    "SwaggerDocsConfig": ("vertebra.extensions.django.openapi", "SwaggerDocsConfig"),
    "TaskEnvelope": ("vertebra.extensions.django.queues", "TaskEnvelope"),
    "TemplateBridge": ("vertebra.extensions.django.templates", "TemplateBridge"),
    "ViteBridge": ("vertebra.extensions.django.vite", "ViteBridge"),
    "ViteConfig": ("vertebra.extensions.django.vite", "ViteConfig"),
    "WebMCPConfig": ("vertebra.extensions.django.mcp", "WebMCPConfig"),
    "WebMCPManifest": ("vertebra.extensions.django.mcp", "WebMCPManifest"),
    "WebMCPToolDescriptor": ("vertebra.extensions.django.mcp", "WebMCPToolDescriptor"),
    "attach_mcp_routes": ("vertebra.extensions.django.mcp", "attach_mcp_routes"),
    "attach_openapi_docs": ("vertebra.extensions.django.openapi", "attach_openapi_docs"),
    "attach_vite": ("vertebra.extensions.django.vite", "attach_vite"),
    "build_webmcp_manifest": ("vertebra.extensions.django.mcp", "build_webmcp_manifest"),
    "compile_fastpath_routes": ("vertebra.extensions.django.fastpath", "compile_fastpath_routes"),
    "convert_django_pattern": ("vertebra.extensions.django.router", "convert_django_pattern"),
    "dispatch_django_direct": ("vertebra.extensions.django.fastpath", "dispatch_django_direct"),
    "dispatch_mcp_request": ("vertebra.extensions.django.mcp", "dispatch_mcp_request"),
    "extract_openapi_schema": ("vertebra.extensions.django.openapi", "extract_openapi_schema"),
    "mcp_prompt": ("vertebra.extensions.django.mcp", "mcp_prompt"),
    "mcp_resource": ("vertebra.extensions.django.mcp", "mcp_resource"),
    "mcp_tool": ("vertebra.extensions.django.mcp", "mcp_tool"),
    "model_to_struct": ("vertebra.extensions.django.orm", "model_to_struct"),
    "models_to_structs": ("vertebra.extensions.django.orm", "models_to_structs"),
    "offload_middlewares": ("vertebra.extensions.django.offload", "offload_middlewares"),
    "render_webmcp_manifest_json": ("vertebra.extensions.django.mcp", "render_webmcp_manifest_json"),
    "rows_to_structs": ("vertebra.extensions.django.orm", "rows_to_structs"),
    "vertebra_view": ("vertebra.extensions.django.fastpath", "vertebra_view"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
