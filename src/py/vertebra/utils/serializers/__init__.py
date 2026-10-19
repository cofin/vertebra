"""Serialization utilities for Vertebra.

Aligned with vtb.
"""

from vertebra.utils.serializers._json import (
    JSONSerializer,
    decode_json,
    encode_json,
    encode_json_bytes,
)
from vertebra.utils.serializers._numpy import (
    numpy_array_dec_hook,
    numpy_array_enc_hook,
    numpy_array_predicate,
)
from vertebra.utils.serializers._schema import (
    SchemaSerializer,
    get_collection_serializer,
    get_serializer_metrics,
    reset_serializer_cache,
    schema_dump,
    serialize_collection,
)

__all__ = (
    "JSONSerializer",
    "SchemaSerializer",
    "decode_json",
    "encode_json",
    "encode_json_bytes",
    "get_collection_serializer",
    "get_serializer_metrics",
    "numpy_array_dec_hook",
    "numpy_array_enc_hook",
    "numpy_array_predicate",
    "reset_serializer_cache",
    "schema_dump",
    "serialize_collection",
)
