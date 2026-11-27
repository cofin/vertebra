# Top-Level Package, Typing & Serializers

Top-level application and routing primitives in `vertebra`, base `msgspec.Struct` models and exceptions in `vertebra.typing`, and JSON and Msgpack serializers in `vertebra.utils.serializers`.

## Application & Routing

::: vertebra.framework._app.App
    options:
      heading_level: 3

::: vertebra.framework._app.HandlerInfo
    options:
      heading_level: 3

::: vertebra.framework._router.Controller
    options:
      heading_level: 3

::: vertebra.framework._router.Router
    options:
      heading_level: 3

::: vertebra.framework._router.Route
    options:
      heading_level: 3

## Base Schemas & Exceptions

::: vertebra.typing.BaseStruct
    options:
      heading_level: 3
      members: false

::: vertebra.typing.FrozenBaseStruct
    options:
      heading_level: 3
      members: false

::: vertebra.typing.VertebraError
    options:
      heading_level: 3
      members: false

## JSON & Msgpack Serializers

::: vertebra.utils.serializers._json.JSONSerializer
    options:
      heading_level: 3

::: vertebra.utils.serializers._msgpack.MsgPackSerializer
    options:
      heading_level: 3
