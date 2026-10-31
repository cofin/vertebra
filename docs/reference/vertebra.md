# `vertebra` — Top-Level Package, Typing & Serializers

Top-level application entry points (`App`, `Controller`, `Router`, `Route`, route decorators), base `msgspec.Struct` classes (`vertebra.typing`), and centralized JSON/Msgpack serializers (`vertebra.utils.serializers`).

## Application & Routing Entry Points

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

## Base Schemas & Exceptions (`vertebra.typing`)

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

## JSON & Msgpack Serializers (`vertebra.utils.serializers`)

::: vertebra.utils.serializers._json.JSONSerializer
    options:
      heading_level: 3

::: vertebra.utils.serializers._msgpack.MsgpackCodec
    options:
      heading_level: 3
