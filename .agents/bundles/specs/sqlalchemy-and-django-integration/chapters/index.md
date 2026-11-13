# Chapters: SQLAlchemy, Advanced Alchemy, and Django DB Integration (`sqlalchemy-and-django-integration`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-dbapi-hardening-and-dialect-base`](ch01-dbapi-hardening-and-dialect-base/spec.md) | `planned` | `3` | Harden `vertebra.db.dbapi` (explicit URL, zero stdlib `sqlite3`), engine-neutral `BaseDatabaseConfig` metadata, `_SyncDialectMixin`/`_AsyncDialectMixin`, and `types.py`. |
| [`ch02-sqlalchemy-core-relational-dialects`](ch02-sqlalchemy-core-relational-dialects/spec.md) | `planned` | `3` | Split `postgresql`, `sqlite`, `mysql`, `oracle`, and `mssql` `Sync<Engine>Dialect`/`Async<Engine>Dialect` onto SQLAlchemy built-ins. |
| [`ch03-sqlalchemy-analytical-cloud-dialects`](ch03-sqlalchemy-analytical-cloud-dialects/spec.md) | `planned` | `3` | Split `duckdb`, `bigquery`, `spanner`, and `spangres` onto upstream dialect bases, delete `dialects.py`, and repoint all 36 entry points. |
| [`ch04-alembic-dogpile-and-advanced-alchemy`](ch04-alembic-dogpile-and-advanced-alchemy/spec.md) | `planned` | `3` | Split `alembic/{impl,ops,hooks}.py`, register `dogpile.cache>=1.3.3` backends, delete `sqlalchemy/cache.py`, and wire Advanced Alchemy `1.12.0+`. |
| [`ch05-django-relational-backends`](ch05-django-relational-backends/spec.md) | `planned` | `2` | Rebase Django `6.0+` `postgresql`, `sqlite3`, `mysql`, and `oracle` `DatabaseWrapper` backends onto Django built-ins over `vertebra.db.dbapi`. |
| [`ch06-django-spanner-and-unsupported-cleanup`](ch06-django-spanner-and-unsupported-cleanup/spec.md) | `planned` | `2` | Subclass `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi` and delete unsupported `bigquery`, `duckdb`, and `mssql` Django DB backends. |
| [`ch07-sqlalchemy-django-docs-and-okf`](ch07-sqlalchemy-django-docs-and-okf/spec.md) | `planned` | `2` | Synchronize `docs/usage/{extensions,databases}.md`, `docs/reference/extensions.md`, `sqlspec-alchemy-and-dialects.md`, `litestar.md`, and `database-driver-limitations.md`. |
