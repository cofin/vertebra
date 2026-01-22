"""SQLite-specific DBAPI."""

from vertebra.extensions.sqlalchemy._dbapi import *  # noqa: F403

paramstyle = "qmark"
sqlite_version_info = (3, 30, 0)
