"""Tests for Vertebra Advanced Alchemy integration."""

import unittest.mock

import pytest

try:
    import advanced_alchemy  # noqa: F401

    import vertebra.extensions.sqlalchemy.postgres  # noqa: F401

    HAS_AA = True
except ImportError:
    HAS_AA = False


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
def test_aa_postgres_config() -> None:
    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAsyncConfig

    config = VertebraPostgresAsyncConfig(pool="test_pool")
    # Now defaults to postgresql+vertebra (the async one)
    assert str(config.get_engine().url) == "postgresql+vertebra://test_pool"


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
@pytest.mark.anyio
async def test_aa_repository() -> None:
    from advanced_alchemy.repository import SQLAlchemyAsyncRepository
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAsyncConfig

    class Base(DeclarativeBase):
        pass

    class User(Base):
        __tablename__ = "users"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()

    class UserRepository(SQLAlchemyAsyncRepository[User]):
        model_type = User

    config = VertebraPostgresAsyncConfig(pool="test_pool")
    # Mocking the engine execution would be complex, but we can verify the repository initializes
    # with our config's session.

    async with config.get_session() as session:
        repo = UserRepository(session=session)
        assert repo.session == session


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
@pytest.mark.anyio
async def test_aa_service() -> None:
    from advanced_alchemy.repository import SQLAlchemyAsyncRepository
    from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAsyncConfig

    class Base(DeclarativeBase):
        pass

    class User(Base):
        __tablename__ = "users"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()

    class UserRepository(SQLAlchemyAsyncRepository[User]):
        model_type = User

    class UserService(SQLAlchemyAsyncRepositoryService[User]):
        repository_type = UserRepository

    config = VertebraPostgresAsyncConfig(pool="test_pool")

    async with config.get_session() as session:
        service = UserService(session=session)
        assert service.repository.session == session


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
@pytest.mark.anyio
async def test_aa_storage_backend() -> None:
    from advanced_alchemy.types.file_object import FileObject

    from vertebra.extensions.advanced_alchemy._storage import VertebraStorageBackend

    backend = VertebraStorageBackend(key="test", pool="test_storage")

    # Mock client
    backend._client.storage_read = unittest.mock.AsyncMock(return_value=b"data")  # type: ignore[method-assign]
    backend._client.storage_write = unittest.mock.AsyncMock()  # type: ignore[method-assign]

    # Test get_content_async
    data = await backend.get_content_async("path/to/file")
    assert data == b"data"
    backend._client.storage_read.assert_called_with("test_storage", "path/to/file")

    # Test save_object_async with FileObject
    file_obj = FileObject(backend="test", filename="test.txt")
    result = await backend.save_object_async(file_obj, b"content")
    assert result.size == 7
    backend._client.storage_write.assert_called_with("test_storage", file_obj.path, b"content")
