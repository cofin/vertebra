"""Tests for Vertebra Advanced Alchemy integration."""

import sys
import unittest.mock

import pytest

# Mock native extension
mock_vertebra_native = unittest.mock.MagicMock()
sys.modules["vertebra._vertebra"] = mock_vertebra_native
mock_vertebra_native.ShmRegion = unittest.mock.MagicMock()
mock_vertebra_native.AsyncRing = unittest.mock.MagicMock()
mock_vertebra_native.SpscRing = unittest.mock.MagicMock()

try:
    import advanced_alchemy
    import vertebra.extensions.sqlalchemy.postgres  # noqa: F401

    HAS_AA = True
except ImportError:
    HAS_AA = False


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
def test_aa_postgres_config() -> None:
    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAlchemyAsyncConfig

    config = VertebraPostgresAlchemyAsyncConfig(pool="test_pool")
    # Now defaults to postgresql+vertebra (the async one)
    assert str(config.get_engine().url) == "postgresql+vertebra://test_pool"


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
@pytest.mark.anyio
async def test_aa_repository() -> None:
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
    from advanced_alchemy.repository import SQLAlchemyAsyncRepository
    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAlchemyAsyncConfig
    
    class Base(DeclarativeBase):
        pass
        
    class User(Base):
        __tablename__ = "users"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()
        
    class UserRepository(SQLAlchemyAsyncRepository[User]):
        model_type = User
        
    config = VertebraPostgresAlchemyAsyncConfig(pool="test_pool")
    # Mocking the engine execution would be complex, but we can verify the repository initializes
    # with our config's session.
    
    async with config.get_session() as session:
        repo = UserRepository(session=session)
        assert repo.session == session


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
@pytest.mark.anyio
async def test_aa_service() -> None:
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
    from advanced_alchemy.repository import SQLAlchemyAsyncRepository
    from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService
    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAlchemyAsyncConfig
    
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
        
    config = VertebraPostgresAlchemyAsyncConfig(pool="test_pool")
    
    async with config.get_session() as session:
        service = UserService(session=session)
        assert service.repository.session == session


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
@pytest.mark.anyio
async def test_aa_storage_backend() -> None:
    from vertebra.extensions.advanced_alchemy._storage import VertebraStorageBackend

    backend = VertebraStorageBackend(pool="test_storage")

    # Mock client
    backend._client.storage_read = unittest.mock.AsyncMock(return_value=b"data")  # type: ignore

    data = await backend.read_bytes("path")
    assert data == b"data"
    backend._client.storage_read.assert_called_with("test_storage", "path")
