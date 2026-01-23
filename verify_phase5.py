
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAlchemyAsyncConfig
from vertebra.extensions.advanced_alchemy.mysql import VertebraMysqlAlchemyAsyncConfig
from vertebra.extensions.advanced_alchemy.sqlite import VertebraSqliteAlchemyAsyncConfig
from vertebra.extensions.advanced_alchemy._storage import VertebraStorageBackend
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from advanced_alchemy.repository import SQLAlchemyAsyncRepository
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService

# Register dialects
import vertebra.extensions.sqlalchemy.postgres
import vertebra.extensions.sqlalchemy.mysql
import vertebra.extensions.sqlalchemy.sqlite

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

def verify_configs():
    print("Verifying Advanced Alchemy Configs...")
    
    pg = VertebraPostgresAlchemyAsyncConfig(pool="main")
    assert str(pg.get_engine().url) == "postgresql+vertebra://main"
    print("Postgres config verified.")
    
    my = VertebraMysqlAlchemyAsyncConfig(pool="main")
    assert str(my.get_engine().url) == "mysql+vertebra://main"
    print("MySQL config verified.")
    
    sq = VertebraSqliteAlchemyAsyncConfig(pool="main")
    assert str(sq.get_engine().url) == "sqlite+vertebra://main"
    print("SQLite config verified.")

async def verify_patterns():
    print("\nVerifying Advanced Alchemy Repository/Service...")
    config = VertebraPostgresAlchemyAsyncConfig(pool="main")
    
    async with config.get_session() as session:
        repo = UserRepository(session=session)
        assert repo.session == session
        print("Repository initialization verified.")
        
        service = UserService(session=session)
        assert service.repository.session == session
        print("Service initialization verified.")

async def verify_storage():
    print("\nVerifying Storage Backend...")
    backend = VertebraStorageBackend(pool="test_storage")
    backend._client.storage_read = AsyncMock(return_value=b"test data")
    
    data = await backend.read_bytes("test/path")
    assert data == b"test data"
    print("Storage backend read verified.")

if __name__ == "__main__":
    verify_configs()
    asyncio.run(verify_patterns())
    asyncio.run(verify_storage())
    print("\nPhase 5 verification passed!")
