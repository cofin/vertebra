from abc import ABC, abstractmethod
from typing import Any, List

class BenchmarkRunner(ABC):
    @abstractmethod
    async def setup(self):
        """Initialize connection pool or connection."""
        pass

    @abstractmethod
    async def teardown(self):
        """Close connections."""
        pass

    @abstractmethod
    async def select_1(self):
        """Execute SELECT 1."""
        pass

    @abstractmethod
    async def pk_lookup(self, pk: int) -> Any:
        """Fetch a single row by primary key."""
        pass

    @abstractmethod
    async def large_fetch(self, limit: int) -> List[Any]:
        """Fetch a large number of rows."""
        pass
    
    @abstractmethod
    async def fetch_arrow(self, limit: int) -> Any:
        """Fetch rows as Arrow table (if supported)."""
        pass
