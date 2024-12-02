import asyncio
from typing import Any, Optional
from unittest import result
import asyncpg
from app.config import Config


class DBAdapter:
    def __init__(self, max_concurrent: int = 10):
        self.user = Config.POSTGRES_USER
        self.password = Config.POSTGRES_PASSWORD
        self.database = Config.POSTGRES_DATABASE
        self.host = Config.POSTGRES_HOST
        self.port = Config.POSTGRES_PORT
        self.pool: Optional[asyncpg.pool.Pool] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            user=self.user,
            password=self.password,
            database=self.database,
            host=self.host,
            port=self.port,
        )

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def fetch_one(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> Optional[dict[str, Any]]:
        if self.pool is None:
            await self.connect()
        assert self.pool
        async with self.pool.acquire() as conn:
            assert isinstance(conn, asyncpg.connection.Connection)
            row = await conn.fetchrow(query, *params.values() if params else [])
            return dict(row) if row else None

    async def fetch_all(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        if self.pool is None:
            await self.connect()
        assert self.pool
        async with self.pool.acquire() as conn:
            assert isinstance(conn, asyncpg.connection.Connection)
            rows = await conn.fetch(query, *params.values() if params else [])
            return [dict(row) for row in rows]

    async def execute(self, query: str, params: Optional[dict[str, Any]] = None) -> int:
        if self.pool is None:
            await self.connect()
        assert self.pool
        async with self.pool.acquire() as conn:
            assert isinstance(conn, asyncpg.connection.Connection)
            result = await conn.execute(query, *params.values() if params else [])
            return int(result.split(" ")[-1])

    async def execute_many(
        self, query: str, params_list: list[dict[str, Any]]
    ) -> list[int]:
        if self.pool is None:
            await self.connect()
        assert self.pool
        async with self.pool.acquire() as conn:
            assert isinstance(conn, asyncpg.connection.Connection)
            async with conn.transaction():
                tasks = [
                    conn.execute(query, *params.values()) for params in params_list
                ]
                results = await asyncio.gather(*tasks)
                return [int(result.split(" ")[-1]) for result in results]

    async def execute_many_with_semaphore(
        self, query: str, params_list: list[dict[str, Any]]
    ) -> list[int]:
        if self.pool is None:
            await self.connect()
        assert self.pool
        async with self.pool.acquire() as conn:
            assert isinstance(conn, asyncpg.connection.Connection)
            async with conn.transaction():
                tasks = [
                    self._execute_with_semaphore(conn, query, params)
                    for params in params_list
                ]
                results = await asyncio.gather(*tasks)
                return results

    async def _execute_with_semaphore(
        self, conn: asyncpg.connection.Connection, query: str, params: dict[str, Any]
    ):
        async with self.semaphore:
            result = await conn.execute(query, *params.values())
            return int(result.split(" ")[-1])
