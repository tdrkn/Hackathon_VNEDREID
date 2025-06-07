import os
from datetime import datetime
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost/news")

async def init_pool():
    return await asyncpg.create_pool(DATABASE_URL)

async def ensure_schema(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news (
                id BIGSERIAL PRIMARY KEY,
                source TEXT,
                title TEXT,
                link TEXT UNIQUE,
                body TEXT,
                published_at TIMESTAMPTZ
            )
            """
        )

async def insert_articles(pool, articles):
    if not articles:
        return 0
    records = []
    for a in articles:
        date = a.get("date")
        dt = None
        if isinstance(date, str) and date:
            try:
                dt = datetime.fromisoformat(date)
            except ValueError:
                dt = datetime.utcnow()
        elif isinstance(date, datetime):
            dt = date
        else:
            dt = datetime.utcnow()
        records.append((a.get("source"), a.get("title"), a.get("link"), a.get("text", ""), dt))
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO news(source, title, link, body, published_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (link) DO NOTHING
            """,
            records,
        )
    return len(records)

async def fetch_recent(pool, hours=24):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT source, title, link, body, published_at
            FROM news
            WHERE published_at >= now() - ($1 || ' hours')::interval
            ORDER BY published_at DESC
            """,
            hours,
        )
        return [dict(r) for r in rows]

async def fetch_by_ticker(pool, ticker, limit=50):
    pattern = f"%{ticker.upper()}%"
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT source, title, link, body, published_at
            FROM news
            WHERE upper(title || ' ' || body) LIKE $1
            ORDER BY published_at DESC
            LIMIT $2
            """,
            pattern,
            limit,
        )
        return [dict(r) for r in rows]
