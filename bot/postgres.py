import os
from datetime import datetime
from typing import List, Dict
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
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio (
                user_id BIGINT,
                figi TEXT,
                ticker TEXT,
                name TEXT,
                qty DOUBLE PRECISION,
                currency TEXT,
                price DOUBLE PRECISION,
                value DOUBLE PRECISION
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_news (
                id BIGSERIAL PRIMARY KEY,
                ticker TEXT,
                title TEXT,
                summary TEXT,
                link TEXT UNIQUE,
                published_at TIMESTAMPTZ,
                data JSONB
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


async def replace_portfolio(pool, user_id: int, rows: List[Dict]):
    """Replace portfolio entries for a user."""
    if not rows:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM portfolio WHERE user_id=$1", user_id)
        return 0

    records = [
        (
            user_id,
            r.get("figi"),
            r.get("ticker"),
            r.get("name"),
            r.get("qty"),
            r.get("currency"),
            r.get("price"),
            r.get("value"),
        )
        for r in rows
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM portfolio WHERE user_id=$1", user_id)
            await conn.executemany(
                """
                INSERT INTO portfolio(user_id, figi, ticker, name, qty, currency, price, value)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                records,
            )
    return len(records)


async def fetch_portfolio(pool, user_id: int) -> List[Dict]:
    """Return portfolio rows for a user."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT figi, ticker, name, qty, currency, price, value
            FROM portfolio
            WHERE user_id=$1
            ORDER BY name
            """,
            user_id,
        )
        return [dict(r) for r in rows]


async def insert_ai_articles(pool, articles):
    """Insert analysed articles into ai_news table."""
    if not articles:
        return 0
    records = []
    for a in articles:
        date = a.get("published_at")
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
        records.append(
            (
                a.get("ticker"),
                a.get("title"),
                a.get("summary_text"),
                a.get("link"),
                dt,
                a,
            )
        )
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO ai_news(ticker, title, summary, link, published_at, data)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb)
            ON CONFLICT (link) DO NOTHING
            """,
            records,
        )
    return len(records)


async def fetch_ai_by_ticker(pool, ticker: str, limit: int = 5) -> List[Dict]:
    """Return analysed news for a ticker."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT data FROM ai_news
            WHERE ticker=$1
            ORDER BY published_at DESC
            LIMIT $2
            """,
            ticker.upper(),
            limit,
        )
        return [dict(r["data"]) for r in rows]
