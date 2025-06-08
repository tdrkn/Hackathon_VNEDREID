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
                company_name TEXT,
                news_type TEXT[],
                topics TEXT[],
                region TEXT,
                correlated_markets TEXT[],
                macro_sensitive BOOLEAN,
                likely_to_influence BOOLEAN,
                influence_reason TEXT,
                sentiment TEXT,
                summary_text TEXT,
                raw_text TEXT,
                title TEXT,
                link TEXT UNIQUE,
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

async def fetch_ai_recent(pool, hours: int = 24) -> List[Dict]:
    """Return recently analysed news from ai_news table."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ticker, company_name, news_type, topics, region,
                   correlated_markets, macro_sensitive, likely_to_influence,
                   influence_reason, sentiment, summary_text, raw_text,
                   title, link, published_at
            FROM ai_news
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
        news_type = a.get("news_type")

        if news_type is not None and not isinstance(news_type, list):
            news_type = [str(news_type)]
        topics = a.get("topics")
        if topics is not None and not isinstance(topics, list):
            topics = [str(topics)]
        corr = a.get("correlated_markets")
        if corr is not None and not isinstance(corr, list):

            corr = [str(corr)]

        records.append(
            (
                a.get("ticker"),
                a.get("company_name"),
                news_type,
                topics,
                a.get("region"),
                corr,
                a.get("macro_sensitive"),
                a.get("likely_to_influence"),
                a.get("influence_reason"),
                a.get("sentiment"),
                a.get("summary_text"),
                a.get("raw_text"),
                a.get("title"),
                a.get("link"),
                dt,
            )
        )
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO ai_news(
                ticker, company_name, news_type, topics, region,
                correlated_markets, macro_sensitive, likely_to_influence,
                influence_reason, sentiment, summary_text, raw_text,
                title, link, published_at
            )
            VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
            )
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
            SELECT ticker, company_name, news_type, topics, region,
                   correlated_markets, macro_sensitive, likely_to_influence,
                   influence_reason, sentiment, summary_text, raw_text,
                   title, link, published_at
            FROM ai_news
            WHERE ticker=$1
            ORDER BY published_at DESC
            LIMIT $2
            """,
            ticker.upper(),
            limit,
        )
        return [dict(r) for r in rows]
