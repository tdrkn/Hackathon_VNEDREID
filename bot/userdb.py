import os
from typing import List, Optional
import aiosqlite

DB_PATH = os.path.join(os.path.dirname(__file__), "user_data.db")


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS subscriptions (user_id INTEGER, ticker TEXT, UNIQUE(user_id, ticker))"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS tokens (user_id INTEGER PRIMARY KEY, token TEXT)"
        )
        await conn.commit()


async def add_subscription(user_id: int, ticker: str) -> List[str]:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO subscriptions (user_id, ticker) VALUES (?, ?)",
            (user_id, ticker.upper()),
        )
        await conn.commit()
        async with conn.execute(
            "SELECT ticker FROM subscriptions WHERE user_id=?",
            (user_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [row[0] for row in rows]


async def add_subscriptions(user_id: int, tickers) -> List[str]:
    tickers_up = {t.upper() for t in tickers if t}
    if not tickers_up:
        return await get_subscriptions(user_id)
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.executemany(
            "INSERT OR IGNORE INTO subscriptions (user_id, ticker) VALUES (?, ?)",
            [(user_id, t) for t in tickers_up],
        )
        await conn.commit()
        async with conn.execute(
            "SELECT ticker FROM subscriptions WHERE user_id=?",
            (user_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [row[0] for row in rows]


async def remove_subscription(user_id: int, ticker: str) -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "DELETE FROM subscriptions WHERE user_id=? AND ticker=?",
            (user_id, ticker.upper()),
        )
        await conn.commit()


async def get_subscriptions(user_id: int) -> List[str]:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            "SELECT ticker FROM subscriptions WHERE user_id=?",
            (user_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [row[0] for row in rows]


async def get_rankings() -> List[tuple]:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            "SELECT ticker, COUNT(*) as cnt FROM subscriptions GROUP BY ticker ORDER BY cnt DESC"
        ) as cur:
            rows = await cur.fetchall()
    return rows


async def load_token(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            "SELECT token FROM tokens WHERE user_id=?",
            (user_id,),
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else None


async def save_token(user_id: int, token: str) -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO tokens(user_id, token) VALUES (?, ?)",
            (user_id, token),
        )
        await conn.commit()
