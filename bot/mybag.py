"""Utilities for fetching user portfolio from Tinkoff Invest."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import aiosqlite
from tinkoff.invest import Client, InstrumentIdType
from tinkoff.invest.exceptions import UnauthenticatedError


DB_PATH = os.path.join(os.path.dirname(__file__), "subscriptions.db")


# ------------------------------------------------------------------
# Helpers copied from the standalone script
# ------------------------------------------------------------------

def _q_to_float(q) -> float:
    return q.units + q.nano / 1e9


async def load_token(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute("SELECT token FROM tokens WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None


async def save_token(user_id: int, token: str) -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR REPLACE INTO tokens(user_id, token) VALUES (?, ?)",
            (user_id, token),
        )
        await conn.commit()


async def ensure_tokens_table() -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS tokens (user_id INTEGER PRIMARY KEY, token TEXT)"
        )
        await conn.commit()


def _make_resolver(instr):
    cache: Dict[str, Tuple[str, str]] = {}

    def _query(id_type: InstrumentIdType, id_value: str):
        try:
            data = instr.get_instrument_by(id_type=id_type, id=id_value).instrument
            return data.ticker, data.name
        except Exception:
            return None, None

    def resolve(uid: str, figi: str, itype: str, currency: str):
        if uid in cache:
            return cache[uid]

        if itype.lower() == "currency":
            res = (currency.upper(), currency.upper())
            cache[uid] = res
            return res

        ticker, name = _query(InstrumentIdType.INSTRUMENT_ID_TYPE_UID, uid)

        if not ticker:
            ticker, name = _query(InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, figi)

        if not ticker:
            ticker, name = "—", "Unknown instrument"

        cache[uid] = (ticker, name)
        return ticker, name

    return resolve


def _build_portfolio(token: str) -> str:
    try:
        with Client(token=token, app_name="tinvest_portfolio") as cli:
            accounts = cli.users.get_accounts().accounts
    except UnauthenticatedError:
        return "[AUTH ERROR] Токен отклонён."

    if not accounts:
        return "У этого токена нет брокерских счетов."

    account_id = accounts[0].id

    with Client(token=token, app_name="tinvest_portfolio") as cli:
        positions = cli.operations.get_portfolio(account_id=account_id).positions
        resolver = _make_resolver(cli.instruments)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = [f"Portfolio for account {account_id} — {ts}", "=" * 96]

        if not positions:
            lines.append("(Portfolio is empty)")
            return "\n".join(lines)

        header = (
            f"{'FIGI':<12} {'Ticker':<8} {'Name':<30} {'Qty':>10} "
            f"{'Currency':<8} {'Price':>14} {'Value':>14}"
        )
        lines.append(header)
        lines.append("-" * len(header))

        for pos in positions:
            figi = pos.figi
            qty = _q_to_float(pos.quantity)
            curr = pos.average_position_price.currency or "—"
            price = _q_to_float(pos.current_price)
            value = price * qty
            ticker, name = resolver(pos.instrument_uid, figi, pos.instrument_type, curr)

            lines.append(
                f"{figi:<12} {ticker:<8} {name:<30} {qty:10,.3f} {curr:<8} {price:14,.2f} {value:14,.2f}"
            )

        return "\n".join(lines)


async def get_portfolio_text(token: str) -> str:
    """Return portfolio table for given token."""
    return await asyncio.to_thread(_build_portfolio, token)
