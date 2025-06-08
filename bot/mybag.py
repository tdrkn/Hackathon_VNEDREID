"""Utilities for fetching user portfolio from Tinkoff Invest."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List, Dict as TDict

from tinkoff.invest import Client, InstrumentIdType
from tinkoff.invest.exceptions import UnauthenticatedError


from .userdb import load_token, save_token


# ------------------------------------------------------------------
# Helpers copied from the standalone script
# ------------------------------------------------------------------

def _q_to_float(q) -> float:
    return q.units + q.nano / 1e9




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


def _collect_portfolio(token: str) -> Tuple[str, List[TDict]]:
    try:
        with Client(token=token, app_name="tinvest_portfolio") as cli:
            accounts = cli.users.get_accounts().accounts
    except UnauthenticatedError:
        return "[AUTH ERROR] Токен отклонён.", []

    if not accounts:
        return "У этого токена нет брокерских счетов.", []

    account_id = accounts[0].id

    rows: List[TDict] = []
    with Client(token=token, app_name="tinvest_portfolio") as cli:
        positions = cli.operations.get_portfolio(account_id=account_id).positions
        resolver = _make_resolver(cli.instruments)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = [f"Portfolio for account {account_id} — {ts}", "=" * 96]

        if not positions:
            lines.append("(Portfolio is empty)")
            return "\n".join(lines), rows

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
            rows.append(
                {
                    "figi": figi,
                    "ticker": ticker,
                    "name": name,
                    "qty": float(qty),
                    "currency": curr,
                    "price": float(price),
                    "value": float(value),
                }
            )

        return "\n".join(lines), rows


async def get_portfolio_text(token: str) -> str:
    """Return portfolio table for given token."""
    text, _ = await asyncio.to_thread(_collect_portfolio, token)
    return text


async def get_portfolio_data(token: str) -> List[TDict]:
    """Return portfolio rows for given token."""
    _, rows = await asyncio.to_thread(_collect_portfolio, token)
    return rows
