from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict

from tinkoff.invest import Client, CandleInterval


def _q_to_float(q) -> float:
    return q.units + q.nano / 1e9


def _fetch_history(token: str, ticker: str, days: int) -> List[Dict]:
    with Client(token=token, app_name="tinvest_history") as cli:
        instruments = cli.instruments.find_instrument(query=ticker).instruments
        if not instruments:
            return []
        figi = instruments[0].figi
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        resp = cli.market_data.get_candles(
            figi=figi,
            from_=start,
            to=end,
            interval=CandleInterval.CANDLE_INTERVAL_DAY,
        )
        data = [
            {
                "date": c.time.date(),
                "open": _q_to_float(c.open),
                "high": _q_to_float(c.high),
                "low": _q_to_float(c.low),
                "close": _q_to_float(c.close),
            }
            for c in resp.candles
        ]
        return data


async def get_ticker_history(token: str, ticker: str, days: int = 30) -> List[Dict]:
    return await asyncio.to_thread(_fetch_history, token, ticker, days)
