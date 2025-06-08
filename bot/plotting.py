import io
from typing import List, Dict

import matplotlib
matplotlib.use('Agg')  # headless backend
import matplotlib.pyplot as plt

def make_portfolio_chart(rows: List[Dict]) -> io.BytesIO | None:
    """Return a bar chart image for portfolio data as BytesIO."""
    tickers = []
    values = []
    for r in rows:
        ticker = r.get("ticker")
        value = r.get("value")
        if ticker and ticker not in ("-", "—") and isinstance(value, (int, float)):
            tickers.append(ticker)
            values.append(value)
    if not values:
        return None
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(tickers, values, color="skyblue")
    ax.set_title("Portfolio value by ticker")
    ax.set_xlabel("Ticker")
    ax.set_ylabel("Value")
    ax.tick_params(axis="x", rotation=45)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf


def make_price_history_chart(points: List[Dict]) -> io.BytesIO | None:
    """Return a line chart image for ticker price history."""
    if not points:
        return None
    dates = [p.get("date") for p in points]
    closes = [p.get("close") for p in points]
    if not closes:
        return None
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(dates, closes, marker="o", color="green")
    ax.set_title("Price history")
    ax.set_xlabel("Date")
    ax.set_ylabel("Close price")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf

