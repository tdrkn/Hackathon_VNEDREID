import io
from typing import List, Dict

import matplotlib
matplotlib.use('Agg')  # headless backend
import matplotlib.pyplot as plt
import pandas as pd
import mplfinance as mpf

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


def _smma(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return [None] * len(values)
    res = [None] * (period - 1)
    sma = sum(values[:period]) / period
    res.append(sma)
    for i in range(period, len(values)):
        prev = res[-1]
        val = (prev * (period - 1) + values[i]) / period
        res.append(val)
    return res


def make_price_history_chart(points: List[Dict]) -> io.BytesIO | None:
    """Return a candlestick chart image with Alligator indicator."""
    if not points:
        return None
    df = pd.DataFrame(points)
    if not {"open", "high", "low", "close", "date"}.issubset(df.columns):
        return None
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    median = (df["high"] + df["low"]) / 2
    df["jaw"] = pd.Series(_smma(median.tolist(), 21)).shift(8)
    df["teeth"] = pd.Series(_smma(median.tolist(), 11)).shift(5)
    df["lips"] = pd.Series(_smma(median.tolist(), 8)).shift(3)


    apds = []
    for name, color in [("jaw", "skyblue"), ("teeth", "red"), ("lips", "green")]:
        series = df[name].dropna()
        if not series.empty:
            apds.append(mpf.make_addplot(df[name], color=color))


    mc = mpf.make_marketcolors(up="green", down="red")
    style = mpf.make_mpf_style(
        base_mpf_style="nightclouds", marketcolors=mc, gridstyle=":"
    )

    fig, axlist = mpf.plot(
        df,
        type="candle",
        style=style,
        addplot=apds,
        returnfig=True,
        ylabel="Price",
        tight_layout=True,

    )
    ax = axlist[0]

    last = df.iloc[-1]
    labels = [
        f"O:{last['open']:.2f}",
        f"H:{last['high']:.2f}",
        f"L:{last['low']:.2f}",
        f"C:{last['close']:.2f}",
    ]
    for name in ("jaw", "teeth", "lips"):
        val = last[name]
        if pd.notna(val):
            labels.append(f"{name}:{val:.2f}")
    ax.legend(labels, fontsize="small")

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf

