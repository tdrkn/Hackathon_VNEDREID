import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os

import feedparser
import pandas as pd
from newspaper import Article

from .storage import (
    save_articles_to_csv,
    save_articles_to_db,
    save_articles_to_csv_async,
    save_articles_to_db_async,
)

_FEED_CACHE: Dict[str, feedparser.FeedParserDict] = {}
_ARTICLE_CACHE: Dict[str, str] = {}

# Executor for heavy network operations
EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv("WORKERS", "8")))

def _get_feed(url: str):
    """Return parsed feed, caching results to avoid redundant network calls."""
    if url not in _FEED_CACHE:
        _FEED_CACHE[url] = feedparser.parse(url)
    return _FEED_CACHE[url]

async def _get_feed_async(url: str):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(EXECUTOR, _get_feed, url)


def _get_article_text(url: str) -> str:
    """Download article text with caching."""
    if not url:
        return ""
    if url in _ARTICLE_CACHE:
        return _ARTICLE_CACHE[url]
    try:
        article = Article(url)
        article.download()
        article.parse()
        text = article.text
    except Exception:
        text = ""
    _ARTICLE_CACHE[url] = text
    return text


async def _get_article_text_async(url: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(EXECUTOR, _get_article_text, url)

RSS_FEEDS: Dict[str, str] = {
    "\u0420\u0411\u041A \u0413\u043b\u0430\u0432\u043d\u044b\u0435 \u043d\u043e\u0432\u043e\u0441\u0442\u0438": "https://static.feed.rbc.ru/rbc/internal/rss.rbc.ru/rbc.ru/mainnews.rss",
    "\u041a\u043e\u043c\u043c\u0435\u0440\u0441\u0430\u043d\u0442\u044a \u2014 \u042d\u043a\u043e\u043d\u043e\u043c\u0438\u043a\u0430": "https://www.kommersant.ru/RSS/section-economics.xml",
    "\u0426\u0411 \u0420\u0424 \u2014 \u041d\u043e\u0432\u043e\u0441\u0442\u0438": "http://www.cbr.ru/rss/RssNews",
    "Banki.ru \u2014 \u041b\u0435\u043d\u0442\u0430": "https://www.banki.ru/news/lenta/?r1=rss&r2=news",
    "Finam \u2014 \u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u043a\u043e\u043c\u043f\u0430\u043d\u0438\u0439": "https://www.finam.ru/analysis/conews/rsspoint/",
    "Finam \u2014 \u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u043e\u0431\u043b\u0438\u0433\u0430\u0446\u0438\u0439": "https://bonds.finam.ru/news/today/rss.asp",
    "Moex \u2014 \u0412\u0441\u0435 \u043d\u043e\u0432\u043e\u0441\u0442\u0438": "https://moex.com/export/news.aspx?cat=100",
    "Moex \u2014 \u0413\u043b\u0430\u0432\u043d\u044b\u0435 \u043d\u043e\u0432\u043e\u0441\u0442\u0438": "https://moex.com/export/news.aspx?cat=101",
    "Moex \u2014 \u0418\u0442\u043e\u0433\u0438 \u0442\u043e\u0440\u0433\u043e\u0432": "https://www.moex.com/export/news.aspx?cat=102",
    "Moex \u2014 \u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u043b\u0438\u0441\u0442\u0438\u043d\u0433\u0430": "https://www.moex.com/export/news.aspx?cat=104",
    "Moex \u2014 \u0420\u0438\u0441\u043a-\u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440\u044b": "https://www.moex.com/export/news.aspx?cat=122",
    "Moex \u2014 \u041c\u0435\u0440\u043e\u043f\u0440\u0438\u044f\u0442\u0438\u044f": "https://www.moex.com/export/news.aspx?cat=300",
    "\u0422\u0410\u0421\u0421 \u2014 Business & Economy": "https://tass.com/rss/v2.xml",
    "Profinance \u2014 \u0424\u043e\u043d\u0434\u043e\u0432\u044b\u0439 \u0440\u044b\u043d\u043e\u043a": "https://www.profinance.ru/fond.xml",
    "Profinance \u2014 \u042d\u043a\u043e\u043d\u043e\u043c\u0438\u043a\u0430": "https://www.profinance.ru/econom.xml",
}


def _is_today(entry_date_struct) -> bool:
    """Return True if the given date struct represents today."""
    if not entry_date_struct:
        return False
    pub_date = (
        datetime(*entry_date_struct[:6], tzinfo=timezone.utc).astimezone()
    )
    return pub_date.date() == datetime.now().astimezone().date()


def _is_recent(entry_date_struct, delta_hours: int) -> bool:
    """Return True if the entry date is within the given number of hours."""
    if not entry_date_struct:
        return False
    pub_date = (
        datetime(*entry_date_struct[:6], tzinfo=timezone.utc).astimezone()
    )
    return pub_date >= datetime.now().astimezone() - timedelta(hours=delta_hours)


def collect_today_news() -> pd.DataFrame:
    """Collect news from RSS feeds published today with article texts."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    collected: List[dict] = []

    for source, url in RSS_FEEDS.items():
        feed = _get_feed(url)
        for entry in feed.entries:
            entry_date_struct = entry.get("published_parsed") or entry.get("updated_parsed")
            if _is_today(entry_date_struct):
                link = entry.get("link", "")
                text = _get_article_text(link)
                collected.append(
                    {
                        "\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a": source,
                        "\u0414\u0430\u0442\u0430": today_str,
                        "\u0417\u0430\u0433\u043e\u043b\u043e\u0432\u043e\u043a": entry.get("title", "\u0411\u0435\u0437 \u0437\u0430\u0433\u043e\u043b\u043e\u0432\u043a\u0430"),
                        "\u0421\u0441\u044b\u043b\u043a\u0430": link,
                        "\u0422\u0435\u043a\u0441\u0442": text,
                    }
                )

    return pd.DataFrame(collected)


def save_today_news(directory: str = ".") -> str:
    """Save today's news to a CSV file in the given directory."""
    df = collect_today_news()
    if df.empty:
        print("[\u2139] \u0417\u0430 \u0441\u0435\u0433\u043e\u0434\u043d\u044f \u043d\u043e\u0432\u043e\u0441\u0442\u0435\u0439 \u043d\u0435\u0442.")
        return ""

    today_str = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(directory, f"news_{today_str}.csv")
    records = df.to_dict(orient="records")
    save_articles_to_csv(records, path)
    save_articles_to_db(records)
    print(f"[\u2714] \u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u043e {len(df)} \u043d\u043e\u0432\u043e\u0441\u0442\u0435\u0439 \u0432 {path}")
    return path

async def save_today_news_async(directory: str = ".") -> str:
    return await asyncio.to_thread(save_today_news, directory)

def collect_recent_news(hours: int = 24) -> List[dict]:
    """Collect articles from the last `hours` hours from all RSS feeds."""
    collected: List[dict] = []
    for source, url in RSS_FEEDS.items():
        feed = _get_feed(url)
        for entry in feed.entries:
            entry_date_struct = entry.get("published_parsed") or entry.get("updated_parsed")
            if _is_recent(entry_date_struct, hours):
                link = entry.get("link", "")
                text = _get_article_text(link)
                pub_date = (
                    datetime(*entry_date_struct[:6], tzinfo=timezone.utc).astimezone()
                    if entry_date_struct
                    else None
                )
                collected.append(
                    {
                        "source": source,
                        "date": pub_date.strftime("%Y-%m-%d %H:%M") if pub_date else "",
                        "title": entry.get("title", ""),
                        "link": link,
                        "text": text,
                    }
                )
    return collected


async def collect_recent_news_async(hours: int = 24) -> List[dict]:
    """Asynchronous version of collect_recent_news using threads."""
    tasks = [
        asyncio.create_task(_collect_recent_from_feed_async(source, url, hours))
        for source, url in RSS_FEEDS.items()
    ]
    results = await asyncio.gather(*tasks)
    collected: List[dict] = []
    for articles in results:
        collected.extend(articles)
    return collected


async def _collect_recent_from_feed_async(source: str, url: str, hours: int) -> List[dict]:
    feed = await _get_feed_async(url)
    articles: List[dict] = []
    for entry in feed.entries:
        entry_date_struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if _is_recent(entry_date_struct, hours):
            link = entry.get("link", "")
            text = await _get_article_text_async(link)
            pub_date = (
                datetime(*entry_date_struct[:6], tzinfo=timezone.utc).astimezone()
                if entry_date_struct
                else None
            )
            articles.append(
                {
                    "source": source,
                    "date": pub_date.strftime("%Y-%m-%d %H:%M") if pub_date else "",
                    "title": entry.get("title", ""),
                    "link": link,
                    "text": text,
                }
            )
    return articles


def collect_ticker_news(ticker: str) -> List[dict]:
    """Collect all articles containing the given ticker from all RSS feeds."""
    ticker_up = ticker.upper()
    collected: List[dict] = []
    for source, url in RSS_FEEDS.items():
        feed = _get_feed(url)
        for entry in feed.entries:
            text_summary = f"{entry.get('title', '')} {entry.get('summary', '')}"
            if ticker_up in text_summary.upper():
                link = entry.get("link", "")
                text = _get_article_text(link)
                collected.append(
                    {
                        "source": source,
                        "title": entry.get("title", ""),
                        "link": link,
                        "text": text,
                    }
                )
    return collected


async def collect_ticker_news_async(ticker: str) -> List[dict]:
    """Asynchronous version of collect_ticker_news using threads."""
    ticker_up = ticker.upper()
    tasks = [
        asyncio.create_task(_collect_ticker_from_feed_async(ticker_up, source, url))
        for source, url in RSS_FEEDS.items()
    ]
    results = await asyncio.gather(*tasks)
    collected: List[dict] = []
    for articles in results:
        collected.extend(articles)
    return collected


async def _collect_ticker_from_feed_async(ticker_up: str, source: str, url: str) -> List[dict]:
    feed = await _get_feed_async(url)
    articles: List[dict] = []
    for entry in feed.entries:
        text_summary = f"{entry.get('title', '')} {entry.get('summary', '')}"
        if ticker_up in text_summary.upper():
            link = entry.get("link", "")
            text = await _get_article_text_async(link)
            articles.append(
                {
                    "source": source,
                    "title": entry.get("title", ""),
                    "link": link,
                    "text": text,
                }
            )
    return articles


if __name__ == "__main__":
    save_today_news()
