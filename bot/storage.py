import os
import sqlite3
import pandas as pd
from datetime import datetime, timezone


def _format_datetime(dt_str: str) -> str:
    """Return ISO formatted datetime with timezone or empty string."""
    if not dt_str:
        return ""
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return dt_str


def save_articles_to_csv(articles, path="articles.csv"):
    """Append articles to CSV file, avoiding duplicates."""
    if not articles:
        return
    df = pd.DataFrame(articles)
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        df = (
            pd.concat([old_df, df])
            .drop_duplicates(subset=["title", "source"])
            .reset_index(drop=True)
        )
    df.to_csv(path, index=False)
    return path


def save_articles_to_db(articles, db_path="articles.db"):
    """Save articles to a SQLite database. Each link is stored once."""
    if not articles:
        return
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            title TEXT,
            link TEXT UNIQUE,
            text TEXT
        )
        """
    )
    for a in articles:
        c.execute(
            "INSERT OR IGNORE INTO articles(source, title, link, text) VALUES (?, ?, ?, ?)",
            (a.get("source"), a.get("title"), a.get("link"), a.get("text", "")),
        )
    conn.commit()
    conn.close()
    return db_path


def save_news_to_csv(articles, path="news.csv"):
    """Save articles to a CSV compatible with the Postgres `news` table."""
    if not articles:
        return
    rows = []
    for a in articles:
        rows.append(
            {
                "title": a.get("title", ""),
                "body": a.get("text", ""),
                "published_at": _format_datetime(a.get("date", "")),
                "source": a.get("source", ""),
                "news_type": "corporate",
                "region": "",
                "topics": "{}",
                "related_markets": "{}",
                "macro_sensitive": "false",
                "likely_to_influence": "false",
                "influence_reason": "",
            }
        )
    df = pd.DataFrame(
        rows,
        columns=[
            "title",
            "body",
            "published_at",
            "source",
            "news_type",
            "region",
            "topics",
            "related_markets",
            "macro_sensitive",
            "likely_to_influence",
            "influence_reason",
        ],
    )
    df.to_csv(path, index=False, encoding="utf-8")
    return path
