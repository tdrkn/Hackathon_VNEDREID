import os
import sqlite3
import pandas as pd




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

