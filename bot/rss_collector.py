import os
from datetime import datetime, timezone
from typing import List, Dict

import feedparser
import pandas as pd

RSS_FEEDS: Dict[str, str] = {
    "\u0420\u0411\u041A \u0413\u043b\u0430\u0432\u043d\u044b\u0435 \u043d\u043e\u0432\u043e\u0441\u0442\u0438": "https://static.feed.rbc.ru/rbc/internal/rss.rbc.ru/rbc.ru/mainnews.rss",
    "\u041a\u043e\u043c\u043c\u0435\u0440\u0441\u0430\u043d\u0442\u044a \u2014 \u042d\u043a\u043e\u043d\u043e\u043c\u0438\u043a\u0430": "https://www.kommersant.ru/RSS/section-economics.xml",
    "\u0426\u0411 \u0420\u0424 \u2014 \u041d\u043e\u0432\u043e\u0441\u0442\u0438": "http://www.cbr.ru/rss/RssNews",
    "Banki.ru \u2014 \u041b\u0435\u043d\u0442\u0430": "https://www.banki.ru/news/lenta/?r1=rss&r2=news",
    "Finam \u2014 \u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u043a\u043e\u043c\u043f\u0430\u043d\u0438\u0439": "https://www.finam.ru/analysis/conews/rsspoint/",
    "Finam \u2014 \u041d\u043e\u0432\u043e\u0441\u0442\u0438 \u043e\u0431\u043b\u0438\u0433\u0430\u0446\u0438\u0439": "https://bonds.finam.ru/news/today/rss.asp",
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


def collect_today_news() -> pd.DataFrame:
    """Collect news from RSS feeds published today."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    collected: List[dict] = []

    for source, url in RSS_FEEDS.items():
        feed = feedparser.parse(url)
        for entry in feed.entries:
            entry_date_struct = entry.get("published_parsed") or entry.get("updated_parsed")
            if _is_today(entry_date_struct):
                collected.append(
                    {
                        "\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a": source,
                        "\u0414\u0430\u0442\u0430": today_str,
                        "\u0417\u0430\u0433\u043e\u043b\u043e\u0432\u043e\u043a": entry.get("title", "\u0411\u0435\u0437 \u0437\u0430\u0433\u043e\u043b\u043e\u0432\u043a\u0430"),
                        "\u0421\u0441\u044b\u043b\u043a\u0430": entry.get("link", ""),
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
    if os.path.exists(path):
        old_df = pd.read_csv(path)
        df = (
            pd.concat([old_df, df])
            .drop_duplicates(subset=["\u0417\u0430\u0433\u043e\u043b\u043e\u0432\u043e\u043a", "\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a"])
            .reset_index(drop=True)
        )
    df.to_csv(path, index=False)
    print(f"[\u2714] \u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u043e {len(df)} \u043d\u043e\u0432\u043e\u0441\u0442\u0435\u0439 \u0432 {path}")
    return path


if __name__ == "__main__":
    save_today_news()
