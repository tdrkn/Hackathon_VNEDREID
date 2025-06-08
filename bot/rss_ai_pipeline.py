import os
import asyncio
import pandas as pd

from .rss_collector import collect_recent_news_async
from .postgres import (
    init_pool,
    ensure_schema,
    insert_articles,
    insert_ai_articles,
)
from .gemini import analyze_text


async def main(hours: int = 24) -> None:
    """Collect recent news, analyse them with Gemini and save to DB."""
    pool = await init_pool()
    await ensure_schema(pool)

    articles = await collect_recent_news_async(hours)
    if articles:
        await insert_articles(pool, articles)
    analysed = []
    for art in articles:
        text = f"{art.get('title','')}\n{art.get('text','')}"
        result = await analyze_text(text)
        if not result:
            continue
        result["title"] = art.get("title")
        result["link"] = art.get("link")
        result["published_at"] = art.get("date")
        analysed.append(result)

    if analysed:
        await insert_ai_articles(pool, analysed)
        df = pd.DataFrame(analysed)
        csv_path = os.path.join(os.path.dirname(__file__), "news_analysis.csv")
        df.to_csv(csv_path, index=False)
        print(f"Файл сохранён: {csv_path}")

    await pool.close()


if __name__ == "__main__":
    hours = int(os.getenv("HOURS", "24"))
    asyncio.run(main(hours))
