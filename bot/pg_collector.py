import asyncio
import logging

from .rss_collector import collect_recent_news_async
from .postgres import (
    init_pool,
    ensure_schema,
    insert_articles,
    insert_ai_articles,
)
from .gemini import analyze_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

async def main():
    try:
        pool = await init_pool()
    except Exception as e:
        logging.error("Failed to connect to PostgreSQL: %s", e)
        return
    await ensure_schema(pool)
    articles = await collect_recent_news_async(24)
    saved = await insert_articles(pool, articles)
    logging.info("Saved %d articles to database", saved)

    analyzed = []
    for art in articles:
        text = f"{art.get('title','')}\n{art.get('text','')}"
        result = await analyze_text(text)
        if result:
            result['title'] = art.get('title')
            result['link'] = art.get('link')
            result['published_at'] = art.get('date')
            analyzed.append(result)

    if analyzed:
        saved_ai = await insert_ai_articles(pool, analyzed)
        logging.info("Saved %d analysed articles", saved_ai)

    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
