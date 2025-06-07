import asyncio
import logging

from .rss_collector import collect_recent_news_async
from .postgres import init_pool, ensure_schema, insert_articles

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
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
