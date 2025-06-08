import asyncio
import logging
import argparse

from .rss_collector import collect_recent_news_async
from .postgres import (
    init_pool,
    ensure_schema,
    insert_articles,
    insert_ai_articles,
)
from .gemini import analyze_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


async def main(hours: int) -> None:
    """Collect recent news once, analyse them and save to PostgreSQL."""
    try:
        pool = await init_pool()
    except Exception as e:
        logging.error("Failed to connect to PostgreSQL: %s", e)
        return

    await ensure_schema(pool)

    logging.info("Collecting news for the last %d hours", hours)
    articles = await collect_recent_news_async(hours)
    saved = await insert_articles(pool, articles)
    logging.info("Saved %d raw articles", saved)

    analysed = []
    for art in articles:
        text = f"{art.get('title','')}\n{art.get('text','')}"
        result = await analyze_text(text)
        if result:
            result['title'] = art.get('title')
            result['link'] = art.get('link')
            result['published_at'] = art.get('date')
            analysed.append(result)

    if analysed:
        saved_ai = await insert_ai_articles(pool, analysed)
        logging.info("Saved %d analysed articles", saved_ai)
    else:
        logging.info("No articles were analysed")

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load recent news and summaries into PostgreSQL"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=10,
        help="Number of past days to collect (default: 10)",
    )
    parser.add_argument(
        "--hours",
        type=int,
        help="Override time range in hours",
    )
    args = parser.parse_args()
    hours = args.hours if args.hours is not None else args.days * 24
    asyncio.run(main(hours))
