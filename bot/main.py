import os
import logging

import aiosqlite


from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()

from .postgres import (
    init_pool as init_pg_pool,
    ensure_schema,
    fetch_by_ticker,
)
from .rss_collector import collect_recent_news_async

from .storage import save_articles_to_csv_async
from .portfolio import load_portfolio, TOKEN_ENV


DB_PATH = os.path.join(os.path.dirname(__file__), 'subscriptions.db')
LOG_PATH = os.path.join(os.path.dirname(__file__), 'bot.log')

# Thread pool for future blocking tasks
THREAD_POOL = ThreadPoolExecutor(max_workers=int(os.getenv('WORKERS', '8')))
PG_POOL = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(),
    ],
)



async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            'CREATE TABLE IF NOT EXISTS subscriptions (user_id INTEGER, ticker TEXT, UNIQUE(user_id, ticker))'
        )
        await conn.commit()


async def add_subscription(user_id: int, ticker: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            'INSERT OR IGNORE INTO subscriptions (user_id, ticker) VALUES (?, ?)',
            (user_id, ticker.upper()),
        )
        await conn.commit()
        async with conn.execute('SELECT ticker FROM subscriptions WHERE user_id=?', (user_id,)) as cur:
            rows = await cur.fetchall()
    return [row[0] for row in rows]



async def remove_subscription(user_id: int, ticker: str) -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            'DELETE FROM subscriptions WHERE user_id=? AND ticker=?',
            (user_id, ticker.upper()),
        )
        await conn.commit()


async def get_subscriptions(user_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute('SELECT ticker FROM subscriptions WHERE user_id=?', (user_id,)) as cur:
            rows = await cur.fetchall()
    return [row[0] for row in rows]


async def get_rankings():
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            'SELECT ticker, COUNT(*) as cnt FROM subscriptions GROUP BY ticker ORDER BY cnt DESC'
        ) as cur:
            rows = await cur.fetchall()
    return rows


async def pg_startup(app) -> None:
    global PG_POOL
    try:
        PG_POOL = await init_pg_pool()
        await ensure_schema(PG_POOL)
    except Exception as e:
        logging.error("PostgreSQL unavailable: %s", e)
        PG_POOL = None


async def pg_shutdown(app) -> None:
    if PG_POOL:
        await PG_POOL.close()


async def pg_startup(app) -> None:
    global PG_POOL
    try:
        PG_POOL = await init_pg_pool()
        await ensure_schema(PG_POOL)
    except Exception as e:
        logging.error("PostgreSQL unavailable: %s", e)
        PG_POOL = None


async def pg_shutdown(app) -> None:
    if PG_POOL:
        await PG_POOL.close()


def summarize_text(text: str, sentences: int = 3) -> str:
    parser = PlaintextParser.from_string(text, Tokenizer('english'))
    summarizer = LsaSummarizer()
    summary = summarizer(parser.document, sentences)
    return ' '.join(str(sentence) for sentence in summary)


def _parse_hours(args) -> int:
    """Parse time interval arguments and return hours."""
    if not args:
        return 24
    unit = args[0].lower()
    qty = 1
    if len(args) > 1:
        try:
            qty = int(args[1])
        except ValueError:
            qty = 1
    if unit.startswith('hour'):
        return qty
    if unit.startswith('day'):
        return qty * 24
    if unit.startswith('week'):
        return qty * 24 * 7
    try:
        return int(unit)
    except ValueError:
        return 24

async def get_news_digest(ticker: str, limit: int = 3) -> str:
    """Return news digest for ticker from Postgres database."""
    if PG_POOL is None:
        return 'База данных недоступна.'
    articles_data = await fetch_by_ticker(PG_POOL, ticker, limit * 5)
    if not articles_data:
        return 'Статьи не найдены.'

    digest_parts = []
    for art in articles_data[:limit]:
        text = art.get('body') or ''

        summary = await asyncio.to_thread(summarize_text, text) if text else ''

        digest_parts.append(f"*{art['title']}*\n{summary}\n{art['link']}")

    return '\n\n'.join(digest_parts)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(

        'Привет! Используйте /subscribe <TICKER>, чтобы подписаться на новости. '
        'Доступные команды: /subscribe, /unsubscribe, /digest, /news, /log, /rank, /portfolio, /getrisk, /help'

    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(

        '/start - приветственное сообщение\n'
        '/subscribe <TICKER> - подписаться на тикер\n'
        '/unsubscribe <TICKER> - отписаться от тикера\n'
        '/digest - получить новостной дайджест по подпискам\n'
        '/rank - показать самые популярные тикеры\n'
        '/news [hours|days|weeks N] - свежие новости за период\n'
        '/log - показать последние строки лога\n'
        '/portfolio - показать портфель\n'
        '/getrisk - вывести список тикер - риск\n'
        '/help - показать эту справку'

    )



async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:

        await update.message.reply_text('Использование: /subscribe <TICKER>')
        return
    ticker = context.args[0]
    await add_subscription(update.effective_user.id, ticker)
    await update.message.reply_text(f'Вы подписались на {ticker.upper()}')
    logging.info("%s subscribed to %s", update.effective_user.id, ticker.upper())



async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:

        await update.message.reply_text('Использование: /unsubscribe <TICKER>')
        return
    ticker = context.args[0]
    await remove_subscription(update.effective_user.id, ticker)
    await update.message.reply_text(f'Вы отписались от {ticker.upper()}')
    logging.info("%s unsubscribed from %s", update.effective_user.id, ticker.upper())



async def digest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tickers = await get_subscriptions(update.effective_user.id)
    if not tickers:

        await update.message.reply_text('У вас нет подписок.')

        return
    await update.message.reply_text('Собираю новости, пожалуйста подождите...')
    tasks = [asyncio.create_task(get_news_digest(t)) for t in tickers]

    digests = await asyncio.gather(*tasks, return_exceptions=True)
    results = []
    for t, d in zip(tickers, digests):
        if isinstance(d, Exception):
            msg = 'Ошибка получения новостей'
        else:
            msg = d
        results.append(f'*{t}*\n{msg}')
    messages = results
    await update.message.reply_text('\n\n'.join(messages), parse_mode='Markdown')
    logging.info("Digest sent to %s for %d tickers", update.effective_user.id, len(tickers))


async def rank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ranking = await get_rankings()
    if not ranking:

        await update.message.reply_text('Подписок ещё нет.')

        return
    lines = [f'{idx+1}. {ticker} - {count}' for idx, (ticker, count) in enumerate(ranking)]
    await update.message.reply_text('\n'.join(lines))
    logging.info("Rank command used by %s", update.effective_user.id)


async def news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fetch recent news from RSS feeds and send the headlines."""
    hours = _parse_hours(context.args)
    await update.message.reply_text('Собираю новости, пожалуйста подождите...')
    articles = await collect_recent_news_async(hours)
    if not articles:
        await update.message.reply_text('Новостей нет.')
        return
      
    await save_articles_to_csv_async(articles, 'articles.csv')

    lines = [f"*{a['title']}*\n{a['link']}" for a in articles[:10]]
    await update.message.reply_text('\n\n'.join(lines), parse_mode='Markdown')
    logging.info("News command used by %s, %d articles", update.effective_user.id, len(articles))


async def portfolio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show user's portfolio with risk column."""
    token = os.getenv(TOKEN_ENV)
    if not token:
        await update.message.reply_text('TINKOFF_INVEST_TOKEN not set')
        return

    try:
        acc_id, rows = await asyncio.to_thread(load_portfolio, token, None)
    except Exception as exc:
        await update.message.reply_text(f'Ошибка: {exc}')
        return

    header = f"{'Ticker':<8} {'Qty':>10} {'Curr':<6} {'Price':>10} {'Value':>10} {'Risk':<8}"
    lines = [header, '-' * len(header)]
    for _, ticker, _name, qty, curr, price, value, risk in rows:
        lines.append(f"{ticker:<8} {qty:10,.3f} {curr:<6} {price:10.2f} {value:10.2f} {risk:<8}")

    await update.message.reply_text('<pre>' + '\n'.join(lines) + '</pre>', parse_mode='HTML')


async def getrisk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send ticker-risk pairs from portfolio."""
    token = os.getenv(TOKEN_ENV)
    if not token:
        await update.message.reply_text('TINKOFF_INVEST_TOKEN not set')
        return

    try:
        _, rows = await asyncio.to_thread(load_portfolio, token, None)
    except Exception as exc:
        await update.message.reply_text(f'Ошибка: {exc}')
        return

    pairs = [f"{ticker} - {risk}" for _figi, ticker, _name, _qty, _curr, _price, _value, risk in rows]
    await update.message.reply_text('\n'.join(pairs))


async def show_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send last 20 lines of the log file."""
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()[-20:]
        await update.message.reply_text(''.join(lines) or 'Лог пуст.')
    else:
        await update.message.reply_text('Файл лога не найден.')

def main():
    token = os.getenv('TELEGRAM_TOKEN')
    if not token:
        raise RuntimeError('TELEGRAM_TOKEN not set')

    asyncio.run(init_db())

    app = (
        ApplicationBuilder()
        .token(token)
        .concurrent_updates(True)
        .post_init(pg_startup)
        .post_shutdown(pg_shutdown)
        .build()
    )

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('subscribe', subscribe))
    app.add_handler(CommandHandler('unsubscribe', unsubscribe))
    app.add_handler(CommandHandler('digest', digest))
    app.add_handler(CommandHandler('rank', rank))
    app.add_handler(CommandHandler('news', news))
    app.add_handler(CommandHandler('portfolio', portfolio_cmd))
    app.add_handler(CommandHandler('getrisk', getrisk))
    app.add_handler(CommandHandler('log', show_log))


    app.run_polling()


if __name__ == '__main__':
    main()
