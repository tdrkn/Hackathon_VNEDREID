import os
import logging
import sqlite3

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()
from .rss_collector import (
    collect_ticker_news_async,
    collect_recent_news_async,
)
from .storage import save_articles_to_csv, save_articles_to_db

DB_PATH = os.path.join(os.path.dirname(__file__), 'subscriptions.db')
LOG_PATH = os.path.join(os.path.dirname(__file__), 'bot.log')

# Thread pool for blocking tasks like RSS fetching and database access
THREAD_POOL = ThreadPoolExecutor(max_workers=int(os.getenv('WORKERS', '8')))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(),
    ],
)


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'CREATE TABLE IF NOT EXISTS subscriptions (user_id INTEGER, ticker TEXT, UNIQUE(user_id, ticker))'
    )
    conn.commit()
    conn.close()


def add_subscription(user_id: int, ticker: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'INSERT OR IGNORE INTO subscriptions (user_id, ticker) VALUES (?, ?)',
        (user_id, ticker.upper()),
    )
    conn.commit()
    c.execute('SELECT ticker FROM subscriptions WHERE user_id=?', (user_id,))
    rows = c.fetchall()
    conn.close()
    return [row[0] for row in rows]


def remove_subscription(user_id: int, ticker: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'DELETE FROM subscriptions WHERE user_id=? AND ticker=?',
        (user_id, ticker.upper()),
    )
    conn.commit()
    conn.close()


def get_subscriptions(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT ticker FROM subscriptions WHERE user_id=?', (user_id,))
    rows = c.fetchall()
    conn.close()
    return [row[0] for row in rows]


def get_rankings():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        'SELECT ticker, COUNT(*) as cnt FROM subscriptions GROUP BY ticker ORDER BY cnt DESC'
    )
    rows = c.fetchall()
    conn.close()
    return rows


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
    """Return news digest for ticker and save found articles to CSV."""
    articles_data = await collect_ticker_news_async(ticker)
    if not articles_data:
        return 'Статьи не найдены.'

    save_articles_to_csv(articles_data)
    save_articles_to_db(articles_data)

    digest_parts = []
    for art in articles_data[:limit]:
        if art.get('text'):
            summary = summarize_text(art['text'])
        else:
            summary = ''
        digest_parts.append(f"*{art['title']}*\n{summary}\n{art['link']}")

    return '\n\n'.join(digest_parts)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(

        'Привет! Используйте /subscribe <TICKER>, чтобы подписаться на новости. '
        'Доступные команды: /subscribe, /unsubscribe, /digest, /news, /log, /rank, /help'

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
        '/help - показать эту справку'

    )



async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:

        await update.message.reply_text('Использование: /subscribe <TICKER>')
        return
    ticker = context.args[0]
    add_subscription(update.effective_user.id, ticker)
    await update.message.reply_text(f'Вы подписались на {ticker.upper()}')
    logging.info("%s subscribed to %s", update.effective_user.id, ticker.upper())



async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:

        await update.message.reply_text('Использование: /unsubscribe <TICKER>')
        return
    ticker = context.args[0]
    remove_subscription(update.effective_user.id, ticker)
    await update.message.reply_text(f'Вы отписались от {ticker.upper()}')
    logging.info("%s unsubscribed from %s", update.effective_user.id, ticker.upper())



async def digest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tickers = get_subscriptions(update.effective_user.id)
    if not tickers:

        await update.message.reply_text('У вас нет подписок.')

        return
    await update.message.reply_text('Собираю новости, пожалуйста подождите...')
    tasks = [asyncio.create_task(get_news_digest(t)) for t in tickers]
    digests = await asyncio.gather(*tasks)
    messages = [f'*{t}*\n{d}' for t, d in zip(tickers, digests)]
    await update.message.reply_text('\n\n'.join(messages), parse_mode='Markdown')
    logging.info("Digest sent to %s for %d tickers", update.effective_user.id, len(tickers))


async def rank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ranking = get_rankings()
    if not ranking:

        await update.message.reply_text('Подписок ещё нет.')

        return
    lines = [f'{idx+1}. {ticker} - {count}' for idx, (ticker, count) in enumerate(ranking)]
    await update.message.reply_text('\n'.join(lines))
    logging.info("Rank command used by %s", update.effective_user.id)


async def news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send recent news from all RSS feeds for the given period."""
    hours = _parse_hours(context.args)
    await update.message.reply_text('Собираю новости, пожалуйста подождите...')
    articles = await collect_recent_news_async(hours)
    if not articles:
        await update.message.reply_text('Новостей нет.')
        return

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(THREAD_POOL, save_articles_to_csv, articles)
    await loop.run_in_executor(THREAD_POOL, save_articles_to_db, articles)

    lines = [f"*{a['title']}*\n{a['link']}" for a in articles[:10]]
    await update.message.reply_text('\n\n'.join(lines), parse_mode='Markdown')
    logging.info("News command used by %s, %d articles", update.effective_user.id, len(articles))


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

    init_db()

    app = (
        ApplicationBuilder()
        .token(token)
        .concurrent_updates(True)
        .build()
    )

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('subscribe', subscribe))
    app.add_handler(CommandHandler('unsubscribe', unsubscribe))
    app.add_handler(CommandHandler('digest', digest))
    app.add_handler(CommandHandler('rank', rank))
    app.add_handler(CommandHandler('news', news))
    app.add_handler(CommandHandler('log', show_log))

    app.run_polling()


if __name__ == '__main__':
    main()
