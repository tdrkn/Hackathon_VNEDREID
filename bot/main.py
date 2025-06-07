import os
import logging
import sqlite3
import feedparser
from newspaper import Article
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

from .rss_collector import RSS_FEEDS

DB_PATH = os.path.join(os.path.dirname(__file__), 'subscriptions.db')

logging.basicConfig(level=logging.INFO)


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


def get_news_digest(ticker: str, limit: int = 3) -> str:
    ticker_up = ticker.upper()
    articles = []
    for source, url in RSS_FEEDS.items():
        feed = feedparser.parse(url)
        for entry in feed.entries:
            text = f"{entry.get('title', '')} {entry.get('summary', '')}"
            if ticker_up in text.upper():
                link = entry.get('link')
                try:
                    article = Article(link)
                    article.download()
                    article.parse()
                    summary = summarize_text(article.text)
                    articles.append(f"*{entry.title}*\n{summary}\n{link}")
                except Exception as e:
                    logging.error('Failed to process article %s: %s', link, e)
                    articles.append(f"{entry.title}\n{link}")
            if len(articles) >= limit:
                break
        if len(articles) >= limit:
            break

    if not articles:
        return 'Статьи не найдены.'

    return '\n\n'.join(articles)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(

        'Привет! Используйте /subscribe <TICKER>, чтобы подписаться на новости. '
        'Доступные команды: /subscribe, /unsubscribe, /digest, /rank, /help'

    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(

        '/start - приветственное сообщение\n'
        '/subscribe <TICKER> - подписаться на тикер\n'
        '/unsubscribe <TICKER> - отписаться от тикера\n'
        '/digest - получить новостной дайджест по подпискам\n'
        '/rank - показать самые популярные тикеры\n'
        '/help - показать эту справку'

    )



async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:

        await update.message.reply_text('Использование: /subscribe <TICKER>')
        return
    ticker = context.args[0]
    add_subscription(update.effective_user.id, ticker)
    await update.message.reply_text(f'Вы подписались на {ticker.upper()}')



async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:

        await update.message.reply_text('Использование: /unsubscribe <TICKER>')
        return
    ticker = context.args[0]
    remove_subscription(update.effective_user.id, ticker)
    await update.message.reply_text(f'Вы отписались от {ticker.upper()}')



async def digest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tickers = get_subscriptions(update.effective_user.id)
    if not tickers:

        await update.message.reply_text('У вас нет подписок.')

        return
    messages = []
    for t in tickers:
        digest_text = get_news_digest(t)
        messages.append(f'*{t}*\n{digest_text}')
    await update.message.reply_text('\n\n'.join(messages), parse_mode='Markdown')


async def rank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ranking = get_rankings()
    if not ranking:

        await update.message.reply_text('Подписок ещё нет.')

        return
    lines = [f'{idx+1}. {ticker} - {count}' for idx, (ticker, count) in enumerate(ranking)]
    await update.message.reply_text('\n'.join(lines))


def main():
    token = os.getenv('TELEGRAM_TOKEN')
    if not token:
        raise RuntimeError('TELEGRAM_TOKEN not set')

    init_db()

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('subscribe', subscribe))
    app.add_handler(CommandHandler('unsubscribe', unsubscribe))
    app.add_handler(CommandHandler('digest', digest))
    app.add_handler(CommandHandler('rank', rank))

    app.run_polling()


if __name__ == '__main__':
    main()
