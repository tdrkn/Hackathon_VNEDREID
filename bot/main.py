import os
import logging

import pandas as pd
import io


from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lsa import LsaSummarizer
import asyncio
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()

from .postgres import (
    init_pool as init_pg_pool,
    ensure_schema,
    fetch_ai_by_ticker,
    fetch_ai_recent,
    fetch_recent,
    replace_portfolio,
    fetch_portfolio,
)
from .storage import CSV_PATH
from .rss_collector import collect_ticker_news_async
from .mybag import (
    get_portfolio_text,
    get_portfolio_data,
)
from .gemini import analyze_portfolio
from .userdb import (
    init_db,
    add_subscription,
    add_subscriptions,
    remove_subscription,
    get_subscriptions,
    load_token,
    save_token,
)

from .plotting import make_portfolio_chart, make_price_history_chart
from .market import get_ticker_history



LOG_PATH = os.path.join(os.path.dirname(__file__), 'bot.log')

# Thread pool for future blocking tasks
THREAD_POOL = ThreadPoolExecutor(max_workers=int(os.getenv('WORKERS', '8')))
PG_POOL = None
WAITING_TOKEN = set()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(),
    ],
)





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
    """Return news digest for ticker from RSS feeds."""
    try:
        articles_data = await collect_ticker_news_async(ticker)
    except Exception as e:
        logging.error('Failed to collect RSS for %s: %s', ticker, e)
        return 'Ошибка получения новостей'

    if not articles_data:
        return 'Статьи не найдены.'

    digest_parts = []
    for art in articles_data[:limit]:
        text = art.get('text') or ''
        summary = await asyncio.to_thread(summarize_text, text) if text else ''
        digest_parts.append(f"*{art['title']}*\n{summary}\n{art['link']}")

    return '\n\n'.join(digest_parts)


async def get_ai_news(ticker: str, limit: int = 3) -> str:
    """Return analysed news summaries for ticker."""
    if PG_POOL is None:
        return 'База данных недоступна.'
    articles_data = await fetch_ai_by_ticker(PG_POOL, ticker, limit)
    if not articles_data:
        return 'Новостей нет.'

    lines = []
    for art in articles_data:
        summary = art.get('summary_text') or ''
        link = art.get('link', '')
        title = art.get('title', '')
        lines.append(f"*{title}*\n{summary}\n{link}")

    return '\n\n'.join(lines)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome message with command buttons."""
    keyboard = [['Все команды', 'Дайджест'], ['Мой портфель', 'Новости']]

    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(

        'Привет! Выберите команду с помощью кнопок ниже.',
        reply_markup=reply_markup,

    )


async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display command buttons."""
    keyboard = [['Все команды', 'Дайджест'], ['Мой портфель', 'Новости']]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        'Выберите команду:',
        reply_markup=reply_markup,

    )


async def handle_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Map button labels to the corresponding commands."""
    text = update.message.text
    if text == 'Все команды':
        await help_command(update, context)
    elif text == 'Дайджест':
        await digest(update, context)
    elif text == 'Мой портфель':
        await mybag(update, context)
    elif text == 'Новости':
        await news(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send detailed help grouped by topics."""
    help_text = (
        'Доступные команды:\n\n'
        '*Управление подписками*\n'
        '/subscribe <TICKER> [...] - подписаться на один или несколько тикеров\n'
        '/unsubscribe <TICKER> - отписаться от тикера\n\n'
        '*Новости*\n'
        '/digest - получить новостной дайджест по подпискам\n'
        '/subscriptions - показать ваши подписки\n'
        '/news [hours|days|weeks N] - свежие новости за период\n'
        '/csv - скачать текущий CSV файл со статьями\n\n'
        '*Портфель*\n'
        '/mybag - показать портфель Тинькофф Инвест\n'
        '/csvbag - скачать ваш портфель в CSV\n'
        '/chart - диаграмма распределения портфеля\n'
        '/history <TICKER> [days] - график цены тикера\n'
        '/analysis - анализ портфеля через Gemini\n\n'
        '*Прочее*\n'
        '/log - показать последние строки лога\n'
        '/help - показать эту справку'
    )
    await update.message.reply_text(help_text, parse_mode='Markdown')



async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:

        await update.message.reply_text('Использование: /subscribe <TICKER> [...]')
        return
    tickers = context.args
    subs = await add_subscriptions(update.effective_user.id, tickers)
    await update.message.reply_text(
        'Текущие подписки: ' + ', '.join(subs)
    )
    logging.info(
        "%s subscribed to %s",
        update.effective_user.id,
        ','.join([t.upper() for t in tickers]),
    )



async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:

        await update.message.reply_text('Использование: /unsubscribe <TICKER>')
        return
    ticker = context.args[0]
    await remove_subscription(update.effective_user.id, ticker)
    await update.message.reply_text(f'Вы отписались от {ticker.upper()}')
    logging.info("%s unsubscribed from %s", update.effective_user.id, ticker.upper())


async def list_subscriptions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    subs = await get_subscriptions(update.effective_user.id)
    if not subs:
        await update.message.reply_text('У вас нет подписок.')
    else:
        await update.message.reply_text('Ваши подписки: ' + ', '.join(subs))



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


async def mybag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show user portfolio using stored token or ask for it."""
    user_id = update.effective_user.id
    token = await load_token(user_id)
    if token:
        await update.message.reply_text('Получаю портфель, пожалуйста подождите...')
        text = await get_portfolio_text(token)
        rows = await get_portfolio_data(token)
        tickers = [r.get('ticker') for r in rows]
        await add_subscriptions(user_id, [t for t in tickers if t and t not in ('-', '—')])
        if PG_POOL:
            try:
                await replace_portfolio(PG_POOL, user_id, rows)
            except Exception as e:
                logging.error('Failed to save portfolio: %s', e)
        await update.message.reply_text(f'```\n{text}\n```', parse_mode='Markdown')
        await update.message.reply_text('Тикеры портфеля добавлены в подписки.')
        return

    WAITING_TOKEN.add(user_id)
    await update.message.reply_text('Отправьте токен Тинькофф Инвест в формате t.*')


async def handle_token_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id not in WAITING_TOKEN:
        return
    token = update.message.text.strip()
    await save_token(user_id, token)
    WAITING_TOKEN.discard(user_id)
    await update.message.reply_text('Токен сохранён. Получаю портфель...')
    text = await get_portfolio_text(token)
    rows = await get_portfolio_data(token)
    tickers = [r.get('ticker') for r in rows]
    await add_subscriptions(user_id, [t for t in tickers if t and t not in ('-', '—')])
    if PG_POOL:
        try:
            await replace_portfolio(PG_POOL, user_id, rows)
        except Exception as e:
            logging.error('Failed to save portfolio: %s', e)
    await update.message.reply_text(f'```\n{text}\n```', parse_mode='Markdown')
    await update.message.reply_text('Тикеры портфеля добавлены в подписки.')


async def chart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a portfolio chart for the user."""
    user_id = update.effective_user.id
    token = await load_token(user_id)
    if not token:
        WAITING_TOKEN.add(user_id)
        await update.message.reply_text('Отправьте токен Тинькофф Инвест в формате t.*')
        return
    await update.message.reply_text('Строю график, пожалуйста подождите...')
    rows = await get_portfolio_data(token)
    buf = make_portfolio_chart(rows)
    if not buf:
        await update.message.reply_text('Не удалось построить график.')
        return
    buf.name = 'portfolio.png'
    await update.message.reply_photo(buf)


async def history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send price history chart for a ticker."""
    if not context.args:
        await update.message.reply_text(
            'Использование: /history <TICKER> [days]'
        )
        return
    ticker = context.args[0]
    days = 30
    if len(context.args) > 1:
        try:
            days = int(context.args[1])
        except ValueError:
            days = 30
    user_id = update.effective_user.id
    token = await load_token(user_id)
    if not token:
        WAITING_TOKEN.add(user_id)
        await update.message.reply_text('Отправьте токен Тинькофф Инвест в формате t.*')
        return
    await update.message.reply_text('Получаю данные, пожалуйста подождите...')
    points = await get_ticker_history(token, ticker, days)
    buf = make_price_history_chart(points)
    if not buf:
        await update.message.reply_text('Не удалось построить график.')
        return
    buf.name = f'{ticker}.png'
    await update.message.reply_photo(buf)


async def analysis(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Analyze user portfolio using Gemini."""
    user_id = update.effective_user.id
    token = await load_token(user_id)
    if not token:
        WAITING_TOKEN.add(user_id)
        await update.message.reply_text('Отправьте токен Тинькофф Инвест в формате t.*')
        return
    await update.message.reply_text('Анализирую портфель, пожалуйста подождите...')
    rows = await get_portfolio_data(token)
    if not rows:
        await update.message.reply_text('Портфель пуст.')
        return
    result = await analyze_portfolio(rows)
    if not result:
        await update.message.reply_text('Не удалось выполнить анализ.')
        return
    await update.message.reply_text(result)

async def news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send recent news stored in PostgreSQL."""
    hours = _parse_hours(context.args)
    await update.message.reply_text('Получаю новости, пожалуйста подождите...')
    if PG_POOL is None:
        await update.message.reply_text('База данных недоступна.')
        return
    try:
        articles = await fetch_ai_recent(PG_POOL, hours)
    except Exception as e:
        logging.error('Failed to fetch news: %s', e)
        await update.message.reply_text('Ошибка получения новостей.')
        return
    if not articles:
        await update.message.reply_text('Новостей нет.')
        return

    lines = [
        f"*{a['title']}*\n{a.get('summary_text','')}\n{a['link']}"
        for a in articles[:10]
    ]
    await update.message.reply_text('\n\n'.join(lines), parse_mode='Markdown')
    logging.info(
        "News command used by %s, %d articles",
        update.effective_user.id,
        len(articles),
    )


async def show_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send last 20 lines of the log file."""
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()[-20:]
        await update.message.reply_text(''.join(lines) or 'Лог пуст.')
    else:
        await update.message.reply_text('Файл лога не найден.')


async def send_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send current news CSV file to the user."""

    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'rb') as f:
            await update.message.reply_document(f, filename='articles.csv')
    else:
        await update.message.reply_text('Файл articles.csv не найден.')


async def send_csvbag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send user's portfolio as CSV file."""
    if PG_POOL is None:
        await update.message.reply_text('База данных недоступна.')
        return
    user_id = update.effective_user.id
    rows = await fetch_portfolio(PG_POOL, user_id)
    if not rows:
        await update.message.reply_text('Данных портфеля нет.')
        return
    df = pd.DataFrame(rows)
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    buffer = io.BytesIO(csv_bytes)
    buffer.name = 'portfolio.csv'
    buffer.seek(0)
    await update.message.reply_document(buffer, filename='portfolio.csv')

def main():
    token = os.getenv('TELEGRAM_TOKEN')
    if not token:
        raise RuntimeError('TELEGRAM_TOKEN not set')

    asyncio.run(init_db())

    # Ensure event loop exists for python-telegram-bot
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    app = (
        ApplicationBuilder()
        .token(token)
        .concurrent_updates(True)
        .post_init(pg_startup)
        .post_shutdown(pg_shutdown)
        .build()
    )

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('menu', show_menu))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('subscribe', subscribe))
    app.add_handler(CommandHandler('unsubscribe', unsubscribe))
    app.add_handler(CommandHandler('subs', list_subscriptions))
    app.add_handler(CommandHandler('subscriptions', list_subscriptions))
    app.add_handler(CommandHandler('digest', digest))
    app.add_handler(CommandHandler('news', news))
    app.add_handler(CommandHandler('csv', send_csv))
    app.add_handler(CommandHandler('csvbag', send_csvbag))
    app.add_handler(CommandHandler('log', show_log))
    app.add_handler(CommandHandler('mybag', mybag))
    app.add_handler(CommandHandler('chart', chart))
    app.add_handler(CommandHandler('history', history))
    app.add_handler(CommandHandler('analysis', analysis))
    app.add_handler(MessageHandler(
        filters.Regex('^(Все команды|Дайджест|Мой портфель|Новости)$'),
        handle_menu_button,
    ))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_token_message))


    app.run_polling()


if __name__ == '__main__':
    main()
