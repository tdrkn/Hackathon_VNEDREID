
# Телеграм-бот для новостных дайджестов

Простой бот, который позволяет подписаться на тикеры акций и получать по ним краткий обзор новостей.

## Возможности
- `/start` и `/help` — вывод подсказок
- `/subscribe <TICKER>` и `/unsubscribe <TICKER>` — управление подписками
- `/digest` — получение новостного дайджеста по подписанным тикерам
- `/rank` — список самых популярных тикеров

Подписки хранятся в базе данных SQLite, поэтому сохраняются между перезапусками.

## Структура проекта
```
.
├── Dockerfile
├── README.md
├── bot/
│   ├── __init__.py
│   └── main.py
├── requirements.txt
└── .env.example
```

## Запуск в Docker
1. Скопируйте `.env.example` в `.env` и укажите токен телеграм-бота.
   ```bash
   cp .env.example .env
   ```
2. Постройте образ:
   ```bash
   docker build -t telegram-digest-bot .
   ```
3. Запустите контейнер:

   ```bash
   docker run --env-file .env telegram-digest-bot
   ```


После запуска бот начнёт опрашивать Telegram и реагировать на команды пользователей.

Все собранные новости сохраняются в два CSV-файла:
`articles.csv` содержит исходные данные, а `news.csv` совместим
с таблицей `news` в PostgreSQL. Его можно загрузить так:

```bash
psql -d mydb -c "\COPY news(title,body,published_at,source,news_type,region,topics,related_markets,macro_sensitive,likely_to_influence,influence_reason) FROM 'news.csv' CSV HEADER"
```

