# Telegram News Digest Bot

This repository contains a simple Telegram bot that allows users to subscribe to stock tickers and receive news digests.

## Features
- `/start` and `/help` for instructions
- `/subscribe <TICKER>` and `/unsubscribe <TICKER>` to manage subscriptions
- `/digest` to fetch the latest news for subscribed tickers
- `/rank` to show the most popular tickers

Subscriptions are stored in a SQLite database for persistence.

## Running with Docker

1. Copy `.env.example` to `.env` and fill in your Telegram bot token.
   ```bash
   cp .env.example .env
   ```
2. Build the image:
   ```bash
   docker build -t telegram-digest-bot .
   ```
3. Run the container:
   ```bash
   docker run --env-file .env telegram-digest-bot
   ```

The bot will start polling Telegram for updates.
