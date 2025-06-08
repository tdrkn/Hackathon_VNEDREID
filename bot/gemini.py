import os
import json
import re
import logging
import asyncio
import google.generativeai as genai

GENAI_API_KEY = os.getenv("GEMINI_TOKEN")
if GENAI_API_KEY:
    genai.configure(api_key=GENAI_API_KEY)

PROMPT_FILE = os.path.join(os.path.dirname(__file__), "gemini_prompt.md")
with open(PROMPT_FILE, "r", encoding="utf-8") as f:
    BASE_PROMPT = f.read()

PORTFOLIO_PROMPT_FILE = os.path.join(os.path.dirname(__file__), "portfolio_prompt.md")
if os.path.exists(PORTFOLIO_PROMPT_FILE):
    with open(PORTFOLIO_PROMPT_FILE, "r", encoding="utf-8") as f:
        PORTFOLIO_PROMPT = f.read()
else:
    PORTFOLIO_PROMPT = ""

async def analyze_text(text: str) -> dict | None:
    if not GENAI_API_KEY:
        logging.error("GEMINI_TOKEN not set")
        return None
    prompt = BASE_PROMPT + "\n\nНОВОСТЬ:\n\n" + text
    model = genai.GenerativeModel("gemini-2.0-flash-001")
    chat = model.start_chat()
    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, chat.send_message, prompt)
        result = response.text
    except Exception as e:
        logging.error("Gemini error: %s", e)
        return None

    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", result, flags=re.DOTALL)
    if match:
        json_text = match.group(1)
    else:
        json_text = result.replace("```", "").strip()
    try:
        data = json.loads(json_text)
    except json.JSONDecodeError:
        logging.error("Failed to parse JSON from Gemini: %s", result)
        return None

    if "raw_text" not in data:
        data["raw_text"] = text
    return data


async def analyze_portfolio(rows: list[dict]) -> str | None:
    """Return Gemini text analysis for the portfolio rows."""
    if not GENAI_API_KEY:
        logging.error("GEMINI_TOKEN not set")
        return None
    if not rows:
        return "Портфель пуст."
    lines = []
    for r in rows:
        ticker = r.get("ticker", "-")
        qty = r.get("qty")
        value = r.get("value")
        curr = r.get("currency", "")
        if qty is not None and value is not None:
            lines.append(f"{ticker}: {qty} шт., {value:.2f} {curr}")
    portfolio_text = "\n".join(lines)
    prompt = PORTFOLIO_PROMPT + "\n\nПОРТФЕЛЬ:\n\n" + portfolio_text
    model = genai.GenerativeModel("gemini-2.0-flash-001")
    chat = model.start_chat()
    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, chat.send_message, prompt)
        result = response.text
    except Exception as e:
        logging.error("Gemini error: %s", e)
        return None
    return result.strip()
