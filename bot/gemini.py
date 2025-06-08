import os
import json
import re
import logging
import asyncio
import google.generativeai as genai

GENAI_API_KEY = os.getenv("GEMINI_TOKEN")
if GENAI_API_KEY:
    genai.configure(api_key=GENAI_API_KEY)

BASE_PROMPT = """Ты — эксперт по финансовым рынкам и аналитике новостей. Твоя задача — проанализировать экономическую или финансовую новость и определить её потенциальное влияние на стоимость конкретной акции.

Следуй следующим шагам:

---
### 1. Извлеки информацию:
- Определи компанию и тикер акции (ticker). Если тикер явно не указан — определи по названию компании (например, \"Apple Inc.\" → \"AAPL\").
- Определи тип новости:
  - \"macroeconomic\" — инфляция, ставки, ВВП, геополитика и т.д.
  - \"sector\" — новости, касающиеся определённой отрасли (например, IT, энергетика).
  - \"corporate\" — новости, касающиеся конкретной компании (отчётность, увольнение CEO и т.д.)

---
### 2. Тематическая классификация:
- Укажи ключевые темы (например: \"interest rates\", \"chip export ban\", \"earnings miss\", \"iPhone demand\").
- Укажи страну или регион, к которому относится событие (если применимо).

---
### 3. Корреляции и чувствительность:
- Проверь, связана ли новость с другими рынками, влияющими на эту акцию (например:
  - нефть для энергетических компаний,
  - доходность облигаций для банков,
  - курс доллара для экспортёров).
- Укажи, какие внешние факторы (сырьё, индексы, валюты, макро-показатели) коррелируют с этой акцией.
- Ответь на вопрос: \"Может ли эта новость повлиять на цену данной акции?\" (да/нет, с пояснением).

---
### 4. Выведи результат в формате JSON:
```json
{
  "ticker": "...",
  "company_name": "...",
  "news_type": [...],
  "topics": [...],
  "region": "...",
  "correlated_markets": [...],
  "macro_sensitive": true/false,
  "likely_to_influence": true/false,
  "influence_reason": "...",
  "sentiment": "positive/negative/neutral",
  "summary_text": "...",
  "raw_text": "..."
}
```

---
ПИШИ summary_text И raw_text ТОЛЬКО НА РУССКОМ!!!!!
### 5. Дополнительно:
- Создай поле "summary_text": короткое резюме новости.
- Создай поле "raw_text": полный исходный текст новости.
ПИШИ summary_text И raw_text ТОЛЬКО НА РУССКОМ!!!!!

"""

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
