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
