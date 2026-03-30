import os
import asyncio
from fastapi import FastAPI, Request
from aiogram import types

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import bot, dp

app = FastAPI()

WEBHOOK_URL = os.getenv("WEBHOOK_URL")

@app.on_event("startup")
async def on_startup():
    if WEBHOOK_URL:
        await bot.set_webhook(f"{WEBHOOK_URL}/api")

@app.post("/api")
async def handle_webhook(request: Request):
    update_data = await request.json()
    update = types.Update(**update_data)
    await dp.feed_update(bot, update)
    return {"status": "ok"}

