"""Одноразовий запуск: забирає нові пости з каналу-джерела і копіює у ваш канал.
Запускається за розкладом у GitHub Actions. Стан (id останнього поста) у state.json."""
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["TG_SESSION"]
SOURCE = os.environ.get("SOURCE_CHANNEL", "nebo_raketa").lstrip("@")
TARGET_RAW = os.environ["TARGET_CHAT"].strip()
ADD_SOURCE_LINK = os.environ.get("ADD_SOURCE_LINK", "1") == "1"

TARGET = int(TARGET_RAW) if TARGET_RAW.lstrip("-").isdigit() else TARGET_RAW
STATE = Path("state.json")

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("poll")


def load_state():
    if STATE.exists():
        return json.loads(STATE.read_text())
    return None


def save_state(last_id: int):
    STATE.write_text(json.dumps({"last_id": last_id}))


def footer(msg) -> str:
    return f"\n\nДжерело: https://t.me/{SOURCE}/{msg.id}" if ADD_SOURCE_LINK else ""


async def with_retry(factory, retries=3):
    for _ in range(retries):
        try:
            return await factory()
        except FloodWaitError as e:
            log.warning("FloodWait %s c", e.seconds)
            await asyncio.sleep(e.seconds + 1)
    raise RuntimeError("Не вдалося відправити після кількох спроб")


async def send_batch(client, batch):
    if len(batch) > 1:  # альбом
        media = [m.media for m in batch if m.media]
        caption = next((m.raw_text for m in batch if m.raw_text), "") + footer(batch[0])
        await with_retry(lambda: client.send_file(TARGET, media, caption=caption))
        return

    msg = batch[0]
    media = None if isinstance(msg.media, MessageMediaWebPage) else msg.media
    text = msg.raw_text or ""
    if not text and not media:
        return
    await with_retry(
        lambda: client.send_message(
            TARGET,
            text + footer(msg),
            file=media,
            formatting_entities=msg.entities,
            link_preview=False,
        )
    )


async def main():
    client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            log.error("Сесія недійсна. Створіть нову через make_session.py і оновіть секрет TG_SESSION.")
            sys.exit(1)

        state = load_state()
        if state is None:
            latest = await client.get_messages(SOURCE, limit=1)
            save_state(latest[0].id if latest else 0)
            log.info("Перший запуск: запам'ятав останній пост, нічого не публікую.")
            return

        last_id = state["last_id"]
        msgs = [m async for m in client.iter_messages(SOURCE, min_id=last_id, reverse=True, limit=100)]
        if not msgs:
            log.info("Нових постів немає.")
            return

        batches = []
        for m in msgs:
            if m.grouped_id and batches and batches[-1][0].grouped_id == m.grouped_id:
                batches[-1].append(m)
            else:
                batches.append([m])

        for batch in batches:
            await send_batch(client, batch)
            last_id = batch[-1].id
            save_state(last_id)
            log.info("Переслано до id %s", last_id)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
