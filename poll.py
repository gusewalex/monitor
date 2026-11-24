"""Цикл на ~4,5 хв: кожні POLL_INTERVAL секунд перевіряє джерело й копіює нові пости.
Запускається в GitHub Actions; наприкінці workflow запускає наступне коло.
Стан (id останнього поста) зберігається в state.json.
Коди виходу: 0 - все гаразд, 2 - фатальна помилка налаштувань (ланцюжок зупиняється)."""
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = "".join(os.environ["TG_SESSION"].split())  # прибираємо випадкові пробіли/переноси
SOURCE = os.environ.get("SOURCE_CHANNEL", "kievinform_ua1").strip().lstrip("@")
TARGET_RAW = os.environ["TARGET_CHAT"].strip()
ADD_SOURCE_LINK = os.environ.get("ADD_SOURCE_LINK", "1") == "1"
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "270"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))
SETTLE_SECONDS = int(os.environ.get("SETTLE_SECONDS", "4"))  # чекаємо, поки допише весь альбом

TARGET = int(TARGET_RAW) if TARGET_RAW.lstrip("-").isdigit() else TARGET_RAW
STATE = Path("state.json")

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("poll")


class Fatal(Exception):
    """Помилка налаштувань: повторні запуски не допоможуть."""


def load_state():
    if STATE.exists():
        return json.loads(STATE.read_text()).get("last_id")
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
            await asyncio.sleep(min(e.seconds + 1, 60))
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


async def poll_once(client, last_id: int) -> int:
    msgs = [m async for m in client.iter_messages(SOURCE, min_id=last_id, reverse=True, limit=100)]

    # не чіпаємо надто свіжі пости: альбом може ще дописуватися
    now = datetime.now(timezone.utc)
    ready = []
    for m in msgs:
        if (now - m.date).total_seconds() < SETTLE_SECONDS:
            break
        ready.append(m)

    batches = []
    for m in ready:
        if m.grouped_id and batches and batches[-1][0].grouped_id == m.grouped_id:
            batches[-1].append(m)
        else:
            batches.append([m])

    for batch in batches:
        await send_batch(client, batch)
        last_id = batch[-1].id
        save_state(last_id)
        log.info("Переслано до id %s", last_id)
    return last_id


async def run():
    if len(SESSION) not in (353, 369):
        raise Fatal(f"TG_SESSION має довжину {len(SESSION)}, а має бути 353. Створіть сесію заново.")

    client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise Fatal("Сесія недійсна. Створіть нову (create_session.bat) і оновіть секрет TG_SESSION.")
        me = await client.get_me()
        if me.bot:
            raise Fatal("Сесія належить БОТУ. Потрібен звичайний акаунт з номером телефону.")

        last_id = load_state()
        if last_id is None:
            latest = await client.get_messages(SOURCE, limit=1)
            last_id = latest[0].id if latest else 0
            save_state(last_id)
            log.info("Перший запуск: запам'ятав останній пост (id %s), старі не публікую.", last_id)

        deadline = time.monotonic() + RUN_SECONDS
        while True:
            try:
                last_id = await poll_once(client, last_id)
            except FloodWaitError as e:
                log.warning("FloodWait під час опитування: %s c", e.seconds)
                await asyncio.sleep(min(e.seconds + 1, 60))
            except (ConnectionError, OSError, asyncio.TimeoutError) as e:
                log.warning("Мережева помилка: %s", e)
            if time.monotonic() + POLL_INTERVAL >= deadline:
                break
            await asyncio.sleep(POLL_INTERVAL)
        log.info("Коло завершено. Останній id: %s", last_id)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Fatal as e:
        log.error("ФАТАЛЬНО: %s", e)
        sys.exit(2)
    except Exception:
        log.exception("Тимчасова помилка, наступне коло спробує ще раз")
        time.sleep(20)
        sys.exit(0)
