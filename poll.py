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
SOURCE = os.environ.get("SOURCE_CHANNEL", "nebo_raketa").strip().lstrip("@")
TARGET_RAW = os.environ["TARGET_CHAT"].strip()
ADD_SOURCE_LINK = os.environ.get("ADD_SOURCE_LINK", "1") == "1"
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "270"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))
SETTLE_SECONDS = int(os.environ.get("SETTLE_SECONDS", "4"))  # чекаємо, поки допише весь альбом
MAX_FAILS = int(os.environ.get("MAX_FAILS", "3"))  # скільки разів пробувати «битий» пост, перш ніж пропустити

TARGET = int(TARGET_RAW) if TARGET_RAW.lstrip("-").isdigit() else TARGET_RAW
STATE = Path("state.json")

CAPTION_LIMIT = 1024   # для Premium-акаунта стане 2048 (див. run)
MESSAGE_LIMIT = 4096
CHUNK_SIZE = 2000      # розмір шматка для дуже довгих текстів (з запасом)

FAILS: dict[int, int] = {}  # id першого повідомлення батча -> кількість невдач

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


def tlen(s: str) -> int:
    """Довжина в UTF-16 (так Telegram рахує символи); з запасом, щоб не ризикувати."""
    return len(s.encode("utf-16-le")) // 2


async def with_retry(factory, retries=3):
    for _ in range(retries):
        try:
            return await factory()
        except FloodWaitError as e:
            log.warning("FloodWait %s c", e.seconds)
            await asyncio.sleep(min(e.seconds + 1, 60))
    raise RuntimeError("Не вдалося відправити після кількох спроб")


async def send_text(client, text: str, entities=None):
    """Текстове повідомлення. Якщо задовге, ріжемо на шматки (без форматування)."""
    if tlen(text) <= MESSAGE_LIMIT:
        await with_retry(
            lambda: client.send_message(
                TARGET, text, formatting_entities=entities, parse_mode=None, link_preview=False
            )
        )
        return
    for i in range(0, len(text), CHUNK_SIZE):
        chunk = text[i:i + CHUNK_SIZE]
        await with_retry(
            lambda c=chunk: client.send_message(TARGET, c, parse_mode=None, link_preview=False)
        )


async def send_batch(client, batch):
    src = next((m for m in batch if m.raw_text), None)  # повідомлення з текстом
    text = src.raw_text if src else ""
    entities = src.entities if src else None
    media = [m.media for m in batch if m.media and not isinstance(m.media, MessageMediaWebPage)]

    if not text and not media:
        return

    full = text + footer(batch[0])

    if not media:
        await send_text(client, full, entities)
        return

    files = media if len(media) > 1 else media[0]
    if tlen(full) <= CAPTION_LIMIT:
        await with_retry(
            lambda: client.send_file(
                TARGET, files, caption=full, formatting_entities=entities, parse_mode=None
            )
        )
    else:
        # підпис задовгий: спершу медіа, потім текст окремим повідомленням
        await with_retry(lambda: client.send_file(TARGET, files))
        await send_text(client, full, entities)


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
        key = batch[0].id
        try:
            await send_batch(client, batch)
        except (FloodWaitError, ConnectionError, OSError, asyncio.TimeoutError):
            raise  # тимчасові проблеми: обробляються у run()
        except Exception:
            FAILS[key] = FAILS.get(key, 0) + 1
            log.exception("Не вдалося переслати пост %s (спроба %s/%s)", key, FAILS[key], MAX_FAILS)
            if FAILS[key] < MAX_FAILS:
                return last_id  # спробуємо ще на наступному опитуванні
            # здаємося, щоб один пост не блокував усю чергу
            log.error("Пропускаю пост %s", key)
            try:
                await send_text(client, f"Не вдалося переслати пост: https://t.me/{SOURCE}/{key}")
            except Exception:
                log.exception("Не вдалося надіслати навіть повідомлення про пропуск")
        last_id = batch[-1].id
        save_state(last_id)
        log.info("Оброблено до id %s", last_id)
    return last_id


async def run():
    global CAPTION_LIMIT
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
        if getattr(me, "premium", False):
            CAPTION_LIMIT = 2048
        log.info("Ліміт підпису: %s", CAPTION_LIMIT)

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
