"""Запускається ОДИН РАЗ на вашому комп'ютері. Друкує рядок сесії для секрету TG_SESSION."""
import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()


def get_creds():
    api_id = os.environ.get("API_ID", "").strip()
    api_hash = os.environ.get("API_HASH", "").strip()

    if not api_id.isdigit() or len(api_hash) != 32:
        print("У .env немає правильних API_ID / API_HASH. Введіть їх зараз.\n")
        while True:
            api_id = input("API_ID (лише цифри): ").strip()
            if api_id.isdigit():
                break
            print("API_ID має складатися лише з цифр.")
        while True:
            api_hash = input("API_HASH (32 символи): ").strip()
            if len(api_hash) == 32:
                break
            print("API_HASH має містити рівно 32 символи.")
    return int(api_id), api_hash


async def main():
    api_id, api_hash = get_creds()
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()  # спитає телефон, код, пароль
    print("\n===== СКОПІЮЙТЕ РЯДОК НИЖЧЕ (це як пароль від акаунта!) =====\n")
    print(client.session.save())
    print("\n===== КІНЕЦЬ =====")
    await client.disconnect()


asyncio.run(main())
