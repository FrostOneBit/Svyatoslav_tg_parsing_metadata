import asyncio

import telethon
from telethon import TelegramClient

from BotCore import process_telethon_update_session
from Config import Telethon_session_name
from Logger_utils import setup_logger

from Database_utils import get_hash_id_api

# --- Здесь будет получение сообщений --- #
async def get_url_messages():
    # -- После будет подключен RabitMQ -- #
    logger = await setup_logger(name='get_utl_messages', log_file="Parser_Utils.log")

    try:

        url_storage = []

        url_test_open_0 = "https://t.me/test_open_0/2"
        url_test_close_0 = "https://t.me/c/2589054501/2"
        url_test_subs_close_1 = "https://t.me/c/2649844635/2"

        url_storage.append(url_test_open_0)
        url_storage.append(url_test_close_0)
        url_storage.append(url_test_subs_close_1)

        return url_storage

    except Exception as ex:
        logger.error(ex)
        return None

# --- Проверка на валидность ссылки --- #
async def identification_url_message():
    logger = await setup_logger(name='identification_url_messages', log_file="Parser_Utils.log")
    try:

        hash_id, hash_api = await get_hash_id_api()

        url_storage = await get_url_messages() #Запрашиваем список ссылок
        for url in url_storage:

            url_split = url.split("/")

            if url_split[3] == "c":
                channel_id = url_split[4]

                result_validate_close_group_telegram = await validate_close_group_telegram(channel_id, hash_id, hash_api)

            else:
                channel_name = url_split[3]
                channel_id = await get_group_id_use_url_message(channel_name, hash_id, hash_api)
                logger.info(channel_id)
    except Exception as ex:
        logger.error(ex)

# --- Получение id группы --- #
async def get_group_id_use_url_message(channel_name, hash_id, hash_api):
    logger = await setup_logger(name='get_group_id_use_url_messages', log_file="Parser_Utils.log")

    try:
        async with TelegramClient(Telethon_session_name, hash_id, hash_api) as client:
            try:
                # Получаем объект канала
                channel = await client.get_entity(channel_name)
                channel_id = channel.id
                logger.info(f"ID группы: {channel_id}")

                # Попытка подписки на канал
                try:
                    await client(telethon.functions.channels.JoinChannelRequest(channel))
                    logger.info(f"Успешная подписка на канал: {channel_name}")
                except Exception as ex:
                    logger.error(f"Ошибка при подписке на канал: {ex}")

                return channel_id

            except Exception as ex:
                logger.error(f"Ошибка при получении объекта канала: {ex}")
                return None

    except Exception as ex:
        logger.error(f"Ошибка при подключении к клиенту Telethon: {ex}")
        return None

# --- Проверка на подписку на данный канал --- #
async def validate_close_group_telegram(channel_id, hash_id, hash_api):
    logger = await setup_logger(name='validate_close_group_telegram', log_file="Parser_Utils.log")

    try:

        async with TelegramClient(Telethon_session_name, hash_id, hash_api) as client:
            try:
                int_close_channel_id = int(f"-100{channel_id}")
                channel = await client.get_entity(int_close_channel_id)
                if channel:
                    logger.info(channel.title)
                else:
                    logger.info(f"Not subs for: {int_close_channel_id}")
            except Exception as ex:
                logger.error(ex)

    except Exception as ex:
        logger.error(ex)

asyncio.run(identification_url_message())