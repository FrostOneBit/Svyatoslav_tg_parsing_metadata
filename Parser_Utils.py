import asyncio
import re

import telethon
from telethon import TelegramClient, functions
from Config import Telethon_session_name
from Logger_utils import setup_logger
from Database_utils import get_hash_id_api, save_group_in_db, save_url_message_in_db, get_url_by_rule_1


# --- Получение URL сообщений --- #
async def get_url_messages_rabbitmq():
    # -- Добавить проверку на существование в бд, чтобы не добавлять дубликаты -- #
    #  -- Переделка под RabbitMQ для получения ссылок -- #

    message_group = [
        "https://t.me/test_open_0/2",
        "https://t.me/c/2589054501/2",
        "https://t.me/c/2649844635/2"
    ]

    return message_group


# --- Основная функция обработки группы по сообщениям--- #
async def process_channel_urls():

    logger = await setup_logger('process_channel_urls', "Parser_Utils.log")

    try:
        api_id, api_hash = await get_hash_id_api()
        message_urls = await get_url_messages_rabbitmq()

        async with TelegramClient(Telethon_session_name, api_id, api_hash) as client:
            dialogs = await client.get_dialogs()
            subscribed_channels = {
                int(f"-100{dialog.entity.id}"): getattr(dialog.entity, 'title', 'Unknown')
                for dialog in dialogs
                if isinstance(dialog.entity, (telethon.tl.types.Channel, telethon.tl.types.Chat))
            }

            result = []

            for url in message_urls:
                try:
                    url_parts = url.split("/")
                    if len(url_parts) < 4:
                        raise ValueError("Недопустимый формат ссылки")

                    is_closed = url_parts[3] == "c"
                    channel_type = "closed" if is_closed else "open"
                    channel_data = int(url_parts[4]) if is_closed else url_parts[3]

                    if is_closed:
                        channel_id = int(f"-100{channel_data}")
                        channel_name = subscribed_channels.get(channel_id, "Unknown")
                        is_subscribed = channel_id in subscribed_channels
                    else:
                        try:
                            entity = await client.get_entity(channel_data)
                            channel_id = int(f"-100{abs(entity.id)}")
                            channel_name = getattr(entity, 'title', None) or getattr(entity, 'username', None) or "Unknown"
                            is_subscribed = channel_id in subscribed_channels
                        except Exception as ex:
                            raise ValueError(f"Ошибка получения entity: {ex}")

                    result.append({
                        "message_url": url,
                        "channel_id": str(channel_id),
                        "channel_name": channel_name,
                        "status": str(is_subscribed),
                        "channel_type": channel_type
                    })

                except Exception as ex:
                    logger.error(f"Ошибка при обработке {url}: {ex}")
                    result.append({
                        "message_url": url,
                        "channel_id": "Unknown",
                        "channel_name": "Unknown",
                        "status": "False",
                        "channel_type": "unknown"
                    })

            saved = await save_group_in_db(result)
            await save_url_message_in_db(saved)

    except Exception as e:
        logger.error(f"Глобальная ошибка обработки ссылок: {e}")


# --- Функция: Правило 1 --- #
async def parse_url_by_rule_1():
    logger = await setup_logger('get_url_by_rule_1', "Rules.log")

    try:
        api_id, api_hash = await get_hash_id_api()
        urls_message = await get_url_by_rule_1()
        messages = []
        async with TelegramClient(Telethon_session_name, api_id, api_hash) as client:
            for url_message in urls_message:
                group_id = url_message[0]
                id = url_message[1]
                match = re.search(r"/(\d+)$", url_message[2])
                if not match:
                    logger.error(f"Не удалось извлечь message_id из ссылки: {url_message[2]}")

                message_id = int(match.group(1))

                try:

                    entity = await client.get_entity(group_id)
                    message = await client.get_messages(entity, ids=message_id)

                    messages.append({
                        "group_id": group_id,
                        "message_id": message_id,
                        "text": message.text,
                        "date": message.date,
                        "sender_id": message.sender_id
                    })

                except Exception as ex:
                    logger.error(ex)

            print(messages)

    except Exception as ex:
        logger.error(ex)

asyncio.run(parse_url_by_rule_1())