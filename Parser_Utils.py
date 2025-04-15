import asyncio

import telethon
from telethon import TelegramClient, functions
from Config import Telethon_session_name
from Logger_utils import setup_logger
from Database_utils import get_hash_id_api, save_group_in_db


# --- Получение URL сообщений --- #
async def get_url_messages():

    # -- Добавить проверку на существование в бд, чтобы не добавлять дубликаты -- #

    return [
        "https://t.me/test_open_0/2",  # открытый канал
        "https://t.me/c/2589054501/2",  # закрытый, НЕ подписан
        "https://t.me/c/2649844635/2"  # закрытый, ПОДПИСАН
    ]


# --- Основная функция обработки URL-каналов --- #
async def process_channel_urls():
    """
    Определение, подписка, вывод
    """
    logger = await setup_logger('process_channel_urls', "Parser_Utils.log")

    try:
        api_id, api_hash = await get_hash_id_api()
        message_urls = await get_url_messages()

        async with TelegramClient(Telethon_session_name, api_id, api_hash) as client:
            dialogs = await client.get_dialogs()

            # Собираем все подписки: {channel_id: title}
            subscribed_channels = {
                (int(f"-100{dialog.entity.id}") if isinstance(dialog.entity, (
                    telethon.tl.types.Channel, telethon.tl.types.Chat)) else dialog.entity.id):
                    getattr(dialog.entity, 'title', 'Unknown')
                for dialog in dialogs
                if hasattr(dialog.entity, 'id')
            }

            result_logs = []

            for message_url in message_urls:
                try:
                    url_parts = message_url.split("/")
                    if len(url_parts) < 4:
                        logger.error(f"Неверная ссылка: {message_url}")
                        continue

                    is_closed = url_parts[3] == "c"
                    channel_type = "closed" if is_closed else "open"
                    channel_data = int(url_parts[4]) if is_closed else url_parts[3]

                    if is_closed:
                        channel_id = int(f"-100{channel_data}")
                        is_subscribed = channel_id in subscribed_channels
                        channel_name = subscribed_channels.get(channel_id, "Unknown")
                    else:
                        try:
                            entity = await client.get_entity(channel_data)
                            channel_id = entity.id

                            # Добавляем -100 только к открытым каналам, когда получаем ID
                            if isinstance(entity, (telethon.tl.types.Channel, telethon.tl.types.Chat)):
                                channel_id = int(f"-100{abs(channel_id)}")

                            channel_name = getattr(entity, 'title', None) or getattr(entity, 'username',
                                                                                     None) or "Unknown"
                            is_subscribed = channel_id in subscribed_channels
                        except Exception as ex:
                            result_logs.append(
                                f"Channel ID: Unknown, Name: {channel_data}, Status: False (Entity error: {ex}), Type: open"
                            )
                            continue

                    result_logs.append(
                        f"Channel ID: {channel_id}, Name: {channel_name}, Status: {'True' if is_subscribed else 'False'}, Type: {channel_type}"
                    )

                except Exception as error:
                    logger.error(f"Ошибка при обработке {message_url}: {error}")
                    result_logs.append(
                        f"Channel ID: {channel_data}, Name: Unknown, Status: False, Type: {channel_type}"
                    )

            # for log_line in result_logs:
            #     logger.info(log_line)
            await save_group_in_db(result_logs)

    except Exception as error:
        logger.error(f"Ошибка в процессе обработки ссылок: {error}")
        return []


# --- Запуск --- #
asyncio.run(process_channel_urls())