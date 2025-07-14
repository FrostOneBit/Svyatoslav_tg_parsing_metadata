import json
import asyncio
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    MessageIdInvalidError
)
from telethon.tl.types import Channel, PeerChannel
from telethon.errors.rpcerrorlist import UsernameNotOccupiedError, UsernameInvalidError

from delay_times import delay
from config import Telethon_session_messages, Telethon_media_path, Telethon_session_audience
from logger_utils import setup_logger
from rabbitmq_utils import send_json_message_to_rabbitmq, get_urls_message_rabbitmq, send_audience_to_rabbitmq

from database_utils import (
    get_hash_id_api,
    save_group_in_db,
    get_url_by_rule_1,
    get_url_by_rule_2,
    get_url_by_rule_3,
    get_url_by_rule_4,
    get_url_by_rule_5,
    update_status_media_by_url,
    update_metadata_by_url,
    get_group_id_for_parse_audience,
    update_status_audience_by_group_id,
    deactivate_url_message_by_id
)


# --- Получение групп по url --- #
async def get_groups_by_url():
    logger = await setup_logger(name="save_group_in_db", log_file="database_utils.log")

    try:
        get_urls = await get_urls_message_rabbitmq()

        if not get_urls:
            logger.info("Нет ссылок для обработки.")
            return

        api_id, api_hash = await get_hash_id_api()

        async with TelegramClient(Telethon_session_audience, api_id, api_hash) as client:
            dialogs = await client.get_dialogs()
            subscribed_ids = {int(f"-100{dialog.entity.id}") for dialog in dialogs if isinstance(dialog.entity, Channel)}

            for url in get_urls:
                try:
                    parts = url.strip().split("/")
                    if len(parts) < 4:
                        logger.warning(f"⚠️ Некорректный формат ссылки: {url}")
                        continue

                    is_private = parts[3] == "c"

                    if is_private:
                        raw_id = int(parts[4])
                        group_id = int(f"-100{raw_id}")
                        channel_type = "close"
                        peer = PeerChannel(raw_id)
                        status = group_id in subscribed_ids

                        username = None
                        if status:
                            try:
                                entity = await client.get_entity(peer)
                                username = getattr(entity, 'username', None)
                            except FloodWaitError as fw:
                                logger.warning(f"FloodWait (private): Ждем {fw.seconds} сек. для группы {group_id}")
                                await asyncio.sleep(fw.seconds)
                                continue
                            except Exception as e:
                                logger.warning(f"⚠️ Ошибка при получении username закрытого канала {group_id}: {e}")

                    else:
                        # Для открытых каналов берем username из URL
                        username = parts[3]
                        try:
                            # Устанавливаем таймаут получения entity
                            entity = await asyncio.wait_for(client.get_entity(username), timeout=10)
                        except FloodWaitError as fw:
                            logger.warning(f"FloodWait (open): Ждем {fw.seconds} сек. для URL '{url}'")
                            await asyncio.sleep(fw.seconds)
                            continue
                        except (UsernameNotOccupiedError, UsernameInvalidError):
                            logger.warning(f"⛔ Канал по ссылке '{url}' не существует. Пропускаем.")
                            continue
                        except TimeoutError:
                            logger.warning(f"⌛ Timeout при получении entity по ссылке '{url}'. Пропускаем.")
                            continue
                        except Exception as e:
                            logger.warning(f"❌ Не удалось получить entity для '{username}': {e}")
                            continue

                        group_id = int(f"-100{entity.id}")
                        channel_type = "open"
                        status = True  # Открытый канал всегда доступен
                        # Если в entity задан username — обновляем его
                        username = getattr(entity, 'username', username)

                    await save_group_in_db(username, group_id, status, channel_type)

                except FloodWaitError as fw:
                    logger.warning(f"FloodWait при обработке URL '{url}': Ждем {fw.seconds} сек.")
                    await asyncio.sleep(fw.seconds)
                    continue
                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке URL '{url}': {e}")

    except Exception as ex:
        logger.error(f"Фатальная ошибка: {ex}")


# --- parse_groups_by_id --- #
async def parse_audience_by_group_id():
    logger = await setup_logger(name='parse_groups_by_id', log_file="parser_utils.log")
    try:
        api_id, api_hash = await get_hash_id_api()

        async with TelegramClient(Telethon_session_audience, api_id, api_hash) as client:
            groups_storage = await get_group_id_for_parse_audience()

            for group_storage in groups_storage:
                db_id, group_id, group_username = group_storage[0], group_storage[1], group_storage[2]
                if not group_username:
                    logger.error(f"Нет username для группы с id {group_id}, пропускаем...")
                    continue

                # Получаем entity для группы
                try:
                    entity = await client.get_entity(group_username)
                except FloodWaitError as fw:
                    logger.warning(f"FloodWait при получении {group_username}: ждём {fw.seconds} сек.")
                    await asyncio.sleep(fw.seconds)
                    continue
                except Exception as e:
                    logger.error(f"Не удалось получить информацию о канале {group_username}: {e}")
                    continue

                if not entity:
                    logger.error(f"Пропускаем группу {group_username} — entity не получен.")
                    continue

                unique_usernames = set()
                logger.info(f"Начинаем парсинг аудитории для: {group_username}")
                message_count = 0       # общее кол-во обработанных сообщений
                commented_count = 0     # сообщений, в которых есть комментарии

                try:
                    async for message in client.iter_messages(entity):
                        message_count += 1
                        # Если у сообщения нет атрибута replies или комментариев, пропускаем его
                        if not message.replies or message.replies.replies == 0:
                            logger.debug(f"Пропускаем сообщение {message.id} без комментариев.")
                            continue

                        commented_count += 1
                        # Обработка комментариев с использованием reply_to=message.id
                        try:
                            async for reply in client.iter_messages(entity, reply_to=message.id, limit=10000):
                                if reply.sender:
                                    username = reply.sender.username or f"id_{reply.sender_id}"
                                    unique_usernames.add(username)
                        except Exception as e:
                            logger.warning(f"⚠️ Ошибка получения комментариев для сообщения {message.id}: {e}")

                        # Периодическое логирование каждых 10 сообщений, у которых есть комментарии
                        if commented_count % 10 == 0:
                            logger.info(f"Обработано {commented_count} сообщений с комментариями " +
                                        f"из общего числа {message_count}. Уникальных пользователей: {len(unique_usernames)}")
                except FloodWaitError as fw:
                    logger.warning(f"FloodWait при парсинге сообщений {group_username}: ждём {fw.seconds} сек.")
                    await asyncio.sleep(fw.seconds)
                except Exception as ex:
                    logger.error(f"Ошибка при парсинге канала {group_username}: {ex}")
                    continue

                logger.info(f"Завершён парсинг {group_username}. Всего обработано сообщений: {message_count}, " +
                            f"сообщений с комментариями: {commented_count}. " +
                            f"Уникальных пользователей: {len(unique_usernames)}")

                try:
                    data_to_send = {
                        "type": "AUDIENCE",
                        "group_username": group_username,
                        "group_id": group_id,
                        "usernames": list(unique_usernames)
                    }
                    await send_audience_to_rabbitmq(data_to_send)
                    await update_status_audience_by_group_id(db_id)
                except Exception as ex:
                    logger.error(f"Ошибка при отправке данных о канале {group_username}: {ex}")

                await asyncio.sleep(delay["parse_audience_by_group_id"])

    except Exception as ex:
        logger.error(f"Ошибка выполнения parse_audience_by_group_id: {ex}")


# --- Получение ссылок для всех правил из бд --- #
async def get_urls_from_db_by_rules():
    logger = await setup_logger(name='get_urls_from_db', log_file="Parser_Utils.log")

    try:

        rule_1_urls_task = get_url_by_rule_1()
        rule_2_urls_task = get_url_by_rule_2()
        rule_3_urls_task = get_url_by_rule_3()
        rule_4_urls_task = get_url_by_rule_4()
        rule_5_urls_task = get_url_by_rule_5()

        rule_results = await asyncio.gather(
            rule_1_urls_task,
            rule_2_urls_task,
            rule_3_urls_task,
            rule_4_urls_task,
            rule_5_urls_task,
            return_exceptions=True
        )

        all_urls_for_parsing = []
        for rule_result in rule_results:
            if isinstance(rule_result, Exception):
                logger.error(f"Ошибка при получении ссылок по одному из правил: {rule_result}")
            else:
                all_urls_for_parsing.extend(rule_result)

        logger.info(f"Собрано {len(all_urls_for_parsing)} ссылок для парсинга.")

        return all_urls_for_parsing

    except Exception as ex:
        logger.error(f"Ошибка при получении ссылок: {ex}")
        return []  # Явно возвращаем пустой список в случае ошибки


# --- Парсинг url для телеграм --- #
async def parse_urls_by_rule():
    logger = await setup_logger(name="parse_urls_by_rule", log_file="parser_utils.log")

    try:
        api_id, api_hash = await get_hash_id_api()
        urls_message = await get_urls_from_db_by_rules()

        if not urls_message:
            logger.info("Список ссылок пуст, завершение.")
            return

        async with TelegramClient(Telethon_session_messages, api_id, api_hash) as client:
            for url_info in urls_message:
                message_db_id, message_url, status_media = url_info

                try:
                    parts = message_url.split("/")
                    # parts[-2] и parts[-1] — это channel/ID и message_id
                    if "t.me/c/" in message_url:
                        # Приватный канал
                        raw_group_id = parts[-2]
                        message_id = int(parts[-1])
                        entity = int(f"-100{raw_group_id}")
                    elif "t.me/" in message_url:
                        # Публичный канал
                        channel_username = parts[-2]
                        message_id = int(parts[-1])
                        try:
                            entity = await client.get_entity(channel_username)
                        except FloodWaitError as fw:
                            logger.warning(f"FloodWait при получении entity {channel_username}: ждём {fw.seconds} сек.")
                            await asyncio.sleep(fw.seconds)
                            entity = await client.get_entity(channel_username)
                        except Exception as e:
                            logger.warning(f"Не удалось получить entity для {channel_username}: {e}")
                            continue
                    else:
                        logger.warning(f"Невалидная ссылка: {message_url}")
                        continue

                    try:
                        message = await client.get_messages(entity, ids=message_id)
                    except FloodWaitError as fw:
                        logger.warning(f"FloodWait при получении сообщения {message_url}: ждём {fw.seconds} сек.")
                        await asyncio.sleep(fw.seconds)
                        message = await client.get_messages(entity, ids=message_id)
                    except MessageIdInvalidError:
                        logger.warning(f"Сообщение не найдено: {message_url}")
                        continue

                    if message is None:
                        logger.warning(f"Сообщение не найдено (None): {message_url}")
                        await deactivate_url_message_by_id(message_url)
                        continue

                    if message.grouped_id:
                        album = await client.get_messages(entity, min_id=0, max_id=message_id + 1, limit=20)
                        album_messages = [msg for msg in album if msg.grouped_id == message.grouped_id]
                    else:
                        album_messages = [message]

                    total_reactions = sum(r.count for r in message.reactions.results) if message.reactions else 0
                    media_paths, text_parts = [], []

                    if not status_media:
                        for msg in album_messages:
                            if msg.media:
                                try:
                                    path = await msg.download_media(file=Telethon_media_path)
                                    media_paths.append(path)
                                except Exception as e:
                                    logger.warning(f"Не удалось скачать медиа из сообщения {msg.id}: {e}")
                            if msg.text:
                                text_parts.append(msg.text)

                        await update_status_media_by_url(message_db_id)

                    comments = await get_comments_by_message_url(client, entity, message_id)

                    messages_data = {
                        "url": message_url,
                        "message_id": message_id,
                        "text": "\n".join(text_parts) if text_parts else None,
                        "date": message.date.strftime("%d.%m.%Y %H:%M:%S"),
                        "sender_id": message.sender.id if message.sender else None,
                        "media_path": media_paths,
                        "reactions": total_reactions,
                        "views": message.views,
                        "comments": comments
                    }

                    await update_metadata_by_url(message_db_id)

                    # Отправляем сообщение сразу после обработки
                    await send_json_message_to_rabbitmq(messages_data)

                    await asyncio.sleep(delay["parse_urls_by_rule"])

                except FloodWaitError as fw:
                    logger.warning(f"FloodWait при обработке {message_url}: ждём {fw.seconds} сек.")
                    await asyncio.sleep(fw.seconds)
                    continue
                except Exception as ex:
                    logger.error(f"Ошибка при обработке сообщения {message_url}: {ex}")
                    continue

    except Exception as ex:
        logger.error(f"Ошибка выполнения парсера: {ex}")


# --- Получение комментариев и вложенных ответов --- #
async def get_comments_by_message_url(client, entity, message_id):
    logger = await setup_logger(name="get_comments_by_message_id", log_file="Parser_Utils.log")
    comments = []
    delay_count = 0  # Счетчик комментариев для задержки

    try:
        replies = client.iter_messages(entity, reply_to=message_id)
        async for reply in replies:
            if not reply.text:
                continue

            sender_username = reply.sender.username if reply.sender else "Admin"
            comment_data = {
                "username": sender_username,
                "text": reply.text,
                "date": reply.date,
                "replies": []
            }

            # Обработка вложенных ответов
            if hasattr(reply, 'replies') and reply.replies and getattr(reply.replies, 'replies', 0) > 0:
                try:
                    sub_replies = client.iter_messages(entity, reply_to=reply.id, limit=10000)
                    async for sub_reply in sub_replies:
                        if sub_reply.reply_to_msg_id != reply.id or not sub_reply.text:
                            continue

                        sub_sender_username = sub_reply.sender.username if sub_reply.sender else "Admin"
                        comment_data["replies"].append({
                            "username": sub_sender_username,
                            "text": sub_reply.text,
                            "date": sub_reply.date
                        })

                except FloodWaitError as fw:
                    logger.warning(f"FloodWaitError при получении вложенных ответов: ждём {fw.seconds} сек.")
                    await asyncio.sleep(fw.seconds)
                    return await get_comments_by_message_url(client, entity, message_id)

                except Exception as e:
                    if "The message ID used in the peer was invalid" in str(e):
                        logger.debug(f"Вложенные ответы для коммента {reply.id} не удалось получить (неверный ID): {e}")
                    else:
                        logger.warning(f"Ошибка при получении вложенных ответов на коммент {reply.id}: {e}")

            comments.append(comment_data)
            delay_count += 1

            if delay_count % 12 == 0:
                await asyncio.sleep(delay["get_comments_by_message_id"])

    except FloodWaitError as fw:
        logger.warning(f"FloodWaitError при получении комментариев: ждём {fw.seconds} сек.")
        await asyncio.sleep(fw.seconds)
        return await get_comments_by_message_url(client, entity, message_id)

    except Exception as e:
        if "The message ID used in the peer was invalid" not in str(e):
            logger.error(f"Не удалось получить комментарии к сообщению {message_id}: {e}")

    # Сортировка по дате
    comments.sort(key=lambda x: x['date'])
    for comment in comments:
        comment['replies'].sort(key=lambda x: x['date'])

    return comments
