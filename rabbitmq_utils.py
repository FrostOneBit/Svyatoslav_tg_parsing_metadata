import pika
import re
import os
import json
import requests
import base64

from logger_utils import setup_logger
from config import swagger_url_links, swagger_token, swagger_platforms, swagger_headers, RB_username, RB_password, RB_host, RB_virtual_host, RB_port, Rb_queue_name


# --- Получение ссылок из RabbitMQ --- #
async def get_urls_message_rabbitmq():
    logger = await setup_logger(name='get_urls_message_rabbitmq', log_file="rabbitmq_utils.log")

    try:
        message_group = []

        # Сбор данных для каждой социальной платформы
        for social_platform in swagger_platforms:
            logger.info(f"Собираем ссылки для платформы: {social_platform}...")
            query_params = {
                "socialMediaType": social_platform,
                "token": swagger_token
            }

            try:
                response = requests.get(swagger_url_links, headers=swagger_headers, params=query_params)
                if response.status_code == 200:
                    platform_links = response.json()
                    message_group.extend(platform_links)  # Добавляем ссылки в общий список
                else:
                    logger.error(f"Ошибка получения данных для {social_platform}: {response.status_code}, {response.text}")
            except Exception as request_exception:
                logger.error(f"Ошибка запроса для {social_platform}: {request_exception}")

            if message_group:
                logger.info(f"Успешно собрано {len(message_group)} ссылок для платформы {social_platform}")
            else:
                logger.warning("Не найдено данных для записи в БД.")

        # Фильтрация: оставляем только ссылки, похожие на сообщения Telegram
        # Ожидаемый формат: https://t.me/<channel>/(<message_id>)[?параметры]
        telegram_link_pattern = re.compile(r"^https?://t\.me/(?:c/)?[\w\-_]+/\d+(?:\?.+)?$")
        filtered_links = [link for link in message_group if telegram_link_pattern.match(link)]
        logger.info(f"После фильтрации осталось {len(filtered_links)} ссылок, похожих на Telegram-сообщения.")
        return filtered_links

    except Exception as general_exception:
        logger.error(f"Ошибка в fetch_social_links: {general_exception}")
        return []


# --- Отправка данных о сообщении в очередь RabbitMQ --- #
async def send_json_message_to_rabbitmq(message_data):
    logger = await setup_logger(name="send_json_to_rabbitmq", log_file="rabbitmq_utils.log")

    try:
        # Подключение к RabbitMQ
        credentials = pika.PlainCredentials(RB_username, RB_password)
        parameters = pika.ConnectionParameters(
            host=RB_host,
            port=RB_port,
            virtual_host=RB_virtual_host,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=Rb_queue_name, durable=True)

        # Обработка комментариев
        comments = [
            {
                "username": comment.get("username", ""),
                "text": comment.get("text", "")
            }
            for comment in message_data.get("comments", [])
        ]

        # Обработка медиафайлов
        media_raw = message_data.get("media_path", [])
        if isinstance(media_raw, str):
            media_paths = [p.strip() for p in media_raw.split(",")] if media_raw else []
        elif isinstance(media_raw, list):
            media_paths = media_raw
        else:
            media_paths = []

        media_files = []
        for path in media_paths:
            try:
                media_files.append({
                    "filename": os.path.basename(path),
                    "data": encode_image_to_base64(path)
                })
            except Exception as e:
                logger.warning(f"Не удалось прочитать файл {path}: {e}")

        # Формирование сообщения
        formatted_message = {
            "type": "MESSAGE",
            "url": message_data.get("url", ""),
            "message_id": str(message_data.get("message_id", "")),
            "text": message_data.get("text", ""),
            "date": message_data.get("date", ""),
            "sender_id": str(message_data.get("sender_id", "")),
            "reactions": str(message_data.get("reactions", 0)),
            "views": str(message_data.get("views", 0)),
            "media_files": media_files,
            "comments": comments
        }

        # Отправка в очередь
        channel.basic_publish(
            exchange='',
            routing_key=Rb_queue_name,
            body=json.dumps(formatted_message, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        # Сохраняем локально (опционально)
        save_folder = "Files/json"
        os.makedirs(save_folder, exist_ok=True)
        raw_url = formatted_message['url'].replace('https://t.me/', '')
        file_name = f"{raw_url.replace('/', '_')}.json"
        file_path = os.path.join(save_folder, file_name)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(formatted_message, f, ensure_ascii=False, indent=2)

        logger.info(f"Сообщение отправлено в RabbitMQ и сохранено в '{file_path}'.")
        connection.close()

    except Exception as ex:
        logger.error(f"Ошибка при отправке данных в RabbitMQ: {ex}")


# --- Кодировка изображения в base64 --- #
def encode_image_to_base64(path):
    with open(path, "rb") as f:
        code = base64.b64encode(f.read()).decode("utf-8")
        return code


# --- Отправка данных о группе в очередь RabbitMQ --- #
async def send_audience_to_rabbitmq(data):
    logger = await setup_logger(name="send_audience_to_rabbitmq", log_file="rabbitmq_utils.log")

    try:
        group_id = data.get("group_id", "unknown")
        save_folder = "Files/json"
        os.makedirs(save_folder, exist_ok=True)

        # Сохраняем данные в JSON файл
        file_path = os.path.join(save_folder, f"audience_{group_id}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"✅ Аудитория сохранена в файл: {file_path}")

        # Отправка в RabbitMQ
        credentials = pika.PlainCredentials(RB_username, RB_password)
        parameters = pika.ConnectionParameters(
            host=RB_host,
            port=RB_port,
            virtual_host=RB_virtual_host,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=Rb_queue_name, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=Rb_queue_name,
            body=json.dumps(data, ensure_ascii=False),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        logger.info(f"📤 Аудитория отправлена в RabbitMQ (group_id={group_id})")
        connection.close()

    except Exception as ex:
        logger.error(f"❌ Ошибка при отправке аудитории в RabbitMQ: {ex}")