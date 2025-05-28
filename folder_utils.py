import os
import shutil
import asyncio
from database_utils import get_hash_id_api
from logger_utils import setup_logger
from config import Telethon_session_messages, Telethon_session_audience

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError


# --- Создание папок --- #
async def create_folder():
    logger = await setup_logger(name="create_folder", log_file="folder_utils.log")

    try:

        folders = [
            "Files/images",
            "Files/json",
            "Files/logs",
            "Session's"
        ]

        for folder in folders:
            os.makedirs(folder, exist_ok=True)

    except Exception as ex:
        logger.error(ex)


# --- Проверка на существование файла сессии --- #
async def check_session_file_existence(session_name):
    logger = await setup_logger(name="check_session_file_existence", log_file="folder_utils.log")

    try:
        # Проверяем, существует ли файл сессии
        if not os.path.exists(session_name):
            logger.warning(f"⚠️ Файл сессии НЕ найден: {session_name}")
            return False

        # Получаем необходимые идентификаторы для API из базы данных
        hash_id, hash_api = await get_hash_id_api()

        # Используем async context manager, чтобы TelegramClient автоматически подключался и отключался
        async with TelegramClient(session_name, hash_id, hash_api) as client:
            # Если клиент не авторизован, возвращаем False
            if not await client.is_user_authorized():
                logger.warning("⚠️ Сессия загружена, но пользователь не авторизован.")
                return False

            # Получаем информацию о пользователе
            me = await client.get_me()
            logger.info(f"✅ Сессия активна. Пользователь: @{me.username or me.id}")

        return True

    except SessionPasswordNeededError:
        logger.warning("⚠️ Необходим пароль двухфакторной аутентификации (2FA). Сессия требует повторной авторизации.")
        return False
    except Exception as ex:
        logger.error(f"❌ Ошибка при проверке работоспособности сессии: {ex}")
        return False


# --- Копирование файла сессии telegram --- #
async def copy_telegram_session_file():
    logger = await setup_logger(name="copy_telegram_session_file", log_file="folder_utils.log")

    try:

        if not os.path.exists(Telethon_session_messages):
            logger.error(f"Файл '{Telethon_session_messages}' не найден!")
            return

        await asyncio.to_thread(shutil.copy, Telethon_session_messages, Telethon_session_audience)
        logger.info(f"Файл: {Telethon_session_messages} успешно скопирован")
    except Exception as ex:
        logger.error(ex)


# --- Удалении файлов сессии --- #
async def delete_telegram_session_files():
    logger = await setup_logger(name="delete_telegram_session_files", log_file="folder_utils.log")

    try:
        files_to_delete = [
            Telethon_session_messages,
            Telethon_session_audience
        ]

        for file_path in files_to_delete:
            if os.path.exists(file_path):
                await asyncio.to_thread(os.remove, file_path)
                logger.info(f"Файл '{file_path}' успешно удалён.")
            else:
                logger.warning(f"Файл '{file_path}' не найден.")
    except Exception as ex:
        logger.error(f"[delete_telegram_session_files] Ошибка при удалении: {ex}")
