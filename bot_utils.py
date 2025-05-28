import asyncio

from config import Telethon_session_audience, Telethon_session_messages
from database import create_tables_for_db
from database_utils import create_default_password_for_admin, save_default_rules, get_admins_from_db, save_urls_message_in_db
from delay_times import delay
from logger_utils import setup_logger
from parser_utils import get_groups_by_url, parse_audience_by_group_id, parse_urls_by_rule
from folder_utils import create_folder, check_session_file_existence
from bot_settings import dp, bot, storage
from message_storage import BotMessage

background_process_task = None
background_audience_task = None


# --- При старте бота запуск функций --- #
async def on_startup(dispatcher):
    logger = await setup_logger(name="on_startup", log_file="bot_utils.log")

    try:
        await create_tables_for_db()
        await create_default_password_for_admin()
        await save_default_rules()
        await create_folder()

        global background_process_task, background_audience_task
        # Запускаем два отдельных воркера
        background_process_task = asyncio.create_task(background_process())  # для get_groups и parse_urls_by_rule
        # background_audience_task = asyncio.create_task(background_parse_audience_by_group_id())  # для parse_audience_by_group_id

    except Exception as ex:
        logger.error(f"[on_startup] Ошибка при инициализации: {ex}")


# --- Основной фоновый процесс --- #
async def background_process():
    logger = await setup_logger(name="background_worker", log_file="bot_utils.log")

    while True:
        try:

            if await check_session_file_existence(Telethon_session_messages):
                logger.info("Запуск save_urls_message_in_db")
                await save_urls_message_in_db()

                logger.info("Запуск parse_urls_by_rule()")
                await parse_urls_by_rule()
            else:
                await alert_admins_about_missing_session_message()

        except Exception as ex:
            logger.error(f"[background_worker] Ошибка при выполнении одной из задач: {ex}")

        # Ожидаем перед следующей итерацией
        await asyncio.sleep(delay['on_startup'])


# --- Отдельный воркер для парсинга аудитории --- #
async def background_parse_audience_by_group_id():
    logger = await setup_logger(name="audience_parser_worker", log_file="bot_utils.log")

    while True:
        try:

            logger.info("Запуск get_groups_by_url()")
            await get_groups_by_url()

            if await check_session_file_existence(Telethon_session_audience):
                logger.info("Запуск parse_audience_by_group_id() из отдельного воркера")
                await parse_audience_by_group_id()

        except Exception as ex:
            logger.error(f"[audience_parser_worker] Ошибка при парсинге аудитории: {ex}")

        await asyncio.sleep(delay["on_startup"])


# --- Оповещение администраторов о том что нет сессии --- #
async def alert_admins_about_missing_session_message():
    logger = await setup_logger(name="alert_admins_about_missing_session_message", log_file="bot_utils.log")

    try:

        list_user = await get_admins_from_db()
        if list_user:
            for telegram_id in list_user:
                await bot.send_message(telegram_id, BotMessage['NotFoundSessionMessage'])

    except Exception as ex:
        logger.error(ex)
