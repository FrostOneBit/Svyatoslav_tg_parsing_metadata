import asyncio
import threading

from config import Telethon_session_audience, Telethon_session_messages
from database import create_tables_for_db
from database_utils import (
    create_default_password_for_admin,
    save_default_rules,
    get_admins_from_db,
    save_urls_message_in_db
)
from delay_times import delay
from logger_utils import setup_logger
from parser_utils import (
    get_groups_by_url,
    parse_audience_by_group_id,
    parse_urls_by_rule
)
from folder_utils import create_folder, check_session_file_existence
from bot_settings import dp, bot, storage
from message_storage import BotMessage

# Глобальные переменные
should_stop = False
background_process_thread = None
background_audience_thread = None


def start_async_task(coro_func):
    """Создает отдельный event loop в новом потоке."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coro_func)
    loop.close()


async def interruptible_sleep(duration):
    """Sleep, прерываемый по флагу should_stop."""
    end_time = asyncio.get_event_loop().time() + duration
    while True:
        remaining = end_time - asyncio.get_event_loop().time()
        if remaining <= 0 or should_stop:
            break
        await asyncio.sleep(min(0.5, remaining))


async def background_process():
    logger = await setup_logger(name="background_worker", log_file="bot_utils.log")
    logger.info("Фоновый процесс 'background_process' запущен.")
    while not should_stop:
        try:
            logger.info("Запуск save_urls_message_in_db")
            await save_urls_message_in_db()

            logger.info("Запуск parse_urls_by_rule")
            await parse_urls_by_rule()
        except Exception as ex:
            logger.error(f"[background_worker] Ошибка выполнения: {ex}")
        await interruptible_sleep(delay['on_startup'])
    logger.info("Фоновый процесс 'background_process' останавливается.")


async def background_parse_audience_by_group_id():
    logger = await setup_logger(name="audience_parser_worker", log_file="bot_utils.log")
    logger.info("Фоновый процесс 'background_parse_audience_by_group_id' запущен.")
    while not should_stop:
        try:
            logger.info("Запуск get_groups_by_url")
            await get_groups_by_url()

            logger.info("Запуск parse_audience_by_group_id")
            await parse_audience_by_group_id()
        except Exception as ex:
            logger.error(f"[audience_parser_worker] Ошибка выполнения: {ex}")
        await interruptible_sleep(delay["on_startup"])
    logger.info("Фоновый процесс 'background_parse_audience_by_group_id' останавливается.")


async def alert_admins_about_missing_session_message():
    logger = await setup_logger(name="alert_admins_about_missing_session_message", log_file="bot_utils.log")
    try:
        admins = await get_admins_from_db()
        if admins:
            for telegram_id in admins:
                await bot.send_message(telegram_id, BotMessage['NotFoundSessionMessage'])
    except Exception as ex:
        logger.error(f"Ошибка оповещения администраторов: {ex}")


async def on_startup(dispatcher):
    logger = await setup_logger(name="on_startup", log_file="bot_utils.log")
    try:
        await create_tables_for_db()
        await create_default_password_for_admin()
        await save_default_rules()
        await create_folder()

        global background_process_thread, background_audience_thread, should_stop

        if await check_session_file_existence(Telethon_session_messages):
            should_stop = False

            background_process_thread = threading.Thread(
                target=start_async_task, args=(background_process(),), daemon=True
            )
            background_process_thread.start()

            background_audience_thread = threading.Thread(
                target=start_async_task, args=(background_parse_audience_by_group_id(),), daemon=True
            )
            background_audience_thread.start()
        else:
            await alert_admins_about_missing_session_message()

    except Exception as ex:
        logger.error(f"[on_startup] Ошибка при инициализации: {ex}")

async def cancel_background_tasks():
    logger = await setup_logger(name="cancel_background_tasks", log_file="bot_utils.log")
    try:
        global background_process_thread, background_audience_thread, should_stop

        should_stop = True
        loop = asyncio.get_running_loop()

        # Список всех активных потоков
        threads_to_join = []

        if background_process_thread is not None:
            logger.info("Ожидание завершения потока background_process_thread")
            threads_to_join.append(background_process_thread)
            background_process_thread = None

        if background_audience_thread is not None:
            logger.info("Ожидание завершения потока background_audience_thread")
            threads_to_join.append(background_audience_thread)
            background_audience_thread = None

        # Ожидаем завершения всех потоков параллельно
        await asyncio.gather(*[
            loop.run_in_executor(None, thread.join)
            for thread in threads_to_join
        ])

        logger.info("Все фоновые потоки успешно остановлены.")
        return True

    except Exception as ex:
        logger.error(f"Ошибка при остановке фоновых потоков: {ex}")
        return False
