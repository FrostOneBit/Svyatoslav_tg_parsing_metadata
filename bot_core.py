import os
import asyncio

import bot_utils
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.utils import executor
from cryptography.fernet import Fernet
from telethon import TelegramClient

from bot_keyboard import get_keyboard_general_admin
from bot_utils import on_startup
from config import Encryption_key, Telethon_session_messages, Telethon_session_audience
from bot_settings import dp
from folder_utils import copy_telegram_session_file, delete_telegram_session_files, check_session_file_existence

from logger_utils import setup_logger
from message_storage import MessageCommand, TelethonMessage, MessageKeyboard
from database_utils import check_user_in_db, register_user_in_db, save_telethon_data, get_hash_id_api, check_password, change_password_for_admin
from states_storage import TelethonStates, TelethonSessionStates, ChangePassword


# --- Команда start --- #
@dp.message_handler(commands=["start"])
async def start_command(message: types.Message):
    logger = await setup_logger(name="start_command", log_file="bot_core.log")

    try:

        await message.answer(f"{MessageCommand['start']}")

    except Exception as ex:
        logger.error(ex)


# --- Команда регистрации --- #
@dp.message_handler(commands=["register"])
async def register_command(message: types.Message):
    logger = await setup_logger(name="register_command", log_file="bot_core.log")

    try:

        command_arg = message.get_args()

        if len(command_arg) == 0:
            await message.answer(f"{MessageCommand['register_empty']}")
            return

        if not await check_user_in_db(message):
            await message.answer(f"{MessageCommand['register_repeat']}")
            return

        result_register_user_in_db = await register_user_in_db(message, command_arg)
        if result_register_user_in_db:
            await message.answer(f"{MessageCommand['register_success']}")
    except Exception as ex:
        logger.error(ex)


# --- Вызов клавиатуры --- #
@dp.message_handler(commands=["keyboard"])
async def command_keyboard(message: types.Message):
    logger = await setup_logger(name="command_keyboard", log_file="bot_core.log")

    try:

        get_keyboard = await get_keyboard_general_admin(message)
        if get_keyboard:
            await message.answer(MessageKeyboard['get_keyboard_admin'], reply_markup=get_keyboard)

    except Exception as ex:
        logger.error(ex)


# --- Просмотр пароля --- #
@dp.message_handler(lambda message: message.text == "Посмотреть пароль")
async def command_check_password(message: types.Message):
    logger = await setup_logger(name="command_check_password", log_file="bot_core.log")

    try:

        result = await check_password()
        await message.answer(f"Пароль к админки: {result[1]}")

    except Exception as ex:
        logger.error(ex)


# --- Смена пароля --- #
@dp.message_handler(lambda message: message.text == "Поменять пароль")
async def command_change_password(message: types.Message):
    logger = await setup_logger(name="command_change_password", log_file="bot_core.log")

    try:

        await message.answer("Укажите новый пароль")
        await ChangePassword.password.set()

    except Exception as ex:
        logger.error(ex)


# --- Проверка сессии --- #
@dp.message_handler(lambda message: message.text == "Проверить сессию")
async def command_check_session(message: types.Message):
    logger = await setup_logger(name="command_check_session", log_file="bot_core.log")

    try:

        await cancel_background_tasks()

        if await check_session_file_existence(Telethon_session_messages):
            await message.answer("Сессия для сообщений активна:")
            if await check_session_file_existence(Telethon_session_audience):
                await message.answer("Сессия для аудитории активна:")

        await on_startup(dp)

    except Exception as ex:
        logger.error(ex)


# --- Добавление или обновление сессии telethon ---
@dp.message_handler(lambda message: message.text == "Обновить данные сессии")
async def command_update_session(message: types.Message):
    logger = await setup_logger(name="command_check_session", log_file="bot_core.log")

    try:

        await message.answer(TelethonMessage['start'])
        await message.answer(TelethonMessage['update_start'])
        await TelethonStates.hash_id_api.set()

    except Exception as ex:
        logger.error(ex)


# --- State ChangePassword --- #
@dp.message_handler(state=ChangePassword.password)
async def process_change_password_for_admin(message: types.Message, state: FSMContext):
    logger = await setup_logger(name="process_change_password_for_admin", log_file="bot_core.log")

    try:
        password = message.text.strip()
        await state.update_data(password=password)

        if await change_password_for_admin(password):
            await message.answer("Вы успешно изменили пароль!")
        else:
            await message.answer("Не удалось изменить пароль. Попробуйте снова.")

    except Exception as ex:
        logger.error(f"Ошибка при смене пароля: {ex}")
        await message.answer("Произошла ошибка. Попробуйте позже.")

    finally:
        await state.finish()


# -- State TelethonState -- #
@dp.message_handler(state=TelethonStates.hash_id_api)
async def process_telethon_update_session(message: types.Message, state: FSMContext):
    logger = await setup_logger(name="process_telethon_update_session", log_file="bot_core.log")

    try:
        if not Encryption_key:
            raise ValueError("Ключ шифрования отсутствует в .env!")

        fernet = Fernet(Encryption_key)

        # Ожидаем ввод: "hash_id hash_api"
        user_input = message.text.split()
        if len(user_input) < 2:
            await message.answer("Ошибка: Пожалуйста, введите `hash_id` и `hash_api`, разделённые пробелом.")
            return

        # Шифруем данные
        encrypted_hash_id = fernet.encrypt(user_input[0].encode()).decode()
        encrypted_hash_api = fernet.encrypt(user_input[1].encode()).decode()

        # Сохраняем зашифрованные данные
        await state.update_data(hash_id=encrypted_hash_id, hash_api=encrypted_hash_api)
        data = await state.get_data()

        if await save_telethon_data(data):
            await message.answer(TelethonMessage['update_success'])
            await message.answer(TelethonMessage['connect_start'])
        else:
            await message.answer(TelethonMessage['update_error'])

    except Exception as ex:
        logger.error(ex)
    finally:
        await state.finish()


@dp.message_handler(lambda message: message.text == "Подключить сессию")
async def command_connect_telethon_session(message: types.Message):
    logger = await setup_logger(name="command_connect_telethon_session", log_file="bot_core.log")
    try:

        await cancel_background_tasks()

        await delete_telegram_session_files()

        # Теперь можно инициировать подключение сессии
        await message.answer("Введите номер телефона для подключения сессии:")
        await TelethonSessionStates.phone_number.set()

    except Exception as ex:
        logger.error(f"Ошибка в команде подключения сессии: {ex}")


# --- Ввод номера телефона --- #
@dp.message_handler(state=TelethonSessionStates.phone_number)
async def process_connect_telethon_session_phone_number(message: types.Message, state: FSMContext):
    logger = await setup_logger(name="process_connect_telethon_session", log_file="BotCore.log")

    try:
        phone_number = message.text.strip()
        await state.update_data(phone_number=phone_number)

        # Получаем параметры API
        hash_id, hash_api = await get_hash_id_api()

        # Путь к папке "Session's"

        # Формируем путь к файлу сессии

        client = TelegramClient(
            Telethon_session_messages,
            hash_id,
            hash_api,
            system_version="4.16.30-vxCUSTOM",
            device_model="CustomDevice",
            app_version="1.0.0"
        )

        await client.connect()
        phone_code = await client.send_code_request(phone_number)
        phone_code_hash = phone_code.phone_code_hash

        # Сохраняем hash в FSM
        await state.update_data(phone_code_hash=phone_code_hash)

        await message.answer(TelethonMessage['connect_process_code_number'])
        await TelethonSessionStates.next()

    except Exception as ex:
        logger.error(f"Ошибка при отправке кода: {ex}")
        await message.answer(TelethonMessage['connect_error'])
        await state.finish()


# --- Ввод кода подтверждения --- #
@dp.message_handler(state=TelethonSessionStates.code_number)
async def process_connect_telethon_session_code_number(message: types.Message, state: FSMContext):
    logger = await setup_logger(name="process_connect_telethon_session_code_number", log_file="bot_core.log")

    try:
        raw_code = message.text.strip()

        # Преобразуем код из XX-XXX в XXXXX
        code_number = ''.join(filter(str.isdigit, raw_code))
        if len(code_number) != 5:
            await message.answer("Неверный формат кода. Пожалуйста, отправьте в формате XX-XXX.")
            return

        data = await state.get_data()
        phone_number = data['phone_number']
        phone_code_hash = data['phone_code_hash']

        hash_id, hash_api = await get_hash_id_api()

        client = TelegramClient(
            Telethon_session_messages,
            hash_id,
            hash_api,
            system_version="4.16.30-vxCUSTOM",
            device_model="CustomDevice",
            app_version="1.0.0"
        )

        # Подключаемся к клиенту
        await client.connect()

        # Пытаемся войти в сессию с кодом
        await client.sign_in(phone=phone_number, code=code_number, phone_code_hash=phone_code_hash)

        # После успешного подключения
        await message.answer(TelethonMessage['connect_success'])

        await copy_telegram_session_file()

        await client.disconnect()

        # -- Включение функций -- #
        await on_startup(dp)

    except Exception as ex:
        logger.error(f"Ошибка при подключении: {ex}")
        await message.answer(TelethonMessage['connect_error'])
        try:
            os.remove(os.path.join(Telethon_session_messages))
        except Exception as ex:
            logger.error(ex)

    finally:
        await state.finish()

        # Отключаем сессию после использования
        await client.disconnect()


# --- Отключение функций --- #
async def cancel_background_tasks():
    logger = await setup_logger(name="cancel_background_tasks", log_file="bot_utils.log")

    if bot_utils.background_process_task is not None and not bot_utils.background_process_task.done():
        bot_utils.background_process_task.cancel()
        try:
            await bot_utils.background_process_task
        except asyncio.CancelledError:
            logger.info("Фоновый процесс background_process успешно остановлен.")
        bot_utils.background_process_task = None

    if bot_utils.background_audience_task is not None and not bot_utils.background_audience_task.done():
        bot_utils.background_audience_task.cancel()
        try:
            await bot_utils.background_audience_task
        except asyncio.CancelledError:
            logger.info("Фоновый процесс background_parse_audience_by_group_id успешно остановлен.")
        bot_utils.background_audience_task = None


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
