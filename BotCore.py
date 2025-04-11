import os
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.utils import executor
from cryptography.fernet import Fernet
from telethon import TelegramClient

from BotKeyboard import get_keyboard_general_admin
from Bot_Utils import on_startup
from Config import Telegram_token_api, Encryption_key

from Logger_utils import setup_logger
from MessageStorage import MessageCommand, TelethonMessage, MessageKeyboard
from Database_utils import check_user_in_db, register_user_in_db, save_telethon_data, get_hash_id_api
from StatesStorage import TelethonStates, TelethonSessionStates

storage = MemoryStorage()
bot = Bot(token=Telegram_token_api)
dp = Dispatcher(bot, storage=storage)


# --- Команда start --- #
@dp.message_handler(commands=["start"])
async def start_command(message: types.Message):
    logger = await setup_logger(name="start_command", log_file="BotCore.log")

    try:

        await message.answer(f"{MessageCommand['start']}")

    except Exception as ex:
        logger.error(ex)


# --- Команда регистрации --- #
@dp.message_handler(commands=["register"])
async def register_command(message: types.Message):
    logger = await setup_logger(name="register_command", log_file="BotCore.log")

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


@dp.message_handler(commands=["keyboard"])
async def command_keyboard(message: types.Message):
    logger = await setup_logger(name="command_keyboard", log_file="BotCore.log")

    try:

        get_keyboard = await get_keyboard_general_admin(message)
        if get_keyboard:
            await message.answer(MessageKeyboard['get_keyboard_admin'], reply_markup=get_keyboard)

    except Exception as ex:
        logger.error(ex)


# --- Проверка сессии --- #
@dp.message_handler(lambda message: message.text == "Проверить сессию")
async def command_check_session(message: types.Message):
    pass


# --- Добавление или обновление сессии telthon ---
@dp.message_handler(lambda message: message.text == "Обновить данные сессии")
async def command_update_session(message: types.Message):
    logger = await setup_logger(name="command_check_session", log_file="BotCore.log")

    try:

        await message.answer(TelethonMessage['start'])
        await message.answer(TelethonMessage['update_start'])
        await TelethonStates.hash_id_api.set()

    except Exception as ex:
        logger.error(ex)


# -- State TelethonState -- #
@dp.message_handler(state=TelethonStates.hash_id_api)
async def process_telethon_update_session(message: types.Message, state: FSMContext):
    logger = await setup_logger(name="process_telethon_update_session", log_file="BotCore.log")

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


# --- Команда подключения сессии --- #
@dp.message_handler(lambda message: message.text == "Подключить сессию")
async def command_connect_telethon_session(message: types.Message):
    logger = await setup_logger(name="command_connect_telethon_session", log_file="BotCore.log")

    try:
        await message.answer(TelethonMessage['connect_process_phone_number'])
        await TelethonSessionStates.phone_number.set()
    except Exception as ex:
        logger.error(ex)


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
        session_folder = "Session's"
        os.makedirs(session_folder, exist_ok=True)

        # Формируем путь к файлу сессии
        session_file = os.path.join(session_folder, f"{phone_number.replace('+', '')}.session")

        client = TelegramClient(
            session_file,
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
    logger = await setup_logger(name="process_connect_telethon_session_code_number", log_file="BotCore.log")

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
        session_file = os.path.join("Session's", f"{phone_number.replace('+', '')}.session")

        client = TelegramClient(
            session_file,
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

    except Exception as ex:
        logger.error(f"Ошибка при подключении: {ex}")
        await message.answer(TelethonMessage['connect_error'])
        try:
            os.remove(os.path.join("Session's", f"{phone_number.replace('+', '')}.session"))
        except Exception as ex:
            logger.error(ex)

    finally:
        await state.finish()

        # Отключаем сессию после использования
        await client.disconnect()


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
