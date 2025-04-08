from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.utils import executor
from cryptography.fernet import Fernet

from BotKeyboard import get_keyboard_general_admin
from Bot_Utils import on_startup
from Config import Telegram_token_api, Encryption_key

from Logger_utils import setup_logger
from MessageStorage import MessageCommand, TelethonMessage
from Database_utils import check_user_in_db, register_user_in_db, save_telethon_data
from StatesStorage import TelethonStates

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
            await message.answer("Test", reply_markup=get_keyboard)

    except Exception as ex:
        logger.error(ex)

# --- Проверка сессии --- #
@dp.message_handler(lambda message: message.text == "Проверить сессию")
async def command_check_session(message: types.Message):
    pass

# --- Добавление или обновление сессии telthon ---
@dp.message_handler(lambda message: message.text == "Обновить сессию")
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
        else:
            await message.answer(TelethonMessage['update_error'])

    except Exception as ex:
        logger.error(ex)
    finally:
        await state.finish()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
