from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor

from Bot_Utils import on_startup
from Config import Telegram_token_api

from Logger_utils import setup_logger
from MessageStorage import MessageCommand
from Database_utils import check_user_in_db, register_user_in_db

storage = MemoryStorage()
bot = Bot(token=Telegram_token_api)
dp = Dispatcher(bot)


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


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
