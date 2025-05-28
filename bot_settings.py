from config import Telegram_token_api

from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage


storage = MemoryStorage()
bot = Bot(token=Telegram_token_api)
dp = Dispatcher(bot, storage=storage)