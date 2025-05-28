from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

from database_utils import check_user_in_db
from logger_utils import setup_logger


# --- Клавиатура для админа --- #
async def get_keyboard_general_admin(message) -> ReplyKeyboardMarkup:
    logger = await setup_logger(name="keyboard_general_admin", log_file="bot_keyboard.log")

    try:
        """
        Если False - значит в базе данных уже есть такой пользователь и мы сможем выдать ему клавиатуру
        """
        if not await check_user_in_db(message):
            button_0 = KeyboardButton("Проверить сессию")
            button_1 = KeyboardButton("Обновить данные сессии")
            button_2 = KeyboardButton("Посмотреть пароль")
            button_3 = KeyboardButton("Поменять пароль")
            button_4 = KeyboardButton("Подключить сессию")

            keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
            keyboard.add(button_0, button_1, button_4)
            keyboard.add(button_2, button_3)

            return keyboard
        else:
            return False
    except Exception as ex:
        logger.error(ex)
