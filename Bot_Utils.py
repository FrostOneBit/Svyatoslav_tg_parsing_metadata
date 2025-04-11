from Database_utils import create_default_password_for_admin, create_tables_for_db
from Logger_utils import setup_logger


# --- При старте бота запуск функций --- #
async def on_startup(dispatcher):
    await create_tables_for_db()
    await create_default_password_for_admin()