from Database_utils import create_default_password_for_admin, create_tables_for_db


async def on_startup(dispatcher):
    await create_tables_for_db()
    await create_default_password_for_admin()