import psycopg2

from Config import (
    DB_host,
    DB_port,
    DB_name,
    DB_password,
    DB_user
)
from Logger_utils import setup_logger

# --- Подключение к базе данных
async def create_connection_to_db():
    logger = await setup_logger(name="create_connection_to_db", log_file="Database.log")
    try:

        connect = psycopg2.connect(
            host=DB_host,
            port=DB_port,
            database=DB_name,
            user=DB_user,
            password=DB_password
        )
        cursor = connect.cursor()

        return connect, cursor

    except Exception as ex:
        logger.error(ex)
        return None, None