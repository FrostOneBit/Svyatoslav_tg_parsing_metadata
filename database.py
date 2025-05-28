import psycopg2

from config import (
    DB_host,
    DB_port,
    DB_name,
    DB_password,
    DB_user
)
from logger_utils import setup_logger


# --- Подключение к базе данных
async def create_connection_to_db():
    logger = await setup_logger(name="create_connection_to_db", log_file="database.log")
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


# --- Создание таблиц для бд --- #
async def create_tables_for_db():
    logger = await setup_logger(name="create_tables_for_db", log_file="database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        # -- Пользователи бота -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Users (
                Id SERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                username TEXT,
                password TEXT NOT NULL, --Либо код пришлашения
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Пароль -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Passwords (
                Id SERIAL PRIMARY KEY,
                Password VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Группы -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Groups (
                Id SERIAL PRIMARY KEY,
                title TEXT,
                group_id BIGINT UNIQUE,
                status BOOLEAN DEFAULT FALSE, --Подписан или нет
                status_audience BOOLEAN DEFAULT FALSE, --Парсинг аудитории
                type TEXT, -- Закрытая или открытая
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Ссылки на сообщения -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Urls_message (
                Id SERIAL PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                _rule_id INTEGER NOT NULL,
                start_of_counter BIGINT,
                end_of_counter BIGINT,
                status BOOLEAN DEFAULT TRUE,
                status_media BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Правила -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Rules (
                Id SERIAL PRIMARY KEY,
                rule_name TEXT NOT NULL,
                number_of_days INTEGER NOT NULL,
                frequency INTEGER NOT NULL, -- Частота  запросов раньше (every day)
                rule_counter BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Telethon -- #
        # - Сделать в зашифрованном виде - #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Telethon (
                Id SERIAL PRIMARY KEY,
                hash_api TEXT NOT NULL,
                hash_id TEXT NOT NULL,
                status BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        return True
    except Exception as ex:
        logger.error(ex)
        return False
    finally:
        connect.commit()
        cursor.close()
        connect.close()
