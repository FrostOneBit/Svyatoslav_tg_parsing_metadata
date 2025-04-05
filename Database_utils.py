from Database import create_connection_to_db
from Logger_utils import setup_logger


# --- Создание таблиц для бд --- #
async def create_tables_for_db():
    logger = await setup_logger(name="create_tables_for_db", log_file="Database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        # -- Роли доступа -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Roles (
                Id INTEEGER PRIMARY KEY,
                Rolename TEXT NOT NULL,
                Rolelevel TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Пользователи бота -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Users (
                Id INTEGER PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                username TEXT,
                _role_id INTEGER NOT NULL,
                password TEXT NOT NULL, --Либо код пришлашения
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Коды приглашения -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Codes (
                Id INTEGER PRIMARY KEY,
                _user_id BIGINT NOT NULL,
                code TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Ссылки на сообщения -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Url_messages (
                Id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                _rule_id INTEGER NOT NULL,
                rule_counter BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Правила -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Rules (
                Id INTEGER PRIMARY KEY,
                Rulename TEXT NOT NULL,
                number_day INTEGER NOT NULL,
                every_day INTEGER NOT NULL,
                rule_counter BIGINT,
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