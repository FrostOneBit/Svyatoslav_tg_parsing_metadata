from Database import create_connection_to_db
from Logger_utils import setup_logger
from Config import Default_password


# --- Создание таблиц для бд --- #
async def create_tables_for_db():
    logger = await setup_logger(name="create_tables_for_db", log_file="Database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

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

        # -- Ссылки на сообщения -- #
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Url_messages (
                Id SERIAL PRIMARY KEY,
                url TEXT NOT NULL,
                _rule_id INTEGER NOT NULL,
                rule_counter BIGINT NOT NULL,
                _group_id BIGINT,
                status BOOLEAN DEFAULT TRUE,
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
                Rulename TEXT NOT NULL,
                number_day INTEGER NOT NULL,
                every_day INTEGER NOT NULL,
                rule_counter BIGINT,
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
                group_id BIGINT,
                status BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # -- Telethon -- #
        # - Сделать в зашифрованом виде - #
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


# --- Проверка человека на регистрацию --- #
async def check_user_in_db(message):
    logger = await setup_logger(name="check_user_in_db", log_file="Database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute(f"SELECT * FROM Users WHERE telegram_id = {message.from_user.id}")
        get_user_db = cursor.fetchone()

        if get_user_db:
            return False
        else:
            return True

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Занесения пароля в базу данных с проверкой --- #
async def create_default_password_for_admin():
    logger = await setup_logger(name="create_default_password_for_admin", log_file="Database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()

        # Проверка существования пароля
        cursor.execute("SELECT * FROM Passwords WHERE Password = %s;", (Default_password,))
        result = cursor.fetchone()  # Получить первую найденную запись

        if result:
            # Если пароль уже существует
            logger.info("Пароль уже существует в базе данных.")
        else:
            # Если пароль не найден, вставляем его
            cursor.execute("INSERT INTO Passwords (Password) VALUES (%s);", (Default_password,))
            logger.info("Пароль успешно добавлен.")

        return True

    except Exception as ex:
        logger.error(f"Ошибка при работе с базой данных: {ex}")
        return False

    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Проверка на валидность пароля --- #
async def password_verification(message_arg):
    logger = await setup_logger(name="password_verification", log_file="Database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute("SELECT * FROM Passwords WHERE Password = %s;", (message_arg,))
        get_password_db = cursor.fetchone()
        if get_password_db is not None:
            return True
        else:
            return False

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Регистрация пользователя --- #
async def register_user_in_db(message, message_arg):
    logger = await setup_logger(name="register_user_in_db", log_file="Database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()
        check_password_verification = await password_verification(message_arg)

        if check_password_verification:
            username = message.from_user.username if message.from_user.username else "unknown"
            cursor.execute("INSERT INTO Users (telegram_id, username, password) VALUES (%s, %s, %s);", (
                message.from_user.id, username, message_arg
            ))
            return True
        else:
            return False
    except Exception as ex:
        logger.error(ex)
        return False
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Сохранение или обновление данных в БД --- #
async def save_telethon_data(data):
    logger = await setup_logger(name="save_telethon_data", log_file="Database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()

        # Проверяем, существует ли запись с данным hash_id
        cursor.execute("SELECT * FROM Telethon WHERE hash_id = %s;", (data['hash_id'],))
        existing_record = cursor.fetchone()  # Получаем первую запись или None

        if existing_record:
            # Если запись существует, обновляем её
            cursor.execute("UPDATE Telethon SET hash_api = %s WHERE hash_id = %s;", (data['hash_api'], data['hash_id']))
        else:
            # Если записи нет, вставляем новую
            cursor.execute("INSERT INTO Telethon (hash_id, hash_api) VALUES (%s, %s);",(data['hash_id'], data['hash_api']))

        return True

    except Exception as ex:
        logger.error(f"Ошибка при сохранении данных: {ex}")
        return False
    finally:
        connect.commit()
        cursor.close()
        connect.close()