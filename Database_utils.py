from cryptography.fernet import Fernet

from Database import create_connection_to_db
from Logger_utils import setup_logger
from Config import Default_password, Encryption_key


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
                title TEXT,
                group_id BIGINT,
                status BOOLEAN DEFAULT FALSE, --Подписан или нет
                type TEXT,
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

        # Получаем первую запись в таблице
        cursor.execute("SELECT * FROM Telethon LIMIT 1;")
        existing_record = cursor.fetchone()  # Получаем первую запись или None

        if existing_record:
            # Если запись существует, обновляем её, используя PRIMARY KEY или id
            cursor.execute("UPDATE Telethon SET hash_id = %s, hash_api = %s WHERE id = %s;",
                           (data['hash_id'], data['hash_api'], existing_record[0]))
        else:
            # Если таблица пуста, вставляем новую запись
            cursor.execute("INSERT INTO Telethon (hash_id, hash_api) VALUES (%s, %s);",
                           (data['hash_id'], data['hash_api']))

        return True

    except Exception as ex:
        logger.error(f"Ошибка при сохранении данных: {ex}")
        return False

    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Получение hash_id, hash_api --- #
async def get_hash_id_api():
    logger = await setup_logger(name="get_hash_id_api", log_file="Database_utils.log")

    try:
        # Инициализируем объект шифрования с использованием ключа
        fernet = Fernet(Encryption_key)

        connect, cursor = await create_connection_to_db()

        # Получаем данные из базы
        cursor.execute("SELECT * FROM Telethon LIMIT 1;")
        get_hash_id_api = cursor.fetchone()

        if get_hash_id_api:
            # Извлекаем зашифрованные данные
            encrypted_hash_id = get_hash_id_api[2]
            encrypted_hash_api = get_hash_id_api[1]

            # Расшифровываем данные
            hash_id = fernet.decrypt(encrypted_hash_id.encode()).decode()
            hash_api = fernet.decrypt(encrypted_hash_api.encode()).decode()

            return hash_id, hash_api

    except Exception as ex:
        logger.error(f"Ошибка при получении данных: {ex}")
        return None, None
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Сохранение группы в бд с проверкой дубликатов и обновлением --- #
async def save_group_in_db(results):
    logger = await setup_logger(name="save_group_in_db", log_file="Database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()

        for result in results:
            parts = result.split(",")
            channel_id = parts[0].split(":")[1].strip()
            channel_name = parts[1].split(":")[1].strip()
            status = parts[2].split(":")[1].strip()
            channel_type = parts[3].split(":")[1].strip()

            # Проверяем, существует ли уже запись с таким channel_id
            cursor.execute("SELECT * FROM Groups WHERE group_id = %s;", (channel_id,))
            existing_group = cursor.fetchone()

            if existing_group:
                # Если запись существует, проверяем, изменился ли channel_name или channel_type
                existing_channel_name = existing_group[0]  # assuming the name is the first column
                existing_channel_type = existing_group[3]  # assuming type is the fourth column

                # Если channel_name или channel_type изменился, обновляем
                if existing_channel_name != channel_name or existing_channel_type != channel_type:
                    cursor.execute("""
                        UPDATE Groups 
                        SET title = %s, type = %s
                        WHERE group_id = %s;
                    """, (channel_name, channel_type, channel_id))
            else:
                # Если записи нет, добавляем новую
                cursor.execute("""
                    INSERT INTO Groups (title, group_id, status, type) 
                    VALUES (%s, %s, %s, %s);
                """, (channel_name, channel_id, status, channel_type))
                logger.info(f"Group {channel_id} added to DB.")

    except Exception as ex:
        logger.error(f"Error during database operation: {ex}")
    finally:
        connect.commit()
        cursor.close()
        connect.close()