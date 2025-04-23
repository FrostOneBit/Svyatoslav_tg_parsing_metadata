import asyncio
from cryptography.fernet import Fernet

from datetime import datetime, timedelta
from Database import create_connection_to_db
from Logger_utils import setup_logger
from Config import Default_password, Encryption_key
from RuleStorage import basic_rule_settings


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
                start_of_counter BIGINT,
                end_of_counter BIGINT,
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
                rule_name TEXT NOT NULL,
                number_of_days INTEGER NOT NULL,
                frequency INTEGER NOT NULL, -- Частота  запросов раньше (every day)
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


# --- Сохранение группы в БД с возвратом ID, URL и статуса --- #
async def save_group_in_db(groups_data):
    logger = await setup_logger("save_group_in_db", "Database_utils.log")
    saved_groups = []

    try:
        connect, cursor = await create_connection_to_db()

        for group in groups_data:
            required_keys = ("channel_id", "channel_name", "status", "channel_type", "message_url")
            if not all(k in group for k in required_keys):
                logger.warning(f"Пропущены поля в структуре: {group}")
                continue

            channel_id = group["channel_id"]
            channel_name = group["channel_name"]
            status = group["status"]
            channel_type = group["channel_type"]
            message_url = group["message_url"]

            cursor.execute(
                "SELECT id, title, type, status FROM Groups WHERE group_id = %s;",
                (channel_id,)
            )
            existing = cursor.fetchone()

            if existing:
                group_db_id, existing_name, existing_type, existing_status = existing

                # Обновляем, если изменилось хотя бы одно из полей
                if (existing_name != channel_name or
                        existing_type != channel_type or
                        str(existing_status).lower() != str(status).lower()):
                    cursor.execute(
                        "UPDATE Groups SET title = %s, type = %s, status = %s WHERE group_id = %s;",
                        (channel_name, channel_type, status, channel_id)
                    )
                    logger.info(
                        f"Обновлена группа: id={channel_id}, name={channel_name}, "
                        f"type={channel_type}, status={status}"
                    )
            else:
                cursor.execute(
                    "INSERT INTO Groups (title, group_id, status, type) VALUES (%s, %s, %s, %s) RETURNING id;",
                    (channel_name, channel_id, status, channel_type)
                )
                group_db_id = cursor.fetchone()[0]
                logger.info(f"Добавлена группа: db_id={group_db_id}, url={message_url}, id={channel_id}")

            saved_groups.append({
                "group_db_id": group_db_id,
                "message_url": message_url,
                "status": status
            })

    except Exception as ex:
        logger.error(f"Ошибка при сохранении группы: {ex}")
    finally:
        connect.commit()
        cursor.close()
        connect.close()

    return saved_groups


# --- Запись сообщений в базу данных с привязкой к группе --- #
async def save_url_message_in_db(results):
    logger = await setup_logger(name="save_url_message", log_file="Database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()

        try:
            cursor.execute("SELECT id, number_of_days, rule_counter FROM Rules WHERE id = 1;")
            rule_data = cursor.fetchone()

            if not rule_data:
                logger.error("Не найдены настройки правил с id=1.")
                return

            rule_id, number_of_days, rule_counter = rule_data
            rule_counter_message = number_of_days * rule_counter

            for result in results:
                group_id = result["group_db_id"]
                message_url = result["message_url"]
                new_status = result["status"]

                # Проверка на существование URL
                cursor.execute("SELECT status FROM Url_messages WHERE url = %s;", (message_url,))
                existing_entry = cursor.fetchone()

                if existing_entry:
                    existing_status = str(existing_entry[0]).lower()
                    if str(new_status).lower() != existing_status:
                        cursor.execute(
                            "UPDATE Url_messages SET status = %s WHERE url = %s;",
                            (new_status, message_url)
                        )
                        logger.info(f"Обновлен статус для URL: {message_url}")
                    else:
                        logger.debug(f"URL уже существует и статус не изменился: {message_url}")
                else:
                    cursor.execute(
                        """
                        INSERT INTO Url_messages (url, _rule_id, start_of_counter, end_of_counter, _group_id, status)
                        VALUES (%s, %s, %s, %s, %s, %s);
                        """,
                        (message_url, rule_id, 0, rule_counter_message, group_id, new_status)
                    )
                    logger.info(f"Добавлен новый URL: {message_url}")

        except Exception as ex:
            logger.error(f"Ошибка внутри блока записи URL: {ex}")

    except Exception as ex:
        logger.error(f"Ошибка подключения к БД: {ex}")
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Запись базовых правил для сообщений --- #
async def save_default_rules():
    logger = await setup_logger(name="save_default_rules", log_file="Database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()

        for basic_rule_setting in basic_rule_settings:
            # Проверяем, существует ли правило в базе
            cursor.execute(
                "SELECT COUNT(*) FROM Rules WHERE id = %s AND rule_name = %s",
                (basic_rule_setting["id"], basic_rule_setting["RuleName"])
            )
            result = cursor.fetchone()

            # Если правило не существует, добавляем его в базу
            if result[0] == 0:
                cursor.execute(
                    """
                    INSERT INTO Rules (id, rule_name, number_of_days, frequency, rule_counter)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (basic_rule_setting["id"], basic_rule_setting["RuleName"], basic_rule_setting["NumberOfDays"], basic_rule_setting["Frequency"], basic_rule_setting["RuleCounter"])
                )
                connect.commit()
                logger.info(f"Rule '{basic_rule_setting['RuleName']}' успешно добавлено.")

            else:
                logger.info(f"Rule '{basic_rule_setting['RuleName']}' уже существует в базе данных.")

    except Exception as ex:
        logger.error(ex)

    finally:
        cursor.close()
        connect.close()


# --- Функция бд: Правило 1 --- #
async def get_url_by_rule_1():
    logger = await setup_logger(name="get_url_by_rule_1", log_file="Database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()
        urls_storage = []
        try:

            cursor.execute("SELECT * FROM Rules WHERE id = 1")
            get_rule_db = cursor.fetchone()
            if get_rule_db:
                number_of_days = get_rule_db[2]
                frequency = get_rule_db[3]
                rule_counter = get_rule_db[4]

                cursor.execute("SELECT * FROM Url_messages WHERE _rule_id = 1 AND Status = TRUE")
                get_urls_message_db = cursor.fetchall()

                if get_urls_message_db:
                    for get_url_message_db in get_urls_message_db:
                        message_id = get_url_message_db[0]
                        message_url = get_url_message_db[1]
                        start_of_counter = get_url_message_db[3]
                        _group_id = get_url_message_db[5]
                        created_at = get_url_message_db[7]
                        updated_at = get_url_message_db[8]

                        cursor.execute("SELECT * FROM Groups WHERE id = %s", (_group_id,))
                        get_group_db = cursor.fetchone()
                        if get_group_db:
                            group_id = get_group_db[2]
                            current_at = datetime.now()

                            if current_at <= created_at + timedelta(days=number_of_days):
                                if current_at >= updated_at + timedelta(days=frequency/rule_counter):
                                    if start_of_counter < (number_of_days * rule_counter):
                                        urls_storage.append((group_id, message_id, message_url))

                    return urls_storage

        except Exception as ex:
            logger.error(ex)

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()