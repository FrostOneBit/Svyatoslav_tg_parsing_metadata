import re
import asyncio

from cryptography.hazmat.primitives.keywrap import aes_key_wrap
from psycopg2 import connect
from select import error
from telethon import TelegramClient
from venv import logger

from cryptography.fernet import Fernet
from datetime import datetime, timedelta
from database import create_connection_to_db
from logger_utils import setup_logger
from config import Default_password, Encryption_key, Telethon_session_messages
from rabbitmq_utils import get_urls_message_rabbitmq
from rule_storage import basic_rule_settings
from telethon.tl.types import PeerChannel, Channel


# --- Проверка человека на регистрацию --- #
async def check_user_in_db(message):
    logger = await setup_logger(name="check_user_in_db", log_file="database_utils.log")

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
    logger = await setup_logger(name="create_default_password_for_admin", log_file="database_utils.log")

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
    logger = await setup_logger(name="password_verification", log_file="database_utils.log")

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
    logger = await setup_logger(name="register_user_in_db", log_file="database_utils.log")

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


# --- Просмотр пароля --- #
async def check_password():
    logger =  await setup_logger(name="check_password", log_file="database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute("SELECT * FROM Passwords")
        result = cursor.fetchone()
        return result

    except Exception as ex:
        logger.error(ex)
        return False


# --- Смена пароля --- #
async def change_password_for_admin(password):
    logger = await setup_logger(name="change_password", log_file="database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute(f"UPDATE Passwords Set Password = '{password}' WHERE id = 1")
        return True
    except Exception as ex:
        logger.error(ex)
        return False
    finally:
        connect.commit()
        cursor.close()
        connect.close()

# --- Сохранение или обновление данных в БД --- #
async def save_telethon_data(data):
    logger = await setup_logger(name="save_telethon_data", log_file="database_utils.log")

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


# --- Запись базовых правил для сообщений --- #
async def save_default_rules():
    logger = await setup_logger(name="save_default_rules", log_file="database_utils.log")

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


# --- Запись групп в бд --- #
async def save_group_in_db(title, group_id, status, type):
    logger = await setup_logger(name="save_group_in_db", log_file="database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()

        cursor.execute("""
            INSERT INTO Groups (title, group_id, status, type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (group_id) DO UPDATE SET
                title = EXCLUDED.title,
                status = EXCLUDED.status,
                type = EXCLUDED.type,
                updated_at = CURRENT_TIMESTAMP
            WHERE
                Groups.title IS DISTINCT FROM EXCLUDED.title OR
                Groups.status IS DISTINCT FROM EXCLUDED.status OR
                Groups.type IS DISTINCT FROM EXCLUDED.type;
        """, (title, group_id, status, type))

        return True

    except Exception as ex:
        logger.error(ex)
        return False

    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Запись url сообщений в бд --- #
async def save_urls_message_in_db():
    logger = await setup_logger(name="save_urls_message", log_file="database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()

        cursor.execute("SELECT number_of_days, rule_counter FROM Rules WHERE id = 1")
        result = cursor.fetchone()
        if result:
            number_of_days, rule_counter = result
            end_of_counter = number_of_days * rule_counter

            get_urls = await get_urls_message_rabbitmq()
            if get_urls:
                for url in get_urls:
                    try:
                        cursor.execute("""
                            INSERT INTO Urls_message (url, _rule_id, start_of_counter, end_of_counter)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (url) DO NOTHING;
                        """, (url, 1, 0, end_of_counter))
                    except Exception as ex:
                        logger.error(f"Ошибка вставки url {url}: {ex}")

    except Exception as ex:
        logger.error(f"Фатальная ошибка: {ex}")
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Получение hash_id, hash_api --- #
async def get_hash_id_api():
    logger = await setup_logger(name="get_hash_id_api", log_file="database_utils.log")

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


# --- Получение администраторов из бд --- #
async def get_admins_from_db():
    logger = await setup_logger(name="get_admins_from_db", log_file="database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        list_user = []

        cursor.execute("SELECT * FROM Users")
        for row in cursor.fetchall():
            telegram_id = row[1]
            list_user.append(telegram_id)

        return list_user

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Функция бд: Правило 1 --- #
async def get_url_by_rule_1():
    logger = await setup_logger(name="get_url_by_rule_1", log_file="database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()
        urls_storage = []

        try:
            # Get rule parameters
            cursor.execute("SELECT * FROM Rules WHERE id = 1")
            rule = cursor.fetchone()
            if not rule:
                return []

            number_of_days = rule[2]
            frequency = rule[3]
            rule_counter = rule[4]

            # Get all active messages for this rule
            cursor.execute("SELECT * FROM Urls_message WHERE _rule_id = 1 AND Status = TRUE")
            urls_message = cursor.fetchall()

            if not urls_message:
                return []

            current_time = datetime.now()

            for message in urls_message:
                message_id = message[0]
                message_url = message[1]
                start_counter = message[3]
                status_media = message[6]
                created_at = message[7]
                updated_at = message[8]

                # Check time and limits
                rule_active_until = created_at + timedelta(days=number_of_days)
                next_allowed_check = updated_at + timedelta(days=frequency / rule_counter)
                max_iterations = number_of_days * rule_counter

                if current_time <= rule_active_until and current_time >= next_allowed_check and start_counter < max_iterations:
                    urls_storage.append((message_id, message_url, status_media))

            return urls_storage

        except Exception as ex:
            logger.error(f"Error executing subqueries in get_url_by_rule_1: {ex}")

    except Exception as ex:
        logger.error(f"Database connection error in get_url_by_rule_1: {ex}")

    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Функция бд: Правило 2 --- #
async def get_url_by_rule_2():
    logger = await setup_logger(name="get_url_by_rule_2", log_file="database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()
        urls_storage = []

        try:
            # Get rule parameters
            cursor.execute("SELECT * FROM Rules WHERE id = 2")
            rule = cursor.fetchone()
            if not rule:
                return []

            number_of_days = rule[2]
            frequency = rule[3]
            rule_counter = rule[4]

            # Get all active messages for this rule
            cursor.execute("SELECT * FROM Urls_message WHERE _rule_id = 2 AND Status = TRUE")
            urls_message = cursor.fetchall()

            if not urls_message:
                return []

            current_time = datetime.now()

            for message in urls_message:
                message_id = message[0]
                message_url = message[1]
                start_counter = message[3]
                status_media = message[6]
                created_at = message[7]
                updated_at = message[8]

                # Check time and limits
                rule_active_until = created_at + timedelta(days=number_of_days)
                next_allowed_check = updated_at + timedelta(days=frequency / rule_counter)
                max_iterations = number_of_days * rule_counter

                if current_time <= rule_active_until and current_time >= next_allowed_check and start_counter < max_iterations:
                    urls_storage.append((message_id, message_url, status_media))

            return urls_storage

        except Exception as ex:
            logger.error(f"Error executing subqueries in get_url_by_rule_2: {ex}")

    except Exception as ex:
        logger.error(f"Database connection error in get_url_by_rule_2: {ex}")

    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Функция бд: Правило 3 --- #
async def get_url_by_rule_3():
    logger = await setup_logger(name="get_url_by_rule_3", log_file="database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()
        urls_storage = []

        try:
            # Get rule parameters
            cursor.execute("SELECT * FROM Rules WHERE id = 3")
            rule = cursor.fetchone()
            if not rule:
                return []

            number_of_days = rule[2]
            frequency = rule[3]
            rule_counter = rule[4]

            # Get all active messages for this rule
            cursor.execute("SELECT * FROM Urls_message WHERE _rule_id = 3 AND Status = TRUE")
            urls_message = cursor.fetchall()

            if not urls_message:
                return []

            current_time = datetime.now()

            for message in urls_message:
                message_id = message[0]
                message_url = message[1]
                start_counter = message[3]
                status_media = message[6]
                created_at = message[7]
                updated_at = message[8]

                # Check time and limits
                rule_active_until = created_at + timedelta(days=number_of_days)
                next_allowed_check = updated_at + timedelta(days=frequency / rule_counter)
                max_iterations = number_of_days * rule_counter

                if current_time <= rule_active_until and current_time >= next_allowed_check and start_counter < max_iterations:
                    urls_storage.append((message_id, message_url, status_media))

            return urls_storage

        except Exception as ex:
            logger.error(f"Error executing subqueries in get_url_by_rule_3: {ex}")

    except Exception as ex:
        logger.error(f"Database connection error in get_url_by_rule_3: {ex}")

    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Функция бд: Правило 4 --- #
async def get_url_by_rule_4():
    logger = await setup_logger(name="get_url_by_rule_4", log_file="database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()
        urls_storage = []

        try:
            # Get rule parameters
            cursor.execute("SELECT * FROM Rules WHERE id = 4")
            rule = cursor.fetchone()
            if not rule:
                return []

            number_of_days = rule[2]
            frequency = rule[3]
            rule_counter = rule[4]

            # Get all active messages for this rule
            cursor.execute("SELECT * FROM Urls_message WHERE _rule_id = 4 AND Status = TRUE")
            urls_message = cursor.fetchall()

            if not urls_message:
                return []

            current_time = datetime.now()

            for message in urls_message:
                message_id = message[0]
                message_url = message[1]
                start_counter = message[3]
                status_media = message[6]
                created_at = message[7]
                updated_at = message[8]

                # Check time and limits
                rule_active_until = created_at + timedelta(days=number_of_days)
                next_allowed_check = updated_at + timedelta(days=frequency / rule_counter)
                max_iterations = number_of_days * rule_counter

                if current_time <= rule_active_until and current_time >= next_allowed_check and start_counter < max_iterations:
                    urls_storage.append((message_id, message_url, status_media))

            return urls_storage

        except Exception as ex:
            logger.error(f"Error executing subqueries in get_url_by_rule_4: {ex}")

    except Exception as ex:
        logger.error(f"Database connection error in get_url_by_rule_4: {ex}")

    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Функция бд: Правило 5 --- #
async def get_url_by_rule_5():
    logger = await setup_logger(name="get_url_by_rule_5", log_file="database_utils.log")

    try:
        connect, cursor = await create_connection_to_db()
        urls_storage = []

        try:
            # Get rule parameters
            cursor.execute("SELECT * FROM Rules WHERE id = 5")
            rule = cursor.fetchone()
            if not rule:
                return []

            number_of_days = rule[2]
            frequency = rule[3]
            rule_counter = rule[4]

            # Get all active messages for this rule
            cursor.execute("SELECT * FROM Urls_message WHERE _rule_id = 5 AND Status = TRUE")
            urls_message = cursor.fetchall()

            if not urls_message:
                return []

            current_time = datetime.now()

            for message in urls_message:
                message_id = message[0]
                message_url = message[1]
                start_counter = message[3]
                status_media = message[6]
                created_at = message[7]
                updated_at = message[8]

                # Check time and limits
                rule_active_until = created_at + timedelta(days=number_of_days)
                next_allowed_check = updated_at + timedelta(days=frequency / rule_counter)
                max_iterations = number_of_days * rule_counter

                if current_time <= rule_active_until and current_time >= next_allowed_check and start_counter < max_iterations:
                    urls_storage.append((message_id, message_url, status_media))

            return urls_storage

        except Exception as ex:
            logger.error(f"Error executing subqueries in get_url_by_rule_5: {ex}")

    except Exception as ex:
        logger.error(f"Database connection error in get_url_by_rule_5: {ex}")

    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Функция: Парсинг аудитории --- #
async def get_group_id_for_parse_audience():
    logger = await setup_logger("get_audience_by_group_id", "get_audience_by_group_id.log")

    try:

        connect, cursor = await create_connection_to_db()
        groups_storage = []
        cursor.execute("SELECT id, group_id, title FROM Groups WHERE Status = TRUE AND Status_audience = FALSE")
        get_groups = cursor.fetchall()
        if get_groups:
            for get_group in get_groups:
                id_db = get_group[0]
                group_id = get_group[1]
                group_title = get_group[2]
                groups_storage.append((id_db, group_id, group_title))

        return groups_storage
    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Обновляем status_media --- #
async def update_status_media_by_url(message_db_id):
    logger = await setup_logger(name="update_status_media_by_url", log_file="database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute("UPDATE Urls_message SET Status_media = TRUE WHERE id = %s", (message_db_id,))

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Обновляем данные о ссылках --- #
async def update_metadata_by_url(message_db_id):
    logger = await setup_logger(name="update_metadata_bu_url", log_file="database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute("SELECT * FROM Urls_message WHERE id = %s", (message_db_id,))
        get_url_message_db = cursor.fetchone()
        if get_url_message_db:
            start_of_counter = get_url_message_db[3] + 1

            try:

                cursor.execute("UPDATE Urls_message SET start_of_counter = %s WHERE id = %s ", (start_of_counter, message_db_id,))
                cursor.execute("UPDATE Urls_message SET Updated_at = CURRENT_TIMESTAMP WHERE id = %s", (message_db_id,))

            except Exception as ex:
                logger.error(ex)

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Аннулирование ссылки в случае если она не найдена --- #
async def deactivate_url_message_by_id(message_url):
    logger = await setup_logger(name="deactivate_url_message_by_ud", log_file="database.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute(f"UPDATE Urls_message SET status = NULL WHERE url = '{message_url}'")

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- update_rule_for_url  --- #
async def update_rule_for_url():
    logger = await setup_logger(name="update_rule_for_url", log_file="database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute("SELECT id, _rule_id, start_of_counter, end_of_counter, created_at, updated_at FROM Urls_message WHERE Status = TRUE")
        get_urls_db = cursor.fetchall()
        if get_urls_db:
            for get_url_db in get_urls_db:
                url_id = get_url_db[0]
                rule_id = get_url_db[1]
                start_of_counter = get_url_db[2]
                end_of_counter = get_url_db[3]
                created_at = get_url_db[4]
                updated_at = get_url_db[5]
                current_at = datetime.now()

                cursor.execute("SELECT * FROM Rules WHERE id = %s", (rule_id,))
                get_rule_db = cursor.fetchone()
                if get_rule_db:
                    number_of_days = get_rule_db[2]
                    frequency = get_rule_db[3]
                    rule_counter = get_rule_db[4]

                    # -- Цикл обновления правил для ссылок -- #
                    if current_at >= (created_at + timedelta(days=number_of_days)) or start_of_counter >= end_of_counter:
                        new_rule_id = rule_id + 1 if rule_id != 5 else rule_id

                        if rule_id != 5:
                            cursor.execute("SELECT * FROM Rules WHERE id = %s", (new_rule_id,))
                            get_new_rule_db = cursor.fetchone()
                            if get_new_rule_db:
                                new_number_of_days = get_new_rule_db[2]
                                new_rule_counter = get_new_rule_db[4]

                                try:
                                    cursor.execute("UPDATE Urls_message SET _rule_id = %s WHERE id = %s", (new_rule_id, url_id))
                                    cursor.execute("UPDATE Urls_message SET start_of_counter = 0 WHERE id = %s", (url_id,))
                                    cursor.execute("UPDATE Urls_message SET end_of_counter = %s WHERE id = %s", (new_number_of_days * new_rule_counter, url_id))
                                    cursor.execute("UPDATE Urls_message SET updated_at = CURRENT_TIMESTAMP WHERE id = %s", (url_id,))
                                except Exception as ex:
                                    logger.error(ex)

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()


# --- Функция: обновление статуса аудитории у группы --- #
async def update_status_audience_by_group_id(db_id):
    logger = await setup_logger(name="update_status_audience_by_group_id", log_file="database_utils.log")

    try:

        connect, cursor = await create_connection_to_db()

        cursor.execute("UPDATE Groups SET Status_audience = TRUE WHERE id = %s", (db_id,))
        cursor.execute("UPDATE Groups SET Updated_at = CURRENT_TIMESTAMP WHERE id = %s", (db_id,))

    except Exception as ex:
        logger.error(ex)
    finally:
        connect.commit()
        cursor.close()
        connect.close()
