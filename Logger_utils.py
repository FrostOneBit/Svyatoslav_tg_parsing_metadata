import os
import logging

async def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """
    Настраивает и возвращает логгер.

    :param name: Имя логгера.
    :param log_file: Имя файла для сохранения логов.
    :param level: Уровень логирования.
    :return: Настроенный логгер.
    """

    # Создаем папку для логов, если она не существует
    log_folder = "Files/Logs"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # Полный путь к файлу логов
    log_file_path = os.path.join(log_folder, log_file)

    # Формат логов
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Обработчик для записи логов в файл
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setFormatter(formatter)

    # Обработчик для вывода логов в консоль
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Создаем или получаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Проверяем, есть ли уже обработчики, чтобы не дублировать их
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
