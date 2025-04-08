from Logger_utils import setup_logger

# --- Здесь будет получение сообщений --- #
async def get_utl_messages():

    # -- После будет подключен RabitMQ -- #

    logger = await setup_logger(name='get_utl_messages', log_file="Parser_Utils.log")

    try:

        urls = []

        url_test_open_0 = "https://t.me/test_open_0/2"
        url_test_close_0 = "https://t.me/c/2589054501/2"

        urls.append(url_test_open_0)
        urls.append(url_test_close_0)

        return urls
    except Exception as ex:
        logger.error(ex)
        return False