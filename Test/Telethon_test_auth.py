from telethon import TelegramClient

# Укажи свои данные
api_id = 24663750
api_hash = 'bb88bd8148e3a2cd1c1380089a68d487'
phone_number = '+79270287070'

# Используем файловую сессию по имени (будет сохранена как my_session.session)
client = TelegramClient(
    'my_session',  # путь до файла сессии
    api_id,
    api_hash,
    device_model="CustomDevice",
    system_version="4.16.30-vxCUSTOM",
    app_version="1.0.0"
)

async def main():
    await client.connect()
    if not await client.is_user_authorized():
        # НЕ будет спрашивать номер, потому что он явно указан здесь
        await client.send_code_request(phone_number)
        code = input('Введите код из Telegram: ')
        await client.sign_in(phone_number, code)

    print("✅ Успешно авторизован!")

    me = await client.get_me()
    print(me.stringify())

    # Здесь можно вставлять свою логику

    await client.disconnect()

with client:
    client.loop.run_until_complete(main())
