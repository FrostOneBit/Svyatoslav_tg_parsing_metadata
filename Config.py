import os

from dotenv import load_dotenv

load_dotenv()

Telegram_token_api = os.getenv("TELEGRAM_TOKEN_API")
Telegram_org_auth = os.getenv("TELEGRAM_ORG_AUTH")

Default_password = os.getenv("DEFAULT_PASSWORD")

DB_user = os.getenv('DB_USER')
DB_host = os.getenv('DB_HOST')
DB_name = os.getenv('DB_NAME')
DB_port = os.getenv('DB_PORT')
DB_password = os.getenv('DB_PASSWORD')

Encryption_key = os.getenv("ENCRYPTION_KEY")