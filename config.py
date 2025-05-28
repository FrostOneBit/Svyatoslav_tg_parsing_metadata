import os

from dotenv import load_dotenv

load_dotenv()

Telegram_token_api = os.getenv("TELEGRAM_TOKEN_API")
Telegram_org_auth = os.getenv("TELEGRAM_ORG_AUTH")

Telethon_session_messages = f"Session's/{os.getenv('TELETHON_SESSION_MESSAGES')}.session"
Telethon_session_audience = f"Session's/{os.getenv('TELETHON_SESSION_AUDIENCE')}.session"

Default_password = os.getenv("DEFAULT_PASSWORD")

DB_user = os.getenv('DB_USER')
DB_host = os.getenv('DB_HOST')
DB_name = os.getenv('DB_NAME')
DB_port = os.getenv('DB_PORT')
DB_password = os.getenv('DB_PASSWORD')

Encryption_key = os.getenv("ENCRYPTION_KEY")

Telethon_media_path = "Files/images/"

#RabbitMQ
RB_host = os.getenv("RB_HOST")
RB_port = os.getenv("RB_PORT")
RB_username = os.getenv("RB_USERNAME")
RB_password = os.getenv("RB_PASSWORD")
RB_virtual_host = os.getenv("RB_VIRTUAL_HOST")
Rb_queue_name = os.getenv("RB_QUEUE_NAME")

#Swagger
swagger_token = os.getenv("SWAGGER_TOKEN")
swagger_url_links = os.getenv("SWAGGER_URL_LINKS")
swagger_platforms = ["TELEGRAM"]
swagger_headers = {
    "accept": "*/*"
}