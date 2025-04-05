import os

from dotenv import load_dotenv

load_dotenv()

DB_user = os.getenv('DB_USER')
DB_host = os.getenv('DB_HOST')
DB_name = os.getenv('DB_NAME')
DB_port = os.getenv('DB_PORT')
DB_password = os.getenv('DB_PASSWORD')
