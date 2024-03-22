import os
from dotenv import load_dotenv

load_dotenv()

SITE_URL = os.environ.get("SITE_URL")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID")
DEBUG = os.environ.get("DEBUG", False)
API_TOKEN = os.environ.get("API_TOKEN")
ON_SERVER = os.environ.get("ON_SERVER", False)
ENVIRONMENT = os.environ.get("ENVIRONMENT")
CE_BOT_TOKEN = os.environ.get("CE_BOT_TOKEN")
