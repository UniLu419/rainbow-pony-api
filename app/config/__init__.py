import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    ENV = os.getenv("ENV", "local")
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8000"))
    POSTGRES_USER = os.getenv("POSTGRES_USER", "")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "")
