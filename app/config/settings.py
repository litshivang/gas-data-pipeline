from dotenv import load_dotenv
import os
from sqlalchemy.engine import URL


load_dotenv()

class Settings:
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = int(os.getenv("POSTGRES_PORT", 5432))
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @property
    def database_url(self) -> str:
        return URL.create(
            drivername="postgresql+psycopg2",
            username=self.DB_USER,
            password=self.DB_PASSWORD,   # SAFE: no manual encoding
            host=self.DB_HOST,
            port=self.DB_PORT,
            database=self.DB_NAME,
        )

settings = Settings()
