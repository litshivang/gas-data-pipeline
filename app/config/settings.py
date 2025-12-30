from dotenv import load_dotenv
import os

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
        return (
            f"postgresql+psycopg2://{self.DB_USER}:"
            f"{self.DB_PASSWORD}@{self.DB_HOST}:"
            f"{self.DB_PORT}/{self.DB_NAME}"
        )

settings = Settings()
