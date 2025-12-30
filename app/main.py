from sqlalchemy import text
from app.db.connection import engine
from app.utils.logger import logger

def test_db_connection():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        logger.info(f"DB connection test result: {result.scalar()}")

if __name__ == "__main__":
    test_db_connection()
