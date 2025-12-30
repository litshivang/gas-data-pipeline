from app.db.connection import engine
from app.db.models import Base
from app.utils.logger import logger

def init_database():
    logger.info("Creating database tables if not exist...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database schema ready.")

if __name__ == "__main__":
    init_database()
