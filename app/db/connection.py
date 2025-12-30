from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from app.config.settings import settings
from app.utils.logger import logger

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)

def get_db_session():
    try:
        db = SessionLocal()
        yield db
    except SQLAlchemyError as e:
        logger.error(f"Database session error: {e}")
        raise
    finally:
        db.close()
