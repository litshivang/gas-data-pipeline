from loguru import logger
import sys
from app.config.settings import settings

logger.remove()
logger.add(
    sys.stdout,
    level=settings.LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
           "<level>{level}</level> | "
           "<cyan>{name}</cyan> | {message}",
)

__all__ = ["logger"]
