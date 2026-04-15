import redis
from app.config.settings import settings

redis_client = redis.Redis(
    host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True
)
