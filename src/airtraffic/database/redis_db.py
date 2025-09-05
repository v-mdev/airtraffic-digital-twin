import redis
from src.airtraffic.config.config import settings

r = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    decode_responses=True,
    username=settings.redis_username,
    password=settings.redis_password,
)