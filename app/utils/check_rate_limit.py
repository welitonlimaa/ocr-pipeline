from app.config.settings import settings


WINDOW_SECONDS = 86400


class RateLimitExceeded(Exception):
    pass


def check_rate_limit(redis_client, client_ip: str):
    key = f"rate_limit:{client_ip}"

    current = redis_client.incr(key)

    if current == 1:
        redis_client.expire(key, WINDOW_SECONDS)

    if redis_client.ttl(key) == -1:
        redis_client.expire(key, WINDOW_SECONDS)

    if current > settings.MAX_REQUESTS_PER_DAY:
        raise RateLimitExceeded()
