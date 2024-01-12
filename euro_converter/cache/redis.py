import redis

from .base import RatesCache


class RedisRatesCache(RatesCache):
    def __init__(self, **kwargs):
        super().__init__()
        self.cache = redis.Redis(**kwargs)

    def load(self) -> None:
        pass

    def save(self) -> None:
        pass
