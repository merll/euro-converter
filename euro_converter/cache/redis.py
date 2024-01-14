from io import BytesIO
from typing import Optional

import redis
import polars as pl

from .base import StoredRatesCache


class RedisRatesCache(StoredRatesCache):
    def __init__(
        self,
        data_key: str = "data",
        last_update_key: str = "last_update",
        headers_key_prefix: str = "last_headers_",
        **kwargs,
    ):
        super().__init__()
        self.cache = redis.Redis(**kwargs)
        self.data_key = data_key
        self.last_update_key = last_update_key
        self.headers_key_prefix = headers_key_prefix

    def load_data(self) -> Optional[pl.DataFrame]:
        if self.cache.exists(self.data_key):
            bin_data = BytesIO(self.cache.get(self.data_key))
            return pl.read_parquet(bin_data)
        else:
            return None

    def load_timestamps(self) -> Optional[dict[str, str | dict[str, dict[str, str]]]]:
        if self.last_update_key:
            return {
                "last_update": self.cache.get(self.last_update_key).decode(),
                "last_headers": {
                    key.decode().removeprefix(self.headers_key_prefix): {
                        k.decode(): v.decode()
                        for k, v in self.cache.hgetall(key).items()
                    }
                    for key in self.cache.keys(f"{self.headers_key_prefix}*")
                },
            }
        else:
            return None

    def save_data(self, data: pl.DataFrame) -> None:
        bin_data = BytesIO()
        data.write_parquet(bin_data)
        self.cache.set(self.data_key, bin_data.getvalue())

    def save_timestamps(
        self, timestamps: dict[str, str | dict[str, dict[str, str]]]
    ) -> None:
        self.cache.set(self.last_update_key, timestamps["last_update"])
        for key, value in timestamps["last_headers"].items():
            self.cache.hset(f"{self.headers_key_prefix}{key}", mapping=value)
