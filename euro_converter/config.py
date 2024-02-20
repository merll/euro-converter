import os
from dataclasses import dataclass
from typing import Mapping

import yaml

from euro_converter.cache import RatesCache
from euro_converter.cache.filecache import FileCache


@dataclass
class AppConfig:
    cache: RatesCache


def get_removing_prefix(d: Mapping, prefix: str) -> dict:
    prefix = f"{prefix.lower()}_"
    return {
        key.lower().removeprefix(prefix): value
        for key, value in d.items()
        if key.lower().startswith(prefix)
    }


def get_config() -> AppConfig:
    config_vars = get_removing_prefix(os.environ, "euro_converter")
    cache_config = config_vars.get("cache")
    if cache_config:
        if cache_config.lower() == "file":
            file_cache_config = get_removing_prefix(config_vars, "file_cache")
            cache = FileCache(**file_cache_config)
        elif cache_config.lower() == "redis":
            from euro_converter.cache.redis import RedisRatesCache

            redis_cache_config = get_removing_prefix(config_vars, "redis_cache")
            cache = RedisRatesCache(**redis_cache_config)
        else:
            cache = RatesCache()
    else:
        cache = RatesCache()
    return AppConfig(
        cache=cache,
    )
