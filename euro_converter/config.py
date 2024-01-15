import os
from dataclasses import dataclass
from typing import Mapping

import yaml

from euro_converter.cache import RatesCache
from euro_converter.cache.filecache import FileCache


@dataclass
class AppConfig:
    cache: RatesCache
    host: str
    port: int
    log_level: str
    log_config: dict


def get_removing_prefix(d: Mapping, prefix: str) -> dict:
    prefix = f"{prefix.lower()}_"
    return {
        key.lower().removeprefix(prefix): value
        for key, value in d.items()
        if key.lower().startswith(prefix)
    }


def get_log_config(log_level: str) -> tuple[str, dict]:
    if not log_level:
        log_level = "error"
    with open("logging.conf", "r") as f:
        log_config = yaml.safe_load(f)
    log_config["root"]["level"] = log_level.upper()
    return log_level.lower(), log_config


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
    log_level, log_config = get_log_config(config_vars.get("log_level"))
    return AppConfig(
        cache=cache,
        host=config_vars.get("host", "*"),
        port=config_vars.get("port", 8000),
        log_level=log_level,
        log_config=log_config,
    )
