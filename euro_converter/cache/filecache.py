import json
from datetime import datetime
from typing import Optional

import polars as pl

from .base import RatesCache
from .data import RatesCacheData
from ..ecb import UpdateTimestamps


class FileCache(RatesCache):
    def __init__(
        self,
        path: Optional[str] = None,
        data_filename: Optional[str] = None,
        timestamps_filename: Optional[str] = None,
    ):
        super().__init__()
        self.path = path or ""
        self.data_filename = data_filename or "data.parquet"
        self.timestamps_filename = timestamps_filename or "last_timestamps.json"

    def load(self) -> None:
        try:
            data = pl.read_parquet(self.data_filename)
            with open(self.timestamps_filename, "r") as cf:
                timestamp_dict = json.load(cf)
        except (IOError, ValueError):
            return None
        self.data = RatesCacheData(
            rates=data,
            last_update=datetime.fromisoformat(timestamp_dict["last_update"]),
            last_timestamps={
                key: UpdateTimestamps(*values)
                for key, values in timestamp_dict["last_timestamps"].items()
            },
        )

    def save(self) -> None:
        data = self.data
        data.rates.write_parquet(self.data_filename)
        with open(self.timestamps_filename, "w") as cf:
            json.dump(
                {
                    "last_update": data.last_update.isoformat(),
                    "last_timestamps": {
                        key: [timestamps.modified_since, timestamps.etag]
                        for key, timestamps in data.last_timestamps.items()
                    },
                },
                cf,
            )
