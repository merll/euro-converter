import json
import os
from typing import Optional

import polars as pl

from .base import StoredRatesCache


class FileCache(StoredRatesCache):
    def __init__(
        self,
        data_name: str = "data.parquet",
        timestamps_name: str = "last_timestamps.json",
    ):
        super().__init__()
        self.data_filename = data_name
        self.timestamps_filename = timestamps_name

    def load_data(self) -> Optional[pl.DataFrame]:
        if os.path.isfile(self.data_filename):
            return pl.read_parquet(self.data_filename)
        else:
            return None

    def load_timestamps(self) -> Optional[dict[str, str | dict[str, dict[str, str]]]]:
        if os.path.isfile(self.timestamps_filename):
            with open(self.timestamps_filename, "r") as cf:
                return json.load(cf)
        else:
            return None

    def save_data(self, data: pl.DataFrame) -> None:
        data.write_parquet(self.data_filename)

    def save_timestamps(
        self, timestamps: dict[str, str | dict[str, dict[str, str]]]
    ) -> None:
        with open(self.timestamps_filename, "w") as cf:
            json.dump(timestamps, cf)
