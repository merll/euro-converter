from datetime import datetime
from typing import Optional

import polars as pl

from .data import RatesCacheData
from ..ecb import UpdateTimestamps


class RatesCache:
    def __init__(self):
        self.data: Optional[RatesCacheData] = None

    def load(self) -> None:
        pass

    def save(self) -> None:
        pass


class StoredRatesCache(RatesCache):
    def load_data(self) -> Optional[pl.DataFrame]:
        raise NotImplementedError()

    def load_timestamps(self) -> Optional[dict[str, str | dict[str, dict[str, str]]]]:
        raise NotImplementedError()

    def save_data(self, data: pl.DataFrame) -> None:
        raise NotImplementedError()

    def save_timestamps(
        self, timestamps: dict[str, str | dict[str, dict[str, str]]]
    ) -> None:
        raise NotImplementedError()

    def load(self) -> None:
        data = self.load_data()
        timestamp_dict = self.load_timestamps()
        if data is not None and timestamp_dict:
            self.data = RatesCacheData(
                rates=data,
                last_update=datetime.fromisoformat(timestamp_dict["last_update"]),
                last_timestamps={
                    key: UpdateTimestamps(**values)
                    for key, values in timestamp_dict["last_headers"].items()
                },
            )

    def save(self) -> None:
        data = self.data
        self.save_data(data.rates)
        self.save_timestamps(
            {
                "last_update": data.last_update.isoformat(),
                "last_headers": {
                    key: {
                        attr: getattr(timestamps, attr)
                        for attr in ["modified_since", "etag"]
                        if getattr(timestamps, attr)
                    }
                    for key, timestamps in data.last_timestamps.items()
                },
            },
        )
