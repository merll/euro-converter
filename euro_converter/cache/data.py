from dataclasses import dataclass
from datetime import datetime

import polars as pl

from ..ecb import UpdateTimestamps


@dataclass
class RatesCacheData:
    rates: pl.DataFrame
    last_update: datetime
    last_timestamps: dict[str, UpdateTimestamps]
