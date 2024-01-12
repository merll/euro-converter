import enum
import logging
import sys
from datetime import date, timedelta, datetime
from decimal import Decimal
from typing import Optional

import polars as pl

from ..cache import RatesCache, RatesCacheData
from ..ecb import update_from_ecb, UpdateType, get_update_type
from .utils import (
    get_parsed_rates_df,
    get_single_df,
    get_multi_df,
    get_result_list,
    get_single_result,
)

log = logging.getLogger(__name__)


CONVERT_TO = pl.col("value") * pl.col("rate")
CONVERT_FROM = pl.col("value") / pl.col("rate")
CACHE_TIMEOUT = timedelta(hours=1)


class ConversionType(enum.Enum):
    FROM = "from"
    TO = "to"


class CurrencyCalculator:
    def __init__(self, cache: RatesCache):
        self.cache = cache
        self.last_cache_check = None
        self._check_cache()

    @property
    def data(self) -> Optional[RatesCacheData]:
        return self.cache.data

    @data.setter
    def data(self, value):
        self.cache.data = value

    def _load_cache(self):
        try:
            self.cache.load()
        except Exception:
            exc = sys.exc_info()
            log.warning("Failed to load from cache.", exc_info=exc)

    def _save_cache(self):
        try:
            self.cache.save()
        except Exception:
            exc = sys.exc_info()
            log.warning("Failed to update cache.", exc_info=exc)

    def update(self) -> bool:
        data = self.data
        update_type = get_update_type(data.last_update if data else None)
        if update_type is not UpdateType.FULL:
            timestamps = data.last_timestamps.get(update_type.value)
        else:
            timestamps = None

        updated_data, response_timestamps = update_from_ecb(update_type, timestamps)
        if updated_data is None:
            if data and response_timestamps:
                data.last_timestamps[update_type.value] = response_timestamps
            return False

        updated_df = get_parsed_rates_df(updated_data)
        if update_type is UpdateType.FULL:
            self.data = RatesCacheData(
                rates=updated_df,
                last_update=datetime.utcnow(),
                last_timestamps={},
            )
            log.info("Created new dataframe.")
        else:
            if response_timestamps:
                data.last_timestamps[update_type.value] = response_timestamps
            data.rates = data.rates.update(updated_df, on="date", how="outer").sort("date")
            data.last_update = datetime.utcnow()
            log.info("Merged updated dataframe.")
        self._save_cache()
        return True

    def _check_cache(self) -> None:
        now = datetime.utcnow()
        if not self.last_cache_check or self.last_cache_check + CACHE_TIMEOUT < now:
            self._load_cache()

    def _merge_into(self, data: pl.DataFrame, currency: str) -> pl.DataFrame:
        self._check_cache()
        date_rates = self.data.rates.select(
            pl.col("date"), pl.col(currency).alias("rate")
        ).set_sorted("date")
        merged = data.join_asof(date_rates, left_on="date", right_on="date")
        return merged

    def convert(
        self,
        conversion_type: ConversionType,
        currency: str,
        currency_date: date,
        value: Decimal,
        decimals: Optional[int] = None,
    ):
        data = get_single_df(currency_date, value)
        if conversion_type is ConversionType.FROM:
            expression = CONVERT_FROM
        else:
            expression = CONVERT_TO
        merged = self._merge_into(data, currency.upper())
        return get_single_result(merged.select(expression), decimals=decimals)

    def convert_multiple(
        self,
        conversion_type: ConversionType,
        currency: str,
        values: list[tuple[date, Decimal]],
        keep_order: bool = False,
        decimals: Optional[int] = None,
    ):
        data = get_multi_df(values, add_row_index=keep_order)
        if conversion_type is ConversionType.FROM:
            expression = CONVERT_FROM
        else:
            expression = CONVERT_TO
        merged = self._merge_into(data, currency.upper())
        result = merged.with_columns(expression.alias("result"))
        return get_result_list(result, restore_sort=keep_order, decimals=decimals)
