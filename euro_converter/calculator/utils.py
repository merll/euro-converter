from datetime import date
from decimal import Decimal
from typing import Iterable, Any, Optional

import polars as pl


DECIMAL_TYPE = pl.Decimal(12, 6)


def get_single_df(currency_date: date, value: Decimal):
    return pl.DataFrame(
        {"date": [currency_date], "value": [value]},
        schema={"date": pl.Date, "value": DECIMAL_TYPE},
    ).set_sorted("date")


def get_multi_df(data: list[[date, Decimal]], add_row_index: bool = False) -> pl.DataFrame:
    df = pl.DataFrame(
        data, schema={"date": pl.Date, "value": DECIMAL_TYPE}, orient="row"
    )
    if add_row_index:
        df = df.with_row_count("idx")
    return df.sort("date")


def get_single_result(df: pl.DataFrame, decimals: Optional[int] = None) -> Decimal:
    result = df.item(0, 0)
    if decimals is not None:
        return round(result, decimals)
    else:
        return result


def get_result_list(
    df: pl.DataFrame, restore_sort=False, decimals: Optional[int] = None
) -> list[tuple[date, Decimal]]:
    if restore_sort:
        df = df.sort("idx")
    results = df.select(pl.col("date"), pl.col("result")).rows()
    if decimals is not None:
        return [(dt, round(value, decimals)) for dt, value in results]
    else:
        # noinspection PyTypeChecker
        return results


def get_parsed_rates_df(rates: Iterable[dict[str, Any]]) -> pl.DataFrame:
    df = pl.DataFrame(rates)
    selects = [
        pl.col(col_name).cast(pl.Date if col_name == "date" else DECIMAL_TYPE)
        for col_name in df.columns
    ]
    return df.select(*selects).sort("date")
