from datetime import date, datetime
from decimal import Decimal

import polars as pl
import pytest

from euro_converter.cache import RatesCache, RatesCacheData
from euro_converter.calculator import CurrencyCalculator
from euro_converter.calculator.utils import DECIMAL_TYPE

RATES_DATA = [
    {"date": "2023-01-01", "CAA": "2.0", "CBB": "3.0", "CCC": "0.5"},
    {"date": "2023-01-04", "CAA": "2.01", "CBB": "6.000001", "CCC": "0.500001"},
    {"date": "2023-01-05", "CAA": "2.02", "CBB": "3.000002", "CCC": "0.500002"},
]
RATES_ROWS = [
    {
        "date": date(2023, 1, 1),
        "CAA": Decimal("2.000000"),
        "CBB": Decimal("3.000000"),
        "CCC": Decimal("0.500000"),
    },
    {
        "date": date(2023, 1, 4),
        "CAA": Decimal("2.010000"),
        "CBB": Decimal("6.000001"),
        "CCC": Decimal("0.500001"),
    },
    {
        "date": date(2023, 1, 5),
        "CAA": Decimal("2.020000"),
        "CBB": Decimal("3.000002"),
        "CCC": Decimal("0.500002"),
    },
]
RATES_SCHEMA = {
    "date": pl.Date,
    "CAA": DECIMAL_TYPE,
    "CBB": DECIMAL_TYPE,
    "CCC": DECIMAL_TYPE,
}


@pytest.fixture(scope="session")
def rates_data():
    return RatesCacheData(
        rates=pl.DataFrame(RATES_ROWS, schema=RATES_SCHEMA),
        last_update=datetime(2023, 1, 5),
        last_timestamps={},
    )


@pytest.fixture(scope="session")
def updated_calculator(rates_data):
    calc = CurrencyCalculator(cache=RatesCache())
    calc.data = rates_data
    return calc
