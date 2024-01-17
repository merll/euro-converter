from datetime import datetime, date
from decimal import Decimal

import pytest

import polars as pl
from freezegun import freeze_time

from euro_converter.cache import RatesCache, RatesCacheData
from euro_converter.calculator import CurrencyCalculator, ConversionType
from euro_converter.ecb import UpdateTimestamps, UpdateType
from tests.calculator.fixtures import (
    RATES_ROWS,
    RATES_DATA,
    RATES_SCHEMA,
    rates_data,  # noqa
    updated_calculator,
)


@pytest.mark.parametrize(
    "init_rates, last_update, update_rates, expected_update_type",
    [
        pytest.param(
            RATES_ROWS[:1], datetime(2022, 1, 1), RATES_DATA, UpdateType.FULL, id="full"
        ),
        pytest.param(
            RATES_ROWS[:1],
            datetime(2023, 1, 1),
            RATES_DATA[1:].reverse(),
            UpdateType.INCREMENTAL_90_DAYS,
            id="inc-90",
        ),
        pytest.param(
            RATES_ROWS[:1],
            datetime(2023, 1, 5),
            RATES_DATA[1:],
            UpdateType.INCREMENTAL_DAILY,
            id="inc-daily",
        ),
        pytest.param(
            RATES_ROWS[:1],
            datetime(2023, 1, 5),
            None,
            UpdateType.INCREMENTAL_DAILY,
            id="inc-none",
        ),
    ],
)
def test_update(
    init_rates, last_update, update_rates, expected_update_type, monkeypatch
):
    update_args = []

    def update_func(*args):
        update_args.extend(args)
        return update_rates, UpdateTimestamps(modified_since="test2")

    monkeypatch.setattr(
        "euro_converter.calculator.calc.update_from_ecb",
        update_func,
    )
    update_time = datetime.fromisoformat("2023-01-05T12:01:00")
    cache = RatesCache()
    cache.data = RatesCacheData(
        rates=pl.DataFrame(init_rates, schema=RATES_SCHEMA),
        last_update=last_update,
        last_timestamps={
            expected_update_type.value: UpdateTimestamps(modified_since="test1")
        },
    )
    with freeze_time(update_time):
        calc = CurrencyCalculator(cache)
        update_done = calc.update()
    if expected_update_type is UpdateType.FULL:
        assert update_args == [UpdateType.FULL, None]
    else:
        assert update_args == [expected_update_type, UpdateTimestamps(modified_since="test1")]

    assert calc.data.rates is not None
    if update_rates is not None:
        assert update_done
        assert calc.data.rates.rows(named=True) == RATES_ROWS
        assert calc.data.last_update == update_time
    else:
        assert not update_done
        assert calc.data.rates.rows(named=True) == RATES_ROWS[:1]
        assert calc.data.last_update == last_update
    assert calc.last_cache_check == update_time
    if expected_update_type is UpdateType.FULL:
        assert calc.data.last_timestamps == {}
    else:
        assert calc.data.last_timestamps == {
            expected_update_type.value: UpdateTimestamps(modified_since="test2")
        }


@pytest.mark.parametrize(
    "conversion_type, input_currency, input_date, input_value, input_decimals, expected_result",
    [
        (
            ConversionType.TO,
            "CAA",
            date(2023, 1, 1),
            Decimal("1"),
            6,
            Decimal("2.000000"),
        ),
        (
            ConversionType.FROM,
            "CCC",
            date(2023, 1, 1),
            Decimal("1"),
            6,
            Decimal("2.000000"),
        ),
        (
            ConversionType.FROM,
            "CBB",
            date(2023, 1, 1),
            Decimal("1"),
            3,
            Decimal("0.333"),
        ),
        (
            ConversionType.TO,
            "CAA",
            date(2023, 1, 2),
            Decimal("1"),
            6,
            Decimal("2.000000"),
        ),
        (
            ConversionType.FROM,
            "CCC",
            date(2023, 1, 2),
            Decimal("1"),
            6,
            Decimal("2.000000"),
        ),
        (
            ConversionType.FROM,
            "CBB",
            date(2023, 1, 2),
            Decimal("1"),
            3,
            Decimal("0.333"),
        ),
        (
            ConversionType.TO,
            "CAA",
            date(2023, 1, 4),
            Decimal("1"),
            6,
            Decimal("2.010000"),
        ),
        (
            ConversionType.FROM,
            "CCC",
            date(2023, 1, 4),
            Decimal("1"),
            6,
            Decimal("1.999996"),
        ),
        (
            ConversionType.FROM,
            "CCC",
            date(2023, 1, 4),
            Decimal("1"),
            5,
            Decimal("2.00000"),
        ),
        (
            ConversionType.FROM,
            "CBB",
            date(2023, 1, 4),
            Decimal("1"),
            5,
            Decimal("0.16667"),
        ),
    ],
)
def test_convert_single(
    conversion_type,
    input_currency,
    input_date,
    input_value,
    input_decimals,
    expected_result,
    updated_calculator,
):
    result = updated_calculator.convert(
        conversion_type, input_currency, input_date, input_value, input_decimals
    )
    assert result == expected_result


@pytest.mark.parametrize(
    "conversion_type, input_currency, input_values, keep_order, input_decimals, expected_result",
    [
        (
            ConversionType.TO,
            "CAA",
            [
                (date(2023, 1, 2), Decimal("1")),
                (date(2023, 1, 4), Decimal("1")),
                (date(2023, 1, 1), Decimal("1")),
            ],
            False,
            6,
            [
                (date(2023, 1, 1), Decimal("2.000000")),
                (date(2023, 1, 2), Decimal("2.000000")),
                (date(2023, 1, 4), Decimal("2.010000")),
            ],
        ),
        (
            ConversionType.TO,
            "CAA",
            [
                (date(2023, 1, 2), Decimal("1")),
                (date(2023, 1, 4), Decimal("1")),
                (date(2023, 1, 1), Decimal("1")),
            ],
            True,
            2,
            [
                (date(2023, 1, 2), Decimal("2.00")),
                (date(2023, 1, 4), Decimal("2.01")),
                (date(2023, 1, 1), Decimal("2.00")),
            ],
        ),
        (
            ConversionType.FROM,
            "CCC",
            [
                (date(2023, 1, 2), Decimal("1")),
                (date(2023, 1, 4), Decimal("1")),
                (date(2023, 1, 1), Decimal("1")),
            ],
            False,
            6,
            [
                (date(2023, 1, 1), Decimal("2.000000")),
                (date(2023, 1, 2), Decimal("2.000000")),
                (date(2023, 1, 4), Decimal("1.999996")),
            ],
        ),
        (
            ConversionType.FROM,
            "CCC",
            [
                (date(2023, 1, 2), Decimal("1")),
                (date(2023, 1, 4), Decimal("1")),
                (date(2023, 1, 1), Decimal("1")),
            ],
            True,
            2,
            [
                (date(2023, 1, 2), Decimal("2.00")),
                (date(2023, 1, 4), Decimal("2.00")),
                (date(2023, 1, 1), Decimal("2.00")),
            ],
        ),
    ],
)
def test_convert_multiple(
    conversion_type,
    input_currency,
    input_values,
    keep_order,
    input_decimals,
    expected_result,
    updated_calculator,
):
    result = updated_calculator.convert_multiple(
        conversion_type, input_currency, input_values, keep_order, input_decimals
    )
    assert result == expected_result
