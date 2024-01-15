import logging.config
from datetime import date
from decimal import Decimal
from typing import Optional

import uvicorn
from fastapi import FastAPI

from euro_converter.calculator import CurrencyCalculator, ConversionType
from euro_converter.config import get_config

app_config = get_config()
logging.config.dictConfig(app_config.log_config)

app = FastAPI(
    title="Euro Converter",
    version="0.1.0",
)
calculator = CurrencyCalculator(cache=app_config.cache)


@app.post("/update")
def update() -> bool:
    return calculator.update()


@app.get("/to-{currency}/{currency_date}/{value}")
def convert_to_currency(
    currency: str,
    currency_date: date,
    value: Decimal,
    decimals: Optional[int] = 3,
) -> Decimal:
    return calculator.convert(
        ConversionType.TO, currency, currency_date, value, decimals=decimals
    )


@app.get("/from-{currency}/{currency_date}/{value}")
def convert_from_currency(
    currency: str,
    currency_date: date,
    value: Decimal,
    decimals: Optional[int] = 3,
) -> Decimal:
    return calculator.convert(
        ConversionType.FROM, currency, currency_date, value, decimals=decimals
    )


@app.post("/to-{currency}")
def convert_multi_to_currency(
    currency: str,
    dates_values: list[tuple[date, Decimal]],
    keep_order: Optional[bool] = True,
    decimals: Optional[int] = 3,
) -> list[tuple[date, Decimal]]:
    return calculator.convert_multiple(
        ConversionType.TO,
        currency,
        dates_values,
        keep_order=keep_order,
        decimals=decimals,
    )


@app.post("/from-{currency}")
def convert_multi_from_currency(
    currency: str,
    dates_values: list[tuple[date, Decimal]],
    keep_order: Optional[bool] = True,
    decimals: Optional[int] = 3,
) -> list[tuple[date, Decimal]]:
    return calculator.convert_multiple(
        ConversionType.FROM,
        currency,
        dates_values,
        keep_order=keep_order,
        decimals=decimals,
    )


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=app_config.host,
        port=app_config.port,
        log_level=app_config.log_level,
        log_config=app_config.log_config,
    )
