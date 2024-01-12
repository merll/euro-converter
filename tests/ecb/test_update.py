import pytest

from .fixtures import (
    request_data,  # noqa
    ecb_request_mock,
)
from euro_converter.ecb import update_from_ecb, UpdateType, UpdateTimestamps


EXPECTED_RESPONSE = [
    {
        "date": "2024-01-10",
        "USD": "1.0946",
        "JPY": "159.0",
        "BGN": "1.9558",
        "CZK": "24.562",
        "DKK": "7.4582",
        "GBP": "0.86023",
        "HUF": "378.35",
        "PLN": "4.341",
        "RON": "4.9728",
        "SEK": "11.197",
        "CHF": "0.9336",
        "ISK": "150.1",
        "NOK": "11.2915",
        "TRY": "32.8087",
        "AUD": "1.6334",
        "BRL": "5.3508",
        "CAD": "1.4649",
        "CNY": "7.8476",
        "HKD": "8.5602",
        "IDR": "17032.14",
        "ILS": "4.1184",
        "INR": "90.8755",
        "KRW": "1443.77",
        "MXN": "18.5983",
        "MYR": "5.0806",
        "NZD": "1.7567",
        "PHP": "61.593",
        "SGD": "1.4573",
        "THB": "38.338",
        "ZAR": "20.4139",
    },
]


@pytest.mark.parametrize(
    "update_type, input_timestamps, expected_timestamp",
    [
        pytest.param(
            UpdateType.FULL,
            UpdateTimestamps(modified_since="test"),
            UpdateTimestamps(),
            id="full",
        ),
        pytest.param(
            UpdateType.INCREMENTAL_90_DAYS,
            None,
            UpdateTimestamps(modified_since="test"),
            id="inc-90",
        ),
        pytest.param(
            UpdateType.INCREMENTAL_DAILY,
            UpdateTimestamps(etag="test"),
            UpdateTimestamps(etag="test"),
            id="inc-daily",
        ),
    ],
)
def test_update(update_type, input_timestamps, expected_timestamp, ecb_request_mock):
    data, response_timestamps = update_from_ecb(update_type, input_timestamps)
    request_history = ecb_request_mock.request_history
    assert len(request_history) == 1
    request = request_history[0]
    if input_timestamps and input_timestamps.modified_since:
        assert (
            request.headers.get("If-Modified-Since") == input_timestamps.modified_since
        )
    else:
        assert "If-Modified-Since" not in request.headers
    if input_timestamps and input_timestamps.etag:
        assert request.headers.get("If-None-Match") == input_timestamps.etag
    else:
        assert "If-None-Match" not in request.headers
    assert response_timestamps == expected_timestamp
    assert list(data) == EXPECTED_RESPONSE
