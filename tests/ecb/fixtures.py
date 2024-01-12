from pathlib import Path

import pytest

from euro_converter.ecb import UpdateType
from euro_converter.ecb.download import URLS


TEST_RESPONSES = [
    (UpdateType.FULL, {}),
    (UpdateType.INCREMENTAL_90_DAYS, {"Last-Modified": "test"}),
    (UpdateType.INCREMENTAL_DAILY, {"ETag": "test"}),
]


@pytest.fixture(scope="session")
def request_data() -> str:
    path = Path(__file__).parent
    with open(path / "data/test-data.xml", "r") as f:
        return f.read()


@pytest.fixture()
def ecb_request_mock(request_data, requests_mock):
    for update_type, headers in TEST_RESPONSES:
        requests_mock.get(URLS[update_type], text=request_data, headers=headers)
    return requests_mock
