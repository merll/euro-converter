from http.client import HTTPResponse
from typing import Optional

from requests import Session

from .types import UpdateType, UpdateTimestamps


BASE_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-{0}.xml"
URLS = {
    UpdateType.FULL: BASE_URL.format("hist"),
    UpdateType.INCREMENTAL_90_DAYS: BASE_URL.format("hist-90d"),
    UpdateType.INCREMENTAL_DAILY: BASE_URL.format("daily"),
}

default_session = Session()


def get_data(
    update_type: UpdateType,
    latest_timestamps: Optional[UpdateTimestamps] = None,
    session: Optional[Session] = None,
) -> tuple[HTTPResponse, UpdateTimestamps]:
    request_headers = {}
    url = URLS[update_type]
    if latest_timestamps is not None:
        if modified_since := latest_timestamps.modified_since:
            request_headers["If-Modified-Since"] = modified_since
        if last_etag := latest_timestamps.etag:
            request_headers["If-None-Match"] = last_etag
    session = session or default_session
    response = session.get(url, stream=True, headers=request_headers)
    if not response.ok:
        response.raise_for_status()
    response.raw.decode_content = True
    updated_timestamps = UpdateTimestamps(
        response.headers.get("Last-Modified"),
        response.headers.get("ETag"),
    )
    return response.raw, updated_timestamps
