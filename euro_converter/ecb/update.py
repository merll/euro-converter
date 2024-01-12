import logging
from typing import Iterator, Optional, Any

from .download import get_data
from .parser import parse_xml
from .types import UpdateType, UpdateTimestamps

log = logging.getLogger(__name__)


def update_from_ecb(
    update_type: UpdateType,
    request_timestamps: UpdateTimestamps,
) -> tuple[Optional[Iterator[dict[str, Any]]], UpdateTimestamps]:
    response, response_timestamps = get_data(update_type, request_timestamps)
    if response_timestamps:
        log.info(
            "Response %d. Timestamp: %s; ETag: %s",
            response.status,
            response_timestamps.modified_since,
            response_timestamps.etag,
        )
    if response.status == 304:
        log.info("Skipping update.")
        return None, response_timestamps
    else:
        log.info("Received update.")
        return parse_xml(response), response_timestamps
