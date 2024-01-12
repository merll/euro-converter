import enum
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional


DELTA_1 = timedelta(days=1)
DELTA_90 = timedelta(days=90)


class UpdateType(enum.Enum):
    FULL = "full"
    INCREMENTAL_90_DAYS = "incremental-90-days"
    INCREMENTAL_DAILY = "incremental-daily"


def get_update_type(latest_check: Optional[datetime] = None) -> UpdateType:
    if not latest_check:
        return UpdateType.FULL
    now = datetime.utcnow()
    delta = now - latest_check
    if delta < DELTA_1:
        return UpdateType.INCREMENTAL_DAILY
    elif DELTA_1 <= delta < DELTA_90:
        return UpdateType.INCREMENTAL_90_DAYS
    return UpdateType.FULL


@dataclass
class UpdateTimestamps:
    modified_since: Optional[str] = None
    etag: Optional[str] = None
