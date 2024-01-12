from typing import Optional

from .data import RatesCacheData


class RatesCache:
    def __init__(self):
        self.data: Optional[RatesCacheData] = None

    def load(self) -> None:
        pass

    def save(self) -> None:
        pass
