"""Extracts all booking-domain tables: guests, reservations, occupancy_snapshots."""

from etl.extract.base_extractor import BaseExtractor


class BookingExtractor(BaseExtractor):
    def _tables(self) -> list[tuple[str, bool]]:
        return [
            ("guests",               True),
            ("reservations",         True),
            ("occupancy_snapshots",  True),
        ]
