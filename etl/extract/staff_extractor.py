"""Extracts all staff-domain tables."""

from etl.extract.base_extractor import BaseExtractor


class StaffExtractor(BaseExtractor):
    def _tables(self) -> list[tuple[str, bool]]:
        return [
            ("departments", True),
            ("employees",   True),
            ("schedules",   True),
            ("attendance",  True),
        ]
