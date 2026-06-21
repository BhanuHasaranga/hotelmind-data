"""Extracts all hotel-domain tables: hotels, branches, floors, room_types, amenities, rooms."""

from etl.extract.base_extractor import BaseExtractor


class HotelExtractor(BaseExtractor):
    def _tables(self) -> list[tuple[str, bool]]:
        return [
            ("hotels",              True),
            ("branches",            True),
            ("floors",              True),
            ("room_types",          True),
            ("amenities",           True),
            ("room_type_amenities", False),  # junction table — full extract
            ("rooms",               True),
        ]
