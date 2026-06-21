"""Unit tests for BaseExtractor and domain extractors."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from etl.extract.base_extractor import BaseExtractor, ExtractResult
from etl.extract.hotel_extractor import HotelExtractor
from etl.extract.booking_extractor import BookingExtractor
from etl.extract.restaurant_extractor import RestaurantExtractor
from etl.extract.staff_extractor import StaffExtractor
from etl.watermark.watermark_store import WatermarkStore


def _make_source_conn(rows: list[dict]) -> MagicMock:
    """Mock a psycopg2 connection that returns *rows* from any query."""
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchall.return_value = [dict(r) for r in rows]

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _make_wm_store(watermark: datetime | None = None) -> MagicMock:
    store = MagicMock(spec=WatermarkStore)
    store.get.return_value = watermark or datetime(1970, 1, 1, tzinfo=timezone.utc)
    return store


class TestBaseExtractor:
    def test_extract_incremental_returns_rows(self):
        rows = [{"id": "abc", "updated_at": datetime(2025, 6, 1, tzinfo=timezone.utc)}]
        conn    = _make_source_conn(rows)
        wm_store = _make_wm_store()

        class ConcreteExtractor(BaseExtractor):
            def _tables(self):
                return [("hotels", True)]

        extractor = ConcreteExtractor(conn, wm_store)
        results   = extractor.extract_all()

        assert len(results) == 1
        assert results[0].table_name == "hotels"
        assert results[0].row_count == 1

    def test_extract_full_skips_watermark(self):
        rows = [{"room_type_id": "x", "amenity_id": "y"}]
        conn    = _make_source_conn(rows)
        wm_store = _make_wm_store()

        class ConcreteExtractor(BaseExtractor):
            def _tables(self):
                return [("room_type_amenities", False)]

        extractor = ConcreteExtractor(conn, wm_store)
        results   = extractor.extract_all()

        assert results[0].row_count == 1
        # Watermark.get should NOT be called for non-incremental tables
        wm_store.get.assert_not_called()

    def test_watermark_updated_after_extract(self):
        rows = [{"id": "a", "updated_at": datetime(2025, 6, 15, tzinfo=timezone.utc)}]
        conn     = _make_source_conn(rows)
        wm_store = _make_wm_store()

        class ConcreteExtractor(BaseExtractor):
            def _tables(self):
                return [("hotels", True)]

        extractor = ConcreteExtractor(conn, wm_store)
        extractor.extract_all()

        wm_store.set.assert_called_once()
        call_args = wm_store.set.call_args[0]
        assert call_args[0] == "hotels"
        assert call_args[1] == datetime(2025, 6, 15, tzinfo=timezone.utc)

    def test_empty_result_row_count_zero(self):
        conn     = _make_source_conn([])
        wm_store = _make_wm_store()

        class ConcreteExtractor(BaseExtractor):
            def _tables(self):
                return [("hotels", True)]

        extractor = ConcreteExtractor(conn, wm_store)
        results   = extractor.extract_all()
        assert results[0].row_count == 0


class TestDomainExtractors:
    def test_hotel_extractor_covers_all_tables(self):
        ext = HotelExtractor.__new__(HotelExtractor)
        tables = ext._tables()
        names  = [t[0] for t in tables]
        assert "hotels" in names
        assert "branches" in names
        assert "rooms" in names
        assert "room_type_amenities" in names

    def test_booking_extractor_tables(self):
        ext   = BookingExtractor.__new__(BookingExtractor)
        names = [t[0] for t in ext._tables()]
        assert "reservations" in names
        assert "guests" in names

    def test_restaurant_extractor_tables(self):
        ext   = RestaurantExtractor.__new__(RestaurantExtractor)
        names = [t[0] for t in ext._tables()]
        assert "restaurant_orders" in names
        assert "order_items" in names

    def test_staff_extractor_tables(self):
        ext   = StaffExtractor.__new__(StaffExtractor)
        names = [t[0] for t in ext._tables()]
        assert "employees" in names
        assert "attendance" in names
