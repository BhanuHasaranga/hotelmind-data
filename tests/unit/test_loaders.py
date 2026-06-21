"""Unit tests for BaseLoader and StagingLoader."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from etl.extract.base_extractor import ExtractResult
from etl.load.base_loader import BaseLoader
from etl.load.staging_loader import StagingLoader


def _make_wh_conn() -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestBaseLoader:
    def test_load_empty_result_returns_zero(self):
        conn, _ = _make_wh_conn()
        loader = BaseLoader(conn)
        result = ExtractResult("hotels", rows=[], watermark=datetime.now(timezone.utc))
        assert loader.load(result) == 0

    def test_load_commits_after_batch(self):
        conn, cursor = _make_wh_conn()
        loader = BaseLoader(conn)
        rows = [{"id": "abc", "name": "Grand Hotel", "star_rating": 5, "is_active": True,
                 "city": None, "country": None, "address": None, "phone": None,
                 "email": None, "created_at": None, "updated_at": None}]
        result = ExtractResult("hotels", rows=rows, watermark=datetime.now(timezone.utc))

        with patch("psycopg2.extras.execute_batch"):
            loader.load(result)

        conn.commit.assert_called_once()

    def test_load_returns_row_count(self):
        conn, _ = _make_wh_conn()
        loader = BaseLoader(conn)
        rows = [{"id": str(i), "name": f"Hotel {i}"} for i in range(5)]
        result = ExtractResult("hotels", rows=rows, watermark=datetime.now(timezone.utc))

        with patch("psycopg2.extras.execute_batch"):
            count = loader.load(result)

        assert count == 5

    def test_composite_pk_detection(self):
        """room_type_amenities uses composite PK, not 'id'."""
        conn, _ = _make_wh_conn()
        loader = BaseLoader(conn)
        pk = loader.COMPOSITE_PKS.get("room_type_amenities")
        assert pk == ["room_type_id", "amenity_id"]


class TestStagingLoader:
    def test_load_all_returns_summaries(self):
        conn, _ = _make_wh_conn()
        loader = StagingLoader(conn)

        results = [
            ExtractResult("hotels", rows=[{"id": "x"}], watermark=datetime.now(timezone.utc)),
            ExtractResult("branches", rows=[], watermark=datetime.now(timezone.utc)),
        ]

        with patch.object(loader._loader, "load", side_effect=[1, 0]):
            summaries = loader.load_all(results)

        assert len(summaries) == 2
        assert summaries[0].success is True
        assert summaries[0].rows_loaded == 1
        assert summaries[1].rows_loaded == 0

    def test_failed_table_does_not_stop_others(self):
        conn, _ = _make_wh_conn()
        loader = StagingLoader(conn)

        results = [
            ExtractResult("hotels", rows=[{"id": "a"}], watermark=datetime.now(timezone.utc)),
            ExtractResult("rooms",  rows=[{"id": "b"}], watermark=datetime.now(timezone.utc)),
        ]

        def side_effect(result):
            if result.table_name == "hotels":
                raise RuntimeError("DB error")
            return 1

        with patch.object(loader._loader, "load", side_effect=side_effect):
            summaries = loader.load_all(results)

        assert summaries[0].success is False
        assert summaries[0].error is not None
        assert summaries[1].success is True   # second table still processed
