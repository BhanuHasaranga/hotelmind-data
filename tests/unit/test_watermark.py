"""Unit tests for WatermarkStore."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest

from etl.watermark.watermark_store import WatermarkStore, _EPOCH


def _make_conn(fetchone_return=None):
    """Build a mock psycopg2 connection."""
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = fetchone_return

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestWatermarkGet:
    def test_returns_epoch_when_no_row(self):
        conn, cursor = _make_conn(fetchone_return=None)
        store = WatermarkStore(conn)
        result = store.get("hotels")
        assert result == _EPOCH

    def test_returns_stored_watermark(self):
        wm = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        conn, cursor = _make_conn(fetchone_return=(wm,))
        store = WatermarkStore(conn)
        result = store.get("hotels")
        assert result == wm

    def test_attaches_utc_to_naive_datetime(self):
        naive = datetime(2025, 6, 1, 0, 0, 0)  # no tzinfo
        conn, cursor = _make_conn(fetchone_return=(naive,))
        store = WatermarkStore(conn)
        result = store.get("reservations")
        assert result.tzinfo == timezone.utc


class TestWatermarkSet:
    def test_commits_after_upsert(self):
        conn, cursor = _make_conn()
        store = WatermarkStore(conn)
        wm = datetime(2025, 7, 1, tzinfo=timezone.utc)
        store.set("reservations", wm, rows_extracted=42)
        conn.commit.assert_called_once()

    def test_executes_upsert_with_correct_params(self):
        conn, cursor = _make_conn()
        store = WatermarkStore(conn)
        wm = datetime(2025, 7, 1, tzinfo=timezone.utc)
        store.set("orders", wm, rows_extracted=10)
        cursor.execute.assert_called_once()
        args = cursor.execute.call_args[0]
        params = args[1]
        assert params["table_name"] == "orders"
        assert params["last_extracted"] == wm
        assert params["rows_extracted"] == 10

    def test_attaches_utc_to_naive_watermark(self):
        conn, cursor = _make_conn()
        store = WatermarkStore(conn)
        naive_wm = datetime(2025, 7, 1)  # no tzinfo
        store.set("hotels", naive_wm, rows_extracted=0)
        params = cursor.execute.call_args[0][1]
        assert params["last_extracted"].tzinfo == timezone.utc
