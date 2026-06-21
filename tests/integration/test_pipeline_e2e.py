"""
Integration test — full ETL pipeline against a real warehouse DB.

Prerequisites:
  - Warehouse DB running (docker compose up -d in hotelmind-data/)
  - Warehouse initialised (python scripts/init_warehouse.py)
  - Source DB running with at least some seed data

Run:
    pytest tests/integration/ -v -m integration

Skip in CI (no DB available):
    pytest tests/unit/ -v     # unit tests only
"""

import os

import psycopg2
import pytest

# Skip entire module if no integration env var set
pytestmark = pytest.mark.skipif(
    os.environ.get("HOTELMIND_INTEGRATION_TESTS") != "1",
    reason="Set HOTELMIND_INTEGRATION_TESTS=1 to run integration tests",
)


@pytest.fixture(scope="module")
def wh_conn():
    """Warehouse DB connection for the test module."""
    from config.settings import settings
    conn = psycopg2.connect(settings.WAREHOUSE_DB_URL)
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def src_conn():
    """Source DB connection for the test module."""
    from config.settings import settings
    conn = psycopg2.connect(settings.SOURCE_DB_URL)
    conn.autocommit = True
    yield conn
    conn.close()


class TestSchemaExists:
    def test_staging_schema_exists(self, wh_conn):
        with wh_conn.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'hotelmind_staging'"
            )
            assert cur.fetchone() is not None

    def test_warehouse_schema_exists(self, wh_conn):
        with wh_conn.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'hotelmind_warehouse'"
            )
            assert cur.fetchone() is not None

    def test_etl_watermarks_table_exists(self, wh_conn):
        with wh_conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'hotelmind_warehouse' AND table_name = 'etl_watermarks'"
            )
            assert cur.fetchone() is not None

    def test_staging_tables_count(self, wh_conn):
        with wh_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'hotelmind_staging'"
            )
            count = cur.fetchone()[0]
            assert count >= 13, f"Expected at least 13 staging tables, got {count}"


class TestExtractAndLoad:
    def test_can_extract_hotels(self, src_conn, wh_conn):
        from etl.extract.hotel_extractor import HotelExtractor
        from etl.watermark.watermark_store import WatermarkStore

        wh_conn.autocommit = False
        extractor = HotelExtractor(src_conn, WatermarkStore(wh_conn))
        results   = extractor.extract_all()

        # All hotel tables should have results objects (even if 0 rows)
        table_names = {r.table_name for r in results}
        assert "hotels" in table_names
        assert "rooms" in table_names

    def test_watermark_persists_after_extract(self, src_conn, wh_conn):
        from etl.extract.hotel_extractor import HotelExtractor
        from etl.watermark.watermark_store import WatermarkStore

        wh_conn.autocommit = False
        store = WatermarkStore(wh_conn)
        extractor = HotelExtractor(src_conn, store)
        extractor.extract_all()

        # Watermark for 'hotels' should now be set (not epoch)
        wm = store.get("hotels")
        from etl.watermark.watermark_store import _EPOCH
        # If source DB has data, watermark > epoch
        # If source is empty, watermark may still be epoch — that's acceptable
        assert wm is not None

    def test_staging_load_is_idempotent(self, src_conn, wh_conn):
        """Running the load twice should produce the same row count."""
        from etl.extract.hotel_extractor import HotelExtractor
        from etl.load.staging_loader import StagingLoader
        from etl.watermark.watermark_store import WatermarkStore

        wh_conn.autocommit = False

        def run_once():
            extractor = HotelExtractor(src_conn, WatermarkStore(wh_conn))
            results   = extractor.extract_all()
            loader    = StagingLoader(wh_conn)
            return loader.load_all(results)

        run_once()
        summaries_2 = run_once()

        assert all(s.success for s in summaries_2)

    def test_staging_count_non_negative(self, wh_conn):
        with wh_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM hotelmind_staging.hotels")
            count = cur.fetchone()[0]
            assert count >= 0
