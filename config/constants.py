"""
Project-wide constants: schema names, source table registry, status enumerations.
"""

# ── Schema names ─────────────────────────────────────────────────────────────
STAGING_SCHEMA = "hotelmind_staging"
WAREHOUSE_SCHEMA = "hotelmind_warehouse"

# ── Source tables to extract (in dependency order) ────────────────────────────
# Each entry: (source_table_name, primary_key_column, has_updated_at)
SOURCE_TABLES: list[tuple[str, str, bool]] = [
    # Hotel domain
    ("hotels",              "id", True),
    ("branches",            "id", True),
    ("floors",              "id", True),
    ("room_types",          "id", True),
    ("amenities",           "id", True),
    ("room_type_amenities", "room_type_id", False),   # junction, no updated_at
    ("rooms",               "id", True),
    # Booking domain
    ("guests",              "id", True),
    ("reservations",        "id", True),
    ("occupancy_snapshots", "id", True),
    # Restaurant domain
    ("food_categories",     "id", True),
    ("menu_items",          "id", True),
    ("restaurant_tables",   "id", True),
    ("restaurant_orders",   "id", True),
    ("order_items",         "id", True),
    # Staff domain
    ("departments",         "id", True),
    ("employees",           "id", True),
    ("schedules",           "id", True),
    ("attendance",          "id", True),
]

# ── Reservation statuses ──────────────────────────────────────────────────────
class ReservationStatus:
    PENDING      = "PENDING"
    CONFIRMED    = "CONFIRMED"
    CHECKED_IN   = "CHECKED_IN"
    CHECKED_OUT  = "CHECKED_OUT"
    CANCELLED    = "CANCELLED"
    NO_SHOW      = "NO_SHOW"
    TERMINAL     = {CANCELLED, NO_SHOW}
    ACTIVE       = {PENDING, CONFIRMED, CHECKED_IN}

# ── Room statuses ─────────────────────────────────────────────────────────────
class RoomStatus:
    AVAILABLE    = "AVAILABLE"
    OCCUPIED     = "OCCUPIED"
    MAINTENANCE  = "MAINTENANCE"
    CLEANING     = "CLEANING"

# ── Restaurant order statuses ─────────────────────────────────────────────────
class OrderStatus:
    OPEN         = "OPEN"
    CLOSED       = "CLOSED"
    CANCELLED    = "CANCELLED"

# ── Attendance statuses ───────────────────────────────────────────────────────
class AttendanceStatus:
    PRESENT      = "PRESENT"
    ABSENT       = "ABSENT"
    LATE         = "LATE"
    HALF_DAY     = "HALF_DAY"
