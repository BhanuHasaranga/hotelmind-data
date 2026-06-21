"""
Data quality validator for HotelMind staging tables.

Runs SQL-based assertions against staging tables before dbt transforms.
No external dependency on Great Expectations — plain psycopg2 checks.

Usage:
    from quality.validator import run_staging_validation
    results = run_staging_validation(warehouse_conn)
    if not results.all_passed:
        raise RuntimeError("Data quality gates failed")
"""

import logging
from dataclasses import dataclass, field

import psycopg2

from config.constants import STAGING_SCHEMA

log = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    suite_name: str
    passed: bool
    failed_expectations: list[str] = field(default_factory=list)
    statistics: dict = field(default_factory=dict)


@dataclass
class ValidationSummary:
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failed_suites(self) -> list[str]:
        return [r.suite_name for r in self.results if not r.passed]


def _run_checks(
    conn: psycopg2.extensions.connection,
    suite_name: str,
    checks: list[tuple[str, str]],
) -> ValidationResult:
    """Run a list of (label, sql) checks. SQL must return a single boolean row."""
    cur = conn.cursor()
    failed: list[str] = []
    for label, sql in checks:
        cur.execute(sql)
        row = cur.fetchone()
        passed = bool(row[0]) if row else False
        if not passed:
            log.warning("  FAIL [%s] %s", suite_name, label)
            failed.append(label)
        else:
            log.debug("  PASS [%s] %s", suite_name, label)
    cur.close()

    total = len(checks)
    ok = total - len(failed)
    return ValidationResult(
        suite_name=suite_name,
        passed=len(failed) == 0,
        failed_expectations=failed,
        statistics={"evaluated_expectations": total, "successful_expectations": ok},
    )


def _validate_reservations(conn: psycopg2.extensions.connection) -> ValidationResult:
    s = f"{STAGING_SCHEMA}.reservations"
    checks = [
        ("not_empty",             f"SELECT COUNT(*) > 0 FROM {s}"),
        ("id_not_null",           f"SELECT COUNT(*) = 0 FROM {s} WHERE id IS NULL"),
        ("room_id_not_null",      f"SELECT COUNT(*) = 0 FROM {s} WHERE room_id IS NULL"),
        ("guest_id_not_null",     f"SELECT COUNT(*) = 0 FROM {s} WHERE guest_id IS NULL"),
        ("check_in_not_null",     f"SELECT COUNT(*) = 0 FROM {s} WHERE check_in_date IS NULL"),
        ("check_out_not_null",    f"SELECT COUNT(*) = 0 FROM {s} WHERE check_out_date IS NULL"),
        ("total_amount_not_null", f"SELECT COUNT(*) = 0 FROM {s} WHERE total_amount IS NULL"),
        ("status_valid",          f"SELECT COUNT(*) = 0 FROM {s} WHERE status NOT IN "
                                  "('PENDING','CONFIRMED','CHECKED_IN','CHECKED_OUT','CANCELLED','NO_SHOW')"),
        ("total_amount_gte_0",    f"SELECT COUNT(*) * 1.0 / NULLIF(COUNT(*),0) >= 0.99 FROM {s} WHERE total_amount >= 0"),
        ("id_unique",             f"SELECT COUNT(*) = COUNT(DISTINCT id) FROM {s}"),
    ]
    return _run_checks(conn, "staging_reservations", checks)


def _validate_orders(conn: psycopg2.extensions.connection) -> ValidationResult:
    s = f"{STAGING_SCHEMA}.restaurant_orders"
    checks = [
        ("not_empty",         f"SELECT COUNT(*) > 0 FROM {s}"),
        ("id_not_null",       f"SELECT COUNT(*) = 0 FROM {s} WHERE id IS NULL"),
        ("branch_id_not_null",f"SELECT COUNT(*) = 0 FROM {s} WHERE branch_id IS NULL"),
        ("status_not_null",   f"SELECT COUNT(*) = 0 FROM {s} WHERE status IS NULL"),
        ("status_valid",      f"SELECT COUNT(*) = 0 FROM {s} WHERE status NOT IN ('OPEN','CLOSED','CANCELLED')"),
        ("total_gte_0",       f"SELECT COUNT(*) * 1.0 / NULLIF(COUNT(*),0) >= 0.99 FROM {s} WHERE total_amount >= 0"),
        ("id_unique",         f"SELECT COUNT(*) = COUNT(DISTINCT id) FROM {s}"),
    ]
    return _run_checks(conn, "staging_restaurant_orders", checks)


def _validate_employees(conn: psycopg2.extensions.connection) -> ValidationResult:
    s = f"{STAGING_SCHEMA}.employees"
    checks = [
        ("not_empty",           f"SELECT COUNT(*) > 0 FROM {s}"),
        ("id_not_null",         f"SELECT COUNT(*) = 0 FROM {s} WHERE id IS NULL"),
        ("department_not_null", f"SELECT COUNT(*) = 0 FROM {s} WHERE department_id IS NULL"),
        ("email_not_null",      f"SELECT COUNT(*) = 0 FROM {s} WHERE email IS NULL"),
        ("hire_date_not_null",  f"SELECT COUNT(*) = 0 FROM {s} WHERE hire_date IS NULL"),
        ("id_unique",           f"SELECT COUNT(*) = COUNT(DISTINCT id) FROM {s}"),
        ("email_unique",        f"SELECT COUNT(*) = COUNT(DISTINCT email) FROM {s}"),
    ]
    return _run_checks(conn, "staging_employees", checks)


def run_staging_validation(conn: psycopg2.extensions.connection) -> ValidationSummary:
    """
    Run all data quality suites against the staging schema.
    Returns a ValidationSummary with pass/fail per suite.
    """
    log.info("Starting data quality validation …")
    summary = ValidationSummary()

    validators = [
        ("staging_reservations",     _validate_reservations),
        ("staging_restaurant_orders",_validate_orders),
        ("staging_employees",        _validate_employees),
    ]

    for suite_name, validate_fn in validators:
        try:
            result = validate_fn(conn)
            summary.results.append(result)
            if result.passed:
                log.info("Quality PASS: %s  (%s)", result.suite_name, result.statistics)
            else:
                log.warning(
                    "Quality FAIL: %s  failed_expectations=%s",
                    result.suite_name,
                    result.failed_expectations,
                )
        except Exception as exc:
            log.error("Validation error for '%s': %s", suite_name, exc, exc_info=True)
            summary.results.append(
                ValidationResult(suite_name=suite_name, passed=False, failed_expectations=[str(exc)])
            )

    log.info(
        "Data quality validation complete: %d/%d passed",
        sum(1 for r in summary.results if r.passed),
        len(summary.results),
    )
    return summary
