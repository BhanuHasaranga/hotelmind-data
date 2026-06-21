"""
Data quality validator for HotelMind staging tables.

Uses Great Expectations to validate data after staging load, before dbt runs.
Runs programmatic (in-memory) expectations — no GE context file required.

Usage:
    from quality.validator import run_staging_validation
    results = run_staging_validation(warehouse_conn)
    if not results.all_passed:
        raise RuntimeError("Data quality gates failed")
"""

import logging
from dataclasses import dataclass, field

import great_expectations as gx
import pandas as pd
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


def _load_table(conn: psycopg2.extensions.connection, table: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {STAGING_SCHEMA}.{table}", conn)


def _validate_reservations(df: pd.DataFrame) -> ValidationResult:
    """Validate staging reservations table."""
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas("reservations_ds")
    da = ds.add_dataframe_asset("reservations")
    batch = da.add_batch_definition_whole_dataframe("reservations_batch")

    suite_name = "staging_reservations"
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="room_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="guest_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="check_in_date"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="check_out_date"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="total_amount"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["PENDING", "CONFIRMED", "CHECKED_IN", "CHECKED_OUT", "CANCELLED", "NO_SHOW"],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_amount",
            min_value=0,
            mostly=0.99,  # allow 1% anomalies
        )
    )
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name=suite_name, data=batch, suite=suite)
    )
    results = vd.run(batch_parameters={"dataframe": df})
    failed = [r["expectation_config"]["type"] for r in results.results if not r["success"]]

    return ValidationResult(
        suite_name=suite_name,
        passed=results.success,
        failed_expectations=failed,
        statistics=dict(results.statistics),
    )


def _validate_orders(df: pd.DataFrame) -> ValidationResult:
    """Validate staging restaurant_orders table."""
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas("orders_ds")
    da = ds.add_dataframe_asset("orders")
    batch = da.add_batch_definition_whole_dataframe("orders_batch")

    suite_name = "staging_restaurant_orders"
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="branch_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="status"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["OPEN", "CLOSED", "CANCELLED"],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_amount",
            min_value=0,
            mostly=0.99,
        )
    )
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name=suite_name, data=batch, suite=suite)
    )
    results = vd.run(batch_parameters={"dataframe": df})
    failed = [r["expectation_config"]["type"] for r in results.results if not r["success"]]

    return ValidationResult(
        suite_name=suite_name,
        passed=results.success,
        failed_expectations=failed,
        statistics=dict(results.statistics),
    )


def _validate_employees(df: pd.DataFrame) -> ValidationResult:
    """Validate staging employees table."""
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas("employees_ds")
    da = ds.add_dataframe_asset("employees")
    batch = da.add_batch_definition_whole_dataframe("employees_batch")

    suite_name = "staging_employees"
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="department_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="email"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="hire_date"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="email"))

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name=suite_name, data=batch, suite=suite)
    )
    results = vd.run(batch_parameters={"dataframe": df})
    failed = [r["expectation_config"]["type"] for r in results.results if not r["success"]]

    return ValidationResult(
        suite_name=suite_name,
        passed=results.success,
        failed_expectations=failed,
        statistics=dict(results.statistics),
    )


def run_staging_validation(conn: psycopg2.extensions.connection) -> ValidationSummary:
    """
    Run all data quality suites against the staging schema.
    Returns a ValidationSummary with pass/fail per suite.
    """
    log.info("Starting data quality validation …")
    summary = ValidationSummary()

    validators = [
        ("reservations",       _validate_reservations),
        ("restaurant_orders",  _validate_orders),
        ("employees",          _validate_employees),
    ]

    for table_name, validate_fn in validators:
        try:
            df = _load_table(conn, table_name)
            if df.empty:
                log.warning("Quality FAIL: '%s' is empty — no rows extracted", table_name)
                summary.results.append(
                    ValidationResult(
                        suite_name=f"staging_{table_name}",
                        passed=False,
                        failed_expectations=["table_is_empty"],
                    )
                )
                continue

            result = validate_fn(df)
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
            log.error("Validation error for '%s': %s", table_name, exc, exc_info=True)
            summary.results.append(
                ValidationResult(suite_name=f"staging_{table_name}", passed=False, failed_expectations=[str(exc)])
            )

    log.info(
        "Data quality validation complete: %d/%d passed",
        sum(1 for r in summary.results if r.passed),
        len(summary.results),
    )
    return summary
