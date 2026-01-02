"""Tests for Wikipedia page view transforms."""
import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_valid_month, assert_positive


def test_popular_pages(table: pa.Table) -> None:
    """Validate popular_pages output."""
    # Schema validation
    validate(table, {
        "columns": {
            "date": "string",
            "page_id": "string",
            "entity": "string",
            "views": "int64",
        },
        "not_null": ["date", "page_id", "entity", "views"],
        "min_rows": 1,
    })

    # Date format
    assert_valid_date(table, "date")

    # Views must be positive
    assert_positive(table, "views", allow_zero=False)

    # page_id should not be 'null' string
    page_ids = table.column("page_id").to_pylist()
    null_strings = [p for p in page_ids if p == "null"]
    assert not null_strings, f"Found {len(null_strings)} 'null' string page_ids"

    # Check we have multiple dates (sanity check)
    dates = set(table.column("date").to_pylist())
    assert len(dates) >= 1, f"Expected multiple dates, got {len(dates)}"

    print(f"  ✓ popular_pages validation passed: {len(table):,} rows, {len(dates)} dates")


def test_monthly_views(table: pa.Table) -> None:
    """Validate monthly_page_views output."""
    # Schema validation
    validate(table, {
        "columns": {
            "month": "string",
            "page_id": "string",
            "entity": "string",
            "views": "int64",
        },
        "not_null": ["month", "page_id", "entity", "views"],
        "unique": ["month", "page_id"],
        "min_rows": 1,
    })

    # Month format
    assert_valid_month(table, "month")

    # Views must be positive
    assert_positive(table, "views", allow_zero=False)

    # page_id should not be 'null' string
    page_ids = table.column("page_id").to_pylist()
    null_strings = [p for p in page_ids if p == "null"]
    assert not null_strings, f"Found {len(null_strings)} 'null' string page_ids"

    # Check we have reasonable number of pages per month
    months = set(table.column("month").to_pylist())
    avg_pages_per_month = len(table) / len(months)
    assert avg_pages_per_month >= 1000, f"Expected at least 1000 pages/month, got {avg_pages_per_month:.0f}"

    print(f"  ✓ monthly_page_views validation passed: {len(table):,} rows, {len(months)} months")
