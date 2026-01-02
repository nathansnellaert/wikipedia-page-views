"""Transform Wikipedia page views into two datasets:

1. popular_pages - Daily views for pages that were ever in top 10k on any day
2. monthly_page_views - All pages aggregated by month
"""
import os
from glob import glob
from pathlib import Path
from collections import defaultdict
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from subsets_utils import upload_data, get_data_dir, save_raw_parquet

TOP_K_PER_DAY = 10_000


def get_raw_parquet_files() -> list[Path]:
    """Get all raw parquet files sorted by date."""
    raw_dir = Path(get_data_dir()) / "raw"
    files = list(raw_dir.glob("page_views_*.parquet"))
    return sorted(files)


def build_popular_pages_set(files: list[Path]) -> set[str]:
    """Pass 1: Build set of page_ids that were ever in top 10k on any day."""
    popular_pages = set()

    for i, file in enumerate(files):
        table = pq.read_table(file)

        # Sort by views descending and take top 10k
        indices = pc.sort_indices(table, sort_keys=[("views", "descending")])
        top_k = pc.take(table, indices[:TOP_K_PER_DAY])

        # Add page_ids to set
        page_ids = top_k.column("page_id").to_pylist()
        popular_pages.update(page_ids)

        if (i + 1) % 100 == 0:
            print(f"  Pass 1: Scanned {i + 1}/{len(files)} days, {len(popular_pages):,} unique popular pages")

    print(f"  Pass 1 complete: {len(popular_pages):,} unique pages were in top {TOP_K_PER_DAY:,} on at least one day")
    return popular_pages


def transform_popular_pages(files: list[Path], popular_pages: set[str]) -> pa.Table:
    """Pass 2: Filter all data to only popular pages."""
    all_tables = []

    for i, file in enumerate(files):
        table = pq.read_table(file)

        # Filter to popular pages
        page_ids = table.column("page_id")
        mask = pc.is_in(page_ids, value_set=pa.array(list(popular_pages)))
        filtered = table.filter(mask)

        if len(filtered) > 0:
            all_tables.append(filtered)

        if (i + 1) % 100 == 0:
            print(f"  Pass 2: Processed {i + 1}/{len(files)} days")

    if not all_tables:
        return pa.table({
            "date": pa.array([], type=pa.string()),
            "page_id": pa.array([], type=pa.string()),
            "entity": pa.array([], type=pa.string()),
            "views": pa.array([], type=pa.int64()),
        })

    combined = pa.concat_tables(all_tables)
    print(f"  Pass 2 complete: {len(combined):,} total rows for popular pages")
    return combined


def transform_monthly_views(files: list[Path]) -> pa.Table:
    """Aggregate all page views by month."""
    # Accumulate monthly totals: (month, page_id) -> (entity, total_views)
    monthly_data = defaultdict(lambda: {"entity": None, "views": 0})

    for i, file in enumerate(files):
        table = pq.read_table(file)

        dates = table.column("date").to_pylist()
        page_ids = table.column("page_id").to_pylist()
        entities = table.column("entity").to_pylist()
        views = table.column("views").to_pylist()

        for date, page_id, entity, view_count in zip(dates, page_ids, entities, views):
            month = date[:7]  # "YYYY-MM"
            key = (month, page_id)
            monthly_data[key]["entity"] = entity
            monthly_data[key]["views"] += view_count

        if (i + 1) % 100 == 0:
            print(f"  Monthly aggregation: Processed {i + 1}/{len(files)} days")

    # Convert to table
    months = []
    page_ids = []
    entities = []
    views = []

    for (month, page_id), data in monthly_data.items():
        months.append(month)
        page_ids.append(page_id)
        entities.append(data["entity"])
        views.append(data["views"])

    table = pa.table({
        "month": pa.array(months, type=pa.string()),
        "page_id": pa.array(page_ids, type=pa.string()),
        "entity": pa.array(entities, type=pa.string()),
        "views": pa.array(views, type=pa.int64()),
    })

    print(f"  Monthly aggregation complete: {len(table):,} rows")
    return table


def run():
    """Transform raw page views into publishable datasets."""
    from .test import test_popular_pages, test_monthly_views

    files = get_raw_parquet_files()
    if not files:
        print("No raw parquet files found. Run ingest first.")
        return

    print(f"Found {len(files)} raw parquet files")

    # 1. Popular pages (two-pass)
    print("\n[1/2] Building popular_pages dataset...")
    popular_pages_set = build_popular_pages_set(files)
    popular_table = transform_popular_pages(files, popular_pages_set)

    test_popular_pages(popular_table)
    save_raw_parquet(popular_table, "wikipedia/popular_pages")
    print(f"  Saved popular_pages: {len(popular_table):,} rows")

    # 2. Monthly views (all pages)
    print("\n[2/2] Building monthly_page_views dataset...")
    monthly_table = transform_monthly_views(files)
    test_monthly_views(monthly_table)

    # Save monthly as partitioned by month
    months = set(monthly_table.column("month").to_pylist())
    for month in sorted(months):
        mask = pc.equal(monthly_table.column("month"), month)
        month_data = monthly_table.filter(mask)
        save_raw_parquet(month_data, f"wikipedia/monthly/{month}")

    print(f"\n  Saved monthly_page_views: {len(monthly_table):,} rows across {len(months)} months")

    print("\nTransforms complete!")


if __name__ == "__main__":
    run()
