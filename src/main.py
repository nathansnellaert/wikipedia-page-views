import argparse
import os
import sys

from subsets_utils import validate_environment
from ingest import page_views as ingest_page_views
from transforms.page_views import main as transform_page_views


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from Wikipedia")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    needs_continuation = False

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        needs_continuation = ingest_page_views.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        transform_page_views.run()

    if needs_continuation:
        print("\nExiting with code 2 to signal continuation needed")
        sys.exit(2)


if __name__ == "__main__":
    main()
