import os
import sys

os.environ['CONNECTOR_NAME'] = 'wikipedia-page-views'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment
from assets.page_views.page_views import process_page_views

# Note: Not publishing to Subsets yet - just uploading raw data to R2
# TODO: Add upload_data + publish once data pipeline is ready


def main():
    validate_environment()
    needs_continuation = process_page_views()

    # Exit with code 2 to signal workflow should re-trigger
    if needs_continuation:
        print("\nExiting with code 2 to signal continuation needed")
        sys.exit(2)


if __name__ == "__main__":
    main()