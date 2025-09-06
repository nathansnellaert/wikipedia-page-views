import os
os.environ['CONNECTOR_NAME'] = 'wikipedia-page-views'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.page_views.page_views import process_page_views

def main():
    validate_environment()
    process_page_views()

if __name__ == "__main__":
    main()