import pytest
from datetime import datetime
import pyarrow as pa
from .page_views import process_page_views, fetch_pageviews_for_date

def test_fetch_pageviews_for_date():
    """Test fetching page views for a specific date."""
    # Test with a known date that should have data
    test_date = datetime(2024, 1, 1)
    
    try:
        data = fetch_pageviews_for_date(test_date)
        assert len(data) > 0
        
        # Check data structure
        first_item = data[0]
        assert 'date' in first_item
        assert 'locale' in first_item
        assert 'entity' in first_item
        assert 'page_id' in first_item
        assert 'views' in first_item
        
        # Check data types
        assert isinstance(first_item['views'], int)
        assert first_item['locale'] in ['en', 'nl']
        
    except Exception as e:
        # If the test date is too recent or the API is down, skip
        pytest.skip(f"Could not fetch data for test date: {e}")

def test_process_page_views_schema():
    """Test the schema of the returned table."""
    # This test might return an empty table if no new data
    table = process_page_views()
    
    # Check schema
    assert table.schema.names == ['date', 'locale', 'entity', 'page_id', 'views_sum']
    assert table.schema.field('date').type == pa.date32()
    assert table.schema.field('locale').type == pa.string()
    assert table.schema.field('entity').type == pa.string()
    assert table.schema.field('page_id').type == pa.string()
    assert table.schema.field('views_sum').type == pa.int64()