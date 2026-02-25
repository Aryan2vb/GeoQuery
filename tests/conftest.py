"""
Test configuration and fixtures.
"""
import pytest
from unittest.mock import Mock


@pytest.fixture
def mock_db_connection():
    """Create a mock database connection."""
    conn = Mock()
    
    # Mock cursor
    cursor = Mock()
    cursor.fetchall.return_value = []
    cursor.description = []
    
    # Make cursor work as context manager
    cursor_ctx = Mock()
    cursor_ctx.__enter__ = Mock(return_value=cursor)
    cursor_ctx.__exit__ = Mock(return_value=False)
    conn.cursor.return_value = cursor_ctx
    
    return conn


@pytest.fixture
def mock_llm():
    """Create a mock LLM provider."""
    llm = Mock()
    llm.generate.return_value = """
# Reasoning: Test reasoning
```sql
SELECT * FROM planet_osm_point;
```
"""
    llm.extract_sql.return_value = "SELECT * FROM planet_osm_point;"
    return llm
