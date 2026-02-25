"""
Usage Example for Dr. Yashvardhan Sharma's Geospatial AI Middleware

This module demonstrates how to use the osm_sql_generator package
to convert Natural Language queries to PostGIS SQL for Delhi OSM data.
"""

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

from osm_sql_generator import (
    get_spatial_metadata,
    generate_spatial_prompt,
    execute_with_retry,
    natural_language_to_sql,
    OpenAIProvider,
    GroqProvider,
)


def get_db_connection():
    """Create database connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        database=os.getenv("PGDATABASE", "delhi_db"),
        user=os.getenv("PGUSER", "aryansoni"),
        password=os.getenv("PGPASSWORD", ""),
    )


def get_llm():
    """Get LLM provider from environment."""
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key or api_key == "your-openai-api-key-here":
        raise ValueError("Please set OPENAI_API_KEY in .env file")
    return GroqProvider(api_key=api_key)


def example_metadata_fetch():
    """Example 1: Fetch spatial metadata from database."""
    print("=" * 60)
    print("EXAMPLE 1: Fetch Spatial Metadata")
    print("=" * 60)

    conn = get_db_connection()
    try:
        metadata = get_spatial_metadata(conn)
        print(metadata)
    finally:
        conn.close()


def example_direct_execution():
    """Example 2: Direct execution with retry logic."""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Direct Execution with Retry")
    print("=" * 60)

    conn = get_db_connection()
    llm = get_llm()

    user_query = "Hospitals in Delhi"
    result = natural_language_to_sql(user_query, conn, llm)

    print(f"Query: {user_query}")
    print(f"Success: {result['success']}")
    print(f"SQL: {result['sql']}")
    print(f"Reasoning: {result['reasoning']}")
    print(f"Data: {result['data']}")

    conn.close()


def example_proximity_query():
    """Example 3: Proximity query with ST_DWithin."""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Proximity Query (ST_DWithin)")
    print("=" * 60)

    conn = get_db_connection()
    llm = get_llm()

    user_query = "Parks within 5km of Connaught Place"
    result = natural_language_to_sql(user_query, conn, llm)

    print(f"Query: {user_query}")
    print(f"SQL: {result['sql']}")
    print(f"Reasoning: {result['reasoning']}")
    print(f"Results: {result['data']}")

    conn.close()


def example_road_length():
    """Example 4: Road length calculation."""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Road Length Calculation")
    print("=" * 60)

    conn = get_db_connection()
    llm = get_llm()

    user_query = "Length of roads near Metro"
    result = natural_language_to_sql(user_query, conn, llm)

    print(f"Query: {user_query}")
    print(f"SQL: {result['sql']}")
    print(f"Reasoning: {result['reasoning']}")
    print(f"Results: {result['data']}")

    conn.close()


if __name__ == "__main__":
    print("Geospatial AI Middleware - Usage Examples")
    print("For Dr. Yashvardhan Sharma, BITS Pilani")
    print()

    example_direct_execution()
