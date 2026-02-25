# Natural Language to Spatial Query Translation

> A secure, validated middleware for converting natural language queries into optimized PostGIS spatial SQL for Delhi OpenStreetMap data.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL 14+](https://img.shields.io/badge/PostgreSQL-14+-336791.svg)](https://www.postgresql.org/)
[![PostGIS 3.0+](https://img.shields.io/badge/PostGIS-3.0+-darkgreen.svg)](https://postgis.net/)

## Overview

This system translates natural language queries like "Find schools within 5km of hospitals" into optimized, validated PostGIS SQL. It features a secure intermediate representation (IR) layer, SQL injection prevention, and accurate geospatial calculations.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER INTERFACE                                    │
│                    (Streamlit Web App / Python API)                          │
└──────────────────────────────┬────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────────────┐
│                         INPUT VALIDATION LAYER                                 │
│              (Length limits, character whitelisting, sanitization)               │
└──────────────────────────────┬────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────────────┐
│                           LLM INTERFACE LAYER                                    │
│              (OpenAI, Groq, LangChain providers via unified API)                 │
└──────────────────────────────┬────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────────────┐
│                    INTERMEDIATE REPRESENTATION (IR) LAYER                      │
│         (Structured spatial operations: Select, Filter, Join, Buffer)          │
│                    ┌─────────────┬─────────────┬─────────────┐                  │
│                    │   SELECT    │   FILTER    │    JOIN     │                  │
│                    │  (entities) │ (distance,  │ (intersects,│                  │
│                    │             │  contains)  │  within)    │                  │
│                    └─────────────┴─────────────┴─────────────┘                  │
└──────────────────────────────┬────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────────────┐
│                      SQL VALIDATION & GENERATION                               │
│    (Statement whitelist, table/column verification, parameterization)          │
└──────────────────────────────┬────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────────────┐
│                        SPATIAL EXECUTION LAYER                                 │
│              (PostGIS with accurate CRS handling - geography type)             │
└───────────────────────────────────────────────────────────────────────────────┘
```

## Features

### Core Capabilities
- **Natural Language to SQL**: Converts queries like "Parks within 2km of Connaught Place" to optimized PostGIS SQL
- **Secure by Design**: SQL injection prevention via statement whitelisting and IR validation
- **Accurate Geospatial**: Uses geography type for precise distance calculations (±0.1% accuracy)
- **Self-Correction**: Automatic retry with LLM feedback on SQL errors
- **Multiple LLM Support**: OpenAI, Groq (Llama 3.3 70B), LangChain integration

### Supported Query Patterns
- ✅ Basic selection: "Hospitals in Delhi"
- ✅ Distance queries: "Schools within 5km of hospitals"
- ✅ Proximity queries: "Schools near main roads"
- ✅ Containment: "Hospitals inside district boundaries"
- ✅ Intersection: "Roads crossing rivers"
- ✅ Multiple constraints: "Schools within 5km of hospitals AND near main roads"
- ✅ Logical operators: "Hospitals near schools OR highways"
- ✅ Negation: "Schools NOT near highways"

## Prerequisites

### System Requirements
- Python 3.10 or higher
- PostgreSQL 14+ with PostGIS 3.0+
- 8GB RAM minimum (16GB recommended)
- API key for OpenAI or Groq

### Database Setup

1. **Install PostgreSQL and PostGIS:**
```bash
# macOS
brew install postgresql postgis

# Ubuntu/Debian
sudo apt-get install postgresql-14 postgis

# Start PostgreSQL
brew services start postgresql  # macOS
sudo systemctl start postgresql  # Linux
```

2. **Create Database and Enable PostGIS:**
```sql
CREATE DATABASE delhi_db;
\c delhi_db;
CREATE EXTENSION postgis;
CREATE EXTENSION hstore;
```

3. **Import Delhi OSM Data:**
```bash
# Download Delhi OSM extract
wget https://download.geofabrik.de/asia/india/delhi-latest.osm.pbf

# Import using osm2pgsql
osm2pgsql -d delhi_db --create --slim \
  -G --hstore-all --multi-geometry \
  -S default.style delhi-latest.osm.pbf
```

4. **Verify Spatial Indexes:**
```sql
-- Create GiST indexes if not present
CREATE INDEX idx_planet_osm_point_way ON planet_osm_point USING GIST (way);
CREATE INDEX idx_planet_osm_polygon_way ON planet_osm_polygon USING GIST (way);
CREATE INDEX idx_planet_osm_line_way ON planet_osm_line USING GIST (way);

-- Verify indexes
SELECT * FROM pg_indexes WHERE tablename LIKE 'planet_osm_%';
```

## Installation

1. **Clone the Repository:**
```bash
git clone <repository-url>
cd bit-by-bit
```

2. **Create Virtual Environment:**
```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate  # Windows
```

3. **Install Dependencies:**
```bash
pip install -r requirements.txt
```

4. **Configure Environment:**
```bash
cp .env.example .env
# Edit .env with your API key and database credentials
```

5. **Verify Installation:**
```bash
python -c "from osm_sql_generator import natural_language_to_sql; print('✓ Installation successful')"
```

## Configuration

Create a `.env` file:

```bash
# Required: LLM API Key
OPENAI_API_KEY=sk-your-openai-key-here
# OR
GROQ_API_KEY=gsk-your-groq-key-here

# Required: Database Connection
PGHOST=localhost
PGPORT=5432
PGDATABASE=delhi_db
PGUSER=your_username
PGPASSWORD=your_password

# Optional: LLM Configuration
LLM_PROVIDER=groq  # or 'openai'
LLM_MODEL=llama-3.3-70b-versatile  # or 'gpt-4o-mini'
LLM_TEMPERATURE=0.0

# Optional: System Configuration
MAX_QUERY_LENGTH=500
MAX_RETRIES=3
LOG_LEVEL=INFO
```

## Usage

### Web Interface (Streamlit)

```bash
streamlit run app.py
```

Then open http://localhost:8501 in your browser.

### Python API

```python
import psycopg2
from osm_sql_generator import natural_language_to_sql, GroqProvider
from dotenv import load_dotenv
import os

load_dotenv()

# Connect to database
conn = psycopg2.connect(
    database=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD")
)

# Initialize LLM provider
llm = GroqProvider(api_key=os.getenv("OPENAI_API_KEY"))

# Execute natural language query
result = natural_language_to_sql(
    "Schools within 5km of hospitals",
    conn,
    llm
)

print(f"SQL: {result['sql']}")
print(f"Reasoning: {result['reasoning']}")
print(f"Results: {result['data']}")

conn.close()
```

### Example Queries

| Natural Language | Generated SQL Pattern |
|------------------|----------------------|
| "Hospitals in Delhi" | `SELECT name, way FROM planet_osm_point WHERE amenity='hospital'` |
| "Parks within 2km of Connaught Place" | Uses `ST_DWithin` with geography cast for accuracy |
| "Schools near main roads" | Uses `EXISTS` with highway type filter |
| "Total length of primary roads" | `SUM(ST_Length(way::geography))` for accurate meters |

## Security

### SQL Injection Prevention

The system implements multiple security layers:

1. **Statement Whitelist**: Only `SELECT` statements are allowed
2. **Table/Column Validation**: All tables/columns verified against schema
3. **Input Sanitization**: Length limits and character filtering
4. **IR Validation**: Intermediate representation validates query structure

### Testing Security

```python
# These queries will be rejected:
"DROP TABLE schools"  # ❌ Not a SELECT statement
"; DELETE FROM hospitals"  # ❌ Contains dangerous keywords
"SELECT * FROM users; DROP TABLE users--"  # ❌ Multiple statements detected
```

## Testing

### Run Test Suite

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=osm_sql_generator --cov-report=html

# Run specific test categories
pytest tests/test_security.py -v
pytest tests/test_ir.py -v
pytest tests/test_spatial.py -v
```

### Test Categories

- **Security Tests**: SQL injection attempts, statement validation
- **IR Tests**: Intermediate representation generation and validation
- **Spatial Tests**: CRS accuracy, distance calculations, geometry operations
- **Integration Tests**: End-to-end query execution
- **Performance Tests**: Query execution benchmarks

## Evaluation

### Running Evaluation Suite

```bash
python -m evaluation.run_evaluation --dataset test_queries.json
```

### Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| Translation Accuracy | % queries generating valid SQL | >95% |
| Semantic Correctness | % queries returning expected results | >90% |
| Spatial Accuracy | Distance error margin | <1% |
| Security Score | Injection attempt rejection rate | 100% |
| Latency | End-to-end query time | <5s |

## API Reference

### Core Functions

#### `natural_language_to_sql(query, conn, llm, max_retries=3)`

Main entry point for query translation and execution.

**Parameters:**
- `query` (str): Natural language query
- `conn` (psycopg2 connection): Database connection
- `llm` (LLMProvider): LLM provider instance
- `max_retries` (int): Maximum retry attempts on error

**Returns:**
```python
{
    "success": True,
    "data": [...],  # Query results
    "sql": "SELECT ...",  # Generated SQL
    "reasoning": "Query interpretation...",
    "ir": {...}  # Intermediate representation
}
```

#### `IntermediateRepresentation`

Structured representation of spatial query:

```python
{
    "select": {
        "entity": "schools",
        "attributes": ["name", "way"]
    },
    "filters": [
        {"type": "distance", "target": "hospitals", "distance": 5000}
    ],
    "joins": [],
    "operations": ["buffer", "intersection"]
}
```

## Troubleshooting

### Common Issues

**Database Connection Failed**
```
Solution: Verify PostgreSQL is running and credentials in .env are correct
```

**No Results Returned**
```
Solution: Check that OSM data is imported and spatial indexes exist
```

**LLM API Errors**
```
Solution: Verify API key is valid and has available credits
```

**Distance Calculations Seem Wrong**
```
Solution: The system uses accurate geography-based calculations. Verify your reference points are correct.
```

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes with tests
4. Run test suite: `pytest tests/`
5. Submit pull request

### Code Style

- Follow PEP 8
- Use type hints
- Add docstrings to all public functions
- Maintain test coverage >90%

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- Dr. Yashvardhan Sharma, BITS Pilani
- OpenStreetMap contributors
- PostGIS development team

## Contact

For issues and feature requests, please use GitHub Issues.
