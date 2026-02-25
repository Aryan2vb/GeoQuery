def generate_spatial_prompt(user_query, schema_json):
    """
    Constructs the few-shot prompt with spatial rules for Delhi OSM data.
    """
    prompt = f"""
You are an expert PostGIS DBA for an OpenStreetMap (OSM) dataset of Delhi.
Your goal is to convert Natural Language into valid, optimized SQL.

DATABASE SCHEMA:
{schema_json}

CRITICAL CRS INFORMATION:
- The database uses SRID 3857 (Web Mercator / Pseudo-Mercator)
- Distances are in METERS directly - NO need to cast to geography
- NEVER use ::geography or ST_Distance(geography) - it will fail because geography only works with WGS84 (SRID 4326)
- Use ST_DWithin(way, ref_way, meters) directly with the way column

SPATIAL RULES:
1. Use the 'way' column for all geometry operations (NO ::geography).
2. The SRID is 3857 (Web Mercator). Distances are in METERS directly.
3. Use ST_DWithin(way, ref_way, distance_in_meters) for proximity queries.
4. Use ST_Intersects or ST_Contains for spatial joins.
5. IMPORTANT: When searching for features near ANY other feature (e.g., "schools near any hospital"), use EXISTS or JOIN - NEVER use subquery in ST_DWithin as it returns multiple rows.
6. NEVER cast to geography ::geography - it will cause errors with SRID 3857.

EXAMPLES:
Q: "Hospitals in Delhi"
# Reasoning: User wants points labeled as hospitals.
SQL: SELECT name, way FROM planet_osm_point WHERE amenity='hospital';

Q: "Parks within 2km of Connaught Place"
# Reasoning: Find the point 'Connaught Place', buffer it by 2000m. Use LIMIT 1 for single geometry.
SQL: SELECT name FROM planet_osm_polygon WHERE leisure='park' AND ST_DWithin(way, (SELECT way FROM planet_osm_point WHERE name='Connaught Place' LIMIT 1), 2000);

Q: "Total length of primary roads"
# Reasoning: Sum the lengths of lines tagged as primary highways.
SQL: SELECT SUM(ST_Length(way)) FROM planet_osm_line WHERE highway='primary';

Q: "Schools within 5km of any hospital"
# Reasoning: Use EXISTS to find schools that have ANY hospital within 5km. DO NOT use subquery in ST_DWithin.
SQL: SELECT s.name FROM planet_osm_point s WHERE sschool' AND EXISTS.amenity=' (SELECT 1 FROM planet_osm_point h WHERE h.amenity='hospital' AND ST_DWithin(s.way, h.way, 5000));

Q: "Hospitals near main roads"
# Reasoning: Find hospitals that are within 1000m of any primary/secondary/tertiary road using EXISTS.
SQL: SELECT h.name FROM planet_osm_point h WHERE h.amenity='hospital' AND EXISTS (SELECT 1 FROM planet_osm_line r WHERE r.highway IN ('primary','secondary','tertiary','trunk') AND ST_DWithin(h.way, r.way, 1000));

Q: "5 metro stations near Jahangirpuri"
# Reasoning: Find metro stations (railway=station with subway tag) within 5km of Jahangirpuri, order by distance, limit 5.
SQL: SELECT m.name, ST_Distance(m.way, j.way) as distance FROM planet_osm_point m CROSS JOIN (SELECT way FROM planet_osm_point WHERE name='Jahangirpuri' LIMIT 1) j WHERE m.railway='station' AND m.tags?'subway' AND ST_DWithin(m.way, j.way, 5000) ORDER BY distance LIMIT 5;

USER QUERY: {user_query}
# Reasoning:
SQL:"""
    return prompt


def generate_fix_prompt(original_query, error_message, failed_sql):
    """
    Generates a prompt for the LLM to fix a failed SQL query.
    """
    prompt = f"""
The following SQL query failed with an error. Please fix it.

ORIGINAL USER QUERY: {original_query}

FAILED SQL:
{failed_sql}

ERROR MESSAGE:
{error_message}

CRITICAL CRS REMINDER:
- The database uses SRID 3857 (Web Mercator)
- Distances are in METERS directly - NO geography casting
- NEVER use ::geography - it will fail because geography only works with WGS84 (SRID 4326)
- Use ST_DWithin(way, ref_way, meters) directly

SPATIAL RULES (reminder):
1. Use the 'way' column for geometry - NO ::geography.
2. ST_DWithin(way1, way2, distance_in_meters) - no casting needed.
3. Use LIMIT 1 when using subqueries to get a single geometry.
4. Use EXISTS pattern for "near any" queries.

Please provide the corrected SQL query:
# Reasoning:
SQL:"""
    return prompt
