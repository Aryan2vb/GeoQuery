import json


def get_spatial_metadata(conn):
    """
    Fetches geospatial metadata to provide context to the LLM.
    Identifies tables, geometry columns, SRIDs, and basic attributes.
    """
    metadata = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT f_table_name, f_geometry_column, srid, type 
            FROM geometry_columns 
            WHERE f_table_name LIKE 'planet_osm_%';
        """)
        geo_cols = cur.fetchall()

        for table, col, srid, geom_type in geo_cols:
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table}' AND column_name != '{col}';
            """)
            attributes = [row[0] for row in cur.fetchall()]

            metadata[table] = {
                "geometry_column": col,
                "srid": srid,
                "geometry_type": geom_type,
                "attributes": attributes[:15],
            }

    return json.dumps(metadata, indent=2)
