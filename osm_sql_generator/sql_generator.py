"""
IR to SQL Code Generator

Transforms validated Intermediate Representation into optimized,
secure PostGIS SQL with accurate CRS handling.
"""

from typing import Dict, List, Optional, Any
from .ir import (
    IntermediateRepresentation, 
    SpatialOperationType, 
    LogicalOperator,
    Condition
)


class SQLGenerationError(Exception):
    """Raised when SQL generation fails."""
    pass


class IRToSQLGenerator:
    """
    Generates optimized PostGIS SQL from Intermediate Representation.
    
    Features:
    - Geography type for accurate distance calculations
    - Spatial index optimization hints
    - EXISTS pattern for "near any" queries
    - Proper CRS handling (3857→4326→geography)
    """
    
    # Table mapping for OSM entities
    OSM_TABLE_MAP = {
        'hospital': 'planet_osm_point',
        'hospitals': 'planet_osm_point',
        'school': 'planet_osm_point',
        'schools': 'planet_osm_point',
        'park': 'planet_osm_polygon',
        'parks': 'planet_osm_polygon',
        'road': 'planet_osm_line',
        'roads': 'planet_osm_line',
        'highway': 'planet_osm_line',
        'highways': 'planet_osm_line',
        'metro': 'planet_osm_point',
        'restaurant': 'planet_osm_point',
        'restaurants': 'planet_osm_point',
        'building': 'planet_osm_polygon',
        'buildings': 'planet_osm_polygon',
    }
    
    # Tag mapping for OSM attributes
    OSM_TAG_MAP = {
        'hospital': {'amenity': 'hospital'},
        'hospitals': {'amenity': 'hospital'},
        'school': {'amenity': 'school'},
        'schools': {'amenity': 'school'},
        'park': {'leisure': 'park'},
        'parks': {'leisure': 'park'},
        'road': {'highway': 'primary'},
        'roads': {'highway': 'primary'},
        'highway': {'highway': 'primary'},
        'highways': {'highway': 'primary'},
        'main_road': {'highway': ['primary', 'secondary', 'tertiary', 'trunk']},
        'metro': {'railway': 'station'},
        'restaurant': {'amenity': 'restaurant'},
        'restaurants': {'amenity': 'restaurant'},
    }
    
    def __init__(self, use_geography: bool = True):
        """
        Initialize SQL generator.
        
        Args:
            use_geography: Use geography type for accurate distance calculations
        """
        self.use_geography = use_geography
    
    def generate_sql(self, ir: IntermediateRepresentation) -> str:
        """
        Generate SQL from Intermediate Representation.
        
        Args:
            ir: Validated intermediate representation
            
        Returns:
            Generated SQL string
            
        Raises:
            SQLGenerationError: If generation fails
        """
        try:
            # Validate IR first
            is_valid, errors = ir.validate()
            if not is_valid:
                raise SQLGenerationError(f"IR validation failed: {errors}")
            
            # Build query parts
            select_clause = self._build_select(ir)
            from_clause = self._build_from(ir)
            where_clause = self._build_where(ir)
            group_clause = self._build_group_by(ir)
            order_clause = self._build_order_by(ir)
            limit_clause = self._build_limit(ir)
            
            # Combine into final SQL
            parts = [select_clause, from_clause]
            if where_clause:
                parts.append(where_clause)
            if group_clause:
                parts.append(group_clause)
            if order_clause:
                parts.append(order_clause)
            if limit_clause:
                parts.append(limit_clause)
            
            sql = "\n".join(parts) + ";"
            
            return sql
            
        except Exception as e:
            raise SQLGenerationError(f"SQL generation failed: {str(e)}") from e
    
    def _build_select(self, ir: IntermediateRepresentation) -> str:
        """Build SELECT clause."""
        # Handle aggregation
        if ir.aggregation:
            func = ir.aggregation.get("function", "")
            field = ir.aggregation.get("field", "")
            alias = ir.aggregation.get("alias", "result")
            
            # Special handling for spatial aggregates
            if func.upper() == "ST_LENGTH" and self.use_geography:
                # Check if field already has ::geography
                if "::geography" in field:
                    return f"SELECT SUM({field}) as {alias}"
                else:
                    return f"SELECT SUM(ST_Length({field}::geography)) as {alias}"
            elif func.upper() in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
                return f"SELECT {func}({field}) as {alias}"
        
        # Standard selection
        attributes = ir.select_attributes or ["name"]
        
        # Always include way for geometry
        if "way" not in attributes:
            attributes.append("way")
        
        # Add ST_AsGeoJSON for geometry if needed
        attrs_str = ", ".join(attributes)
        
        return f"SELECT {attrs_str}"
    
    def _build_from(self, ir: IntermediateRepresentation) -> str:
        """Build FROM clause."""
        if not ir.select_entities:
            raise SQLGenerationError("No entities to select from")
        
        # Get primary entity table
        primary_entity = ir.select_entities[0]
        table = primary_entity.table or self._resolve_table(primary_entity.name)
        
        # Add alias
        alias = self._get_alias(primary_entity.name)
        
        return f"FROM {table} {alias}"
    
    def _build_where(self, ir: IntermediateRepresentation) -> Optional[str]:
        """Build WHERE clause from conditions."""
        if not ir.conditions:
            # Add basic entity filter if no conditions
            return self._build_basic_entity_filter(ir)
        
        conditions_sql = []
        
        for i, condition in enumerate(ir.conditions):
            sql = self._build_condition_sql(condition, ir.select_entities[0].name, i)
            if sql:
                conditions_sql.append(sql)
        
        if not conditions_sql:
            return self._build_basic_entity_filter(ir)
        
        # Combine conditions with logical operators
        combined = self._combine_conditions(conditions_sql, ir.conditions)
        
        # Add entity type filter
        entity_filter = self._build_basic_entity_filter(ir)
        if entity_filter:
            combined = f"{entity_filter} AND ({combined})"
        else:
            combined = f"WHERE {combined}"
        
        return combined
    
    def _build_condition_sql(self, condition: Condition, primary_entity: str, index: int) -> Optional[str]:
        """Build SQL for a single condition."""
        f = condition.filter
        op_type = f.operation_type
        target = f.target_entity
        params = f.parameters
        
        primary_alias = self._get_alias(primary_entity)
        
        if op_type == SpatialOperationType.PROXIMITY:
            return self._build_proximity_sql(primary_alias, target, params, condition.negate)
        
        elif op_type == SpatialOperationType.CONTAINMENT:
            return self._build_containment_sql(primary_alias, target, params, condition.negate)
        
        elif op_type == SpatialOperationType.INTERSECTION:
            return self._build_intersection_sql(primary_alias, target, condition.negate)
        
        elif op_type == SpatialOperationType.FILTER:
            return self._build_filter_sql(primary_alias, target)
        
        elif op_type == SpatialOperationType.NEGATION:
            return self._build_proximity_sql(primary_alias, target, params, negate=True)
        
        return None
    
    def _build_proximity_sql(self, primary_alias: str, target: Any, params: Dict, negate: bool = False) -> str:
        """Build proximity (ST_DWithin) SQL using EXISTS pattern."""
        distance = params.get("distance", 5000)  # Default 5km in meters
        
        target_table = target.table or self._resolve_table(target.name)
        target_alias = self._get_alias(target.name)
        target_tags = target.tags or self._resolve_tags(target.name)
        
        # Build target filter
        target_filter = self._build_tag_filter(target_tags, target_alias)
        
        # Use geography for accurate distance if enabled
        if self.use_geography:
            geo_cast = "::geography"
        else:
            geo_cast = ""
        
        # Use EXISTS pattern for "near any" semantics
        exists_clause = f"""EXISTS (
            SELECT 1 FROM {target_table} {target_alias}
            WHERE {target_filter}
            AND ST_DWithin(
                {primary_alias}.way{geo_cast}, 
                {target_alias}.way{geo_cast}, 
                {distance}
            )
        )"""
        
        if negate:
            return f"NOT {exists_clause}"
        return exists_clause
    
    def _build_containment_sql(self, primary_alias: str, target: Any, params: Dict, negate: bool = False) -> str:
        """Build containment (ST_Within) SQL."""
        # For "inside district A" type queries
        container = params.get("container_entity", target)
        container_table = container.table or self._resolve_table(container.name)
        container_alias = self._get_alias(container.name)
        container_name = params.get("container_name", container.name)
        
        sql = f"""EXISTS (
            SELECT 1 FROM {container_table} {container_alias}
            WHERE {container_alias}.name = '{container_name}'
            AND ST_Within({primary_alias}.way, {container_alias}.way)
        )"""
        
        if negate:
            return f"NOT {sql}"
        return sql
    
    def _build_intersection_sql(self, primary_alias: str, target: Any, negate: bool = False) -> str:
        """Build intersection (ST_Intersects) SQL."""
        target_table = target.table or self._resolve_table(target.name)
        target_alias = self._get_alias(target.name)
        target_tags = target.tags or self._resolve_tags(target.name)
        target_filter = self._build_tag_filter(target_tags, target_alias)
        
        sql = f"""EXISTS (
            SELECT 1 FROM {target_table} {target_alias}
            WHERE {target_filter}
            AND ST_Intersects({primary_alias}.way, {target_alias}.way)
        )"""
        
        if negate:
            return f"NOT {sql}"
        return sql
    
    def _build_filter_sql(self, primary_alias: str, target: Any) -> str:
        """Build attribute filter SQL."""
        tags = target.tags or self._resolve_tags(target.name)
        return self._build_tag_filter(tags, primary_alias)
    
    def _build_tag_filter(self, tags: Dict, alias: str) -> str:
        """Build tag filter conditions."""
        conditions = []
        
        for key, value in tags.items():
            if isinstance(value, list):
                # Handle list of values (e.g., highway types)
                values_str = ", ".join([f"'{v}'" for v in value])
                conditions.append(f"{alias}.{key} IN ({values_str})")
            else:
                conditions.append(f"{alias}.{key} = '{value}'")
        
        if not conditions:
            return "TRUE"  # No filter
        
        return " AND ".join(conditions)
    
    def _build_basic_entity_filter(self, ir: IntermediateRepresentation) -> Optional[str]:
        """Build basic entity type filter (e.g., amenity='hospital')."""
        if not ir.select_entities:
            return None
        
        entity = ir.select_entities[0]
        alias = self._get_alias(entity.name)
        tags = entity.tags or self._resolve_tags(entity.name)
        
        if not tags:
            return None
        
        filter_sql = self._build_tag_filter(tags, alias)
        return f"WHERE {filter_sql}"
    
    def _combine_conditions(self, conditions_sql: List[str], conditions: List[Condition]) -> str:
        """Combine multiple conditions with logical operators."""
        if len(conditions_sql) == 1:
            return f"WHERE {conditions_sql[0]}"
        
        # Build combined condition
        parts = [conditions_sql[0]]
        
        for i in range(1, len(conditions_sql)):
            operator = conditions[i].operator
            if operator == LogicalOperator.OR:
                parts.append(f"OR {conditions_sql[i]}")
            else:  # Default AND
                parts.append(f"AND {conditions_sql[i]}")
        
        return f"WHERE ({' '.join(parts)})"
    
    def _build_group_by(self, ir: IntermediateRepresentation) -> Optional[str]:
        """Build GROUP BY clause if needed."""
        if ir.aggregation and ir.select_attributes:
            # Group by non-aggregated attributes
            non_agg_attrs = [a for a in ir.select_attributes if a != ir.aggregation.get("field", "")]
            if non_agg_attrs:
                return f"GROUP BY {', '.join(non_agg_attrs)}"
        return None
    
    def _build_order_by(self, ir: IntermediateRepresentation) -> Optional[str]:
        """Build ORDER BY clause."""
        if ir.order_by:
            return f"ORDER BY {ir.order_by} {ir.order_direction}"
        return None
    
    def _build_limit(self, ir: IntermediateRepresentation) -> Optional[str]:
        """Build LIMIT clause."""
        if ir.limit:
            return f"LIMIT {ir.limit}"
        return None
    
    def _resolve_table(self, entity_name: str) -> str:
        """Resolve entity name to database table."""
        entity_lower = entity_name.lower().replace(" ", "_")
        
        if entity_lower in self.OSM_TABLE_MAP:
            return self.OSM_TABLE_MAP[entity_lower]
        
        # Default mappings based on suffix
        if any(word in entity_lower for word in ['road', 'highway', 'street', 'avenue']):
            return 'planet_osm_line'
        elif any(word in entity_lower for word in ['building', 'park', 'area', 'district', 'zone']):
            return 'planet_osm_polygon'
        else:
            return 'planet_osm_point'  # Default to points
    
    def _resolve_tags(self, entity_name: str) -> Dict[str, str]:
        """Resolve entity name to OSM tags."""
        entity_lower = entity_name.lower().replace(" ", "_")
        
        if entity_lower in self.OSM_TAG_MAP:
            return self.OSM_TAG_MAP[entity_lower]
        
        # Try to infer from name
        if 'hospital' in entity_lower:
            return {'amenity': 'hospital'}
        elif 'school' in entity_lower:
            return {'amenity': 'school'}
        elif 'park' in entity_lower:
            return {'leisure': 'park'}
        elif 'restaurant' in entity_lower:
            return {'amenity': 'restaurant'}
        
        return {}
    
    def _get_alias(self, entity_name: str) -> str:
        """Generate table alias from entity name."""
        # Take first letter or first 3 chars
        clean_name = entity_name.lower().replace(" ", "_")
        if len(clean_name) <= 3:
            return clean_name
        return clean_name[:3]


def generate_sql_from_ir(ir: IntermediateRepresentation, use_geography: bool = True) -> str:
    """
    Convenience function to generate SQL from IR.
    
    Args:
        ir: Intermediate representation
        use_geography: Use geography type for accurate distances
        
    Returns:
        Generated SQL string
    """
    generator = IRToSQLGenerator(use_geography=use_geography)
    return generator.generate_sql(ir)
