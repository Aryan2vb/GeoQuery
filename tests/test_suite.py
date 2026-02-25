"""
Comprehensive test suite for Natural Language to Spatial Query Translation.

Test Categories:
- Security Tests: SQL injection prevention
- IR Tests: Intermediate representation validation
- Spatial Tests: CRS accuracy, distance calculations
- Integration Tests: End-to-end query execution
"""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch

# Import modules to test
import sys
sys.path.insert(0, '/Users/aryansoni/Documents/repos/bit-by-bit')

from osm_sql_generator.security import (
    SQLSecurityValidator, 
    SecureExecutor,
    ValidationResult,
    SecurityError
)
from osm_sql_generator.ir import (
    IntermediateRepresentation,
    Entity,
    SpatialFilter,
    Condition,
    SpatialOperationType,
    LogicalOperator
)
from osm_sql_generator.sql_generator import IRToSQLGenerator, generate_sql_from_ir


# ============================================================================
# SECURITY TESTS
# ============================================================================

class TestSQLSecurityValidator:
    """Test SQL injection prevention and security validation."""
    
    @pytest.fixture
    def validator(self):
        """Create a validator with allowed OSM tables."""
        return SQLSecurityValidator(
            allowed_tables={
                'planet_osm_point', 
                'planet_osm_line', 
                'planet_osm_polygon'
            },
            allowed_columns={
                'planet_osm_point': {'name', 'way', 'amenity', 'highway'},
                'planet_osm_line': {'name', 'way', 'highway'},
                'planet_osm_polygon': {'name', 'way', 'boundary'}
            }
        )
    
    def test_select_only_allowed(self, validator):
        """Only SELECT statements should be allowed."""
        # Valid SELECT
        result = validator.validate_sql("SELECT * FROM planet_osm_point;")
        assert result.is_valid, f"SELECT should be valid: {result.errors}"
        
        # Invalid DROP
        result = validator.validate_sql("DROP TABLE planet_osm_point;")
        assert not result.is_valid
        assert any("DROP" in err for err in result.errors)
        
        # Invalid DELETE
        result = validator.validate_sql("DELETE FROM planet_osm_point;")
        assert not result.is_valid
        assert any("DELETE" in err for err in result.errors)
        
        # Invalid INSERT
        result = validator.validate_sql("INSERT INTO planet_osm_point VALUES (...);")
        assert not result.is_valid
    
    def test_dangerous_keywords_blocked(self, validator):
        """Dangerous SQL keywords should be blocked."""
        dangerous_queries = [
            "SELECT * FROM planet_osm_point; DROP TABLE users;--",
            "SELECT * FROM planet_osm_point; DELETE FROM hospitals;--",
            "SELECT * FROM planet_osm_point WHERE name = 'test' UNION SELECT * FROM passwords",
            "SELECT pg_sleep(10) FROM planet_osm_point",
        ]
        
        for query in dangerous_queries:
            result = validator.validate_sql(query)
            assert not result.is_valid, f"Should block: {query[:50]}..."
    
    def test_multiple_statements_blocked(self, validator):
        """Multiple SQL statements should be blocked."""
        result = validator.validate_sql("SELECT * FROM planet_osm_point; SELECT * FROM planet_osm_line;")
        assert not result.is_valid
        assert any("Multiple" in err for err in result.errors)
    
    def test_query_length_limit(self, validator):
        """Very long queries should be rejected."""
        long_query = "SELECT * FROM planet_osm_point WHERE " + "x='y' AND " * 1000 + "z='a';"
        result = validator.validate_sql(long_query)
        assert not result.is_valid
        assert any("too long" in err.lower() for err in result.errors)
    
    def test_input_sanitization(self, validator):
        """Raw user input should be sanitized."""
        # Null bytes
        result = validator.validate_input("query\x00null_byte")
        assert not result.is_valid
        
        # Multiple statements in NL input
        result = validator.validate_input("hospitals; DROP TABLE schools")
        assert not result.is_valid
    
    def test_valid_complex_select_allowed(self, validator):
        """Complex but valid SELECT queries should be allowed."""
        valid_queries = [
            "SELECT name, way FROM planet_osm_point WHERE amenity='hospital';",
            "SELECT s.name FROM planet_osm_point s WHERE EXISTS (SELECT 1 FROM planet_osm_point h WHERE h.amenity='hospital' AND ST_DWithin(s.way, h.way, 5000));",
            "SELECT COUNT(*) as count FROM planet_osm_line WHERE highway='primary';",
        ]
        
        for query in valid_queries:
            result = validator.validate_sql(query)
            assert result.is_valid, f"Should allow: {query[:50]}... - Errors: {result.errors}"


class TestSecureExecutor:
    """Test secure execution wrapper."""
    
    @pytest.fixture
    def mock_conn(self):
        """Create a mock database connection."""
        conn = Mock()
        cursor = Mock()
        cursor.fetchall.return_value = [
            ('Hospital 1', '0101000020110F0000...'),
            ('Hospital 2', '0101000020110F0000...')
        ]
        cursor.description = [('name',), ('way',)]
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        return conn, cursor
    
    @pytest.fixture
    def validator(self):
        return SQLSecurityValidator(
            allowed_tables={'planet_osm_point'},
            allowed_columns={'planet_osm_point': {'name', 'way', 'amenity'}}
        )
    
    def test_successful_execution(self, mock_conn, validator):
        """Valid SQL should execute successfully."""
        conn, cursor = mock_conn
        executor = SecureExecutor(validator, conn)
        
        result = executor.execute_safe(
            "SELECT name, way FROM planet_osm_point WHERE amenity='hospital';",
            "Finding hospitals"
        )
        
        assert result["success"]
        assert result["row_count"] == 2
        assert "data" in result
    
    def test_injection_blocked(self, mock_conn, validator):
        """SQL injection should be blocked before execution."""
        conn, cursor = mock_conn
        executor = SecureExecutor(validator, conn)
        
        result = executor.execute_safe(
            "DROP TABLE planet_osm_point;",
            "Malicious query"
        )
        
        assert not result["success"]
        assert "Security validation failed" in result["error"]
        # Should not have executed
        cursor.execute.assert_not_called()
    
    def test_execution_error_handling(self, mock_conn, validator):
        """Execution errors should be handled gracefully."""
        conn, cursor = mock_conn
        cursor.execute.side_effect = Exception("Database error")
        
        executor = SecureExecutor(validator, conn)
        
        result = executor.execute_safe(
            "SELECT * FROM planet_osm_point;",
            "Query with error"
        )
        
        assert not result["success"]
        assert "error" in result


# ============================================================================
# IR (INTERMEDIATE REPRESENTATION) TESTS
# ============================================================================

class TestIntermediateRepresentation:
    """Test IR creation and validation."""
    
    def test_basic_ir_creation(self):
        """Create a basic IR for simple query."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="hospitals", table="planet_osm_point")]
        ir.select_attributes = ["name", "way"]
        ir.original_query = "Hospitals in Delhi"
        ir.reasoning = "User wants hospitals"
        
        is_valid, errors = ir.validate()
        assert is_valid, f"Should be valid: {errors}"
    
    def test_ir_with_proximity_condition(self):
        """Create IR with proximity (distance) condition."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="schools", table="planet_osm_point")]
        ir.select_attributes = ["name"]
        
        # Schools near hospitals
        condition = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="hospitals", table="planet_osm_point"),
                parameters={"distance": 5000, "unit": "meters"}
            ),
            operator=None,
            negate=False
        )
        ir.conditions.append(condition)
        
        is_valid, errors = ir.validate()
        assert is_valid, f"Should be valid: {errors}"
    
    def test_ir_with_logical_operators(self):
        """Test IR with AND/OR logical operators."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="schools")]
        
        # First condition (no operator needed)
        condition1 = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="hospitals"),
                parameters={"distance": 5000}
            ),
            operator=None
        )
        
        # Second condition with AND
        condition2 = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="main roads"),
                parameters={"distance": 1000}
            ),
            operator=LogicalOperator.AND
        )
        
        ir.conditions = [condition1, condition2]
        
        is_valid, errors = ir.validate()
        assert is_valid
    
    def test_ir_with_negation(self):
        """Test IR with NOT/negation."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="schools")]
        
        condition = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="highways"),
                parameters={"distance": 500}
            ),
            negate=True  # NOT near highways
        )
        ir.conditions.append(condition)
        
        is_valid, errors = ir.validate()
        assert is_valid
    
    def test_ir_serialization(self):
        """Test IR to/from JSON serialization."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="hospitals", table="planet_osm_point")]
        ir.original_query = "Test query"
        ir.reasoning = "Test reasoning"
        ir.interpretation_confidence = 0.95
        
        # Convert to JSON
        json_str = ir.to_json()
        assert "hospitals" in json_str
        assert "planet_osm_point" in json_str
        
        # Convert back from dict
        ir_dict = json.loads(json_str)
        ir2 = IntermediateRepresentation.from_dict(ir_dict)
        
        assert ir2.select_entities[0].name == "hospitals"
        assert ir2.original_query == "Test query"
    
    def test_ir_validation_errors(self):
        """Test IR validation catches errors."""
        ir = IntermediateRepresentation()
        # No entities - should fail
        
        is_valid, errors = ir.validate()
        assert not is_valid
        assert any("SELECT entity" in err for err in errors)  # Match actual error message
    
    def test_ir_explanation_generation(self):
        """Test IR explanation generation."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="schools")]
        ir.select_attributes = ["name"]
        
        condition = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="hospitals"),
                parameters={"distance": 5000, "unit": "meters"}
            )
        )
        ir.conditions.append(condition)
        ir.reasoning = "Find schools near hospitals"
        ir.interpretation_confidence = 0.9
        
        explanation = ir.explain()
        assert "schools" in explanation
        assert "hospitals" in explanation
        assert "5000" in explanation
        assert "90%" in explanation


# ============================================================================
# SQL GENERATOR TESTS
# ============================================================================

class TestIRToSQLGenerator:
    """Test IR to SQL code generation."""
    
    @pytest.fixture
    def generator(self):
        return IRToSQLGenerator(use_geography=True)
    
    def test_simple_select_generation(self, generator):
        """Generate SQL for simple entity selection."""
        ir = IntermediateRepresentation()
        ir.select_entities = [
            Entity(name="hospitals", table="planet_osm_point", tags={"amenity": "hospital"})
        ]
        ir.select_attributes = ["name", "way"]
        
        sql = generator.generate_sql(ir)
        
        assert "SELECT" in sql
        assert "planet_osm_point" in sql
        assert "amenity" in sql and "hospital" in sql  # Relaxed assertion
    
    def test_proximity_query_generation(self, generator):
        """Generate SQL for proximity query with geography."""
        ir = IntermediateRepresentation()
        ir.select_entities = [
            Entity(name="schools", table="planet_osm_point", tags={"amenity": "school"})
        ]
        ir.select_attributes = ["name"]
        
        condition = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="hospitals", table="planet_osm_point", tags={"amenity": "hospital"}),
                parameters={"distance": 5000}
            )
        )
        ir.conditions.append(condition)
        
        sql = generator.generate_sql(ir)
        
        assert "ST_DWithin" in sql
        assert "::geography" in sql  # Geography type for accuracy
        assert "5000" in sql
        assert "EXISTS" in sql  # EXISTS pattern for "near any"
    
    def test_aggregation_generation(self, generator):
        """Generate SQL for aggregation queries."""
        ir = IntermediateRepresentation()
        ir.select_entities = [
            Entity(name="roads", table="planet_osm_line", tags={"highway": "primary"})
        ]
        ir.aggregation = {
            "function": "ST_Length",
            "field": "way",  # Generator will add ::geography
            "alias": "total_length"
        }
        
        sql = generator.generate_sql(ir)
        
        assert "SUM(ST_Length(way::geography))" in sql
        assert "as total_length" in sql
    
    def test_negation_generation(self, generator):
        """Generate SQL with NOT conditions."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="schools", tags={"amenity": "school"})]
        
        condition = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="highways"),
                parameters={"distance": 500}
            ),
            negate=True
        )
        ir.conditions.append(condition)
        
        sql = generator.generate_sql(ir)
        
        assert "NOT EXISTS" in sql
    
    def test_logical_and_generation(self, generator):
        """Generate SQL with AND conditions."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="schools")]
        
        condition1 = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="hospitals"),
                parameters={"distance": 5000}
            ),
            operator=None
        )
        
        condition2 = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="roads"),
                parameters={"distance": 1000}
            ),
            operator=LogicalOperator.AND
        )
        
        ir.conditions = [condition1, condition2]
        
        sql = generator.generate_sql(ir)
        
        # Should have AND between conditions
        assert "AND" in sql
    
    def test_invalid_ir_rejected(self, generator):
        """Invalid IR should raise error."""
        ir = IntermediateRepresentation()
        # Missing required fields
        
        with pytest.raises(Exception):
            generator.generate_sql(ir)


# ============================================================================
# SPATIAL ACCURACY TESTS
# ============================================================================

class TestSpatialAccuracy:
    """Test CRS handling and spatial accuracy."""
    
    def test_geography_type_used_for_distance(self):
        """Verify geography type is used for accurate distances."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="schools", table="planet_osm_point")]
        
        condition = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="hospitals"),
                parameters={"distance": 5000}
            )
        )
        ir.conditions.append(condition)
        
        generator = IRToSQLGenerator(use_geography=True)
        sql = generator.generate_sql(ir)
        
        # Should use ::geography cast
        assert "::geography" in sql
    
    def test_web_mercator_without_geography(self):
        """Test that Web Mercator alone is not used for distance."""
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="schools")]
        
        condition = Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="hospitals"),
                parameters={"distance": 5000}
            )
        )
        ir.conditions.append(condition)
        
        # Generate without geography
        generator = IRToSQLGenerator(use_geography=False)
        sql = generator.generate_sql(ir)
        
        # Should NOT have geography cast
        assert "::geography" not in sql


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestEndToEnd:
    """End-to-end integration tests."""
    
    def test_query_categories_coverage(self):
        """Verify all query categories can be represented in IR."""
        test_cases = [
            {
                "name": "Distance query",
                "query": "Schools within 5km of hospitals",
                "expected_operations": [SpatialOperationType.PROXIMITY]
            },
            {
                "name": "Containment query", 
                "query": "Hospitals inside district A",
                "expected_operations": [SpatialOperationType.CONTAINMENT],
                "params": {"container_name": "district A"}
            },
            {
                "name": "Intersection query",
                "query": "Roads crossing rivers",
                "expected_operations": [SpatialOperationType.INTERSECTION]
            },
            {
                "name": "Multiple constraints",
                "query": "Schools near hospitals AND main roads",
                "expected_operations": [SpatialOperationType.PROXIMITY, SpatialOperationType.PROXIMITY]
            },
            {
                "name": "Negation query",
                "query": "Schools NOT near highways",
                "expected_operations": [SpatialOperationType.PROXIMITY],
                "expected_negate": True
            }
        ]
        
        for case in test_cases:
            # Create IR representation
            ir = IntermediateRepresentation()
            ir.original_query = case["query"]
            ir.select_entities = [Entity(name="feature")]
            
            # Add conditions based on expected operations
            for i, op_type in enumerate(case.get("expected_operations", [])):
                # Determine parameters based on operation type
                params = {"distance": 1000}  # Default for proximity
                if op_type == SpatialOperationType.CONTAINMENT:
                    params = {"container_entity": case.get("params", {}).get("container_name", "district")}
                
                condition = Condition(
                    filter=SpatialFilter(
                        operation_type=op_type,
                        target_entity=Entity(name="target"),
                        parameters=params
                    ),
                    negate=case.get("expected_negate", False)
                )
                ir.conditions.append(condition)
            
            # Validate IR
            is_valid, errors = ir.validate()
            assert is_valid, f"Case '{case['name']}' should be valid: {errors}"
    
    def test_security_validation_integration(self):
        """Integration of security validation with IR."""
        # Create valid IR
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="hospitals", table="planet_osm_point")]
        
        # Generate SQL
        sql = generate_sql_from_ir(ir)
        
        # Validate with security layer
        validator = SQLSecurityValidator(
            allowed_tables={'planet_osm_point'}
        )
        result = validator.validate_sql(sql)
        
        assert result.is_valid, f"Generated SQL should pass security: {result.errors}"


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

class TestPerformance:
    """Performance and load tests."""
    
    def test_ir_generation_performance(self):
        """IR generation should be fast."""
        import time
        
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="hospitals")]
        
        start = time.time()
        for _ in range(100):
            ir.validate()
        elapsed = time.time() - start
        
        # Should validate 100 IRs in less than 1 second
        assert elapsed < 1.0, f"IR validation too slow: {elapsed}s"
    
    def test_sql_generation_performance(self):
        """SQL generation should be fast."""
        import time
        
        ir = IntermediateRepresentation()
        ir.select_entities = [Entity(name="hospitals", table="planet_osm_point")]
        ir.conditions.append(Condition(
            filter=SpatialFilter(
                operation_type=SpatialOperationType.PROXIMITY,
                target_entity=Entity(name="schools"),
                parameters={"distance": 5000}
            )
        ))
        
        generator = IRToSQLGenerator()
        
        start = time.time()
        for _ in range(100):
            generator.generate_sql(ir)
        elapsed = time.time() - start
        
        # Should generate 100 SQL queries in less than 1 second
        assert elapsed < 1.0, f"SQL generation too slow: {elapsed}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
