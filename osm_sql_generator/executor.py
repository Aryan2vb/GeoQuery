"""
Updated Executor with IR layer, security validation, and accurate CRS handling.
"""

import re
from typing import Dict, List, Optional, Any

from .prompts import generate_spatial_prompt, generate_fix_prompt
from .metadata import get_spatial_metadata
from .security import SQLSecurityValidator, SecureExecutor, create_validator_from_schema
from .ir import IntermediateRepresentation, create_ir_from_nlp
from .sql_generator import IRToSQLGenerator, generate_sql_from_ir


def execute_with_retry(
    query: str, 
    conn, 
    llm, 
    max_retries: int = 3,
    use_geography: bool = True
):
    """
    Executes SQL query with self-correction capability using IR layer.

    Args:
        query: Natural language query from user
        conn: psycopg2 database connection
        llm: LLM provider instance (must have generate() and extract_sql() methods)
        max_retries: Maximum number of retry attempts (default: 3)
        use_geography: Use geography type for accurate distance calculations

    Returns:
        tuple: (results, sql_used, reasoning, ir_dict)
    """
    # Create security validator from schema
    validator = create_validator_from_schema(conn)
    
    # Validate input first
    input_validation = validator.validate_input(query)
    if not input_validation.is_valid:
        return (
            {
                "error": "Input validation failed",
                "validation_errors": input_validation.errors
            },
            None,
            "Input rejected for security reasons",
            None
        )

    conn.rollback()

    schema_json = get_spatial_metadata(conn)
    prompt = generate_spatial_prompt(query, schema_json)

    llm_response = llm.generate(prompt)
    
    # Try to extract IR and SQL from LLM response
    ir, sql, reasoning = _extract_ir_and_sql(llm_response, query)
    
    # If no IR extracted, fall back to SQL extraction
    if ir is None:
        sql = llm.extract_sql(llm_response)
        reasoning = extract_reasoning(llm_response)
        
        # Try to parse SQL into IR (fallback method)
        ir = _parse_sql_to_ir(sql, query, reasoning)
    
    attempt = 0
    while attempt < max_retries:
        # Validate SQL security
        validation = validator.validate_sql(sql)
        
        if not validation.is_valid:
            attempt += 1
            if attempt >= max_retries:
                return (
                    {
                        "error": "Security validation failed after max retries",
                        "validation_errors": validation.errors,
                        "last_sql": sql
                    },
                    sql,
                    reasoning,
                    ir.to_dict() if ir else None
                )
            
            # Generate fix prompt with security guidance
            fix_prompt = generate_fix_prompt(
                query, 
                f"Security validation failed: {validation.errors}", 
                sql
            )
            llm_response = llm.generate(fix_prompt)
            ir, sql, reasoning_update = _extract_ir_and_sql(llm_response, query)
            if ir is None:
                sql = llm.extract_sql(llm_response)
                reasoning_update = extract_reasoning(llm_response)
            reasoning += f"\n[Security Fix {attempt}] " + reasoning_update
            continue
        
        # Execute with safety
        try:
            executor = SecureExecutor(validator, conn)
            result = executor.execute_safe(sql, reasoning)
            
            if result["success"]:
                return (
                    result["data"],
                    sql,
                    reasoning,
                    ir.to_dict() if ir else None
                )
            else:
                # Execution failed, try to fix
                attempt += 1
                error_msg = result.get("error", "Unknown error")
                
                if attempt >= max_retries:
                    return (
                        {
                            "error": "Max retries exceeded",
                            "last_error": error_msg,
                            "last_sql": sql
                        },
                        sql,
                        reasoning,
                        ir.to_dict() if ir else None
                    )

                fix_prompt = generate_fix_prompt(query, error_msg, sql)
                llm_response = llm.generate(fix_prompt)
                ir, sql, reasoning_update = _extract_ir_and_sql(llm_response, query)
                if ir is None:
                    sql = llm.extract_sql(llm_response)
                    reasoning_update = extract_reasoning(llm_response)
                reasoning += f"\n[Retry {attempt}] " + reasoning_update

        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                return (
                    {
                        "error": f"Execution failed: {str(e)}",
                        "last_sql": sql
                    },
                    sql,
                    reasoning,
                    ir.to_dict() if ir else None
                )
            
            fix_prompt = generate_fix_prompt(query, str(e), sql)
            llm_response = llm.generate(fix_prompt)
            ir, sql, reasoning_update = _extract_ir_and_sql(llm_response, query)
            if ir is None:
                sql = llm.extract_sql(llm_response)
                reasoning_update = extract_reasoning(llm_response)
            reasoning += f"\n[Exception Retry {attempt}] " + reasoning_update

    return None, sql, reasoning, ir.to_dict() if ir else None


def natural_language_to_sql(
    user_query: str, 
    conn, 
    llm, 
    max_retries: int = 3,
    use_geography: bool = True
):
    """
    High-level API: Convert natural language to SQL and execute.

    Args:
        user_query: Natural language query (e.g., "Hospitals in Delhi")
        conn: psycopg2 database connection
        llm: LLM provider instance
        max_retries: Maximum retry attempts
        use_geography: Use geography type for accurate distance calculations

    Returns:
        dict: Results containing 'data', 'sql', 'reasoning', 'ir', and optionally 'error'
    """
    result, sql, reasoning, ir = execute_with_retry(
        user_query, conn, llm, max_retries, use_geography
    )

    if isinstance(result, dict) and "error" in result:
        return {
            "success": False,
            "error": result.get("error"),
            "error_details": result.get("last_error") or result.get("validation_errors"),
            "sql_attempted": result.get("last_sql"),
            "reasoning": reasoning,
            "ir": ir
        }

    return {
        "success": True, 
        "data": result, 
        "sql": sql, 
        "reasoning": reasoning,
        "ir": ir,
        "explanation": _generate_explanation(ir, reasoning) if ir else reasoning
    }


def _extract_ir_and_sql(llm_response: str, original_query: str) -> tuple:
    """
    Try to extract IR and SQL from LLM response.
    
    Returns:
        (ir, sql, reasoning) tuple. ir may be None if extraction fails.
    """
    # Try to find IR JSON in response
    ir_pattern = r'```ir\n(.+?)```'
    ir_match = re.search(ir_pattern, llm_response, re.DOTALL)
    
    if ir_match:
        try:
            import json
            ir_json = ir_match.group(1).strip()
            ir_dict = json.loads(ir_json)
            ir = IntermediateRepresentation.from_dict(ir_dict)
            
            # Generate SQL from IR
            sql = generate_sql_from_ir(ir)
            reasoning = ir.reasoning or extract_reasoning(llm_response)
            return ir, sql, reasoning
        except Exception:
            pass  # Fall through to SQL extraction
    
    # Try to extract structured IR from comments
    reasoning = extract_reasoning(llm_response)
    
    # Try to build IR from reasoning + SQL
    sql = None
    sql_pattern = r'```sql\n(.+?)```'
    sql_match = re.search(sql_pattern, llm_response, re.DOTALL)
    if sql_match:
        sql = sql_match.group(1).strip()
    else:
        # Try alternative patterns
        sql_match = re.search(r"(SELECT\s+.+?;)", llm_response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            sql = sql_match.group(1).strip()
    
    return None, sql, reasoning


def _parse_sql_to_ir(sql: str, original_query: str, reasoning: str) -> IntermediateRepresentation:
    """
    Parse SQL query to create IR (fallback method).
    """
    ir = IntermediateRepresentation()
    ir.original_query = original_query
    ir.reasoning = reasoning
    ir.interpretation_confidence = 0.5  # Lower confidence for parsed IR
    
    # Try to extract entity from FROM clause
    from_match = re.search(r'FROM\s+(\w+)\s+(\w+)?', sql, re.IGNORECASE)
    if from_match:
        table = from_match.group(1)
        alias = from_match.group(2) or table[:3]
        
        # Map table to entity name
        entity_name = _table_to_entity(table)
        
        from .ir import Entity, SpatialFilter, Condition, SpatialOperationType
        
        entity = Entity(
            name=entity_name,
            table=table,
            attributes=["name", "way"]
        )
        ir.select_entities.append(entity)
        
        # Extract conditions from WHERE clause
        where_match = re.search(r'WHERE\s+(.+?)(?:ORDER|GROUP|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1).strip()
            
            # Check for proximity patterns (ST_DWithin, EXISTS)
            if 'ST_DWithin' in where_clause:
                # Extract proximity info
                distance_match = re.search(r'ST_DWithin\([^,]+,[^,]+,\s*(\d+)', where_clause)
                if distance_match:
                    distance = int(distance_match.group(1))
                    
                    # Try to find target entity
                    target_match = re.search(r'FROM\s+(\w+)', where_clause, re.IGNORECASE)
                    if target_match:
                        target_table = target_match.group(1)
                        target_name = _table_to_entity(target_table)
                        
                        target_entity = Entity(name=target_name, table=target_table)
                        spatial_filter = SpatialFilter(
                            operation_type=SpatialOperationType.PROXIMITY,
                            target_entity=target_entity,
                            parameters={"distance": distance, "unit": "meters"}
                        )
                        condition = Condition(filter=spatial_filter)
                        ir.conditions.append(condition)
    
    return ir


def _table_to_entity(table: str) -> str:
    """Convert table name to entity name."""
    mapping = {
        'planet_osm_point': 'feature',
        'planet_osm_line': 'road',
        'planet_osm_polygon': 'area'
    }
    return mapping.get(table, 'feature')


def _generate_explanation(ir, reasoning: str) -> str:
    """Generate comprehensive explanation of query execution."""
    explanation_parts = []
    
    # Handle both dict and object IR
    ir_obj = ir
    if isinstance(ir, dict):
        try:
            ir_obj = IntermediateRepresentation.from_dict(ir)
        except Exception:
            ir_obj = None
    
    if ir_obj and hasattr(ir_obj, 'explain'):
        explanation_parts.append("Query Interpretation:")
        explanation_parts.append(ir_obj.explain())
        explanation_parts.append("")
    elif isinstance(ir, dict):
        # Fallback for dict IR
        explanation_parts.append("Query Interpretation (JSON):")
        import json
        explanation_parts.append(json.dumps(ir, indent=2))
        explanation_parts.append("")
    
    explanation_parts.append("Reasoning:")
    explanation_parts.append(reasoning)
    
    return "\n".join(explanation_parts)


def extract_reasoning(llm_response: str) -> str:
    """Extract reasoning comment from LLM response."""
    match = re.search(r"# Reasoning:\s*(.+?)(?:\nSQL:|$)", llm_response, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    # Try alternative patterns
    match = re.search(r"Interpretation:\s*(.+?)(?:\n|$)", llm_response, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    return "No reasoning provided"
