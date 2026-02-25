"""
Security layer for SQL validation and sanitization.

Provides SQL injection prevention through:
1. Statement type whitelisting (SELECT only)
2. Table/column name validation against schema
3. Dangerous keyword detection
4. Query structure validation
"""

import re
import sqlparse
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class SecurityError(Exception):
    """Raised when a security violation is detected."""
    pass


class ValidationError(Exception):
    """Raised when SQL validation fails."""
    pass


@dataclass
class ValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    sanitized_sql: Optional[str] = None


class SQLSecurityValidator:
    """
    Validates SQL queries for security before execution.
    
    Security Layers:
    1. Statement whitelist (SELECT only)
    2. Dangerous keyword blacklist
    3. Table/column validation against schema
    4. Query structure analysis
    """
    
    # Only allow SELECT statements
    ALLOWED_STATEMENTS = {'SELECT'}
    
    # Dangerous keywords that indicate potential injection
    DANGEROUS_KEYWORDS = {
        'DROP', 'DELETE', 'TRUNCATE', 'INSERT', 'UPDATE', 'MERGE',
        'CREATE', 'ALTER', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE',
        'UNION', 'INTO OUTFILE', 'INTO DUMPFILE', 'LOAD_FILE',
        'PG_SLEEP', 'BENCHMARK', 'WAITFOR DELAY', 'xp_cmdshell',
        'sp_executesql', 'information_schema', 'pg_catalog',
        'current_user', 'session_user', 'version()'
    }
    
    # Maximum query length to prevent DoS
    MAX_QUERY_LENGTH = 5000
    
    # Maximum nested subqueries
    MAX_SUBQUERY_DEPTH = 3
    
    def __init__(self, allowed_tables: Optional[Set[str]] = None, 
                 allowed_columns: Optional[Dict[str, Set[str]]] = None):
        """
        Initialize validator with schema constraints.
        
        Args:
            allowed_tables: Set of allowed table names
            allowed_columns: Dict mapping table names to sets of allowed columns
        """
        self.allowed_tables = allowed_tables or set()
        self.allowed_columns = allowed_columns or {}
        
    def validate_input(self, user_input: str) -> ValidationResult:
        """
        Validate raw user input before LLM processing.
        
        Args:
            user_input: Raw natural language query from user
            
        Returns:
            ValidationResult with is_valid and any errors
        """
        errors = []
        warnings = []
        
        # Check length
        if len(user_input) > self.MAX_QUERY_LENGTH:
            errors.append(f"Query too long: {len(user_input)} > {self.MAX_QUERY_LENGTH} characters")
        
        # Check for null bytes
        if '\x00' in user_input:
            errors.append("Null bytes not allowed")
        
        # Check for SQL comment patterns that might be injection attempts
        if re.search(r'--|\/\*|#', user_input):
            warnings.append("Comment patterns detected - these will be handled by SQL validator")
        
        # Check for multiple statement patterns
        if re.search(r';\s*\w+', user_input, re.IGNORECASE):
            errors.append("Multiple statements detected - not allowed")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            sanitized_sql=None
        )
    
    def validate_sql(self, sql: str) -> ValidationResult:
        """
        Validate generated SQL before execution.
        
        Args:
            sql: SQL query string to validate
            
        Returns:
            ValidationResult with is_valid and any errors
        """
        errors = []
        warnings = []
        
        # Basic sanitization
        sql = sql.strip()
        
        # Check length
        if len(sql) > self.MAX_QUERY_LENGTH:
            errors.append(f"SQL query too long: {len(sql)} > {self.MAX_QUERY_LENGTH} characters")
            return ValidationResult(False, errors, warnings)
        
        # Check for dangerous keywords
        upper_sql = sql.upper()
        for keyword in self.DANGEROUS_KEYWORDS:
            # Use word boundary matching
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, upper_sql):
                errors.append(f"Dangerous keyword detected: {keyword}")
        
        # Parse SQL to check structure
        try:
            parsed = sqlparse.parse(sql)
            
            if not parsed:
                errors.append("Could not parse SQL")
                return ValidationResult(False, errors, warnings)
            
            for statement in parsed:
                # Get the first token (should be SELECT)
                first_token = None
                for token in statement.tokens:
                    if token.ttype is not None and not token.is_whitespace:
                        first_token = str(token).upper()
                        break
                
                if first_token not in self.ALLOWED_STATEMENTS:
                    errors.append(f"Only SELECT statements allowed, got: {first_token}")
                
                # Check for multiple statements (semicolon not in string literals)
                sql_without_strings = re.sub(r"'[^']*'", "''", sql)
                sql_without_strings = re.sub(r'"[^"]*"', '""', sql_without_strings)
                semicolon_count = sql_without_strings.count(';')
                # Allow single trailing semicolon, but not multiple statements
                if semicolon_count > 1 or (semicolon_count == 1 and not sql_without_strings.rstrip().endswith(';')):
                    errors.append("Multiple SQL statements detected - only single SELECT allowed")
                
                # Validate subquery depth
                depth = self._count_subquery_depth(statement)
                if depth > self.MAX_SUBQUERY_DEPTH:
                    errors.append(f"Subquery depth {depth} exceeds maximum {self.MAX_SUBQUERY_DEPTH}")
                
                # Extract and validate table/column references
                table_errors = self._validate_tables_and_columns(statement)
                errors.extend(table_errors)
                
        except Exception as e:
            errors.append(f"SQL parsing error: {str(e)}")
        
        # Sanitize if valid
        sanitized = sql if len(errors) == 0 else None
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            sanitized_sql=sanitized
        )
    
    def _count_subquery_depth(self, statement) -> int:
        """Count maximum subquery nesting depth."""
        max_depth = 0
        current_depth = 0
        
        def count_tokens(tokens, depth):
            nonlocal max_depth
            for token in tokens:
                if token.is_group:
                    token_str = str(token).upper()
                    if 'SELECT' in token_str and depth > 0:
                        max_depth = max(max_depth, depth)
                        count_tokens(token.tokens, depth + 1)
                    else:
                        count_tokens(token.tokens, depth)
        
        count_tokens(statement.tokens, 1)
        return max_depth
    
    def _validate_tables_and_columns(self, statement) -> List[str]:
        """Validate table and column names against schema."""
        errors = []
        
        if not self.allowed_tables:
            return errors  # Skip if no schema provided
        
        # Extract table names from FROM and JOIN clauses
        table_pattern = r'\bFROM\s+(\w+)|\bJOIN\s+(\w+)'
        sql_str = str(statement)
        matches = re.findall(table_pattern, sql_str, re.IGNORECASE)
        
        for match in matches:
            table_name = match[0] or match[1]
            if table_name and table_name not in self.allowed_tables:
                errors.append(f"Table '{table_name}' not in allowed tables list")
        
        return errors
    
    def sanitize_for_display(self, sql: str) -> str:
        """
        Sanitize SQL for safe display (remove any potentially sensitive data).
        
        Args:
            sql: SQL query string
            
        Returns:
            Sanitized SQL safe for logging/display
        """
        # Truncate if too long
        if len(sql) > 1000:
            sql = sql[:1000] + "... [truncated]"
        
        # Remove potential PII patterns (emails, phone numbers)
        sql = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', sql)
        sql = re.sub(r'\b\d{3}-\d{3}-\d{4}\b', '[PHONE]', sql)
        
        return sql


class SecureExecutor:
    """
    Wrapper for secure SQL execution with validation.
    
    Usage:
        validator = SQLSecurityValidator(allowed_tables={'planet_osm_point'})
        executor = SecureExecutor(validator, conn)
        result = executor.execute_safe(sql, reasoning)
    """
    
    def __init__(self, validator: SQLSecurityValidator, conn):
        self.validator = validator
        self.conn = conn
        self.execution_log = []
    
    def execute_safe(self, sql: str, reasoning: str = "") -> Dict:
        """
        Execute SQL with full security validation.
        
        Args:
            sql: SQL query to execute
            reasoning: Reasoning for the query (for logging)
            
        Returns:
            Dict with success status, data, and any errors
        """
        # Validate SQL
        validation = self.validator.validate_sql(sql)
        
        if not validation.is_valid:
            return {
                "success": False,
                "error": "Security validation failed",
                "validation_errors": validation.errors,
                "sql_attempted": self.validator.sanitize_for_display(sql)
            }
        
        # Execute with safety rollback
        try:
            self.conn.rollback()  # Clear any pending transactions
            
            with self.conn.cursor() as cur:
                cur.execute(validation.sanitized_sql)
                
                if validation.sanitized_sql.strip().upper().startswith('SELECT'):
                    results = cur.fetchall()
                    columns = [desc[0] for desc in cur.description] if cur.description else []
                    
                    # Convert to list of dicts
                    data = []
                    for row in results:
                        row_dict = {}
                        for i, col in enumerate(columns):
                            row_dict[col] = row[i]
                        data.append(row_dict)
                    
                    self.execution_log.append({
                        "sql": self.validator.sanitize_for_display(sql),
                        "reasoning": reasoning,
                        "success": True,
                        "row_count": len(data)
                    })
                    
                    return {
                        "success": True,
                        "data": data,
                        "columns": columns,
                        "row_count": len(data),
                        "sql": sql,
                        "reasoning": reasoning
                    }
                else:
                    # This should never happen due to validation, but safety check
                    return {
                        "success": False,
                        "error": "Non-SELECT statement blocked",
                        "sql_attempted": sql
                    }
                    
        except Exception as e:
            self.execution_log.append({
                "sql": self.validator.sanitize_for_display(sql),
                "reasoning": reasoning,
                "success": False,
                "error": str(e)
            })
            
            return {
                "success": False,
                "error": f"Execution error: {str(e)}",
                "sql_attempted": sql,
                "reasoning": reasoning
            }
    
    def get_execution_log(self) -> List[Dict]:
        """Get log of all executions for auditing."""
        return self.execution_log


def create_validator_from_schema(conn) -> SQLSecurityValidator:
    """
    Create a security validator populated from database schema.
    
    Args:
        conn: Database connection
        
    Returns:
        SQLSecurityValidator configured with schema tables/columns
    """
    allowed_tables = set()
    allowed_columns = {}
    
    with conn.cursor() as cur:
        # Get geometry tables
        cur.execute("""
            SELECT f_table_name 
            FROM geometry_columns 
            WHERE f_table_name LIKE 'planet_osm_%';
        """)
        
        for row in cur.fetchall():
            table_name = row[0]
            allowed_tables.add(table_name)
            
            # Get columns for this table
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s;
            """, (table_name,))
            
            columns = {row[0] for row in cur.fetchall()}
            allowed_columns[table_name] = columns
    
    return SQLSecurityValidator(
        allowed_tables=allowed_tables,
        allowed_columns=allowed_columns
    )
