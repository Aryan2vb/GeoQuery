"""
Evaluation Framework for Natural Language to Spatial Query Translation

Provides metrics for:
- Translation accuracy
- Spatial accuracy  
- Security coverage
- Performance benchmarks
"""

import json
import time
import statistics
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from pathlib import Path


@dataclass
class EvaluationMetrics:
    """Results from evaluation run."""
    # Translation metrics
    total_queries: int = 0
    successful_translations: int = 0
    successful_executions: int = 0
    translation_accuracy: float = 0.0
    execution_accuracy: float = 0.0
    
    # Spatial accuracy
    spatial_accuracy: float = 0.0
    distance_errors: List[float] = field(default_factory=list)
    
    # Security metrics
    injection_attempts: int = 0
    injection_blocked: int = 0
    security_score: float = 0.0
    
    # Performance
    avg_latency: float = 0.0
    p95_latency: float = 0.0
    p99_latency: float = 0.0
    latencies: List[float] = field(default_factory=list)
    
    # Error categorization
    errors: List[Dict] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "translation": {
                "total_queries": self.total_queries,
                "successful_translations": self.successful_translations,
                "successful_executions": self.successful_executions,
                "translation_accuracy": self.translation_accuracy,
                "execution_accuracy": self.execution_accuracy
            },
            "spatial": {
                "accuracy": self.spatial_accuracy,
                "distance_error_mean": statistics.mean(self.distance_errors) if self.distance_errors else 0,
                "distance_error_max": max(self.distance_errors) if self.distance_errors else 0
            },
            "security": {
                "injection_attempts": self.injection_attempts,
                "injection_blocked": self.injection_blocked,
                "security_score": self.security_score
            },
            "performance": {
                "avg_latency_ms": self.avg_latency * 1000,
                "p95_latency_ms": self.p95_latency * 1000,
                "p99_latency_ms": self.p99_latency * 1000
            },
            "errors": self.errors[:10]  # First 10 errors
        }
    
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


class QueryEvaluator:
    """Evaluates natural language query translation quality."""
    
    def __init__(self, conn, llm):
        """
        Initialize evaluator.
        
        Args:
            conn: Database connection
            llm: LLM provider
        """
        self.conn = conn
        self.llm = llm
        self.metrics = EvaluationMetrics()
    
    def evaluate_dataset(self, test_cases: List[Dict]) -> EvaluationMetrics:
        """
        Evaluate a dataset of test cases.
        
        Args:
            test_cases: List of test cases with 'query', 'expected_sql', 'expected_result'
            
        Returns:
            EvaluationMetrics
        """
        self.metrics = EvaluationMetrics()
        self.metrics.total_queries = len(test_cases)
        
        for case in test_cases:
            self._evaluate_single(case)
        
        # Calculate final metrics
        self._calculate_final_metrics()
        
        return self.metrics
    
    def _evaluate_single(self, case: Dict):
        """Evaluate a single test case."""
        from osm_sql_generator import natural_language_to_sql
        
        query = case["query"]
        expected_result = case.get("expected_result")
        
        start_time = time.time()
        
        try:
            result = natural_language_to_sql(query, self.conn, self.llm)
            latency = time.time() - start_time
            self.metrics.latencies.append(latency)
            
            if result.get("success"):
                self.metrics.successful_translations += 1
                
                # Check if result matches expected (if provided)
                if expected_result is not None:
                    if self._results_match(result["data"], expected_result):
                        self.metrics.successful_executions += 1
                    else:
                        self.metrics.errors.append({
                            "query": query,
                            "type": "result_mismatch",
                            "expected": expected_result,
                            "actual": result["data"]
                        })
                else:
                    self.metrics.successful_executions += 1
                    
                # Check spatial accuracy if distance is involved
                if "distance" in query.lower():
                    self._evaluate_spatial_accuracy(result, case)
            else:
                self.metrics.errors.append({
                    "query": query,
                    "type": "execution_failed",
                    "error": result.get("error"),
                    "sql": result.get("sql_attempted")
                })
                
        except Exception as e:
            latency = time.time() - start_time
            self.metrics.latencies.append(latency)
            
            self.metrics.errors.append({
                "query": query,
                "type": "exception",
                "error": str(e)
            })
    
    def _results_match(self, actual: List, expected: List) -> bool:
        """Check if results match expected values."""
        if len(actual) != len(expected):
            return False
        
        # Basic comparison - can be enhanced
        for i, (a, e) in enumerate(zip(actual, expected)):
            if a != e:
                return False
        
        return True
    
    def _evaluate_spatial_accuracy(self, result: Dict, case: Dict):
        """Evaluate spatial accuracy for distance-based queries."""
        expected_distance = case.get("expected_distance_meters")
        
        if expected_distance and result.get("data"):
            # Calculate actual distance from results if possible
            # This is a simplified check
            self.metrics.distance_errors.append(0.0)  # Placeholder
    
    def _calculate_final_metrics(self):
        """Calculate final aggregated metrics."""
        if self.metrics.total_queries > 0:
            self.metrics.translation_accuracy = (
                self.metrics.successful_translations / self.metrics.total_queries
            )
            self.metrics.execution_accuracy = (
                self.metrics.successful_executions / self.metrics.total_queries
            )
        
        if self.metrics.latencies:
            self.metrics.avg_latency = statistics.mean(self.metrics.latencies)
            sorted_latencies = sorted(self.metrics.latencies)
            p95_idx = int(len(sorted_latencies) * 0.95)
            p99_idx = int(len(sorted_latencies) * 0.99)
            self.metrics.p95_latency = sorted_latencies[min(p95_idx, len(sorted_latencies)-1)]
            self.metrics.p99_latency = sorted_latencies[min(p99_idx, len(sorted_latencies)-1)]


class SecurityEvaluator:
    """Evaluates security coverage."""
    
    def __init__(self, validator):
        """
        Initialize security evaluator.
        
        Args:
            validator: SQLSecurityValidator instance
        """
        self.validator = validator
    
    def evaluate_security(self) -> Dict:
        """
        Run comprehensive security evaluation.
        
        Returns:
            Dict with security metrics
        """
        test_cases = self._get_injection_test_cases()
        
        blocked = 0
        results = []
        
        for case in test_cases:
            result = self.validator.validate_sql(case["sql"])
            is_blocked = not result.is_valid
            
            if is_blocked:
                blocked += 1
            
            results.append({
                "name": case["name"],
                "sql_preview": case["sql"][:50],
                "blocked": is_blocked,
                "expected": case["should_block"],
                "passed": is_blocked == case["should_block"]
            })
        
        total = len(test_cases)
        security_score = blocked / total if total > 0 else 0
        
        return {
            "total_tests": total,
            "blocked": blocked,
            "security_score": security_score,
            "results": results,
            "passed_all": all(r["passed"] for r in results)
        }
    
    def _get_injection_test_cases(self) -> List[Dict]:
        """Get SQL injection test cases."""
        return [
            {
                "name": "Basic DROP TABLE",
                "sql": "DROP TABLE planet_osm_point;",
                "should_block": True
            },
            {
                "name": "DELETE statement",
                "sql": "DELETE FROM planet_osm_point WHERE id=1;",
                "should_block": True
            },
            {
                "name": "UNION injection",
                "sql": "SELECT * FROM planet_osm_point UNION SELECT * FROM passwords;",
                "should_block": True
            },
            {
                "name": "Comment injection",
                "sql": "SELECT * FROM planet_osm_point; -- comment",
                "should_block": False  # Comments are OK in SELECT
            },
            {
                "name": "pg_sleep DoS",
                "sql": "SELECT pg_sleep(100) FROM planet_osm_point;",
                "should_block": True
            },
            {
                "name": "Valid complex SELECT",
                "sql": "SELECT name, way FROM planet_osm_point WHERE amenity='hospital';",
                "should_block": False
            },
            {
                "name": "Valid with EXISTS",
                "sql": "SELECT s.name FROM planet_osm_point s WHERE EXISTS (SELECT 1 FROM planet_osm_point h WHERE ST_DWithin(s.way, h.way, 5000));",
                "should_block": False
            }
        ]


def get_default_test_cases() -> List[Dict]:
    """
    Get default test cases for evaluation.
    
    Returns:
        List of test case dictionaries
    """
    return [
        {
            "query": "Hospitals in Delhi",
            "category": "basic_selection",
            "description": "Simple entity selection"
        },
        {
            "query": "Parks within 2km of Connaught Place",
            "category": "distance_query",
            "description": "Distance-based proximity",
            "expected_distance_meters": 2000
        },
        {
            "query": "Schools within 5km of any hospital",
            "category": "distance_query",
            "description": "Multi-entity distance query",
            "expected_distance_meters": 5000
        },
        {
            "query": "Schools near main roads",
            "category": "proximity_query",
            "description": "Unbounded proximity query"
        },
        {
            "query": "Total length of primary roads",
            "category": "aggregation",
            "description": "Spatial aggregation with SUM/ST_Length"
        },
        {
            "query": "Schools within 5km of hospitals AND near main roads",
            "category": "multiple_constraints",
            "description": "Multiple spatial constraints"
        },
        {
            "query": "Hospitals near schools OR highways",
            "category": "logical_operators",
            "description": "OR logical operator"
        },
        {
            "query": "Schools NOT near highways",
            "category": "negation",
            "description": "NOT/negation operator"
        },
        {
            "query": "Roads crossing rivers",
            "category": "intersection",
            "description": "ST_Intersects operation"
        },
        {
            "query": "Hospitals inside district boundaries",
            "category": "containment",
            "description": "ST_Within operation"
        }
    ]


def run_full_evaluation(conn, llm, output_path: Optional[str] = None) -> Dict:
    """
    Run full evaluation suite.
    
    Args:
        conn: Database connection
        llm: LLM provider
        output_path: Optional path to save results JSON
        
    Returns:
        Dict with all evaluation results
    """
    print("=" * 70)
    print("RUNNING FULL EVALUATION SUITE")
    print("=" * 70)
    
    # Query evaluation
    print("\n1. Evaluating Query Translation...")
    test_cases = get_default_test_cases()
    
    evaluator = QueryEvaluator(conn, llm)
    metrics = evaluator.evaluate_dataset(test_cases)
    
    print(f"   Total queries: {metrics.total_queries}")
    print(f"   Translation accuracy: {metrics.translation_accuracy:.1%}")
    print(f"   Execution accuracy: {metrics.execution_accuracy:.1%}")
    
    # Security evaluation
    print("\n2. Evaluating Security...")
    from osm_sql_generator.security import create_validator_from_schema
    
    validator = create_validator_from_schema(conn)
    security_evaluator = SecurityEvaluator(validator)
    security_results = security_evaluator.evaluate_security()
    
    metrics.injection_attempts = security_results["total_tests"]
    metrics.injection_blocked = security_results["blocked"]
    metrics.security_score = security_results["security_score"]
    
    print(f"   Injection tests: {security_results['total_tests']}")
    print(f"   Blocked: {security_results['blocked']}")
    print(f"   Security score: {security_results['security_score']:.1%}")
    
    # Combine results
    results = {
        "translation": metrics.to_dict()["translation"],
        "spatial": metrics.to_dict()["spatial"],
        "security": metrics.to_dict()["security"],
        "performance": metrics.to_dict()["performance"],
        "security_details": security_results,
        "errors": metrics.to_dict()["errors"]
    }
    
    # Save to file if requested
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to: {output_path}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("EVALUATION SUMMARY")
    print("=" * 70)
    print(f"Translation Accuracy:  {metrics.translation_accuracy:.1%}")
    print(f"Execution Accuracy:    {metrics.execution_accuracy:.1%}")
    print(f"Security Score:        {metrics.security_score:.1%}")
    print(f"Avg Latency:           {metrics.avg_latency*1000:.0f}ms")
    print(f"P95 Latency:           {metrics.p95_latency*1000:.0f}ms")
    print(f"Errors:                {len(metrics.errors)}")
    print("=" * 70)
    
    return results


if __name__ == "__main__":
    # Example usage
    import os
    from dotenv import load_dotenv
    import psycopg2
    from osm_sql_generator import GroqProvider
    
    load_dotenv()
    
    # Connect to database
    conn = psycopg2.connect(
        database=os.getenv("PGDATABASE", "delhi_db"),
        user=os.getenv("PGUSER", "aryansoni"),
        password=os.getenv("PGPASSWORD", "")
    )
    
    # Initialize LLM
    llm = GroqProvider(api_key=os.getenv("OPENAI_API_KEY"))
    
    # Run evaluation
    results = run_full_evaluation(
        conn, 
        llm, 
        output_path="evaluation_results.json"
    )
    
    conn.close()
