"""
Intermediate Representation (IR) Layer for Spatial Query Translation.

The IR provides a structured, human-readable intermediate format between
natural language and SQL. This enables:
- Query validation before SQL generation
- Semantic analysis and optimization
- Reproducible translations
- Security enforcement
"""

from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum, auto
import json


class SpatialOperationType(Enum):
    """Types of spatial operations supported."""
    SELECT = auto()
    FILTER = auto()
    PROXIMITY = auto()  # Near, within distance
    CONTAINMENT = auto()  # Within, inside
    INTERSECTION = auto()  # Crosses, intersects
    AGGREGATE = auto()  # Count, sum, length
    JOIN = auto()
    BUFFER = auto()
    NEGATION = auto()  # NOT operations


class LogicalOperator(Enum):
    """Logical operators for combining conditions."""
    AND = "AND"
    OR = "OR"


@dataclass
class Entity:
    """Represents a spatial entity (e.g., 'schools', 'hospitals')."""
    name: str
    table: Optional[str] = None
    geometry_type: Optional[str] = None  # point, line, polygon
    attributes: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "table": self.table,
            "geometry_type": self.geometry_type,
            "attributes": self.attributes,
            "tags": self.tags
        }


@dataclass
class SpatialFilter:
    """Represents a spatial filter condition."""
    operation_type: SpatialOperationType
    target_entity: Entity
    parameters: Dict[str, Any] = field(default_factory=dict)
    # For proximity: {"distance": 5000, "unit": "meters"}
    # For containment: {"container_entity": Entity}
    # For intersection: {"intersecting_entity": Entity}
    
    def to_dict(self) -> Dict:
        return {
            "operation_type": self.operation_type.name,
            "target_entity": self.target_entity.to_dict(),
            "parameters": self.parameters
        }


@dataclass
class Condition:
    """Represents a logical condition that can be combined."""
    filter: SpatialFilter
    operator: Optional[LogicalOperator] = None  # AND/OR with previous condition
    negate: bool = False
    
    def to_dict(self) -> Dict:
        return {
            "filter": self.filter.to_dict(),
            "operator": self.operator.value if self.operator else None,
            "negate": self.negate
        }


@dataclass
class IntermediateRepresentation:
    """
    Structured representation of a spatial query.
    
    This is the core IR that sits between natural language and SQL.
    It is human-readable, validatable, and transformable.
    """
    
    # What to select
    select_entities: List[Entity] = field(default_factory=list)
    select_attributes: List[str] = field(default_factory=list)
    
    # Where to filter
    conditions: List[Condition] = field(default_factory=list)
    
    # How to combine
    logical_operator: LogicalOperator = LogicalOperator.AND
    
    # Aggregation
    aggregation: Optional[Dict[str, Any]] = None
    # {"function": "SUM", "field": "ST_Length(way)", "alias": "total_length"}
    
    # Ordering and limits
    order_by: Optional[str] = None
    order_direction: str = "ASC"
    limit: Optional[int] = None
    
    # Metadata
    original_query: str = ""
    interpretation_confidence: float = 0.0
    reasoning: str = ""
    
    def to_dict(self) -> Dict:
        """Convert IR to dictionary for serialization."""
        return {
            "select_entities": [e.to_dict() for e in self.select_entities],
            "select_attributes": self.select_attributes,
            "conditions": [c.to_dict() for c in self.conditions],
            "logical_operator": self.logical_operator.value,
            "aggregation": self.aggregation,
            "order_by": self.order_by,
            "order_direction": self.order_direction,
            "limit": self.limit,
            "original_query": self.original_query,
            "interpretation_confidence": self.interpretation_confidence,
            "reasoning": self.reasoning
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert IR to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'IntermediateRepresentation':
        """Create IR from dictionary."""
        ir = cls()
        
        # Reconstruct entities
        for entity_data in data.get("select_entities", []):
            entity = Entity(
                name=entity_data["name"],
                table=entity_data.get("table"),
                geometry_type=entity_data.get("geometry_type"),
                attributes=entity_data.get("attributes", []),
                tags=entity_data.get("tags", {})
            )
            ir.select_entities.append(entity)
        
        ir.select_attributes = data.get("select_attributes", [])
        
        # Reconstruct conditions
        for cond_data in data.get("conditions", []):
            filter_data = cond_data["filter"]
            target_entity_data = filter_data["target_entity"]
            
            target_entity = Entity(
                name=target_entity_data["name"],
                table=target_entity_data.get("table"),
                geometry_type=target_entity_data.get("geometry_type"),
                attributes=target_entity_data.get("attributes", []),
                tags=target_entity_data.get("tags", {})
            )
            
            spatial_filter = SpatialFilter(
                operation_type=SpatialOperationType[filter_data["operation_type"]],
                target_entity=target_entity,
                parameters=filter_data.get("parameters", {})
            )
            
            operator = None
            if cond_data.get("operator"):
                operator = LogicalOperator(cond_data["operator"])
            
            condition = Condition(
                filter=spatial_filter,
                operator=operator,
                negate=cond_data.get("negate", False)
            )
            ir.conditions.append(condition)
        
        ir.logical_operator = LogicalOperator(data.get("logical_operator", "AND"))
        ir.aggregation = data.get("aggregation")
        ir.order_by = data.get("order_by")
        ir.order_direction = data.get("order_direction", "ASC")
        ir.limit = data.get("limit")
        ir.original_query = data.get("original_query", "")
        ir.interpretation_confidence = data.get("interpretation_confidence", 0.0)
        ir.reasoning = data.get("reasoning", "")
        
        return ir
    
    def validate(self) -> tuple[bool, List[str]]:
        """
        Validate the IR structure.
        
        Returns:
            (is_valid, list_of_errors)
        """
        errors = []
        
        # Must have at least one entity to select
        if not self.select_entities:
            errors.append("IR must have at least one SELECT entity")
        
        # All entities must have names
        for entity in self.select_entities:
            if not entity.name:
                errors.append("All entities must have names")
        
        # Validate conditions
        for i, condition in enumerate(self.conditions):
            if not condition.filter.target_entity.name:
                errors.append(f"Condition {i} must have a target entity name")
            
            # Validate operation parameters
            op_type = condition.filter.operation_type
            params = condition.filter.parameters
            
            if op_type == SpatialOperationType.PROXIMITY:
                if "distance" not in params:
                    errors.append(f"Proximity condition {i} must have 'distance' parameter")
                if params.get("distance", 0) <= 0:
                    errors.append(f"Proximity condition {i} must have positive distance")
            
            elif op_type == SpatialOperationType.CONTAINMENT:
                if "container_entity" not in params:
                    errors.append(f"Containment condition {i} must have 'container_entity'")
        
        # Validate aggregation
        if self.aggregation:
            if "function" not in self.aggregation:
                errors.append("Aggregation must specify 'function'")
            valid_funcs = ["COUNT", "SUM", "AVG", "MIN", "MAX", "ST_LENGTH"]
            if self.aggregation.get("function", "").upper() not in valid_funcs:
                errors.append(f"Aggregation function must be one of {valid_funcs}")
        
        return len(errors) == 0, errors
    
    def explain(self) -> str:
        """
        Generate human-readable explanation of the IR.
        
        Returns:
            Explanation string describing what the query does
        """
        parts = []
        
        # Explain what we're selecting
        entity_names = [e.name for e in self.select_entities]
        parts.append(f"Select: {', '.join(entity_names)}")
        
        if self.select_attributes:
            parts.append(f"Attributes: {', '.join(self.select_attributes)}")
        
        # Explain conditions
        if self.conditions:
            parts.append("\nConditions:")
            for i, condition in enumerate(self.conditions):
                filter_desc = self._explain_condition(condition)
                prefix = f"  {i+1}. "
                if condition.negate:
                    prefix += "NOT "
                parts.append(f"{prefix}{filter_desc}")
                
                if condition.operator and i < len(self.conditions) - 1:
                    parts.append(f"     {condition.operator.value} (next condition)")
        
        # Explain aggregation
        if self.aggregation:
            func = self.aggregation.get("function", "")
            field = self.aggregation.get("field", "")
            alias = self.aggregation.get("alias", "")
            parts.append(f"\nAggregate: {func}({field}) AS {alias}")
        
        # Explain limits
        if self.limit:
            parts.append(f"Limit: {self.limit} results")
        
        # Add reasoning
        if self.reasoning:
            parts.append(f"\nInterpretation: {self.reasoning}")
        
        parts.append(f"\nConfidence: {self.interpretation_confidence:.0%}")
        
        return "\n".join(parts)
    
    def _explain_condition(self, condition: Condition) -> str:
        """Generate explanation for a single condition."""
        f = condition.filter
        op_type = f.operation_type
        target = f.target_entity.name
        params = f.parameters
        
        if op_type == SpatialOperationType.PROXIMITY:
            distance = params.get("distance", 0)
            unit = params.get("unit", "meters")
            return f"Within {distance} {unit} of {target}"
        
        elif op_type == SpatialOperationType.CONTAINMENT:
            container = params.get("container_name", target)
            return f"Inside {container}"
        
        elif op_type == SpatialOperationType.INTERSECTION:
            return f"Intersecting with {target}"
        
        elif op_type == SpatialOperationType.FILTER:
            tags = f.target_entity.tags
            tag_desc = ", ".join([f"{k}={v}" for k, v in tags.items()])
            return f"Where {tag_desc}"
        
        elif op_type == SpatialOperationType.NEGATION:
            return f"Not near {target}"
        
        else:
            return f"{op_type.name} {target}"


class IRValidator:
    """
    Validates IR against schema and security constraints.
    """
    
    def __init__(self, allowed_tables: Optional[set] = None):
        self.allowed_tables = allowed_tables or set()
    
    def validate_against_schema(self, ir: IntermediateRepresentation) -> tuple[bool, List[str]]:
        """
        Validate IR entities against database schema.
        
        Args:
            ir: Intermediate representation to validate
            
        Returns:
            (is_valid, list_of_errors)
        """
        errors = []
        
        if not self.allowed_tables:
            return True, []
        
        # Validate all entities have valid tables
        for entity in ir.select_entities:
            if entity.table and entity.table not in self.allowed_tables:
                errors.append(f"Entity '{entity.name}' uses unknown table '{entity.table}'")
        
        for condition in ir.conditions:
            entity = condition.filter.target_entity
            if entity.table and entity.table not in self.allowed_tables:
                errors.append(f"Condition entity '{entity.name}' uses unknown table '{entity.table}'")
        
        return len(errors) == 0, errors


def create_ir_from_nlp(
    query: str,
    parsed_entities: List[Dict],
    spatial_relations: List[Dict],
    reasoning: str,
    confidence: float = 0.0
) -> IntermediateRepresentation:
    """
    Factory function to create IR from NLP parsing results.
    
    Args:
        query: Original natural language query
        parsed_entities: List of entities extracted from query
        spatial_relations: List of spatial relationships
        reasoning: Interpretation reasoning
        confidence: Confidence score
        
    Returns:
        IntermediateRepresentation
    """
    ir = IntermediateRepresentation()
    ir.original_query = query
    ir.reasoning = reasoning
    ir.interpretation_confidence = confidence
    
    # Create entities from parsed results
    for entity_data in parsed_entities:
        entity = Entity(
            name=entity_data.get("name", ""),
            table=entity_data.get("table"),
            geometry_type=entity_data.get("geometry_type"),
            attributes=entity_data.get("attributes", ["name", "way"]),
            tags=entity_data.get("tags", {})
        )
        ir.select_entities.append(entity)
    
    # Create conditions from spatial relations
    for relation in spatial_relations:
        op_type = SpatialOperationType[relation.get("type", "FILTER")]
        
        target_entity = Entity(
            name=relation.get("target", ""),
            table=relation.get("target_table"),
            tags=relation.get("target_tags", {})
        )
        
        spatial_filter = SpatialFilter(
            operation_type=op_type,
            target_entity=target_entity,
            parameters=relation.get("parameters", {})
        )
        
        condition = Condition(
            filter=spatial_filter,
            operator=LogicalOperator(relation.get("operator", "AND")),
            negate=relation.get("negate", False)
        )
        
        ir.conditions.append(condition)
    
    return ir
