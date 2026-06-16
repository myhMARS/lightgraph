"""
Query builder DSL for LightGraph.

Usage:
    from lightgraph import Q

    results = db.hybrid_search(
        fulltext=Q.text("desc", "机械键盘 静音"),
        vector=Q.vec("embedding", my_array, topk=20),
        filters=[Q.eq("category", "外设"), Q.range("price", 100, 500)],
        fusion="weighted_sum",
        alpha=0.4, beta=0.6,
        limit=10,
    )
"""

from enum import Enum
from typing import Optional, List, Any, Union
import numpy as np


class FusionMethod(Enum):
    WEIGHTED_SUM = "weighted_sum"
    RRF = "reciprocal_rank_fusion"
    CONVEX = "convex"


class Direction(Enum):
    OUT = "out"
    IN = "in"
    BOTH = "both"


class QueryResult:
    """A single result from a hybrid query."""
    def __init__(self, node_id: int, score: float, props: dict,
                 path: Optional[List[int]] = None):
        self.node_id = node_id
        self._score = score
        self._path = path or []
        self._props = props

    def __getitem__(self, key):
        if key == "_score":
            return self._score
        if key == "_path":
            return self._path
        return self._props.get(key)

    def __repr__(self):
        return f"QueryResult(id={self.node_id}, score={self._score:.4f})"

    def __iter__(self):
        """Allow dict(result) casting."""
        return iter({"_score": self._score, "_path": self._path,
                      **self._props, "node_id": self.node_id}.items())


class Q:
    """Static methods for building query predicates and clauses."""

    @staticmethod
    def text(property: str, query: str) -> dict:
        """Full-text search clause."""
        return {"type": "fulltext", "property": property, "query": query}

    @staticmethod
    def vec(property: str, vector: Union[List[float], "np.ndarray"],
            topk: int = 20, ef: int = 200) -> dict:
        """Vector similarity search clause."""
        if isinstance(vector, np.ndarray):
            vector = vector.astype(np.float32).tolist()
        return {"type": "vector", "property": property,
                "vector": vector, "topk": topk, "ef": ef}

    @staticmethod
    def eq(property: str, value: Any) -> dict:
        return {"type": "eq", "property": property, "value": value}

    @staticmethod
    def gt(property: str, value: float) -> dict:
        return {"type": "gt", "property": property, "value": value}

    @staticmethod
    def gte(property: str, value: float) -> dict:
        return {"type": "gte", "property": property, "value": value}

    @staticmethod
    def lt(property: str, value: float) -> dict:
        return {"type": "lt", "property": property, "value": value}

    @staticmethod
    def lte(property: str, value: float) -> dict:
        return {"type": "lte", "property": property, "value": value}

    @staticmethod
    def range(property: str, lo: float, hi: float) -> dict:
        return {"type": "range", "property": property, "lo": lo, "hi": hi}

    @staticmethod
    def in_(property: str, values: List[Any]) -> dict:
        return {"type": "in", "property": property, "values": values}

    @staticmethod
    def and_(*predicates: dict) -> dict:
        return {"type": "and", "predicates": list(predicates)}

    @staticmethod
    def or_(*predicates: dict) -> dict:
        return {"type": "or", "predicates": list(predicates)}
