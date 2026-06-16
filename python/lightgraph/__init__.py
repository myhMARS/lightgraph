"""
LightGraph — High-performance distributed graph database.

Python SDK via PyO3 native bindings.
"""

# When built with maturin, the native module is `lightgraph._core`
try:
    from lightgraph._core import Database, Session, NodeId, EdgeId
except ImportError:
    # Pure Python stub for development
    Database = None
    Session = None
    NodeId = int
    EdgeId = int

from lightgraph.query import Q, QueryResult, FusionMethod, Direction
from lightgraph.config import Config

__version__ = "0.1.0"
__all__ = ["Database", "Session", "Q", "QueryResult", "Config",
           "NodeId", "EdgeId", "FusionMethod", "Direction"]
