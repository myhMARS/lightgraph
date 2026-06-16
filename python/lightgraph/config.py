"""
Configuration for LightGraph database.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class Config:
    """Database configuration."""

    # Data directory
    data_dir: Path = Path("~/.lightgraph/data").expanduser()

    # Index settings
    fulltext_analyzer: str = "cjk"  # "cjk" | "standard" | "ngram"
    fulltext_bm25_k1: float = 1.2
    fulltext_bm25_b: float = 0.75

    vector_dim: Optional[int] = None  # auto-detect
    vector_metric: str = "cosine"    # "cosine" | "euclidean" | "dot"
    vector_m: int = 16               # HNSW M parameter
    vector_ef_construction: int = 200
    vector_ef_search: int = 100

    # Hybrid query
    fusion_alpha: float = 0.5        # text weight
    fusion_beta: float = 0.5         # vector weight
    filter_timing: str = "adaptive"  # "pre" | "post" | "adaptive"

    # Persistence
    wal_group_commit_us: int = 100   # group commit window (microseconds)
    snapshot_interval_secs: int = 600
    snapshot_max_wal_mb: int = 512

    # Distributed
    cluster_nodes: list = field(default_factory=list)
    virtual_nodes: int = 256         # hash ring virtual nodes
    raft_election_timeout_ms: int = 3000
    raft_heartbeat_interval_ms: int = 500

    # Performance
    query_cache_size: int = 1024     # compiled query plan cache
    max_result_size: int = 10000
