# LightGraph

高性能分布式图数据库 | High-Performance Distributed Graph Database

## 特性

- **全内存架构** — 毫秒级查询延迟，读路径完全无锁
- **CJK 全文检索** — Bigram 分词 + FST + RoaringBitmap + BM25 打分
- **向量语义搜索** — HNSW 图索引，支持 Cosine/Euclidean/Dot
- **谓词前置过滤** — 属性索引返回位图，缩小候选集后再打分
- **混合查询** — 全文 + 向量 + 谓词 + 图遍历，自适应融合策略
- **持久化** — WAL group commit + FlatBuffers 快照 + 崩溃恢复
- **分布式** — Raft 共识 + 一致性哈希分片
- **Python SDK** — PyO3 原生绑定，零序列化开销

## 架构

```
          Python SDK (PyO3)
               │
    ┌──────────┼──────────┐
    ▼          ▼          ▼
 全文索引   向量索引   属性索引
 (FST+CJK)  (HNSW)   (BTree+Bitmap)
    │          │          │
    └──────────┼──────────┘
               ▼
         混合查询引擎
      (谓词前置过滤 + 融合排序)
               │
               ▼
         图遍历 (邻接表)
               │
    ┌──────────┴──────────┐
    ▼                     ▼
  存储层              持久化层
 (DashMap+SlotMap)   (WAL+Snapshot)
```

## 开发路线图

参见 [ROADMAP.md](ROADMAP.md) — 18 Sprint 迭代规划。

## 快速开始

```bash
# Rust
cargo add lightgraph

# Python
pip install lightgraph
```

```python
from lightgraph import LightGraph, Q

db = LightGraph.open("~/.lightgraph/data")

# 创建索引
db.create_fulltext_index("Document", ["title", "body"], analyzer="cjk")
db.create_vector_index("Document", "embedding", dim=768, metric="cosine")
db.create_property_index("Document", "category")

# 写入
with db.session() as sess:
    sess.create_node("Document", {
        "title": "深入理解并发编程",
        "body": "本文介绍Rust中的无锁数据结构...",
        "category": "技术",
        "embedding": [0.12, -0.34, ...]
    })

# 混合查询
results = db.hybrid_search(
    fulltext=Q.text("body", "无锁 并发 数据结构"),
    vector=Q.vec("embedding", query_emb, topk=20),
    filters=[Q.eq("category", "技术")],
    fusion="weighted_sum", alpha=0.4, beta=0.6,
    limit=10,
)
```

## License

MIT
