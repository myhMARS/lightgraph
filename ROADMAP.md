# LightGraph — 高性能分布式图数据库 开发路线图

## 目标

- 全内存架构，毫秒级查询延迟
- CJK 全文检索索引（FST + RoaringBitmap + BM25）
- HNSW 向量索引（Cosine/Euclidean/DotProduct）
- 属性索引（谓词前置过滤 → 位图交并）
- 混合查询（全文 + 向量 + 谓词 + 图遍历），融合策略自适应
- 持久化 WAL + Snapshot，崩溃恢复
- 分布式：Raft 共识 + 一致性哈希分片
- Python SDK（PyO3 原生绑定）
- 每个模块独立测试，全链路压测

## 技术栈

- **内核**: Rust (DashMap, BTreeMap, roaring, fst, hnsw, parking_lot)
- **持久化**: 自定义 WAL + FlatBuffers Snapshot
- **分布式**: 自研 Raft + gRPC
- **SDK**: PyO3 + maturin
- **测试**: Rust 侧 `cargo test` + Python 侧 `pytest`

---

## 迭代规划（18 Sprint，每 Sprint 1 周）

### Sprint 1: 项目骨架 + 基础存储

| 任务 | 产出 | 测试 |
|------|------|------|
| Cargo 项目初始化，ci 配置 | `Cargo.toml`, `.github/workflows/ci.yml` | `cargo build` 通过 |
| Node 存储 (`NodeStore`) | `DashMap<NodeId, Node>` + Slab 分配 | `test_node_store.rs` |
| Edge 存储 (`EdgeStore`) | 双向邻接表 SlotMap | `test_edge_store.rs` |
| 属性列式存储 (`PropStore`) | 按 Label 分列的列式属性存储 | `test_prop_store.rs` |

### Sprint 2: CRUD + 基本事务

| 任务 | 产出 | 测试 |
|------|------|------|
| 节点/边 CRUD API | create/read/update/delete | `test_crud.rs` |
| 简化 MVCC（u64 版本号 + 快照隔离） | `TxManager` | `test_mvcc.rs` |
| 事务 begin/commit/rollback | `Transaction` 结构 | `test_transaction.rs` |

### Sprint 3: WAL 持久化

| 任务 | 产出 | 测试 |
|------|------|------|
| WAL 记录格式定义 | `WalRecord` 枚举 (Insert/Update/Delete) | 序列化测试 |
| WAL 写入 + Group Commit | `WalWriter` (批量 fsync, 100µs攒批) | `test_wal_write.rs` |
| WAL 重放恢复 | `WalReader` 顺序重放 | `test_wal_replay.rs` |
| 崩溃恢复集成 | 启动时自动检测并重放 WAL | `test_crash_recovery.rs` |

### Sprint 4: Snapshot 快照

| 任务 | 产出 | 测试 |
|------|------|------|
| FlatBuffers Schema (节点/边/索引元数据) | `schema.fbs` | schema 编译通过 |
| 全量 Snapshot 写入 | `SnapshotWriter` (多线程并行序列化) | `test_snapshot_write.rs` |
| mmap 反序列化恢复 | `SnapshotReader` (零拷贝读取) | `test_snapshot_read.rs` |
| 版本管理 + manifest | `Manifest` (记录最新 snapshot + 已合并 WAL) | `test_manifest.rs` |
| 启动恢复流程 | snapshot → WAL 重放 → 就绪 | `test_recovery_full.rs` |

### Sprint 5: 属性索引

| 任务 | 产出 | 测试 |
|------|------|------|
| 等值索引 | `(label, prop, value) → RoaringBitmap` | `test_eq_index.rs` |
| 范围索引 (BTreeMap) | `(label, prop) → BTreeMap<Value, Bitmap>` | `test_range_index.rs` |
| 多谓词组合 (AND/OR/NOT) | `Predicate` AST + `evaluate()` 位图运算 | `test_predicate.rs` |
| 索引增量维护 | 节点变更时实时更新索引 | `test_index_maintenance.rs` |

### Sprint 6: 全文检索索引（CJK）

| 任务 | 产出 | 测试 |
|------|------|------|
| CJK Bigram 分析器 | `CjkAnalyzer` — 中文/日文/韩文 bigram 分词 | `test_cjk_tokenizer.rs` |
| FST 词典构建 | `FstDict` — 紧凑前缀树 | `test_fst.rs` |
| 倒排索引 | `InvertedIndex` — term → RoaringBitmap + positions | `test_inverted_index.rs` |
| BM25 打分 | TF-IDF 变体, k1=1.2, b=0.75 | `test_bm25.rs` |
| 全文检索入口 | `FullTextIndex::search(query) → Vec<(NodeId, f32)>` | `test_fulltext_search.rs` |

### Sprint 7: 向量索引（HNSW）

| 任务 | 产出 | 测试 |
|------|------|------|
| 距离函数 (Cosine/Euclidean/Dot) | SIMD 加速向量运算 | `test_distance.rs` |
| HNSW 图构建 | 分层插入, M=16, ef_construction=200 | `test_hnsw_build.rs` |
| HNSW ANN 搜索 | 顶层下降到 layer0, ef_search=100 | `test_hnsw_search.rs` |
| 召回率验证 | brute-force vs HNSW, recall@10 ≥ 95% | `test_hnsw_recall.rs` |

### Sprint 8: 混合查询引擎第一版

| 任务 | 产出 | 测试 |
|------|------|------|
| 查询 DSL (Rust 侧构建器) | `QueryBuilder` 链式 API | `test_query_builder.rs` |
| 谓词前置过滤 | 先走属性索引 → 缩小候选集 → 全文/向量打分 | `test_pre_filter.rs` |
| 谓词后置过滤 | 先打分 → 再过滤（自适应选择） | `test_post_filter.rs` |
| 自适应策略选择 | 估算选择率, <10% 用前置, ≥10% 用后置 | `test_adaptive.rs` |
| 融合排序 | `score = α * BM25 + β * cosine`, top-k 输出 | `test_hybrid_search.rs` |

### Sprint 9: 图遍历引擎

| 任务 | 产出 | 测试 |
|------|------|------|
| BFS/DFS 遍历算子 | `Expand` 算子，邻接表进行 | `test_bfs_dfs.rs` |
| 路径追踪 | `Path` 记录结构 | `test_path.rs` |
| 边/节点谓词过滤 | 遍历中支持谓词剪枝 | `test_traverse_filter.rs` |
| 图遍历集成混合查询 | 混合搜索结果作为种子，再跳边 | `test_hybrid_traverse.rs` |

### Sprint 10: 查询优化与执行

| 任务 | 产出 | 测试 |
|------|------|------|
| 执行计划编译器 | `QueryPlan` → `Vec<PlanOp>` 管道 | `test_compiler.rs` |
| 算子实现 (Scan/Filter/Expand/Project/Limit) | 火山模型管道 | `test_operators.rs` |
| 代价估算 | 基于索引统计信息选择最优计划 | `test_cost_model.rs` |
| 查询缓存 | LRU 缓存编译好的执行计划 | `test_query_cache.rs` |

### Sprint 11: 分布式 — Raft 共识

| 任务 | 产出 | 测试 |
|------|------|------|
| Raft 核心 (Leader Election) | `RaftNode` — 选举定时器 + 投票 | `test_raft_election.rs` |
| Log Replication | Leader append → Follower 确认 | `test_raft_log.rs` |
| Raft 状态机集成 | 将 WAL 记录 apply 到本地存储 | `test_raft_sm.rs` |
| 成员变更 | 动态添加/移除节点 | `test_raft_membership.rs` |

### Sprint 12: 分布式 — 数据分片

| 任务 | 产出 | 测试 |
|------|------|------|
| 一致性哈希环 | 虚拟节点 + 节点/边按 ID 分片 | `test_hash_ring.rs` |
| 分片感知路由 | 查询路由到目标分片 | `test_shard_router.rs` |
| 跨分片遍历 | 一跳跨越分片边界时远程调用 | `test_cross_shard_traverse.rs` |
| 再平衡 | 增删节点时数据迁移 | `test_rebalance.rs` |

### Sprint 13: 分布式 — 索引 + 高可用

| 任务 | 产出 | 测试 |
|------|------|------|
| 全文索引分片 | 每个分片的本地 FST + 查询时跨分片合并 | `test_ft_distributed.rs` |
| 向量索引分片 | 每个分片的本地 HNSW + 查询时结果合并 | `test_vec_distributed.rs` |
| 故障转移 | Leader 宕机 → 自动选主 → 读副本接管 | `test_failover.rs` |
| 只读副本 | Follower 可读，分担查询压力 | `test_read_replica.rs` |

### Sprint 14: 性能基准 + 调优

| 任务 | 产出 | 测试 |
|------|------|------|
| 标准数据集加载 (LDBC SNB / Wikipedia) | 加载脚本 + 预处理 | benchmark 产出一致结果 |
| 读性能压测 | 吞吐 + p50/p99 延迟 | `bench_read.rs` |
| 写性能压测 | TPS + 索引更新延迟 | `bench_write.rs` |
| 混合查询压测 | 各种组合 + QPS 上限 | `bench_hybrid.rs` |
| 分布式压测 (3/5/7节点) | 扩展比 (2x节点 → 1.8x吞吐) | `bench_distributed.rs` |

### Sprint 15: PyO3 绑定

| 任务 | 产出 | 测试 |
|------|------|------|
| PyO3 基础绑定 | `#[pyclass] Database, Session, Node` | `test_pyo3_basic.py` |
| 释放 GIL 的查询调用 | `py.allow_threads(|| db.query())` | 并发安全性验证 |
| Maturin 构建 | `maturin develop` / `pip install lightgraph` | CI 构建 wheel |

### Sprint 16: Python SDK

| 任务 | 产出 | 测试 |
|------|------|------|
| `LightGraph.open()` 高层 API | 连接 + 数据库打开 | `test_open.py` |
| Session / 事务上下文管理器 | `with db.session() as sess:` | `test_session.py` |
| `Q` 查询构建器 | `Q.text() / Q.vec() / Q.eq() / Q.range()` | `test_q.py` |
| `ResultSet` 迭代器 | 流式返回结果 + `_score` / `_path` | `test_resultset.py` |
| Numpy 向量互转 | `numpy.ndarray` ↔ 内部 Vec<f32> | `test_numpy.py` |

### Sprint 17: Python 集成测试 + 文档

| 任务 | 产出 | 测试 |
|------|------|------|
| Python 混合查询端到端 | 全文+向量+谓词+遍历 → 结果正确 | `test_integration.py` |
| Python 批处理 | 批量插入 10万节点 + 索引构建 | `test_batch.py` |
| 多线程并发查询 | Python 多线程同时查询，无 GIL 阻塞 | `test_concurrent.py` |
| 文档 + 示例 | README / API文档 / QuickStart | CI 文档构建 |

### Sprint 18: 终极测试 + 发布

| 任务 | 产出 | 测试 |
|------|------|------|
| 性能白皮书 | 单机/分布式场景的延迟分布图 + QPS 表 |  |
| 与 Neo4j CE 对比 | 同等硬件/数据，同查询，延迟对比 |  |
| 1.0 release | GitHub Release + PyPI wheel |  |
