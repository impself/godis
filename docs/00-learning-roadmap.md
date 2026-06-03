# Godis 学习路线

> 项目: github.com/hdt3213/godis — Go 语言实现的 Redis 服务器 (~1.7 万行代码，不含测试)
> 定位: 学习高并发中间件开发的优秀范例

## 项目目录总览

```
godis/
├── main.go                    # 程序入口：读取配置 → 选择模式 → 启动服务器
├── config/                    # 配置文件解析 (redis.conf)
├── interface/                 # 模块间接口定义 (解耦层)
│   ├── tcp/                   #   TCP Handler 接口
│   ├── redis/                 #   Connection / Reply 接口
│   └── database/              #   DBEngine 接口
├── lib/                       # 工具库 (不依赖其他模块，可独立学习)
│   ├── sync/wait/             #   带超时的 WaitGroup
│   ├── sync/atomic/           #   原子 Boolean
│   ├── logger/                #   异步日志库
│   ├── utils/                 #   通用工具 (随机字符串等)
│   ├── timewheel/             #   时间轮 (key 过期调度)
│   ├── consistenthash/        #   一致性哈希
│   ├── wildcard/              #   通配符匹配
│   ├── geohash/               #   GeoHash 算法
│   ├── pool/                  #   连接池
│   └── idgenerator/           #   Snowflake ID 生成器
├── tcp/                       # TCP 服务器 (Accept 循环 / 优雅关闭)
├── redis/                     # Redis 协议层
│   ├── parser/                #   RESP 协议解析器
│   ├── protocol/              #   回复协议构造 (Ok/Err/Bulk/Int...)
│   ├── connection/            #   连接状态管理 (含 FakeConn 测试替身)
│   ├── client/                #   Redis 客户端 (集群内部通信)
│   └── server/                #   完整 Redis 服务器
│       ├── std/               #     标准网络模型 (net.Listener)
│       └── gnet/              #     gnet 高性能网络模型
├── datastruct/                # 数据结构层 ⚠️ 核心
│   ├── dict/                  #   并发哈希表 (Simple + Concurrent)
│   ├── list/                  #   双向链表 + quicklist
│   ├── set/                   #   集合 (基于字典)
│   ├── sortedset/             #   有序集合 (跳表实现)
│   ├── bitmap/                #   位图
│   └── lock/                  #   Key 级读写锁
├── database/                  # 存储引擎层 ⚠️ 核心中的核心
│   ├── database.go            #   单 DB 引擎 (并行内核)
│   ├── router.go              #   命令路由表 (注册 + 分发)
│   ├── commandinfo.go         #   命令元信息 (标志位、键位置)
│   ├── server.go              #   多 DB 服务器 + 慢日志
│   ├── keys.go                #   通用 Key 命令 (DEL/EXISTS/EXPIRE...)
│   ├── string.go              #   String 命令 (SET/GET/INCR...)
│   ├── list.go                #   List 命令 (LPUSH/LRANGE...)
│   ├── hash.go                #   Hash 命令 (HSET/HGET...)
│   ├── set.go                 #   Set 命令 (SADD/SINTER...)
│   ├── sortedset.go           #   SortedSet 命令 (ZADD/ZRANGE...)
│   ├── geo.go                 #   GEO 命令 (GEOADD/GEORADIUS...)
│   ├── transaction.go         #   事务 (MULTI/EXEC/WATCH)
│   ├── tx_utils.go            #   事务工具函数
│   ├── systemcmd.go           #   系统命令 (AUTH/INFO/SLOWLOG...)
│   ├── slowlog.go             #   慢日志实现
│   ├── persistence.go         #   持久化入口
│   ├── cluster_helper.go      #   集群辅助 (集群感知的 DB 包装)
│   ├── replication_master.go  #   主节点复制逻辑
│   └── replication_slave.go   #   从节点复制逻辑
├── aof/                       # 持久化层
│   ├── aof.go                 #   AOF 写入 / 加载 / Fsync
│   ├── rewrite.go             #   AOF 重写 (BGREWRITEAOF)
│   ├── marshal.go             #   命令序列化
│   └── rdb.go                 #   RDB 快照
├── pubsub/                    # 发布订阅
│   ├── hub.go                 #   消息中心
│   └── pubsub.go              #   订阅/发布逻辑
├── cluster/                   # 集群层 (最高级)
│   ├── cluster.go             #   集群入口 (实现 DBEngine 接口)
│   ├── raft/                  #   Raft 共识 (元数据一致性)
│   ├── core/                  #   集群核心
│   │   ├── core.go            #     启动 / 初始化
│   │   ├── node_manager.go    #     节点管理
│   │   ├── migration.go       #     数据迁移
│   │   ├── tcc.go             #     TCC 分布式事务
│   │   ├── connection.go      #     集群连接
│   │   ├── command.go         #     命令路由
│   │   └── cron.go            #     定时任务
│   └── commands/              #   跨节点原子命令
│       ├── del.go             #     分布式 DEL
│       ├── mset.go            #     分布式 MSET
│       ├── rename.go          #     分布式 RENAME
│       ├── tcc_utils.go       #     TCC 工具函数
│       └── default.go         #     默认命令 (单 slot 直通)
└── docs/                      # 学习笔记
```

---

## 学习路线设计思路

遵循 README 建议的阅读顺序，从数据流入服务器的入口（TCP）开始，逐步深入到存储引擎、集群：

```
客户端请求 ─→ TCP 服务器 ─→ Redis 协议解析 ─→ 存储引擎 ─→ 数据结构
                                                              │
                                                         持久化/复制
                                                              │
                                                          集群层
```

lib/ 工具库穿插在相关阶段中学习，用到的时再讲。

---

## 第一阶段：接口定义 (interface/)

> 模块解耦的关键。代码量极少 (~100 行) 但定义了整个项目的契约。
> 先看接口再看实现，学习时更有方向感。
> 预计 < 1 天

| 序号 | 模块 | 文件 | 学习目标 |
|------|------|------|----------|
| 1.1 | `interface/tcp` | `handler.go` | `Handler` 接口 — TCP 层和上层解耦 |
| 1.2 | `interface/redis` | `reply.go`, `conn.go` | `Connection` / `Reply` 接口 — 协议层抽象 |
| 1.3 | `interface/database` | `db.go` | `DBEngine` 接口 — 存储引擎和集群层的统一契约 |

**重点理解**: `DBEngine` 接口使得 `database.DB` 和 `cluster.Cluster` 可以对上层透明切换，这是 main.go 中单机/集群模式切换的基础。

---

## 第二阶段：TCP 服务器 (tcp/)

> 客户端连接进入的第一道关卡。理解网络层如何接收连接、分发请求、优雅关闭。
> 涉及 lib/: `lib/sync/wait`（优雅关闭）、`lib/logger`（日志）
> 预计 1 天

| 序号 | 模块 | 文件 | 行数 | 学习目标 |
|------|------|------|------|----------|
| 2.1 | TCP 服务器 | `tcp/server.go` | ~120 | Accept 循环、每连接 goroutine、信号监听、优雅关闭 |
| 2.2 | Echo Handler | `tcp/echo.go` | ~90 | Handler 接口的示例实现 |

**涉及的 lib/ 模块**（按需学习）:

| 模块 | 文件 | 行数 | 学习目标 | Go 知识点 |
|------|------|------|----------|-----------|
| `lib/sync/wait` | `wait.go` | 44 | 带超时的 WaitGroup，优雅关闭基础 | `sync.WaitGroup`, buffered channel |
| `lib/sync/atomic` | `bool.go` | 21 | 原子操作实现无锁并发 | `sync/atomic` |
| `lib/logger` | `logger.go`, `files.go` | ~120 | 异步日志、channel 解耦 | goroutine 消费者模式 |

---

## 第三阶段：Redis 协议层 (redis/)

> 字节流 ↔ Redis 命令/回复的完整编解码链路。
> 预计 2-3 天

| 序号 | 模块 | 文件 | 行数 | 学习目标 |
|------|------|------|------|----------|
| 3.1 | RESP 回复构造 | `redis/protocol/reply.go`, `consts.go`, `errors.go` | ~150 | RESP 协议回复类型 (Ok/Err/Bulk/Int/ Multi...) |
| 3.2 | **RESP 协议解析** | `redis/parser/parser.go`, `parserv2.go` | ~450 | 字节流 → 结构化命令，状态机解析 |
| 3.3 | 连接管理 | `redis/connection/conn.go`, `fake.go` | ~150 | 连接状态、`sync.Pool` 复用 Buffer、事务队列 |
| 3.4 | Redis 客户端 | `redis/client/client.go` | ~250 | 完整请求-响应循环的另一面，集群内部通信基础 |
| 3.5 | **标准 Redis 服务器** | `redis/server/std/server.go` | ~350 | **组合层**：TCP + 协议 + 数据库引擎 → 完整 Redis 服务器 |
| 3.6 | gnet 服务器 | `redis/server/gnet/server.go` | ~250 | gnet 高性能网络模型 (可选，对比 std 理解) |

**实战建议**: 完成 3.5 后，用 `redis-cli -p 6399` 连接 godis，发送 `PING` / `SET foo bar`，在 `std/server.go` 的 `HandleFunc` 打断点，走一遍完整链路。

---

## 第四阶段：数据结构层 (datastruct/) ⚠️ 核心

> Redis 之所以快，核心在于数据结构的设计。
> 涉及 lib/: `lib/timewheel`（key 过期调度）、`lib/wildcard`（KEYS 通配符）、`lib/geohash`（GEO 命令）
> 预计 3-4 天

| 序号 | 模块 | 文件 | 行数 | 学习目标 | 重要性 |
|------|------|------|------|----------|--------|
| 4.1 | `datastruct/dict` | `dict.go`, `simple.go`, `concurrent.go` | ~400 | **并发哈希表**：分段锁 vs `sync.Map`，key 粒度 RWLocks | ⭐⭐⭐ |
| 4.2 | `datastruct/lock` | `lock_map.go` | ~100 | Key 级读写锁 — 并行内核的隔离基础 | ⭐⭐⭐ |
| 4.3 | `datastruct/list` | `interface.go`, `linked.go`, `quicklist.go` | ~350 | 双向链表 + quicklist (Redis 3.2+ 列表底层) | ⭐⭐ |
| 4.4 | `datastruct/set` | `set.go` | ~80 | 基于字典的集合 | ⭐ |
| 4.5 | `datastruct/sortedset` | `sortedset.go`, `skiplist.go`, `border.go` | ~500 | **跳表 (Skip List)** — 有序集合核心，最重要的数据结构 | ⭐⭐⭐ |
| 4.6 | `datastruct/bitmap` | `bitmap.go` | ~250 | 位图位操作 | ⭐⭐ |

**涉及的 lib/ 模块**（按需学习）:

| 模块 | 文件 | 行数 | 学习目标 | 在哪里用到 |
|------|------|------|----------|-----------|
| `lib/timewheel` | `timewheel.go`, `delay.go` | ~180 | 时间轮算法，高效批量 key 过期 | `database/database.go` 中 TTL 调度 |
| `lib/wildcard` | `wildcard.go` | ~60 | 通配符匹配 | `database/keys.go` 中 KEYS 命令 |
| `lib/geohash` | `geohash.go`, `neighbor.go` | ~200 | GeoHash 编码 | `database/geo.go` 中 GEO 命令 |

**学习建议**:
- **dict** 和 **sortedset** 是重中之重，值得反复阅读
- 4.1 和 4.2 结合第五阶段的 `database.go` 一起理解——它们共同构成了"并行内核"
- sortedset 的跳表实现建议画图理解节点层级和查找路径

---

## 第五阶段：存储引擎层 (database/) ⚠️ 核心中的核心

> Redis 服务的心脏。理解这一层就理解了 Godis 单机版的全部。
> 预计 4-5 天

| 序号 | 模块 | 文件 | 行数 | 学习目标 |
|------|------|------|------|----------|
| 5.1 | 命令路由 | `router.go` | ~100 | 命令注册表、`executor/prepare/undo` 三元组 |
| 5.2 | 命令元信息 | `commandinfo.go` | ~130 | 命令标志位、键位置声明 (COMMAND 命令) |
| 5.3 | **数据库引擎** | `database.go` | ~320 | **并行内核**：命令只锁涉及的 key，不同 key 并行执行 |
| 5.4 | Key 命令 | `keys.go` | ~250 | DEL/EXISTS/EXPIRE/TTL/KEYS/TYPE 等 |
| 5.5 | String | `string.go` | ~250 | SET/GET/MSET/SETNX/INCR/INCRBY 等 |
| 5.6 | List | `list.go` | ~200 | LPUSH/RPUSH/LPOP/LRANGE/LINDEX 等 |
| 5.7 | Hash | `hash.go` | ~150 | HSET/HGET/HDEL/HGETALL/HLEN 等 |
| 5.8 | Set | `set.go` | ~150 | SADD/SREM/SINTER/SUNION/SCARD 等 |
| 5.9 | SortedSet | `sortedset.go` | ~300 | ZADD/ZRANGE/ZRANK/ZSCORE/ZPOPMIN 等 |
| 5.10 | Geo | `geo.go` | ~200 | GEOADD/GEODIST/GEORADIUS (结合 `lib/geohash`) |
| 5.11 | 事务 | `transaction.go`, `tx_utils.go` | ~230 | MULTI/EXEC/WATCH 乐观锁、undo log 回滚 |
| 5.12 | 系统命令 | `systemcmd.go`, `slowlog.go` | ~200 | AUTH/INFO/SLOWLOG/COMMAND/SELECT 等 |
| 5.13 | 多 DB 服务器 | `server.go` | ~250 | 多数据库管理、`atomic.Value` 无锁 FLUSHDB |
| 5.14 | 持久化入口 | `persistence.go` | ~60 | AOF / RDB 持久化接入 |
| 5.15 | 集群辅助 | `cluster_helper.go` | ~80 | 集群感知的 DB 包装 (为集群模式提供支持) |

**核心理解目标**:
1. **命令处理全链路**: `client request` → `parser` → `router` → `execNormalCommand` → `lock keys` → `execFunc` → `reply`
2. **并行内核**: `database.go` 中的 `execNormalCommand` 只锁涉及的 key，不同 key 的命令可以并行
3. **事务隔离**: `transaction.go` 通过 WATCH + versionMap + undo log 实现原子性和隔离性

---

## 第六阶段：集群层 (cluster/)

> 分布式系统的精华，涉及共识算法、分布式事务。
> 涉及 lib/: `lib/consistenthash`（一致性哈希）、`lib/pool`（连接池）、`lib/idgenerator`（Snowflake ID）
> 预计 3-5 天

| 序号 | 模块 | 文件 | 行数 | 学习目标 |
|------|------|------|------|----------|
| 6.1 | 集群入口 | `cluster.go` | 40 | `Cluster` 结构体 — 实现 `DBEngine` 接口 |
| 6.2 | Raft 共识 | `raft/raft.go`, `fsm.go`, `utils.go` | ~300 | HashiCorp Raft 库使用、FSM 实现、集群元数据一致性 |
| 6.3 | 集群核心 | `core/*.go` | ~800 | 节点管理、slot 分配、数据迁移、定时任务 |
| 6.4 | TCC 分布式事务 | `core/tcc.go` | ~160 | Try-Commit-Catch 模式 — 跨节点原子操作 |
| 6.5 | 跨节点命令 | `commands/*.go` | ~500 | 分布式 DEL/MSET/RENAME 及默认命令路由 |

**涉及的 lib/ 模块**（按需学习）:

| 模块 | 文件 | 行数 | 学习目标 | 在哪里用到 |
|------|------|------|----------|-----------|
| `lib/consistenthash` | `consistenthash.go` | ~120 | 一致性哈希，虚拟节点 | `cluster/` 中 slot 映射 |
| `lib/pool` | `pool.go` | ~100 | 连接池/对象池 | `redis/client/` 中连接管理 |
| `lib/idgenerator` | `snowflake.go` | ~80 | Snowflake 唯一 ID | `cluster/` 中分布式 ID |

---

## 第七阶段：持久化 + 发布订阅 + 主从复制

> 数据可靠性保障和消息通信。
> 预计 2-3 天

| 序号 | 模块 | 文件 | 行数 | 学习目标 |
|------|------|------|------|----------|
| 7.1 | AOF | `aof/aof.go` | ~320 | 命令写入 channel → 批量刷盘，Fsync 策略 |
| 7.2 | AOF 重写 | `aof/rewrite.go` | ~250 | BGREWRITEAOF：遍历数据库生成精简 AOF |
| 7.3 | 序列化 | `aof/marshal.go` | ~80 | CmdLine → RESP 格式 |
| 7.4 | RDB | `aof/rdb.go` | ~200 | RDB 快照保存/加载 |
| 7.5 | 发布订阅 | `pubsub/hub.go`, `pubsub.go` | ~200 | channel → 订阅者列表映射 |
| 7.6 | 主节点复制 | `database/replication_master.go` | ~250 | 管理 slave、PSYNC 增量同步、命令传播 |
| 7.7 | 从节点复制 | `database/replication_slave.go` | ~300 | 连接 master、全量/增量同步、命令回放 |

---

## 第八阶段：入口与配置

> 串联全局。验证是否真正理解了整个项目。
> 预计 < 1 天

| 序号 | 模块 | 文件 | 行数 | 学习目标 |
|------|------|------|------|----------|
| 8.1 | 入口 | `main.go` | 76 | 程序启动全流程：配置 → 日志 → 选择模式 (单机/集群/gnet) → 启动 |
| 8.2 | 配置 | `config/config.go` | 184 | 配置文件解析、默认值 |

---

## 核心设计亮点 (学完全部后回顾)

1. **并行内核** — `database.go:execNormalCommand`: 只锁涉及的 key，不同 key 并行执行
2. **无锁 FLUSHDB** — `server.go`: `atomic.Value.Store(newDB)` 替换整个数据库实例
3. **乐观锁事务** — `transaction.go`: WATCH + versionMap CAS + undo log 回滚
4. **TCC 分布式事务** — `cluster/core/tcc.go`: Try-Commit-Catch 跨节点原子操作
5. **时间轮过期** — `lib/timewheel`: O(1) 添加，批量扫描过期 key
6. **优雅关闭** — `tcp/server.go` + `lib/sync/wait`: 完整优雅关闭链路
7. **双网络模型** — `redis/server/std` + `redis/server/gnet`: 标准库 vs 高性能网络框架对比

---

## 学习建议

- **先全局后局部**: 先读 `main.go` 建立全局视野，再按阶段深入
- **边读边跑**: 用 `redis-cli -p 6399` 连接 godis，发命令，跟调试器走链路
- **先单机后集群**: 关闭集群模式 (`cluster-enable no`) 吃透单机版
- **以命令为线索**: 选一个命令 (如 SET)，从 `main.go` → `Exec` → `string.go:execSet` 画完整链路
- **重点数据结构**: dict (并发哈希表) 和 sortedset (跳表) 最值得单独研究
- **看测试学用法**: 每个 `*_test.go` 都是最好的用法示例和边界 case 参考

---

## 学习进度

| 阶段 | 模块 | 状态 | 笔记 |
|------|------|------|------|
| 2.x | lib/sync/wait + lib/sync/atomic + lib/logger | ✅ 已完成 | [01-lib-sync-wait.md](01-lib-sync-wait.md), [02-lib-logger.md](02-lib-logger.md) |
| 1.1-1.3 | interface/* | ✅ 已完成 | [03-interface.md](03-interface.md) |
| 2.1-2.2 | tcp/* | ✅ 已完成 | [04-tcp-server.md](04-tcp-server.md) |
| 3.1-3.6 | redis/* | ✅ 已完成 | [05-redis-protocol.md](05-redis-protocol.md) |
| 4.1-4.6 | datastruct/* | ⏳ 待学习 | |
| 5.1-5.15 | database/* | ⏳ 待学习 | |
| 6.1-6.5 | cluster/* | ⏳ 待学习 | |
| 7.1-7.7 | aof + pubsub + replication | ⏳ 待学习 | |
| 8.1-8.2 | main + config | ⏳ 待学习 | |
