# Phase 1: interface/ — 项目契约层

> **文件**: `interface/tcp/handler.go` (16行), `interface/redis/reply.go` (6行), `interface/redis/conn.go` (37行), `interface/database/db.go` (44行)
> **核心**: 三个关键接口 + 两个类型别名，定义了整个项目的解耦契约

---

## 一句话总结 + 比喻

interface/ 是 Godis 的**宪法**——它不干任何实际工作，但规定了各层之间如何"外交"。没有这层接口，tcp 层直接依赖 database 层的具体实现，整个项目就粘成一坨了。

---

## 核心接口一览

### 1. `interface/tcp` — 网络层契约

```go
type HandleFunc func(ctx context.Context, conn net.Conn)

type Handler interface {
    Handle(ctx context.Context, conn net.Conn)
    Close() error
}
```

- `Handler` 接口：`tcp/server.go` 持有它，处理每个连接
- `HandleFunc` 函数类型：方便把函数直接当 Handler 用
- `context.Context`：优雅关闭时通知所有活跃连接收工
- `Close()`：TCP 关闭时通知 Handler 清理资源（关数据库、停 AOF 等）

### 2. `interface/redis.Reply` — 回复契约

```go
type Reply interface {
    ToBytes() []byte
}
```

项目中最精简的接口。不管什么回复类型，最终都序列化成字节发出去。

### 3. `interface/redis.Connection` — 连接契约

承载连接的全部状态：认证、发布订阅、事务队列、数据库选择、主从角色。共 20+ 方法。

接口虽大但不拆分——Go 的隐式实现意味着拆成多个小接口后，实现者照样要全部实现，反而增加复杂度。

另一个关键用途：`FakeConn` 测试替身，数据库/集群的单元测试不需要启动真实网络。

### 4. `interface/database` — 存储引擎契约（最关键）

```go
// DB — 最小公共接口（单机 + 集群都必须实现）
type DB interface {
    Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
    AfterClientClose(c redis.Connection)
    Close()
}

// DBEngine — 完整能力接口（只有单机 database.Server 实现）
type DBEngine interface {
    DB
    LoadRDB(...)
    ExecMulti(...)
    ForEach(...)
    RWLocks(...)
    // ...
}
```

**为什么分两级？**

```go
// main.go 中：
var db idatabase.DB         // 类型是 DB 接口，不是具体类型
if clusterEnable {
    db = cluster.MakeCluster()       // 集群实现
} else {
    db = database.NewStandaloneServer() // 单机实现
}
```

`DB` 是上层需要的最小集（只要能 `Exec` 就行）。`DBEngine` 是 AOF、事务等完整能力，只有单机模式需要。

如果不分两级，`cluster.Cluster` 也必须实现 `LoadRDB`、`ForEach` 等——但集群数据分布在多节点上，强行实现要么写空方法要么设计别扭。

**`CmdLine = [][]byte`**: 类型别名（不是新类型），纯粹为了可读性。

**`DataEntity.Data interface{}`**: 一个 key 可以存 String/List/Hash 等任何类型，通过类型断言取出。

---

## 设计模式

| 模式 | 体现 |
|------|------|
| 依赖倒置 (DIP) | 上层依赖接口不依赖实现 |
| 策略模式 | 单机 DB 和 Cluster 可互换，运行时通过配置选择 |
| 接口隔离变体 | DB（最小集）和 DBEngine（完整集）分级暴露能力 |
| 测试替身 | FakeConn 实现 Connection 接口，无网络测试 |

---

## 模块联动

```
interface/tcp.Handler    ←── tcp/server.go 持有
                         ←── redis/server/std 实现

interface/redis.Reply    ←── redis/protocol/* 实现各种 Reply
                         ←── database/* 输出 Reply

interface/redis.Connection ←── redis/connection/conn.go 实现（真实）
                           ←── redis/connection/fake.go 实现（测试）
                           ←── database/* 和 cluster/* 接收参数

interface/database.DB    ←── database.Server 实现（单机）
                         ←── cluster.Cluster 实现（集群）
                         ←── main.go 中根据配置选择
```

**修改接口的影响范围极大**——给 Connection 加一个方法，所有实现者都要改。接口设计要谨慎，宁少勿多。
