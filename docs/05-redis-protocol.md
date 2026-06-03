# Phase 3: redis/ — Redis 协议层

> **文件**: `redis/protocol/reply.go` + `consts.go` + `errors.go`, `redis/parser/parser.go` + `parserv2.go`, `redis/connection/conn.go` + `fake.go`, `redis/client/client.go`, `redis/server/std/server.go`
> **核心**: RESP 协议编解码 + 连接管理 + 完整 Redis 服务器组装

---

## 一句话总结 + 比喻

redis/ 是 Godis 的**翻译官团队**：protocol 是翻译字典（RESP 协议格式），parser 是听力秘书（把字节流翻译成命令），client 是传话员（向其他节点发命令），connection 是客人档案（记录每个连接的状态），server/std 是前台主管（把所有人协调到一起）。

---

## 3.1 redis/protocol — RESP 回复构造

### RESP 协议速览

Redis 使用 RESP（REdis Serialization Protocol）通信，通过首字节区分类型：

```
+OK\r\n           简单字符串（Status）
-ERR ...\r\n      错误（Error）
:123\r\n          整数（Int）
$3\r\nfoo\r\n     批量字符串（Bulk String）
*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n   数组（Multi Bulk）
```

### 核心设计：预分配字节切片

```go
// 单例回复：预计算字节，零分配
var pongBytes = []byte("+PONG\r\n")
func (r *PongReply) ToBytes() []byte {
    return pongBytes  // 直接返回，不每次拼接
}

var theOkReply = new(OkReply)
func MakeOkReply() *OkReply {
    return theOkReply  // 返回同一个实例
}
```

**为什么 OkReply/PongReply/QueuedReply 要预分配？**

这些回复内容固定、使用频繁（每个写命令都返回 OK）。预分配避免了每次调用 `ToBytes()` 时的字符串拼接和内存分配。而且 `theOkReply` 单例模式意味着所有调用共享同一个对象。

### BulkReply：动态内容

```go
func (r *BulkReply) ToBytes() []byte {
    if r.Arg == nil {
        return nullBulkBytes  // $-1\r\n，预分配的
    }
    return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}
```

**为什么 BulkReply 不预分配？** 内容是动态的（GET foo 返回什么值都有可能），无法预分配。这里用字符串拼接，可以接受。

### MultiBulkReply：预计算缓冲区大小

```go
func (r *MultiBulkReply) ToBytes() []byte {
    var buf bytes.Buffer
    // 先算总长度
    bufLen := 1 + len(strconv.Itoa(argLen)) + 2
    for _, arg := range r.Args {
        bufLen += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2
    }
    buf.Grow(bufLen)  // 一次性分配，避免扩容
    // 再写入
    buf.WriteString("*")
    // ...
}
```

**为什么先算长度再分配？** `bytes.Buffer` 默认从小容量开始，写多了会多次扩容（每次分配 + 拷贝）。先 `Grow` 到精确大小，只分配一次。这在高频场景（每个命令回复都经过这里）下性能差异明显。

### ErrorReply 接口的双重身份

```go
type ErrorReply interface {
    Error() string   // 满足 Go 的 error 接口
    ToBytes() []byte // 满足 redis.Reply 接口
}
```

ErrorReply 同时是 `error` 和 `Reply`。这意味着 `database.Exec()` 返回的错误回复可以直接当作 Go error 处理，也可以直接序列化发给客户端。一个对象两种视角，很巧妙。

---

## 3.2 redis/parser — RESP 协议解析器

### 两个解析器：parser.go vs parserv2.go

| 文件 | 用途 | 调用方 |
|------|------|--------|
| `parser.go` (ParseStream) | 服务端：从 TCP 连接流式解析 | `redis/server/std`, `redis/client` |
| `parserv2.go` (ParseV2) | 客户端简化版：解析单条命令 | 内部通信 |

### ParseStream — 核心状态机

```go
func ParseStream(reader io.Reader) <-chan *Payload {
    ch := make(chan *Payload)
    go parse0(reader, ch)  // 异步解析，通过 channel 输出
    return ch
}
```

**为什么返回 channel 而不是直接返回结果？**

因为 TCP 是流式的——数据可能分多次到达。`parse0` 内部用 `bufio.Reader.ReadBytes('\n')` 阻塞等待一行完整数据。返回 channel 让调用者不用阻塞在解析上，可以同时做其他事。

### parse0 状态机核心

```go
func parse0(rawReader io.Reader, ch chan<- *Payload) {
    reader := bufio.NewReader(rawReader)
    for {
        line, err := reader.ReadBytes('\n')  // 读一行
        // ...
        switch line[0] {
        case '+':  // 简单字符串
        case '-':  // 错误
        case ':':  // 整数
        case '$':  // 批量字符串（需再读 body）
        case '*':  // 数组（递归读取子元素）
        default:   // 兼容内联命令（空格分隔的纯文本）
        }
    }
}
```

**default 分支的秘密——兼容内联命令**

如果用户不用 RESP 协议而是直接输入 `SET foo bar\n`（redis-cli 的纯文本模式），`line[0]` 不是 RESP 标识符。此时用空格分割当内联命令处理：

```go
default:
    args := bytes.Split(line, []byte{' '})
    ch <- &Payload{Data: protocol.MakeMultiBulkReply(args)}
```

这就是为什么 `redis-cli` 中直接输入 `PING` 也能工作。

### parseBulkString — 最关键的子解析

```go
func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
    strLen, _ := strconv.ParseInt(string(header[1:]), 10, 64)
    if strLen == -1 {
        ch <- &Payload{Data: protocol.MakeNullBulkReply()}  // $-1\r\n = null
        return nil
    }
    body := make([]byte, strLen+2)  // +2 是 \r\n
    _, err = io.ReadFull(reader, body)  // 精确读取指定长度
    ch <- &Payload{Data: protocol.MakeBulkReply(body[:len(body)-2])}
}
```

**为什么用 `io.ReadFull` 而不是 `ReadBytes('\n')`？**

因为 Bulk String 的值里**可以包含 `\r\n`**（二进制安全）。如果用 `ReadBytes('\n')`，值中间的换行会提前截断。`io.ReadFull` 按精确字节数读取，不受内容影响。

### parseRDBBulkString — 主从复制的特殊处理

```go
// RDB 数据和后续 AOF 之间没有 CRLF 分隔
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
    // 精确读取 RDB 长度，不会多读一个字节
}
```

普通 BulkString 读 `strLen+2`（包含尾部 `\r\n`），但 RDB 数据后面直接跟 AOF，没有 `\r\n`。所以这里只读 `strLen` 字节。这是主从复制全量同步时的特殊协议。

---

## 3.3 redis/connection — 连接管理

### 位运算管理标志位

```go
const (
    flagSlave  = uint64(1 << iota)  // 001
    flagMaster                        // 010
    flagMulti                         // 100
)

func (c *Connection) InMultiState() bool {
    return c.flags&flagMulti > 0    // 检查第 3 位
}

func (c *Connection) SetMultiState(state bool) {
    if !state {
        c.flags &= ^flagMulti       // 清除第 3 位
    } else {
        c.flags |= flagMulti        // 设置第 3 位
    }
}
```

**为什么用位运算而不是多个 bool 字段？**

- 一个 `uint64` 可以存 64 个标志位，只需 8 字节
- 三个 bool 字段要 3 字节，而且读/写不原子
- 位运算可以通过一次 `&` 同时检查多个标志

### sync.Pool 复用 Connection

```go
var connPool = sync.Pool{
    New: func() interface{} { return &Connection{} },
}

func NewConn(conn net.Conn) *Connection {
    c, _ := connPool.Get().(*Connection)  // 从池中取
    c.conn = conn
    return c
}

func (c *Connection) Close() error {
    // 清理状态...
    connPool.Put(c)  // 放回池中
}
```

和 `lib/logger` 中的 `sync.Pool` 同理：高并发下频繁创建/销毁 Connection 对象，用池复用减少 GC 压力。Close 时把对象放回池而不是让 GC 回收。

### FakeConn — 测试替身

不打开真实 TCP 连接，用内存 buffer 模拟读写。数据库、集群等模块的单元测试用 FakeConn，不需要启动网络。

---

## 3.4 redis/client — Redis 客户端

### Pipeline 模式架构

```
Send(args) → pendingReqs → handleWrite → 网络发送 → waitingReqs
                                                              ↓
                        handleRead ← 网络接收 ← ParseStream ←─┘
                                                              ↓
                                                    finishRequest → 唤醒调用者
```

**三个 goroutine各司其职：**

| goroutine | 职责 |
|-----------|------|
| `handleWrite` | 从 pendingReqs 取请求 → 序列化 → 写入网络 |
| `handleRead` | 从网络读取 → ParseStream 解析 → finishRequest 匹配响应 |
| `heartbeat` | 每 10 秒发 PING 保活 |

**为什么需要 pendingReqs 和 waitingReqs 两个 channel？**

请求和响应是异步的：发出去的请求可能还没收到响应，新请求又来了。`pendingReqs` 排队待发送的请求，`waitingReqs` 排队等待响应的请求。handleWrite 从 pending 取出发，handleRead 收到响应后从 waiting 取出来匹配。

**为什么 `Send()` 是同步接口？**

```go
func (client *Client) Send(args [][]byte) redis.Reply {
    req.waiting.Add(1)
    client.pendingReqs <- req     // 投递请求
    req.waiting.WaitWithTimeout(maxWait)  // 等 3 秒
    return req.reply
}
```

虽然是内部异步（三个 goroutine 并行），但对外暴露同步接口。调用者（如集群内部通信）只需 `reply := client.Send(cmd)`，不用管异步细节。`WaitWithTimeout` 保证不会永久阻塞。

### 自动重连

```go
func (client *Client) reconnect() {
    for i := 0; i < 3; i++ {  // 最多重试 3 次
        conn, err = net.Dial("tcp", client.addr)
        // ...
    }
    // 重连后，让 waitingReqs 中的请求都返回错误
    for req := range client.waitingReqs {
        req.err = errors.New("connection closed")
        req.waiting.Done()  // 唤醒等待者
    }
    // 重建 channel，重启 handleRead
}
```

---

## 3.5 redis/server/std — 完整 Redis 服务器

### 组合：TCP + 协议 + 数据库

```go
type Handler struct {
    activeConn sync.Map
    db         idatabase.DB  // ← 依赖接口，不依赖具体实现
    closing    atomic.Boolean
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
    client := connection.NewConn(conn)          // 包装连接
    ch := parser.ParseStream(conn)              // 启动解析 goroutine
    for payload := range ch {
        r, ok := payload.Data.(*protocol.MultiBulkReply)
        result := h.db.Exec(client, r.Args)     // 交给数据库执行
        client.Write(result.ToBytes())           // 返回结果
    }
}
```

**这就是 Godis 单机版的完整请求处理链路：**

```
TCP 连接 → Connection 包装 → ParseStream 解析
    → 取出 MultiBulkReply.Args（命令行）
    → db.Exec(client, cmdLine)（存储引擎执行）
    → client.Write(reply.ToBytes())（回复客户端）
```

**为什么只处理 `MultiBulkReply`？**

Redis 客户端发送命令统一使用 Multi Bulk 格式（`*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`）。其他类型（Status/Int/Error）是服务端回复格式，客户端不会发过来。

### MakeHandler 中的策略选择

```go
func MakeHandler() *Handler {
    var db idatabase.DB
    if config.Properties.ClusterEnable {
        db = cluster.MakeCluster()           // 集群模式
    } else {
        db = database.NewStandaloneServer()  // 单机模式
    }
    return &Handler{db: db}
}
```

这就是 `interface/database.DB` 接口的实际使用——Handler 不关心底层是单机还是集群，只调 `db.Exec()`。

---

## 设计模式

| 模式 | 体现 |
|------|------|
| 单例 | OkReply/PongReply/QueuedReply 预分配共享实例 |
| 状态机 | parser.parse0 根据首字节分发解析逻辑 |
| 生产者-消费者 | ParseStream 返回 channel，parser 生产，server 消费 |
| Pipeline | client 三个 goroutine 分工：写入、读取、心跳 |
| 策略模式 | Handler.db 可以是单机 DB 或集群 Cluster |
| 对象池 | Connection 的 sync.Pool 复用 |

---

## 模块联动

```
redis/server/std (组装层)
  ├─ 依赖 interface/tcp.Handler     ← 实现 TCP Handler 接口
  ├─ 依赖 interface/database.DB     ← 存储引擎（单机/集群）
  ├─ 使用 redis/parser.ParseStream  ← 解析客户端命令
  ├─ 使用 redis/connection.NewConn  ← 包装 TCP 连接
  ├─ 使用 redis/protocol            ← 构造回复
  ├─ 使用 lib/sync/atomic           ← closing 标志
  └─ 使用 tcp.ListenAndServeWithSignal ← 启动 TCP 服务器

redis/client (集群内部通信)
  ├─ 使用 redis/parser.ParseStream  ← 解析服务端回复
  ├─ 使用 redis/protocol            ← 构造请求
  └─ 使用 lib/sync/wait             ← 超时等待

redis/parser
  └─ 使用 redis/protocol            ← 创建 Reply 对象
```

---

## Go 知识点

| 知识点 | 说明 |
|--------|------|
| `bufio.Reader.ReadBytes` | 按分隔符读一行，适合文本协议 |
| `io.ReadFull` | 精确读取 N 字节，适合二进制协议 |
| `bytes.Buffer.Grow` | 预分配缓冲区，避免多次扩容 |
| 位运算标志 | `flags |= flag` 设置, `flags & flag > 0` 检查, `flags &= ^flag` 清除 |
| `sync.Pool` | 连接对象复用 |
| channel 通信 | parser 和 server 之间、client 各 goroutine 之间 |

---

## 动手实验

1. **走一遍完整链路**: 启动 godis，用 `redis-cli -p 6399` 发送 `SET foo bar`，在 `std/server.go:104`（`h.db.Exec`）打断点，观察 `r.Args` 的内容是 `[["SET", "foo", "bar"]]`

2. **内联命令**: 用 `telnet localhost 6399` 连上去，直接输入 `PING\r\n`（不是 RESP 格式），观察是否正常返回 `+PONG\r\n`

3. 思考题：parser 的 `parse0` 中 `default` 分支处理内联命令时，如果命令包含空格的值（如 `SET key "hello world"`）会怎样？
