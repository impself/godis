# Phase 2: tcp/ — TCP 服务器

> **文件**: `tcp/server.go` (~98行), `tcp/echo.go` (~90行)
> **核心**: Accept 循环 + 每连接一个 goroutine + 信号监听 + 优雅关闭

---

## 一句话总结 + 比喻

tcp/ 是 Godis 的**前台接待处**：永远开门迎客（Accept 循环），每个来客分配一个专属接待员（goroutine），接到停业通知后等所有接待员送走客人再关门（优雅关闭）。

---

## 核心源码剖析

### 1. 信号监听：ListenAndServeWithSignal

```go
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
    closeChan := make(chan struct{})
    sigCh := make(chan os.Signal)
    // 监听系统信号：Ctrl+C (SIGINT)、kill (SIGTERM) 等
    signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
    go func() {
        sig := <-sigCh
        switch sig {
        case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
            closeChan <- struct{}{} // 通知主流程该关闭了
        }
    }()
    listener, err := net.Listen("tcp", cfg.Address)
    // ...
    ListenAndServe(listener, handler, closeChan)
    return nil
}
```

**为什么用两个 channel（sigCh + closeChan）而不是直接在 goroutine 里关 listener？**

因为关闭逻辑集中在 `ListenAndServe` 中。信号 goroutine 只负责"传递消息"，具体怎么关由 `ListenAndServe` 决定。这样关闭逻辑不分散，好维护。

**为什么 closeChan 是 `chan struct{}`？**

`struct{}` 零内存占用。这个 channel 只传递"到了"这个信号，不需要携带任何数据。用 `chan bool` 也行，但 `struct{}` 是 Go 的惯用写法，表达"只关心时序不关心内容"。

### 2. Accept 循环：ListenAndServe

```go
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
    errCh := make(chan error, 1)
    // 关闭监控 goroutine：收到信号或 Accept 出错时触发关闭
    go func() {
        select {
        case <-closeChan:        // 外部信号（Ctrl+C）
            logger.Info("get exit signal")
        case er := <-errCh:      // Accept 异常
            logger.Info(fmt.Sprintf("accept error: %s", er.Error()))
        }
        logger.Info("shutting down...")
        _ = listener.Close()     // 关监听器 → Accept 立即返回 error
        _ = handler.Close()      // 关所有活跃连接
    }()

    ctx := context.Background()
    var waitDone sync.WaitGroup
    for {
        conn, err := listener.Accept()  // 阻塞等待新连接
        if err != nil {
            if ne, ok := err.(net.Error); ok && ne.Timeout() {
                time.Sleep(5 * time.Millisecond) // 临时错误，重试
                continue
            }
            errCh <- err  // 严重错误，触发关闭
            break
        }
        ClientCounter++
        waitDone.Add(1)
        go func() {                     // 每个连接一个 goroutine
            defer func() {
                waitDone.Done()
                atomic.AddInt32(&ClientCounter, -1)
            }()
            handler.Handle(ctx, conn)   // ← 交给 Handler 处理
        }()
    }
    waitDone.Wait()  // 等所有连接处理完才退出
}
```

**为什么 Accept 出错要区分临时错误和严重错误？**

参考了 `net/http/server.go` 的做法。临时错误（如资源暂时不可用）睡 5ms 重试就行；严重错误（如端口被占用）才需要退出。不区分的话，一次临时波动就会把整个服务器搞挂。

**为什么用 `sync.WaitGroup` 而不是之前学的 `wait.WaitWithTimeout`？**

因为这里需要**等所有连接处理完**，不需要超时。信号已经触发了关闭，handler 会尽快处理完。如果要加超时保护，可以在 `handler.Close()` 中实现（echo handler 的 `Close` 就用了 `WaitWithTimeout`）。

### 3. EchoHandler — Handler 接口的测试实现

```go
type EchoHandler struct {
    activeConn sync.Map      // 所有活跃连接
    closing    atomic.Boolean // 是否正在关闭
}

func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
    if h.closing.Get() {
        _ = conn.Close()  // 关闭中，拒绝新连接
        return
    }
    client := &EchoClient{Conn: conn}
    h.activeConn.Store(client, struct{}{})

    reader := bufio.NewReader(conn)
    for {
        msg, err := reader.ReadString('\n') // 读一行
        if err != nil {
            if err == io.EOF {
                h.activeConn.Delete(client) // 客户端主动断开
            }
            return
        }
        client.Waiting.Add(1)
        _, _ = conn.Write([]byte(msg)) // 原样回写
        client.Waiting.Done()
    }
}

func (h *EchoHandler) Close() error {
    h.closing.Set(true)  // 原子标志，拒绝新连接
    h.activeConn.Range(func(key, val interface{}) bool {
        client := key.(*EchoClient)
        _ = client.Close() // 等待发送完成（最多 10 秒）→ 关闭连接
        return true
    })
    return nil
}
```

**为什么 `closing` 用 `atomic.Boolean` 而不是 `sync.Mutex` 保护？**

这是一个只需要读写的标志位，不需要和其它操作组成原子事务。`atomic.Boolean` 是无锁的，比 mutex 快一个数量级。

**为什么 `activeConn` 用 `sync.Map` 而不是普通 map + mutex？**

`activeConn` 的访问模式是：高并发读（Handle 中 Store/Delete）+ 低频遍历（Close 中 Range）。`sync.Map` 针对这种"读多写少、key 稳定"的场景优化，比 map + mutex 性能更好。

**`EchoClient.Close()` 中的 `WaitWithTimeout`**

```go
func (c *EchoClient) Close() error {
    c.Waiting.WaitWithTimeout(10 * time.Second) // 等待当前发送完成
    c.Conn.Close()
    return nil
}
```

这就是之前学的 `lib/sync/wait` 的实际使用场景：关连接前等数据发完，最多等 10 秒。

---

## 优雅关闭完整链路

```
Ctrl+C / kill
    ↓
signal.Notify → sigCh → closeChan
    ↓
listener.Close()          ← 关监听器，Accept 返回 error，不再接受新连接
    ↓
handler.Close()           ← 设 closing=true + 遍历关闭所有活跃连接
    ↓
waitDone.Wait()           ← 等所有 goroutine 退出
    ↓
程序退出
```

---

## 设计模式

| 模式 | 体现 |
|------|------|
| 策略模式 | Handler 接口可替换（EchoHandler 用于测试，Redis Handler 用于生产） |
| 每连接一个 goroutine | Go 网络编程的经典并发模型 |
| 双 channel 解耦 | sigCh（系统信号）→ closeChan（业务通知），关闭逻辑集中管理 |

---

## 模块联动

```
main.go
  └─ stdserver.Serve(listenAddr, handler)
       └─ tcp.ListenAndServeWithSignal(cfg, handler)
            ├─ 依赖 interface/tcp.Handler
            ├─ 使用 lib/logger（日志）
            └─ 使用 lib/sync/atomic（closing 标志）

tcp/echo.go
  ├─ 实现 interface/tcp.Handler（测试用）
  ├─ 使用 lib/sync/atomic.Boolean
  ├─ 使用 lib/sync/wait.Wait（优雅关闭）
  └─ 使用 sync.Map（连接管理）

tcp/server.go 被 redis/server/std/server.go 间接调用
    ↓ 真正的 Handler 实现在 redis/server/std 中
```

---

## Go 知识点

| 知识点 | 说明 |
|--------|------|
| `signal.Notify` | 捕获 OS 信号，转发到 channel |
| `net.Listener.Accept` | 阻塞等待 TCP 连接 |
| `net.Error.Timeout()` | 区分临时错误和致命错误 |
| `sync.Map` | 并发安全的 map，适合读多写少场景 |
| `sync.WaitGroup` | 等待一组 goroutine 完成 |
| `atomic.AddInt32` | 原子计数器（ClientCounter） |

---

## 动手实验

1. 用 EchoHandler 测试优雅关闭：
   ```go
   // 在 main.go 中临时改为：
   handler := tcp.MakeEchoHandler()
   tcp.ListenAndServeWithSignal(&tcp.Config{Address: ":6399"}, handler)
   ```
   然后 `telnet localhost 6399` 连上去，Ctrl+C 关服务器，观察连接是否被优雅关闭

2. 思考题：如果 `errCh` 不是带缓冲的（`make(chan error, 1)`），会发生什么？提示：关闭 goroutine 和 Accept 循环的时序竞争
