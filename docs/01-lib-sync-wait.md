# Phase 1.1: lib/sync/wait + lib/sync/atomic

> **日期**: 2026-06-02
> **文件**: `lib/sync/wait/wait.go` (44行), `lib/sync/atomic/bool.go` (21行)

---

## Wait — 带超时的 WaitGroup

### 源码

```go
type Wait struct {
    wg sync.WaitGroup   // 组合而非继承
}

func (w *Wait) Add(delta int)          { w.wg.Add(delta) }
func (w *Wait) Done()                  { w.wg.Done() }
func (w *Wait) Wait()                  { w.wg.Wait() }

func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
    c := make(chan struct{}, 1)   // ⚠️ 带缓冲——防止 goroutine 泄漏
    go func() {
        defer close(c)
        w.Wait()
        c <- struct{}{}
    }()
    select {
    case <-c:
        return false              // 正常完成
    case <-time.After(timeout):
        return true               // 超时
    }
}
```

### 设计要点

1. **Buffered channel (`make(chan struct{}, 1)`)**: 如果无缓冲，select 走 timeout 分支后 goroutine 中的 `c <- struct{}{}` 会永久阻塞，导致 goroutine 泄漏。1 容量 buffer 让发送总能完成。

2. **三层包装**: `Add/Done/Wait` 只是简单代理给 `sync.WaitGroup`，`WaitWithTimeout` 是唯一新增的能力。

### 在项目中的使用

`WaitWithTimeout` 用于**优雅关闭**：

`redis/connection/conn.go` 中：
- `Write()` 前 `sendingData.Add(1)`，defer `sendingData.Done()` — 标记正在发送
- `Close()` 中 `sendingData.WaitWithTimeout(10 * time.Second)` — 等最多 10 秒让数据写完，超时也强制关闭

因此 **WaitWithTimeout 的 "完成" 不是 "成功"**——超时后返回 true 照样走后续 Close 逻辑。

### Go 知识点

| 知识点 | 说明 |
|--------|------|
| `sync.WaitGroup` | 内部计数器，Add(+1) 和 Done(-1) 配对，Wait() 阻塞直到归零 |
| buffered channel | 容量为 1 保证发送方无接收方时也能写入 |
| `time.After` | 返回一个 channel，超时后收到一个值。适合 select 用 |
| goroutine 泄漏 | 无缓冲 channel + 接收方退出 = 发送方永久阻塞 |

---

## Atomic Boolean

### 源码

```go
type Boolean uint32

func (b *Boolean) Get() bool {
    return atomic.LoadUint32((*uint32)(b)) != 0
}

func (b *Boolean) Set(v bool) {
    if v {
        atomic.StoreUint32((*uint32)(b), 1)
    } else {
        atomic.StoreUint32((*uint32)(b), 0)
    }
}
```

### 设计要点

1. **底层类型相同**: `type Boolean uint32` → 内存布局完全一致，`(*uint32)(b)` 强制转换安全
2. **原子操作 vs 锁**: `atomic.Load/Store` 是 CPU 单条指令，比 `sync.Mutex` 快一个数量级
3. **填补标准库**: Go 1.19 之前没有 `atomic.Bool`，这个文件自己实现了

### Go 知识点

| 知识点 | 说明 |
|--------|------|
| `sync/atomic` | 硬件级原子内存操作，无锁并发基础 |
| `type T U` | 新类型，底层相同可强制转换，但不能直接赋值给底层类型 |
| `atomic.Load/Store` | Load 读不到被写撕裂的值，Store 保证写入完整 |

---

## 模块总结

两个文件共 65 行，构成 Godis 并发控制最底层：

- `WaitWithTimeout` 的 buffered channel 技巧 → 防止 goroutine 泄漏的优雅关闭
- `Boolean` 的原子操作 → 需要并发读写标志位时的零锁方案
