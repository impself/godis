# Phase 1.2: lib/logger — 异步日志库

> **文件**: `lib/logger/logger.go` (~198行), `lib/logger/files.go` (~45行)
> **核心**: goroutine 消费者模式 + channel 解耦 + sync.Pool 复用

---

## 一句话总结 + 比喻

Godis 的"快递分拣中心"：所有模块把日志当作包裹扔进缓冲队列（channel），自己不用等快递送完就能继续干活；背后有一个专职快递员（goroutine）从队列里取包裹、格式化、投递到目的地（stdout/文件）。

---

## 核心源码剖析

### 1. 数据结构

```go
type Logger struct {
    logFile   *os.File       // 日志文件句柄（stdout logger 为 nil）
    logger    *log.Logger    // 标准库的格式化输出器
    entryChan chan *logEntry  // 异步核心：带缓冲的 channel 队列，容量 1e5
    entryPool *sync.Pool     // 对象池：复用 logEntry 减少 GC 压力
}

type logEntry struct {
    msg   string
    level LogLevel
}
```

**为什么用 `chan *logEntry` 而不是 `chan string`？**

保留 level 信息，未来可根据级别决定是否刷盘或触发告警。

**为什么 `entryChan` 容量是 `1e5`（10 万）？**

背压设计：磁盘卡住几秒不会阻塞业务，但超过 10 万条会阻塞写入方——自我保护：磁盘都卡成这样了，继续生产日志也没意义。

### 2. 异步核心：生产者-消费者模式

```go
// 生产者：谁调用 logger.Info() 谁就是生产者
func (logger *Logger) Output(level LogLevel, callerDepth int, msg string) {
    // 1. runtime.Caller 获取调用者文件名和行号
    _, file, line, ok := runtime.Caller(callerDepth)
    formattedMsg := fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], filepath.Base(file), line, msg)

    // 2. 从对象池取一个 logEntry（减少 GC）
    entry := logger.entryPool.Get().(*logEntry)
    entry.msg = formattedMsg
    entry.level = level

    // 3. 投递到 channel，不等待消费
    logger.entryChan <- entry
}

// 消费者：后台 goroutine，在构造函数中启动
go func() {
    for e := range logger.entryChan {
        _ = logger.logger.Output(0, e.msg)
        logger.entryPool.Put(e)  // 用完放回对象池
    }
}()
```

**为什么用 channel 而不是直接写？**

磁盘 I/O 通常要几毫秒，高并发时每秒上万条日志。每条都等 I/O = 业务线程被打满，吞吐量暴跌。Channel 把"生产日志"和"写磁盘"解耦。

### 3. sync.Pool 对象复用

```go
entryPool: &sync.Pool{
    New: func() interface{} { return &logEntry{} },
},
```

高并发下每秒上万条日志 → 上万个 `logEntry{}` → GC 压力暴增。Pool 的行为：
- `Pool.Get()` 从池中取对象
- `Pool.Put()` 用完放回
- GC 时自动清理空闲对象，不会内存泄漏
- goroutine-safe，无需加锁

### 4. 日志文件轮转（FileLogger 独有）

```go
// 消费者 goroutine 中，每条日志检查文件名
logFilename := fmt.Sprintf("%s-%s.%s",
    settings.Name,
    time.Now().Format(settings.TimeFormat),  // 如 "2006-01-02"
    settings.Ext)

if path.Join(settings.Path, logFilename) != logger.logFile.Name() {
    // 日期变了 → 创建新文件
    logFile, err := mustOpen(logFilename, settings.Path)
    logger.logFile = logFile
    logger.logger = log.New(io.MultiWriter(os.Stdout, logFile), "", flags)
}
```

**策略：按日期切割。** 每天零点后第一条日志触发新文件创建。比按大小切割简单得多。

### 5. files.go — 文件操作工具

```go
func mustOpen(fileName, dir string) (*os.File, error) {
    // 权限检查 → 建目录 → 追加模式打开文件
    f, err := os.OpenFile(dir+string(os.PathSeparator)+fileName,
        os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
    return f, nil
}
```

`os.O_APPEND` 追加模式保证并发写入不会互相覆盖。

### 6. 全局 DefaultLogger + 包级函数

```go
var DefaultLogger ILogger = NewStdoutLogger()  // 默认输出到 stdout

func Info(v ...interface{}) {
    msg := fmt.Sprintln(v...)
    DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}
```

**门面模式**：项目其他模块只需 `logger.Info("...")`，不关心底层是 stdout 还是文件。`Setup()` 在 `main.go` 中被调用，替换成 FileLogger。

---

## 设计模式

| 模式 | 体现 |
|------|------|
| 生产者-消费者 | channel 解耦日志写入和日志消费 |
| 对象池 | `sync.Pool` 复用 `logEntry`，减少 GC |
| 门面模式 | 包级函数 `Info/Error/Debug` 隐藏底层实现 |
| 策略模式 | `ILogger` 接口，Stdout/File 两种实现可替换 |
| Fail-fast | 文件操作失败直接 panic，不静默吞错 |

---

## 模块联动

```
main.go
  ├─ logger.Setup()  ──→  创建 FileLogger，替换 DefaultLogger
  └─ 之后所有模块通过 logger.Info/Error/... 写日志

tcp/server.go       ──→  logger.Info("bind: ...")
aof/rewrite.go      ──→  logger.Warn("fsync failed")
cluster/core/*.go   ──→  logger.Error(err)
redis/parser/*.go   ──→  logger.Error(err)
```

Logger 是全局基础设施——几乎所有模块都依赖它，但它不依赖任何业务模块。

---

## 值得注意的设计取舍

1. **channel 满时阻塞 vs 丢弃？** 当前是阻塞。生产级日志库（zap、zerolog）通常提供"满时丢弃"选项
2. **没有优雅关闭**：consumer goroutine 没有退出机制，`for range entryChan` 只有在 channel close 时才退出，但没有地方 close。优雅关闭时可能丢失最后几条日志
3. **日志轮转无锁**：`logFile` 和 `logger` 在消费者 goroutine 中被替换，没有锁。目前只有一个消费者所以安全，但扩展为多消费者会出竞态

---

## Go 知识点

| 知识点 | 说明 |
|--------|------|
| `sync.Pool` | 对象复用池，GC 时自动清理，goroutine-safe |
| `chan *logEntry` | 带缓冲 channel 实现生产者-消费者解耦 |
| `runtime.Caller(depth)` | 获取调用栈信息（文件名、行号），depth=2 跳过 Output 和包级函数 |
| `io.MultiWriter` | 同时写入多个 io.Writer（stdout + file） |
| `log.Logger.Output` | 标准库日志输出，支持 calldepth 跳过包装函数 |

---

## 动手实验

1. 在 `main.go` 的 `Setup` 之后加一行：
   ```go
   logger.Info("logger initialized, writing to file")
   time.Sleep(100 * time.Millisecond) // 等消费者处理完
   ```
   观察日志文件是否在指定目录生成

2. 把 `bufferSize` 改成 `2`，然后用 `go test -race` 跑测试，看看会发生什么

3. 思考题：如果把 `entryChan` 改成无缓冲 channel，会出什么问题？
