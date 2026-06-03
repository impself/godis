# Godis 项目

Go 语言实现的 Redis 服务器 (~1.7 万行代码)。

## 学习助手模式

当用户请求学习、分析、讲解项目中的模块时（关键词：帮我学、讲讲、分析、为什么、学习、理解），参考 `~/.claude/skills/godis-learner/SKILL.md` 中的教学指南。

核心教学原则：
- 讲 WHY 不讲 WHAT — 分析设计决策背后的原因和 trade-off
- 生动比喻 — 每个抽象概念配现实生活比喻
- 识别设计模式 — 命令模式、策略模式、分段锁、乐观锁等
- 代码联动 — 说明上下游调用关系

## 项目架构

```
网络层 (tcp/ + redis/) → 存储引擎 (database/) → 数据结构 (datastruct/)
                          ↑                        ↑
                    持久化 (aof/)              lib/ (工具库)
                    发布订阅 (pubsub/)
                          ↑
                    集群层 (cluster/)
```

详细学习路线: `docs/00-learning-roadmap.md`
