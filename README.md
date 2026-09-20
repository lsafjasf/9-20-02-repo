# Raft 共识过程可视化器

纯原生 HTML/CSS/JS 实现，**双击 `index.html` 即可运行**，无需服务器、无任何依赖。

## 运行

直接双击 `index.html`（推荐 Chrome / Edge / Firefox）。

## 功能

- 集群规模：3 / 5 / 7 节点在线切换；仿真速度 0.5×–4×、暂停/继续、重置
- 节点操作（点卡片选中后）：手动宕机、恢复、单独隔离、取消隔离、设置双向链路延迟、手动快照压缩
- 网络分区：隔离单节点（1 vs 多数派）、对半分区（1v2 / 2v3 / 3v4）、一键恢复
- 客户端：向 Leader 提交 `SET` / `DELETE` 命令；无 Leader 时被拒绝并提示重定向
- 完整 Raft 实现：
  - Leader Election：随机选举超时、term、votedFor、多数票、日志新旧判断（lastLogTerm/lastLogIndex）
  - Heartbeat：固定间隔空 AppendEntries
  - Log Replication：nextIndex/matchIndex、冲突 term 快速回退提示、**冲突后缀截断与 Leader 强制覆盖**
  - Commit：只按多数派计数提交当前 term 的日志（Figure 8 安全规则），commitIndex/lastApplied 推进
  - CheckQuorum：Leader 在宽限期（2000ms）内联系不到多数派立即降级，**保证分区时全集群不出现双 Leader**
  - Snapshot / log compaction：InstallSnapshot RPC 全链路，落后节点先收快照再收增量，恢复后最终一致
- 可视化：每节点显示角色、term、votedFor、commitIndex、lastApplied、日志数组（含快照点、已提交高亮、term 标记）、状态机内容；右侧实时 RPC 消息面板（发出/成功/拒绝/丢失及原因）与集群事件流

## 目录

- `index.html` / `style.css`：页面与样式
- `js/raft-config.js`：常量与工具
- `js/network.js`：网络模型（分区、宕机、延迟、消息队列）
- `js/raft.js`：Raft 节点状态机
- `js/sim.js`：仿真时钟与总线
- `js/ui.js`：渲染与交互
- `test/smoke.js`、`test/chaos.js`：Node 正确性测试（`node test/smoke.js`）

## 关键时序参数

心跳 650ms · 选举超时 2300–3700ms（随机）· 多数派失联宽限 2000ms · 自动快照阈值 5 条
