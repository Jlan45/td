# 连接层重构方案：以 Session 为中心

本方案的目标是修复 [CONNECTION_PARITY.md](CONNECTION_PARITY.md) 中列出的全部差异（H1–H4、M1–M7、L1–L5、F1–F2），
让 gotd 的连接生命周期与 TDLib、Telegram Desktop、Telegram Android 对齐。
文中的编号都对应差异清单。

## 1. 问题的根源

gotd 当前的设计是"一条 TCP 连接对应一个 `mtproto.Conn`"（ARCHITECTURE.md："It is deliberately **single-connection**"）：

- 连接相关的所有状态都挂在 `mtproto.Conn` 上，包括 session、seqno、去重集合、ack 队列和请求登记表（`rpc.Engine`）。
- 连接一断，整个 `Conn` 就作废，再由 `telegram.Client` 新建一个。

官方三家的设计不同：它们都把**逻辑会话**和**传输连接**分开。

| | 长期存在的会话对象 | 可替换的传输连接 |
| --- | --- | --- |
| TDLib | `Session`（持有 `sent_queries_`、`AuthData`） | `SessionConnection` |
| tdesktop | `SessionData` + `SessionPrivate`（持有 `haveSent`、`_sessionId`） | `AbstractConnection` |
| Android | `ConnectionSession` + `ConnectionsManager.runningRequests` | `Connection` / `ConnectionSocket` |

fork 提交 `9ff37d944` 已经把 session_id、seqno 和去重集合移进了 `SessionState`。
剩下的差异大多来自同一个原因：**请求、ack、时间差、salt 仍然只属于单条连接**。
H4、M3、M4、M5 都是这个原因导致的；H1、H2、M1、M2 则是接收路径上的单点缺陷。

所以重构的主线是：**把 `SessionState` 扩展成真正的 `mtproto.Session`，让它拥有所有跨连接的状态；`mtproto.Conn` 只负责驱动一条传输连接。**

## 2. 目标架构

```
telegram.Client
 ├─ primarySession *mtproto.Session        ← 每个逻辑通道一个，生命周期等于 Client（换 DC 时新建）
 ├─ manager.Conn ──► mtproto.Conn ──attach──► Session
 │                   （每条 TCP 一个，可以随时替换）
 └─ pool.DC 的每个槽位 ──► 自己的 *mtproto.Session（P5）

mtproto.Session（长期存在，同一时刻最多挂一个 Conn）
 ├─ identity     auth key ID、session_id、seqno、msgIDGen（带服务器时间差）
 ├─ inbound      ReceivedIDs（窗口 400，区分重复 / 太旧）
 ├─ outbox       请求登记表（状态机）+ ack 队列 + resend 队列 + state 查询队列
 ├─ salts        当前 salt + future salts
 └─ clock        serverTimeOffset（可持久化）

mtproto.Conn（每条连接一个）
 ├─ dial、密钥交换、PFS bind
 ├─ readLoop  → session.inbound → handler / outbox
 ├─ sendLoop  ← session.outbox（打包成容器，P7）
 ├─ 存活检测（progress-aware）
 └─ 退出时 session.detach(cause)
```

### 2.1 不变量

| 编号 | 内容 |
| --- | --- |
| I1 | 一个 `Session` 同一时刻最多挂一个 `Conn`。`attach` 时如果已被占用就返回错误，从而消除"不得并发共享"这条只靠注释约束的规则 |
| I2 | 只在三种情况下生成新的 session_id，其他情况一律沿用：auth key 变化（包括 PFS 临时 key 换新）、bad_msg 32/33、收到太旧的消息 |
| I3 | 请求的生命周期由 `Session.outbox` 管理，与连接无关。`Invoke` 只在三种情况下失败：拿到 RPC 结果或错误、调用方取消、Session 被永久关闭 |
| I4 | 任何没确认处理状态的请求，都不能盲目换新 msg_id 重发；必须先用 `msgs_state_req` 问清楚（M3）。例外是能确定服务器没处理过的情况：新 session 里 `first_msg_id` 之前的请求、key 变化、32/33 重置 |
| I5 | 每条需要确认的入站消息（`seqno&1`，包括容器内层消息）都要 ack，并且先 ack 再去重 |

### 2.2 请求状态机（outbox）

```
            Invoke
              │
              ▼
 ┌──────── Queued ◄────────────────────────────────┐
 │            │ 发送（分配 msg_id，记录容器 ID）     │ 换新 msg_id
 │            ▼                                     │（重置 / key 变化 /
 │          Sent ──────── 收到 msgs_ack ────► Acked │  16/17/64/48 /
 │            │                                │    │  first_msg_id 之前 /
 │            │ 连接断开                        │    │  state=未收到）
 │            ▼                                │    │
 │         Unknown ── msgs_state_req ──► 根据状态 ──┘
 │            │                           已收到 → Acked
 │            ▼                                │
 └─ 取消 → Dropped（rpc_drop_answer）     rpc_result / rpc_error
                                               ▼
                                             Done
```

- **Sent → Unknown**：连接断开时，没 ack 的请求进入 Unknown（TDLib `mark_as_unknown`）。重新挂上连接后，批量发 `msgs_state_req` 查询。
- **Acked**：断线后保持不变，继续等结果。服务器会把结果发到恢复后的同一个 session 上（修复 H4）。
- **连接内超时**：Sent 超过 10s 没响应，同样发 `msgs_state_req` 查询（tdesktop `checkSentRequests`），取代现在"同一 msg_id 重发 5 次就报错"的做法（M3）。
- **`msgs_state_info` 的处理**：
  - `status & 7` 为 1、2、3（服务器没收到）：换新 msg_id 重发。
  - 为 4（已收到）：转为 Acked。
  - 带 `+64` 标志（已生成回复）但本地没收到：发 `msg_resend_req`。
- **`msg_detailed_info` / `msg_new_detailed_info`**：`answer_msg_id` 已收到过就 ack，没收到就发 `msg_resend_req`（M2）。
- **`new_session_created(first_msg_id)`**：`first_msg_id` 之前发出的请求全部换新 msg_id 重发（M4）。

## 3. 分阶段实施

每个阶段是一个独立的 PR，都要能单独合并、带测试、不破坏现有行为。
P0 是前提。P1–P3 是独立的小 PR，可以先合；P4 是核心结构改动；P5–P8 在 P4 之后。

```
P0 ─┬─ P1 ─┐
    ├─ P2  ├─► P4（4a → 4b → 4c → 4d → 4e）─┬─ P5
    └─ P3 ─┘                               ├─ P6（可提前）
                                           ├─ P7
                                           └─ P8（独立）
```

### P0. 测试基础设施（前提）

**目标：** 先让 `tgtest` 能复现每一个差异场景，后续每个修复都先有一个失败的测试。

**改动：**

- `tgtest`：
  - 可选把多条消息装进 `msg_container` 发出，内层带 `seqno&1`。
  - 记录客户端发来的 `msgs_ack`。
  - 对没被 ack 的消息按间隔重发。
- 响应 `msgs_state_req`、`msg_resend_req`；可以主动发 `msg_detailed_info`。
- 故障注入钩子：
  - 指定 msg_id 返回 bad_msg 16/17/32/33/48/64。
  - 返回 -404。
  - 服务器时钟偏移 N 秒。
  - 在 ack 之后、结果之前断开连接（H4 场景）。
  - 发送比窗口更旧的 msg_id（H2 场景）。
- `tgtest` 已经让 session 跟随最新的传输连接（`9ff37d944`），继续沿用。

**验收：** 为 H1–H4、M1–M5 各写一个用 `t.Skip("P<n>")` 标记的复现测试，后续每个阶段解除对应的 Skip。

### P1. 接收路径修正（H1、H2、L1、F2）

**改动：**

- `proto.MessageIDBuf.Consume` 改成返回 `Result{OK, Duplicate, TooOld}`；新增 `ConsumeResult`，旧方法保留做兼容。
- `mtproto/session_state.go`：窗口从 100 调到 400（tdesktop `kIdsBufferSize`）。
- `mtproto/handle_container.go`：每条内层消息都按 I5 处理：先根据 `seqno&1` ack，再去重，最后交给 `handleMessage`。
- `mtproto/read.go`：
  - Duplicate：ack 后忽略（保持现状）。
  - TooOld：**不 ack**，调用 `requestSessionReset`。
- F2：`requestSessionReset` 之后，`nextMsgSeq` 和写入路径直接返回 `ErrSessionReset`，直到连接退出。

**测试：**

- 容器内层消息会被 ack，而且只处理一次。
- 收到太旧的消息时触发 `ErrSessionReset`，并且没有发出 ack。
- 重置窗口期内不会再写出消息。

**风险：** 低。行为只会变得更严格，影响局限在接收路径。

### P2. 密钥异常处理（H3、L4、F1）

**改动：**

- 非 PFS 模式收到 -404：不再原地调用 `createAuthKey`。改为参照 TDLib：先临时用 PFS 走一次 temp key + bind 来验证永久 key。
  - bind 返回 `AUTH_KEY_PERM_EMPTY` 或 -404：返回新的 `mtproto.ErrAuthKeyInvalid`，由 `telegram.Client` 当作永久错误处理（加入 `isPermanentError`），交给调用方，并且**不覆盖**存储里的 key。
  - bind 成功：说明只是 session 层的问题，重连即可。
- `AUTH_KEY_DUPLICATED`：保持永久错误，但新增 `Options.OnAuthKeyInvalid(err)` 回调，让上层决定是否清理存储。默认行为不变。
- F1：`telegram/pool.go` 中主 DC 的数据池改从 `c.session` 取 key（和 `Pool()` 一致）；取到空 key 时返回错误，不再静默生成新 key。

**测试：** 在 `tgtest` 里注入 -404，断言存储里的 key 没被覆盖，Run 返回 `ErrAuthKeyInvalid`。

**风险：** 中。会改变错误语义，需要在 CHANGELOG 里写明。

### P3. 服务器时间差（M1）

**改动：**

- `proto.MessageIDGen` 支持时间偏移：新增 `NewMessageIDGenWithOffset(now, offset func() time.Duration)`。
- `Session.clock`：
  - 更新时机参照 TDLib：每个解密成功的入站 msg_id 都更新一次（`update_server_time_difference`）；在 `new_session_created`、pong、bad_msg 16/17 时强制更新。
  - 计算时扣掉半个 RTT（Android 的做法）。
- `checkMessageID` 改为基于校正后的时间判断。
- bad_msg 16/17：校正时间后，outbox 给请求换新 msg_id 重发，不再报错。（P4 之前先在 `mtproto/rpc.go` 里像处理 48 那样重试一次，P4 之后交给状态机。）
- 持久化：`session.Data` 新增 `ServerTimeOffset int64`（`json:",omitempty"`，向后兼容），`telegram.Client` 启动时恢复。

**测试：** 让 tgtest 时钟偏移 ±10 分钟，客户端仍能正常完成请求；在 16/17 注入下请求能成功。

**风险：** 低到中。msg_id 生成属于热路径，要用 `testutil.ZeroAlloc` 确认没有新增分配。

### P4. Session 化请求管理（H4、M2、M3、M4、M5）——核心

这一阶段拆成 5 个子 PR，按顺序合并。

#### 4a. 引入 `mtproto.Session` 与 outbox（纯重构，行为不变）

- 把 `SessionState` 改名为 `Session` 并扩展。`SessionState` 还没发布，可以直接改名；为了兼容 fork 内的用户，保留一个类型别名，过一个版本后删除。
- 新增 `mtproto/outbox.go`，承接 `rpc.Engine` 中请求登记的部分：
  - `rpc.Engine` 不再自己持有 `rpc` 表，改为依赖 `Outbox` 接口。
  - 不传 `Options.Session` 时，自动创建一个只属于本连接的 Session，行为和现在完全一致。
- `rpc` 包的公开 API 尽量不变，因为 `pool`、`telegram/invoke.go` 依赖 `rpc.ErrEngineClosed`。

**验收：** 全部现有测试通过，没有任何行为变化。

#### 4b. attach / detach 语义

- 新增 `Session.attach(conn) error`（保证 I1）和 `Session.detach(cause)`。
- detach 时：
  - Sent 的请求转为 Unknown。
  - Acked 的请求保持不变，继续等结果，Invoke 不再报错（修复 H4）。
  - Queued 的请求保持不变。
- attach 时：
  - 先补发 Queued 的请求。
  - 再对 Unknown 的请求批量发 `msgs_state_req`。
- `Invoke` 只在 I3 规定的三种情况下返回。只有 Session 被永久关闭时才返回 `rpc.ErrEngineClosed`。
- `telegram.Client.invokeConn` 的"等新连接再重试"保留，作为换 DC 等需要新建 Session 的场景的兜底。主连接重连时它基本不会再被触发。

#### 4c. 状态查询与重发服务消息

- 发送 `msgs_state_req` 和 `msg_resend_req`，处理 `msgs_state_info`、`msgs_all_info`。
- 处理 `msg_detailed_info` / `msg_new_detailed_info`（M2）。
- 连接内超时检查（tdesktop `checkSentRequests`：每 10s 一次，查询已发出超过 10s 的请求），取代现在"同一 msg_id 重发 5 次"的逻辑。`RetryLimitReachedErr` 保留，但改为在多次状态查询都无结果时才返回。

#### 4d. bad_msg 全部交给状态机（M5）

| 码 | 处理 |
| --- | --- |
| 16/17 | 校正时间（P3），换新 msg_id 重发 |
| 32/33 | 重置 session（I2）：所有没 Done 的请求换新 msg_id 重发，和 tdesktop `resetSession` 加 `haveSent` 的效果一致 |
| 48 | 更新 salt 后重发（把 `mtproto/rpc.go` 里现有的特殊处理移进状态机） |
| 64 | 拆开容器重发 |
| 其他 | 请求报错，并关闭连接（与 TDLib 一致） |

#### 4e. new_session_created（M4）

- `first_msg_id` 之前的请求换新 msg_id 重发。
- `Handler` 接口新增 `OnSessionCreated(first int64)`，用独立的可选接口做，避免破坏现有实现方。`manager.Conn` 是主连接时，向 `telegram.Client` 注入 `tg.UpdatesTooLong`（`telegram/handle_updates.go:41` 已能处理），触发 getDifference。

**P4 整体测试：**

- 在 ack 之后、结果之前断开连接：Invoke 最终拿到结果，没有重复执行（服务器侧记录执行次数）。
- 在 ack 之前断开连接：服务器回答"已收到"时不重发；回答"未收到"时重发，且只执行一次。
- 分别注入 32/33/48/64/16/17：请求最终成功。
- 收到 `new_session_created` 时：之前的请求被重发，updates 管理器收到 `UpdatesTooLong`。
- `-race` 并发压力测试：并发 Invoke 的同时反复断开连接。

**风险：** 高，这是结构性改动。缓解办法：

- 4a 只做纯重构，用现有测试兜底。
- 4b–4e 期间保留 `Options.LegacyRequestLifecycle bool`，开启后退回旧的"跟随连接"语义，可以随时回退。观察一个版本后删除。
- 文档：同步更新 ARCHITECTURE.md 的 "Connection: mtproto"、"RPC engine: rpc" 和 "Request lifecycle" 三节（CLAUDE.md 要求 ARCHITECTURE.md 与代码保持同步）。

### P5. 数据连接也持有 Session

官方三家的 download、upload 等会话也都长期存在。

**改动：**

- `pool.DC` 每个槽位持有一个 `*mtproto.Session`，槽位上的连接重建时复用它。
- `telegram.Client.Pool`、`dc()` 通过 `createConnWith` 传入槽位的 Session。
- PFS 数据连接每次换临时 key，按 I2 自然开新 session。

**风险：** 中。需要处理 `pool.DC` 回收空闲连接时 Session 的去留：回收连接时保留 Session，关闭池时才关闭 Session。

### P6. 存活检测与网络感知（M6、M7、L5）

可以在 P1 之后提前做。

**改动：**

- `PingProgressAware` 默认改为 `true`，新增 `PingStrictPong bool` 用于回到旧行为。CHANGELOG 里写明。
- 可选：参照 tdesktop 增加接收超时，发出请求后按 `max(4s, 2×实测 RTT)` 起算，失败一次翻倍，上限 64s，大请求按 8KB/s 延长。由 `Options.AdaptiveReceiveTimeout` 开启。
- 新增 `Client.NetworkChanged(available bool)`：网络可用时立即让主连接 restart，并重置 `connBackoff`；不可用时暂停重连。参照 tdesktop `availableChanges → restart()` 和 TDLib `on_network → backoff.clear()`。
- L5：数据池连接改发普通 `ping`，不用 `ping_delay_disconnect`，或者干脆不发；只有主连接发 `ping_delay_disconnect`（参照 tdesktop）。

**风险：** 低到中。主要风险是默认值变化。

### P7. 出站打包（L2）

**改动：**

- 新增 `sendLoop`：从 outbox 依次取出 ack、ping、state 查询、resend 请求和普通请求，打进一个 `msg_container`。单个容器不超过 16KB（tdesktop `kCutContainerOnSize`），只有一条消息时不打包。
- 打包时 ack 跟着请求一起发；空闲时 ack 的等待时间从 15s 缩短到 10s（tdesktop `kAckSendWaiting`）。
- 可选：quick ack（intermediate/abridged 传输的高位标志），由 `Options.QuickAck` 开启。
- **热路径：** `sendLoop` 和容器编码都要用 `testutil.MaxAlloc` 限制分配，并用 `bin.Pool` 复用缓冲区。

**风险：** 中。会改变出站报文形态，需要在 tgtest 和 `-race` 下充分测试。

### P8. 选址竞速（L3）

独立 PR，不依赖 P4。

**改动：**

- `telegram/dcs`：对同一个 DC 的多个候选地址（IPv4/IPv6、多个端口）同时拨号，先连上的胜出；如果优先级更高的候选还在尝试，最多再等 2s（tdesktop `kWaitForBetterTimeout`）。
- 按地址记录成功和失败的时间，用来排序（TDLib `DcOptionsSet`）。
- 可选：TCP 不通时回退到 HTTP 传输（tdesktop）。
- 新依赖都隔离在子包里（CLAUDE.md 要求）。

**风险：** 低。

## 4. 兼容性策略

| 项目 | 策略 |
| --- | --- |
| `mtproto.New` / `Run` / `Invoke` / `Ping` | 签名不变。不传 `Options.Session` 时行为与现在一致 |
| `mtproto.SessionState` | 改名为 `Session`，保留一个版本的类型别名 |
| `rpc.Engine` | 公开方法保持不变；内部登记表改由 outbox 提供。`ErrEngineClosed` 语义收窄为"Session 已永久关闭" |
| `session.Data` | 只新增 `omitempty` 字段 |
| 默认行为变化 | 有两项：`PingProgressAware` 默认开启（P6）；-404 不再原地重建 key（P2）。这两项都提供回退选项，并写进 CHANGELOG |
| 回退开关 | `LegacyRequestLifecycle`（P4，过渡用，观察一个版本后删除）；`PingStrictPong`（P6） |
| 提交规范 | Conventional Commits，例如 `fix(mtproto): ack container inner messages`、`refactor(mtproto)!: session-scoped request lifecycle` |

## 5. 可观测性

现在 `mtproto/conn.go` 已经接了 `Tracer`。重构时补充以下可观测点：

- **日志：** session 的 attach/detach（session_id、原因、各状态的请求数）；请求状态迁移（Debug 级别）；session 重置的原因（key 变化、32/33、太旧的消息）。
- **指标**（通过 `oteltg` 暴露）：
  - `mtproto.outbox.pending{state}`
  - `mtproto.session.resets{reason}`
  - `mtproto.state_req.sent`
  - `mtproto.resend{reason}`
  - `mtproto.server_time_offset`
  - `mtproto.ping.dead{reason}`

## 6. 工作量与顺序

| 阶段 | 规模 | 依赖 | 解决的问题 |
| --- | --- | --- | --- |
| P0 | 中 | — | 测试前提 |
| P1 | 小 | P0 | H1、H2、L1、F2 |
| P2 | 小到中 | P0 | H3、L4、F1 |
| P3 | 中 | P0 | M1 |
| P4a | 中 | P1 | 纯重构 |
| P4b | 大 | P4a | H4 |
| P4c | 中 | P4b | M2、M3 |
| P4d | 中 | P4c、P3 | M5 |
| P4e | 小 | P4b | M4 |
| P5 | 中 | P4 | 数据连接的 session 持续性 |
| P6 | 小到中 | P1 | M6、M7、L5 |
| P7 | 中到大 | P4 | L2 |
| P8 | 中 | — | L3 |

建议节奏：

1. 第一批：P0、P1、P2、F1。这批修掉所有高危的正确性问题，改动小，可以尽快合并。
2. 第二批：P3、P6。
3. 第三批：P4 全部子 PR。
4. 最后：P5、P7、P8。

## 7. 完成标准

- `CONNECTION_PARITY.md` 里的每一项都有对应的测试，并标记为已解决。
- `make test`（`-race`）、`golangci-lint run`、`make check_generated` 全部通过。
- 热路径的分配测试（`testutil.ZeroAlloc` / `MaxAlloc`）不退化。
- ARCHITECTURE.md 更新为"Session + 可替换连接"的模型。

## 8. 实施进度

分支：`refactor/session-lifecycle`（基于 `main` `b02a40622`）。

决策记录：

- 2026-11-03：`PingProgressAware` 默认开启（P6），保留 `PingStrictPong` 用于回退到旧行为。
- 2026-11-03：P2 调整了 -404 的方案。原方案是确认 key 无效后返回永久错误；现在改为确认无效后沿用原来的处理：丢弃 key、重新生成，客户端以未登录状态继续运行。这样对外语义不变，同时修掉了真正的风险（偶发的 -404 毁掉仍然有效的 key）。因此 `ErrAuthKeyInvalid` 和 `OnAuthKeyInvalid` 都不需要了。L4（`AUTH_KEY_DUPLICATED`）保持为永久错误、不清理存储，由调用方处理。
- 2026-11-03：P4 调整了实现方式。不再把请求登记表从 `rpc.Engine` 整个搬出（原 4a），改为在 `SessionState` 上加一个"孤儿请求"登记表：
  - `rpc.Engine` 仍按连接工作，只是关闭时区分已 ack 和未 ack。
  - mtproto 的 `Invoke` 不阻塞，返回可等待的 `orphanError`，由 `telegram.invokeConn` 调用 `mtproto.AwaitSession` 等待。这样可以避免 Run 内部回调（如 initConnection）等待恢复后的 session，与重连互相卡死（实测出现过一次 15s 的死等）。
  - 没有加 `LegacyRequestLifecycle` 开关：新逻辑只对共享 session（主连接）生效；不调用 `AwaitSession` 的调用方，拿到的错误与原来一致（`orphanError` 会解包成 `ErrEngineClosed` 或 `ErrClosedAfterAck`）。
- 2026-11-03：P0 不单独先做。mtproto 层的单元测试辅助函数已经够用（`encryptFromServer`、`newResumeTestConn`），各阶段按需补充测试设施；端到端需要的 tgtest 故障注入放到 P4 之前完成。

| 阶段 | 状态 | 说明 |
| --- | --- | --- |
| P0 | ✅ 完成（按需） | tgtest：`auth.bindTempAuthKey`、正确的 `first_msg_id`、`msgs_state_req` |
| P1 | ✅ 完成 | 见下方 P1 记录 |
| P2 | ✅ 完成 | H3、F1 已修复；L4 决定保持现状，见 P2 记录 |
| P3 | ✅ 完成 | M1 已修复；时间差暂不持久化，见 P3 记录 |
| P4a–4e | ✅ 完成（方案有调整） | H4、M2、M3、M4、M5 已修复，见 P4 记录 |
| P5 | ⏸ 暂缓（决策） | 见 P5 记录 |
| P6 | ✅ 完成 | M6、M7 已修复；L5 决定保持现状；自适应接收超时没做 |
| P7 | ✅ 完成 | L2 中的出站打包和 ack 合并已完成；quick ack 没做 |
| P8 | ✅ 无需实现（已有竞速） | 见 P8 记录 |

### P1 记录（2026-11-03）

**新发现并修复：H0。** `proto.MessageIDBuf` 实际只记住最后一条 msg_id，这是上游就有的 bug，已改为有序窗口实现。

**改动：**

- `proto/message_id.go`：
  - `MessageIDBuf` 改为有序窗口。
  - 新增 `MessageIDStatus` 和 `Check`；`Consume` 保留，内部调用 `Check`。
- `mtproto/read.go`：
  - 区分 `errDuplicate`（ack 后忽略）和 `errTooOld`（不 ack、不处理，重置 session）。
  - 新增 `consumeContainer`：每条内层消息先根据 `seqno&1` ack，再去重；遇到太旧的消息就重置 session。
- `mtproto/session_state.go`：
  - 窗口改为 400。
  - `requestSessionReset(reason)` 只触发一次，并设置 `sessionResetting`。
- `mtproto/write.go`、`mtproto/read.go`：`sessionResetting` 置位后，写入返回 `errWriteSessionReset`；它包装了 `rpc.ErrEngineClosed`，`invokeConn` 可以安全重试。入站消息直接丢弃。

**测试：**

- `proto/message_id_test.go`：窗口、乱序、满窗口时太旧、非正数 ID、零分配。
- `mtproto/receive_test.go`：
  - 容器内层消息的 ack 与去重。
  - 窗口内较早的消息被补发时，能识别为重复。
  - 太旧的消息（外层和容器内）触发 session 重置。
  - 重置后写入失败，且可以重试。

**验证：** `go test -race` 覆盖 proto、mtproto、telegram、tgtest、pool、rpc，全部通过。本机没有 `golangci-lint`，lint 还没跑。

### P2 记录（2026-11-03）

**H3（-404 时原地重新生成永久 key）：**

- `mtproto/read.go`：非 PFS 模式收到 -404 时，不再调用 `createAuthKey`，改为返回新的 `mtproto.ErrAuthKeyNotFound`，连接随之退出。
- `telegram/pfs.go`：
  - 主连接收到 `ErrAuthKeyNotFound` 时调用 `requestPrimaryKeyVerification`。
  - 下一条主连接临时开启 PFS（`conn_builder.go`），用 temp key + bind 验证存储里的 key。
  - bind 成功（`onSession` 收到带 PermKey 的 session）时清除标记，key 保留。
  - bind 确认 key 无效（`ErrPFSDropKeysRequired`）时，走原有路径丢弃 key。
- 非主 DC（`handleDCConnDead`）和 CDN（`handleCDNConnDead`）收到 `ErrAuthKeyNotFound` 时直接丢弃该 DC 的 key（`dropKeyRequired`），之后重新生成并做授权转移。这与 TDLib 的 `can_drop_main_auth_key_without_logging_out` 一致。
- 连到主 DC 的数据连接收到 -404 时，只标记主 key 需要验证，不会丢弃 key。

**F1（主 DC 数据池取到空 key）：**

- `telegram/pool.go`：连到主 DC 的 `DC`/`MediaOnly` 池改用 `c.session`（主 key）。主 key 为空时返回错误，不再静默生成新 key。这类连接一律使用 `primaryDataConnHandler`，不会写入主 session；dead 时也不会丢弃主 key。
- 删除不再使用的 `isPrimaryDataPFS`。

**tgtest（P0 设施）：** 新增对 `auth.bindTempAuthKey` 的处理：认识该永久 key 时返回 true，否则返回 `ENCRYPTED_MESSAGE_INVALID`。

**测试：**

- `mtproto/conn_pfs_test.go`：`TestHandleAuthKeyNotFoundKeepsKey`。
- `telegram/auth_key_verify_test.go`：
  - 收到 -404 后下一条主连接启用 PFS 验证，验证成功后恢复非 PFS。
  - 验证失败时丢弃 key。
  - 非主 DC 收到 -404 时丢弃该 DC 的 key。
- `telegram/pool_transfer_test.go`：主 DC 数据池在 `c.sessions` 为空时仍使用主 key；主 key 为空时返回错误。
- `telegram/client_auth_key_e2e_test.go`（tgtest 端到端）：存储里放一个服务器不认识的 key，客户端经过 PFS 验证（bind 被拒 3 次）后换成新 key，`Self()` 正常。

**验证：** `go test -race` 覆盖 mtproto、telegram、tgtest、pool、rpc、proto、session 下的所有包，全部通过。

### P3 记录（2026-11-03）

**新发现：** 原来的 `checkMessageID` 用本地时间校验入站 msg_id。本地时钟偏差超过 300s（过去）或 30s（未来）时，所有服务器消息都会被丢弃，客户端连校正的机会都没有。端到端测试证实：客户端时钟偏移 -10 分钟时，`Self()` 永远完成不了。

**改动：**

- `proto/server_time.go`（新增）：`ServerTime` 按 TDLib `update_server_time_difference` 的规则维护时间差：
  - 第一个样本直接采用，之后只增不减。
  - `Reset` 用于 bad_msg 17；`Set` 用于从存储恢复。
  - 只取服务器 msg_id 的秒数部分。
- `proto/message_id.go`：`MessageIDGen` 内置 `ServerTime`，生成 msg_id 时加上偏移（零分配）；新增 `MessageIDGen.ServerTime()`。
- `mtproto`：
  - `Conn.serverTime` 与 `MessageIDGen` 共享，用户自定义的 `MessageIDSource` 则使用独立的一份。
  - `decryptMessage` 先 `Observe` 服务器 msg_id，再用校正后的时间调用 `checkMessageID`。
  - `handleBadMsg(msgID, b)`：17 时用通知自身的 msg_id 重置时间差；16/17 都会打印校正后的偏移。
  - `Invoke`：16/17/48 时换新的 msg_id 和 seqno 重发，最多 3 次（`fixBadMsg`）。48 原来是用同一个 msg_id 重发。
  - `bindTempAuthKey`：16/17 时进入下一次尝试（会换新的 msg_id）。
  - `handleSessionCreated`：告警改为按测得的偏移判断。原来拿自己生成的 `first_msg_id` 和本地时间比较，加了校正以后会误报。

**决定：** 时间差暂不持久化到 `session.Data`。启动后第一个请求如果因时钟偏差被拒，收到 16/17 时会校正并自动重发，代价只是一个往返。需要时可以用 `MessageIDGen.ServerTime().Set` 恢复。

**测试：**

- `proto/server_time_test.go`：更新规则、只取秒数、生成器应用偏移、零分配。
- `mtproto/receive_test.go`：本地时钟偏移 ±10 分钟时，服务器消息仍被接受，生成的 msg_id 跟随服务器时间；bad_msg 17 重置时间差。
- `telegram/client_server_time_e2e_test.go`：客户端时钟偏移 ±10 分钟时 `Self()` 成功。去掉修复后测试超时，证明测试有效。

**验证：** `go test -race` 覆盖 mtproto、telegram、tgtest、pool、rpc、proto、session、exchange 下的所有包，全部通过。

### P6 记录（2026-11-03）

**M6（默认存活检测过严）：**

- `mtproto.Options` 和 `telegram.Options` 新增 `PingStrictPong`，`Conn.pingProgressAware = !PingStrictPong`。也就是说，progress-aware 现在是默认行为。
- `PingProgressAware` 字段保留，但标记为 Deprecated，不再起作用。
- **行为变化：** 默认模式下，ping 发出后 `PingTimeout` 内收到任何数据都算连接存活。需要旧行为的用户可以设置 `PingStrictPong: true`。

**M7（无法感知网络变化）：** 新增 `telegram.Client.NetworkChanged(available bool)`（`telegram/network.go`）。网络恢复时重置 `connBackoff`，并通过 `c.restart` 立即重启主连接，session 保留。网络断开时不做任何处理。

**L5：** 决定保持现状。数据连接继续发 `ping_delay_disconnect`，这样客户端异常消失时，服务器能及时关闭这些连接。

**没做：** 计划里可选的自适应接收超时（tdesktop 那种 4s 到 64s 的做法）。progress-aware 已经解决了误判问题，以后有需要再加。

**测试：**

- `mtproto/ping_test.go`：`TestPingProgressAwareByDefault`。
- `telegram/options_test.go`：`PingStrictPong` 能正确传给 mtproto。
- `telegram/client_reconnect_session_e2e_test.go`：`TestClientNetworkChangedRestartsConnection`，验证会立即重新拨号，`Self()` 成功，且所有请求使用同一个 session。

### P4 记录（2026-11-03）

**rpc：**

- 新增 `rpc.ErrClosedAfterAck`。已 ack 但还没拿到结果时关闭，返回这个错误。它满足 `errors.Is(err, context.Canceled)`，与原来的行为兼容。
- 新增 `Engine.Pending` 和 `Engine.PendingIDs`。

**mtproto：**

- `session_requests.go`（新增）：`SessionState` 维护孤儿请求。
  - `track`、`orphan`、`untrack` 管理请求的登记与释放。
  - `deliverResult`、`deliverError`：结果以原始字节缓存，由等待者解码。
  - `notProcessed`、`notProcessedBefore` 处理服务器明确没有处理的请求。
  - `unqueried`、`addStateQuery`、`stateInfo` 负责状态查询：1、2、3 重发，4 视为已收到。
  - `endRequests` 在 session 结束时（`start`、`reset`、`Close`）收尾：已 ack 的返回 `errAnswerLost`（包装 `ErrClosedAfterAck`），未 ack 的返回 `errNotProcessed`（包装 `ErrEngineClosed`）。
  - 新增导出函数 `AwaitSession` 和 `SessionState.Close`。
- `rpc.go`：共享 session 下，连接关闭时返回 `orphanError`；`errResendInSession` 时换新 msg_id 重发。
- `handle_result.go`、`handle_ack.go`、`handle_bad_msg.go`：引擎不认识的 msg_id，转交 session 登记表处理（结果、错误、ack、bad_msg 都算"未处理"）。
- `handle_state.go`（新增）：
  - `stateQueryLoop`：新的孤儿请求出现时，或每 10s 一次，发 `msgs_state_req`（内容消息）。
  - 处理 `msgs_state_info`。
  - 处理 `msg_detailed_info` / `msg_new_detailed_info`：回复已收到就 ack，否则发 `msg_resend_req`（M2）。
- `handle_session_created.go`：
  - 引擎里 msg_id 小于 `first_msg_id` 的请求，返回 `errResendInSession`，触发换新 msg_id 重发。
  - session 里同样早于 `first_msg_id` 的孤儿请求，标记为未处理。
  - 调用可选接口 `SessionCreatedHandler.OnSessionCreated`（M4）。
- `proto.MessageIDBuf.Contains`。

**telegram：**

- `invoke.go`：`invokeConn` 先调用 `mtproto.AwaitSession`，再判断是否需要在新连接上重试。
- `connect.go`：`Run` 退出时调用 `primarySession.Close()`。
- `manager.Conn.OnSessionCreated` 只对 `ConnModeUpdates` 连接转发。`clientHandler.OnSessionCreated` 会在 goroutine 中注入 `updatesTooLong`，触发 getDifference；`NoUpdates` 模式下跳过。

**tgtest：**

- 修正 `new_session_created` 的 `first_msg_id`：原来用的是新生成的服务端 ID，现在改为 session 收到的第一条客户端消息的 ID，与真实服务器一致。
- 记录每个 session 收到过的 msg_id，并据此回答 `msgs_state_req`。

**M5：**

- 32/33 时 session 重置，进行中的请求在 `endRequests` 中处理：未 ack 的由 `invokeConn` 换新 msg_id 重发，已 ack 的报错（可能已处理，与 TDLib 一致）。
- 64 目前不适用，因为 gotd 不打包出站容器；到 P7 时再处理。

**ARCHITECTURE.md：** 已更新 "Connection: mtproto"、"RPC engine: rpc" 和 "Request lifecycle" 三节。

**测试：**

- `mtproto/session_requests_test.go`：
  - 已 ack 请求的结果，经恢复后的 session 送达。
  - 未 ack 的请求先查询状态：未收到时重发，未知时重发，已收到时继续等结果。
  - session 结束（reset、Close、换 key）时，已 ack 和未 ack 的请求分别得到正确的错误。
  - 非共享 session 的行为不变。
  - `first_msg_id` 之前的请求换新 msg_id 重发。
  - detailed_info 时 ack 或发 resend_req。
- `rpc/engine_test.go`：已 ack 后关闭，返回 `ErrClosedAfterAck`。
- `telegram/client_session_requests_e2e_test.go`（tgtest 端到端）：请求进行中连接断开，无论服务端是否已 ack，客户端都拿到结果，且服务端**只执行一次**。去掉 `AwaitSession` 后，未 ack 的情况会被反复重发和执行，证明测试有效。

**验证：** `go build ./...`、`go vet`、`go test -race -count=1 ./...`（整个仓库）全部通过。

### P5 记录（2026-11-03）：暂缓

原计划是：数据池的每个槽位持有一个 session，连接重建时复用。评估后决定暂缓，原因有两点：

1. **会导致请求一直挂起。** `pool.DC` 只在有请求时才惰性创建连接。如果孤儿请求在等待恢复后的 session，而池里没有新的请求进来，就不会有新连接去恢复 session，请求会一直挂到超时。另外，session 只能在 `OnDead`（旧连接完全退出）之后归还，新连接可能已经拿了一个全新的 session。要做对，需要改造 pool 的连接调度，风险和收益不成比例。
2. **收益很低。** 数据连接上主要是 `upload.getFile` 和 `upload.saveFilePart` 这类幂等请求，重发没有副作用。`AUTH_KEY_DUPLICATED` 也只和主 session 有关。

数据池连接保持"每条连接一个 session"，请求语义与原来一致：未 ack 的在新连接上重试，已 ack 的报错。

### P7 记录（2026-11-03）

**做法：组提交（group commit），不增加任何延迟**（`mtproto/write_batch.go`）：

- 没有写入在进行时，直接走原来的单条发送路径（`writeOne`）。分配次数与基线一致，`BenchmarkWrite/1024b` 都是 14 allocs/op、2848 B/op。
- 有写入正在进行时，新的写入先排队。正在写入的一方完成自己的消息后，把队列按 msg_id 排序、打进 `msg_container` 发出。
  - 单个容器不超过 16KB（tdesktop 的 `kCutContainerOnSize`），最多 1020 条消息；只剩一条时不打包。
  - 内层消息保留各自的 msg_id 和 seqno，按原来的压缩规则编码（`encodePayload`）。
  - 容器自身的 msg_id 在内层之后生成，因此一定更大；seqno 用偶数。容器的原始字节不再压缩（`encryptRaw`）。
- 调用方取消时，排队中的写入不再发送。
- 记住最近 128 个容器与内层 msg_id 的对应关系（`rememberContainer`）。针对容器的 bad_msg（16/17/32/33/48/64）会转给每条内层消息（`notifyBadMsg`）。新增对 64 的处理：换新 msg_id 重发（`fixBadMsg`）。
- `bad_server_salt` 改为在 `handleBadMsg` 中统一更新 salt，不再在每个请求重试时各自更新。
- 新增退出开关 `mtproto.Options.DisableContainers` 和 `telegram.Options.DisableContainers`。

**L2 的效果：** 并发时，ack、ping 和请求会自然合并到同一个容器里。

**没做：** quick ack。

**测试：**

- `mtproto/write_batch_test.go`：
  - 并发写入被打进容器：顺序、容器 ID 更大、容器 seqno 为偶数、内层 seqno 保留。
  - 超过 16KB 时拆开发送。
  - `DisableContainers` 生效。
  - 排队中被取消的写入不会发送。
  - 针对容器的 bad_msg 64 会让请求换新 msg_id 重发。
  - 无竞争时的分配次数与单条发送一致（race 模式下按仓库惯例跳过）。
- `telegram/client_containers_e2e_test.go`：50 个并发请求经 tgtest 端到端执行。实际发出了 12 个容器，所有请求都拿到正确结果。

**验证：** `go test -race -count=1 ./...`（整个仓库）全部通过。

### P8 记录（2026-11-03）：无需实现

复查 `telegram/dcs/plain.go` 后发现，`connect` 本来就会对所有候选地址**并发拨号，先成功者胜出**，其余连接关闭。差异清单里 L3 写的"依次拨号"是错的，已更正。

剩下的细节都决定不做：

- TDLib 的地址成功/失败统计：每次都并发尝试所有地址，坏地址自然会输掉竞速，统计的收益很小。
- tdesktop 的"优先级更高的候选再多等 2 秒"：这只是 IPv4/IPv6 偏好上的策略差异。
- TCP 不通时回退到 HTTP：gotd 的 HTTP 传输是一个独立的 Resolver，用户可以自行组合。

## 9. 收尾验证（2026-11-03）

- `go build ./...`：通过。
- `go test -race -count=1 ./...`：整个仓库全部通过。
- `golangci-lint` v1.64.8（CI 没有固定版本，这里用 `go run` 临时运行）配合 `--new-from-rev=HEAD`：这次改动**没有引入新问题**。修掉的问题包括 gosec G115、gocritic、goimports 的 local-prefixes 分组。仓库里原有的问题（`tdlib_json.go`、`cdn.go`、websocket 弃用等）不在本次范围内。
- 热路径的分配：`BenchmarkWrite/1024b` 与基线一致（14 allocs/op、2848 B/op）；`MessageIDBuf.Check` 和 `MessageIDGen.New` 都是零分配。
- 所有改动都还**没有提交**，分支是 `refactor/session-lifecycle`。
