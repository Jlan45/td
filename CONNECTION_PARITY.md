# 连接生命周期：gotd 与官方客户端的差异

> **状态（2026-11-03，分支 `refactor/session-lifecycle`）：** 除了决定保持现状的 L4、L5，以及决定不做的 L3 细节和 quick ack，其余差异都已修复。实施记录见 [CONNECTION_REFACTOR_PLAN.md](CONNECTION_REFACTOR_PLAN.md)。

本文逐项对比 gotd 与 Telegram 官方开源客户端的 MTProto 连接生命周期实现，列出所有不一致的地方。
每一条都对照两边源码确认过，并注明出处。

**怎么读这份文档：** 各条目正文描述的是**修复前**的 gotd 行为，文件行号以 `b02a40622` 为准；每条开头的"现状"说明修复后的行为和对应的代码位置。

| 对象 | 版本 | 位置 |
| --- | --- | --- |
| gotd/td | `main`（v0.163.0 之后又有 5 个 fork 提交，HEAD `b02a40622`） | 本仓库 |
| TDLib | `4c6377a`（2026-10-27） | `../../telegram/tdlib` |
| Telegram Desktop | `a8de5f9`（2026-11-02） | `../../telegram/tdesktop` |
| Telegram Android | `3de37c8`（2026-10-25） | `../../telegram/android` |

路径缩写：

- `tdlib:` = `tdlib/td/`
- `tdesktop:` = `tdesktop/Telegram/SourceFiles/mtproto/`
- `android:` = `android/TMessagesProj/jni/tgnet/`

## 总览

| # | 级别 | 差异 | 官方三家的做法 |
| --- | --- | --- | --- |
| H0 | 高 | 去重缓冲区实际只记住最后 1 条 msg_id（上游 bug）。**已修复（P1）** | 三家都按窗口完整去重 |
| H1 | 高 | 容器里的内层消息既不 ack 也不去重。**已修复（P1）** | 三家都会逐条 ack 并去重 |
| H2 | 高 | 收到"太旧"的消息时，ack 后直接丢弃。**已修复（P1）** | 三家都会换新 session |
| H3 | 高 | 非 PFS 模式收到 -404 时，原地重新生成永久 key。**已修复（P2）** | TDLib 先用 PFS 验证主 key，确认无效才显式登出；tdesktop 只销毁临时 key |
| H4 | 高 | 已 ack 但还没拿到结果的请求，断线时直接报错；服务器之后发来的结果被丢弃。**已修复（P4）** | TDLib 和 tdesktop 会在同一个 session 里继续等结果 |
| M1 | 中 | 不维护服务器时间差；bad_msg 16/17 直接报错。**已修复（P3）** | 三家都校正时间后重发 |
| M2 | 中 | 忽略 `msg_detailed_info` / `msg_new_detailed_info`。**已修复（P4）** | tdesktop 已收到对应回复就 ack，没收到就发 `msg_resend_req`；TDLib 交给 Session 处理 |
| M3 | 中 | 没有 `msgs_state_req`：没 ack 的请求在连接内盲目重发，断线后换新 msg_id 重发。**断线场景已修复（P4）**；连接内仍用同一个 msg_id 重发，这在协议上是幂等的 | TDLib 和 tdesktop 先查询请求状态再决定 |
| M4 | 中 | 收到 `new_session_created` 后不重发 `first_msg_id` 之前的请求，也不强制 getDifference。**已修复（P4）** | TDLib 两件事都做 |
| M5 | 中 | bad_msg 32/33/64 之后，已 ack 的请求报错，64 直接报错。**已修复（P4 修 32/33，P7 修 64）** | tdesktop 和 Android 会把请求换新 msg_id 重发 |
| M6 | 中 | 默认存活检测要求收到同一个 ping_id 的 pong。**已修复（P6）** | 三家都认为收到任何数据就说明连接还活着 |
| M7 | 中 | 无法感知网络变化。**已修复（P6）** | 三家都在网络变化时立即重连并清零退避 |
| L1 | 低 | 去重窗口只有 100。**已修复（P1）** | tdesktop 400，Android 约 300 |
| L2 | 低 | 出站不打包容器，ack 不随请求一起发，没有 quick ack。**打包已完成（P7）**；quick ack 没做 | 三家都打包 |
| L3 | 低 | ~~选址不做竞速~~：**更正**，`plain.connect` 本来就并发拨号所有候选、先成功者胜出。剩下的差异（地址打分、IPv4 优先多等 2s、自动回退到 HTTP）决定不做（P8） | tdesktop 同时建多条连接竞速；TDLib 给地址打分；Android 轮换地址 |
| L4 | 低 | `AUTH_KEY_DUPLICATED` 时只停止重连，不清理 key。**决定保持现状（P2）** | Android 会 `cleanUp` |
| L5 | 低 | 所有连接（包括数据池）都发 `ping_delay_disconnect`。**决定保持现状（P6）** | tdesktop 只在主 session 发 |
| F1 | 中 | fork 引入：主 DC 的数据池从 `c.sessions` 取 key，取到空 key 时不报错。**已修复（P2）** | — |
| F2 | 低 | fork 引入：收到 32/33 后、连接退出前，仍会用旧 session_id 发消息。**已修复（P1）** | — |

---

## 高

### H0. 去重缓冲区实际只记住 1 条（上游 bug，已修复）

**gotd 原来的做法：** `proto.MessageIDBuf.Consume` 把 `minID` 初始化为 0，而 msg_id 都是正数，所以 `id < minID` 永远不成立。
结果是每次都替换 `buf[0]`：缓冲区里只保存最后一条 msg_id，"比所有已存 ID 都旧"的判断也永远不会触发。
用测试复现过：依次消费 10、20、30 之后，再来一个 10 仍然被接受。
因此，服务器补发的消息只要不是最后一条，都会被重复处理。

**修复（P1）：** 改为按 TDLib `check_message_id_duplicates` 的语义实现有序窗口：

- 新增 `MessageIDBuf.Check`，返回 `MessageIDAccepted`、`MessageIDDuplicate` 或 `MessageIDTooOld`。
- 递增的 msg_id 走 O(1) 快速路径，其余情况二分查找。
- 零分配，有测试覆盖。

### H1. 容器内层消息不 ack、不去重

> **现状（P1，已修复）：** `mtproto/read.go` 的 `consumeContainer` 对每条内层消息先根据 `seqno&1` ack，再去重；遇到太旧的消息就重置 session。

**gotd 的做法：** 只在外层消息上做去重和 ack（`mtproto/read.go:73`，`:111`）。
`processContainerMessage`（`mtproto/handle_container.go:24`）只把消息体交给 `handleMessage`，
内层消息自带的 `msg_id` 和 `seqno` 都没有用到。
容器本身的 seqno 是偶数，所以外层不会触发 ack。

**后果：**

- 服务器经常把 `rpc_result`、updates 等内容消息装在容器里发。这些内层消息需要 ack，但 gotd 一条都不确认，服务器只能一直重发。
- 内层消息没有去重，重发的内容会被再处理一遍。
- 恢复 session 之后这个问题更明显：服务器会补发所有没被确认的消息。

**官方做法：**

- TDLib：`on_slice_packet` 对每条消息（包括容器里的）检查 `seq_no & 1`，需要时调用 `send_ack`（`tdlib:mtproto/SessionConnection.cpp:511-513`，容器经过 `:522`）。
- tdesktop：内层消息先 `_ackRequestData.push_back(inMsgId)`，再调用 `registerMsgId` 去重（`tdesktop:session_private.cpp:1529-1542`）。
- Android：内层消息先 `addMessageToConfirm`，再检查 `isMessageIdProcessed`（`android:ConnectionsManager.cpp:1144-1158`）。

**建议：** 在 `processContainerMessage` 里对每条内层消息做和外层相同的处理：先根据 `seqno&1` 决定是否 ack，再去重（重复或太旧的处理见 H2），最后交给 `handleMessage`。

### H2. "太旧"的消息被 ack 后丢弃

> **现状（P1，已修复）：** `proto.MessageIDBuf.Check` 能区分 `Duplicate` 和 `TooOld`。太旧的消息不 ack、不处理，调用 `requestSessionReset(sessionResetTooOld)`。

这里的"太旧"指：去重缓冲区已满，而新消息的 msg_id 比缓冲区里所有 ID 都小。

**gotd 的做法：** `MessageIDBuf.Consume` 对"重复"和"太旧"都返回 false。fork 提交 `6b15b5466` 之后，这两种情况都被当作 `errDuplicate`，ack 后丢弃（`mtproto/read.go:73`，`:82-87`）。

**后果：** 一条从没处理过的消息被确认了，服务器不会再重发，消息就此丢失，比如某个 `rpc_result`。

**官方做法：**

- TDLib：去重检查返回码 2（"Ignore very old"，`tdlib:mtproto/AuthData.cpp:34-36`），随后调用 `on_session_failed`，换新 session（`tdlib:mtproto/SessionConnection.cpp:740-743`）。只有返回码 1（真正的重复）才 ack（`:736-739`）。
- tdesktop：`TooOld` 映射到 `HandleResult::ResetSession`（`tdesktop:session_private.cpp:1413-1414`，`details/mtproto_received_ids_manager.cpp:19-24`）。
- Android：处理状态为 2 且带消息体时，`recreateSession()` 后 `reconnect()`（`android:ConnectionsManager.cpp:972-976`）。

**建议：** 让 `Consume` 区分重复和太旧。太旧时调用 `requestSessionReset`，不要 ack。

### H3. 非 PFS 模式收到 -404 时原地重新生成永久 key

> **现状（P2，已修复）：** 收到 -404 时返回 `mtproto.ErrAuthKeyNotFound`。主连接的下一次连接以 PFS bind 验证永久 key（`telegram/pfs.go` 的 `requestPrimaryKeyVerification`）：验证通过就保留 key；确认失效才走原有的丢弃路径。非主 DC 和 CDN 直接丢弃该 DC 的 key。没有采用"返回永久错误"的方案，对外语义不变。

**gotd 的做法：** `handleAuthKeyNotFound` 在非 PFS 模式下直接调用 `createAuthKey`（`mtproto/read.go:148-170`）。
新生成的 key 还没有授权，但随后的 `new_session_created` 会触发 `OnSession`，进而通过 `saveSession` **把这个未授权的 key 写进存储**，覆盖原来的 key。
结果是用户被"静默登出"，上层收不到任何明确信号。

**官方做法：**

- TDLib：非 PFS 模式下收到 -404 时，先切换到 PFS，用 bind 验证主 key（"Use PFS to check main key"）。确认主 key 无效后，主 DC 调用 `G()->log_out("Main PFS authorization key is invalid")` 显式登出；非主 DC 只丢弃 key（`tdlib:telegram/net/Session.cpp:629-663`）。
- tdesktop：-404 时调用 `destroyTemporaryKey`，只销毁临时 key 然后重启（`tdesktop:session_private.cpp:2685-2711`）。tdesktop 总是使用临时 key（`kTemporaryExpiresIn`，`:43`）。

**建议：** 不要原地重新生成 key。应该先用 bind 验证永久 key；确认失效后返回明确的错误（比如 `AUTH_KEY_UNREGISTERED`），由上层决定是否重新登录。在确认之前不要覆盖已存储的 key。

### H4. 已 ack 未出结果的请求在断线时被放弃

> **现状（P4，已修复）：** 共享 session（主连接）上，连接断开时 `Invoke` 返回可等待的 `orphanError`，`telegram.invokeConn` 调用 `mtproto.AwaitSession` 等待：已 ack 的请求在恢复后的 session 上拿到结果（`mtproto/session_requests.go`）；session 结束时才报 `rpc.ErrClosedAfterAck`。

**gotd 的做法：**

- **还没 ack 的请求**：连接断开时返回 `rpc.ErrEngineClosed`（`rpc/engine.go:226-236`），由 `telegram.Client.invokeConn`（`telegram/invoke.go:92-132`）或 `pool.DC.Invoke` 在新连接上换新 msg_id 重试。这部分和 Android 的做法一致。
- **已经 ack 但还没收到结果的请求**：返回 `engine forcibly closed`，不会重试（`rpc/engine.go:178-190`）。

问题出在后一种。fork 提交 `9ff37d944` 让主连接恢复原 session 后，服务器会把这些请求的 `rpc_result` 发到新连接上。可是新连接的 `rpc.Engine` 不认识这些 msg_id，只打一条 `rpc callback not set` 日志就丢掉了（`rpc/engine.go:277`）。

**官方做法：** 请求挂在 Session 上，跨连接一直保留，所以结果到了照样能交付。

- TDLib：`sent_queries_` 属于 `Session`；`on_closed` 只把本连接上**没 ack** 的请求标记为 unknown，已 ack 的继续等结果（`tdlib:telegram/net/Session.cpp:675-700`）。
- tdesktop：`haveSent` 在 `SessionData` 里，跨 `SessionPrivate` 重启保留（`tdesktop:session_private.cpp:267-300`）。

**建议：** 把待处理请求的登记表从 `rpc.Engine` 移到 `SessionState` 这一层。session 恢复后，已 ack 的请求继续等待结果，而不是报错。

---

## 中

### M1. 没有服务器时间差

> **现状（P3，已修复）：** `proto.ServerTime` 按 TDLib 规则维护时间差（首个样本直接采用，之后只增不减，17 时重置），并内置在 `MessageIDGen` 中。入站消息先观测时间差再校验 msg_id。16/17/48 时换新的 msg_id 重发。时间差没有持久化。

**gotd 的做法：**

- msg_id 一直用本地时钟生成。`MessageIDGen` 使用 `opt.Clock`，可以接 `clock/ntp`，但不会根据服务器时间修正。
- `handleSessionCreated` 发现时间偏差时只打警告（`mtproto/handle_session_created.go:31`）。
- bad_msg 16/17 交给 `rpc.NotifyError`，作为错误返回给调用方，不会重试（`telegram/invoke.go:130` 只对连接失效类错误重试）。

**官方做法：**

- TDLib：每收到一个包都调用 `update_server_time_difference`（`tdlib:mtproto/AuthData.cpp:156`），结果共享并持久化（`tdlib:telegram/net/Session.cpp:251`）。16 会让请求重发，17 会重置时间差（`tdlib:mtproto/SessionConnection.cpp:344-355`）。
- tdesktop：16/17/64 时调用 `correctUnixtimeWithBadLocal` 后 `resend`（`tdesktop:session_private.cpp:1595-1637`）。
- Android：根据 pong 和 bad_msg 计算 `timeDifference`，扣掉半个 RTT（`android:ConnectionsManager.cpp:1209-1214`，`:1586-1590`）。

**建议：** 用收到的 msg_id 维护时间差，让 `MessageIDGen` 使用修正后的时钟；16/17 时校正并重发。

### M2. 忽略 msg_detailed_info / msg_new_detailed_info

> **现状（P4，已修复）：** `mtproto/handle_state.go` 的 `handleDetailedInfo`：回复已收到就 ack，否则发 `msg_resend_req`。`msg_detailed_info` 同时视为对请求的 ack。

**gotd 的做法：** 直接 `return nil`（`mtproto/handle_message.go:39-41`）。

**官方做法：**

- tdesktop：如果 `answer_msg_id` 已经收到过，就 ack；没收到就放进 `_resendRequestData`，发 `msg_resend_req`（`tdesktop:session_private.cpp:1799-1848`）。
- TDLib：交给 `on_message_info` 处理（`tdlib:mtproto/SessionConnection.cpp:495-507`）。

**后果：** 服务器用这两条消息告诉客户端"答案已经单独发过/可以按需索取"。gotd 既不确认也不索取，可能拿不到这条回复。

### M3. 没有 msgs_state_req，只会盲目重发

> **现状（P4，断线场景已修复）：** 未 ack 的孤儿请求由 `stateQueryLoop` 发 `msgs_state_req` 查询：1、2、3 重发，4 继续等结果。连接内仍然是同一个 msg_id 每 10s 重发一次，这在协议上是幂等的，所以没有改。

**gotd 的做法：**

- 连接内：每 10s 没收到 ack，就用原来的 msg_id 再发一次，5 次后报 `RetryLimitReachedErr`（`rpc/engine.go:240-260`）。
- 断线后：没 ack 的请求直接换新 msg_id 重发（见 H4）。
- 整个仓库没有发送 `msgs_state_req` 和 `msg_resend_req` 的代码。

**官方做法：**

- tdesktop：每 10s 扫描一次超过 10s 还没响应的请求，发 `msgs_state_req` 查询状态（`tdesktop:session_private.cpp:267-300`，常量在 `:59`、`:63`）。
- TDLib：断线时把没 ack 的请求标记为 unknown，批量调用 `get_state_info` 查询（`tdlib:telegram/net/Session.cpp:691`，`:1298-1305`）。
- Android：和 gotd 一样直接重发（`android:ConnectionsManager.cpp:2616-2624`）。

**后果：** 如果服务器已经执行了请求，只是 ack 丢了，换新 msg_id 重发会让非幂等操作（比如发消息）执行两次。

### M4. new_session_created 之后不补发、不补更新

> **现状（P4，已修复）：** `first_msg_id` 之前的请求换新 msg_id 重发（包括还在引擎里的和孤儿请求）。updates 连接会注入 `updatesTooLong`，触发 getDifference（`SessionCreatedHandler`）。

**gotd 的做法：** 只做三件事：设置 salt、发出 `gotSession` 信号、调用 `OnSession`（`mtproto/handle_session_created.go:14-42`）。

**官方做法（TDLib）：**

- `first_msg_id` 之前发出的请求，服务器在新 session 里不会处理，全部重发。
- 如果是主 session，注入 `updatesTooLong`，强制调用 getDifference。

见 `tdlib:telegram/net/Session.cpp:703-734`。gotd 已经能处理 `UpdatesTooLong`（`telegram/handle_updates.go:41`），接上即可。

### M5. bad_msg 32/33/64 之后不重发请求

> **现状（P4、P7，已修复）：** 32/33 时 session 重置，未 ack 的请求换新 msg_id 重发，已 ack 的报错（与 TDLib 一致）。64 只会在打包容器时出现：针对容器的 bad_msg 会转给每条内层消息，再换新 msg_id 重发。

**gotd 的做法：**

- 32/33：fork 提交后会重置 session（`mtproto/handle_bad_msg.go:57-61`）。没 ack 的请求会经 `invokeConn` 重试；已 ack 的请求和触发错误的那条请求都会报错。
- 64：直接报错。

**官方做法：**

- tdesktop：32/33 走 `ResetSession`，`haveSent` 里的请求换新 msg_id 重发；64 校正后重发（`tdesktop:session_private.cpp:1595-1652`）。
- Android：16/17/19/32/33/64 都会 `recreateSessions` 后调用 `clearRequestsForDatacenter`，把请求重新排队（`android:ConnectionsManager.cpp:1574-1596`）。
- TDLib：32/33/64 返回错误关闭连接（`tdlib:mtproto/SessionConnection.cpp:365-381`），请求由 Session 处理。

### M6. 默认存活检测过严

> **现状（P6，已修复）：** 默认改为 progress-aware，`PingStrictPong: true` 可以恢复旧行为；`PingProgressAware` 字段已标记为 Deprecated。

**gotd 的做法：** 默认每 60s 发一次 `ping_delay_disconnect(75)`，15s 内必须收到**同一个 ping_id** 的 pong（`mtproto/ping.go:117-120`，`options.go:170-173`）。
fork 提交 `7c0821698` 加了可选的 `PingProgressAware`，但默认没开。

**官方做法：** 三家都认为"收到任何数据，说明连接还活着"。

- TDLib："Real pong can be delayed by many big packets"，收到任何主包都刷新 pong 时间（`tdlib:mtproto/SessionConnection.cpp:580-583`）。阈值按 RTT 自适应（`SessionConnection.h:145-163`）。
- tdesktop：发出请求后的接收超时从 4s 开始，失败一次翻倍到 64s，大请求按 8KB/s 延长（`tdesktop:session_private.cpp:37-38`，`:1136-1177`）。
- Android：socket 在 timeout 时长内**没有任何事件**才断开（`android:ConnectionSocket.cpp:709`）。

**建议：** 默认开启 `PingProgressAware`。

### M7. 无法感知网络变化

> **现状（P6，已修复）：** 新增 `telegram.Client.NetworkChanged(available bool)`：网络恢复时重置退避并立即重启主连接，session 保留。

gotd 只能等 ping 超时或读写出错才发现断线，没有外部触发入口。

**官方做法：**

- tdesktop：`NetworkReachability::availableChanges` 触发所有 session 的 `restart()`（`tdesktop:mtp_instance.cpp:335-338`）。
- TDLib：`ConnectionCreator::on_network` / `on_online` 清空退避和限流（`tdlib:telegram/net/ConnectionCreator.cpp:531-579`）。
- Android：Java 层的 ConnectivityManager 调用 `setNetworkAvailable`（`android:ConnectionsManager.cpp:3872`）。

**建议：** 提供 `Client` 级别的入口（比如 `NetworkChanged()`），调用后立即重连并重置 `connBackoff`。

---

## 低

- **L1 去重窗口（P1，已修复）：** 原来 gotd 是 100（实际受 H0 影响只有 1），现在改为 400，与 tdesktop 一致（`tdesktop:details/mtproto_received_ids_manager.h:15`）。Android 是超过 300 时删掉最旧的 100（`android:ConnectionSession.cpp:68-75`）。
- **L2 出站打包（P7，打包已完成）：** 原来 gotd 每条消息单独加密发送，ack 也单独发（每 15s 或攒够 20 条）。现在并发写入时会打进 `msg_container`（`mtproto/write_batch.go`），没有并发时走原来的路径，没有额外延迟。quick ack 没有做。参照：tdesktop 的 ack 最多等 10s（`tdesktop:session_private.cpp:66`），TDLib 最多等 30s（`tdlib:mtproto/SessionConnection.h:129`）。
- **L3 选址（已更正）：** gotd 的 `plain.connect`（`telegram/dcs/plain.go`）对所有候选地址并发拨号，先成功者胜出。原文说"依次拨号"是错的。剩下的差异：
  - tdesktop 会按优先级再多等 2s，选更好的连接（`session_private.cpp:1011-1110`，`:2369-2438`）。
  - TDLib `DcOptionsSet` 记录每个地址的成功/失败时间（`DcOptionsSet.h:28-49`）。
  - Android 用 `nextAddressOrPort` 轮换地址（`Connection.cpp:690-700`）。
- **L4 AUTH_KEY_DUPLICATED（决定保持现状）：** gotd 把它当作永久错误，停止重连（`telegram/connect.go:94-106`），但 key 还留在存储里，由调用方决定如何处理。Android 在未登录时调用 `cleanUp(true, dc)` 清理（`android:ConnectionsManager.cpp:1523-1527`）。由于主连接现在会跨重连保留 session，正常重连已不会触发这个错误。
- **L5 谁发 ping（决定保持现状）：** gotd 每条连接都跑 `pingLoop`，这样客户端异常消失时，服务器能及时关闭数据连接。tdesktop 只在主 session 发 `ping_delay_disconnect`，其它 session 发普通 `ping`（`tdesktop:session_private.cpp:585-623`）。

---

## fork 提交引入的问题

- **F1（中，P2 已修复）** `telegram/pool.go:202`：连到主 DC 的 `DC`/`MediaOnly` 池跳过了授权转移，但用的 key 来自 `c.sessions[主DC]`，而不是 `c.session`。这一项在主连接 `OnSession` 之前是空的，被 `handlePrimaryConnDead` 清空之后也是空的。这时池会生成一个未授权的新 key，而且不报错。非 PFS 模式下，这个连接的 `onSession` 还可能把这个新 key 写进主 session。现在改为从 `c.session` 取 key，取不到就返回错误；这类连接一律不写主 session。
- **F2（低，P1 已修复）** `mtproto/session_state.go:142`：`requestSessionReset` 已经清空了 `SessionState`，但连接要过一会儿才退出。这段时间里，连接还在用旧的 session_id 发消息，seqno 却已经从 0 重新开始；去重也临时换成了连接自己那份一直没用过的缓冲区。现在 `sessionResetting` 置位后，写入返回可重试的错误，入站消息直接丢弃。

---

## 已经和官方一致的部分

修复前就一致的：

| 方面 | 说明 |
| --- | --- |
| session_id 跨重连保留 | fork 提交 `9ff37d944`。tdesktop 只在 key 变化或 resetSession 时换 session_id（`session_private.cpp:404-430`，`:2472-2479`）；TDLib 的 Session 跨 `on_closed` 存活（`Session.cpp:261-265`）；Android 断线不重建 session（`Connection.cpp:657-730`） |
| salt | 每小时拉一次 future salts，取 5 分钟后仍有效的那个（`mtproto/salt.go`）；收到 48 后更新 salt 并换新 msg_id 重发 |
| 重连退避 | 100ms 起指数增长到 5s，不限总时长，就绪后重置（`telegram/options.go`）。tdesktop 是 1ms 到 64s，TDLib 用限流加 backoff |
| 选址竞速 | `plain.connect` 并发拨号所有候选，先成功者胜出 |
| initConnection | 每条新连接都发，拿到 config 后再持久化 session |
| PFS 默认关闭 | 和 TDLib 一致（`use_pfs` 默认 false，会话数大于 1 时才强制开启，见 `tdlib:telegram/net/NetQueryDispatcher.cpp:369`）；tdesktop 和 Android 默认开启 |
| CDN 不用 PFS | 和三家一致 |
| `*_MIGRATE` | 自动迁移并重启主连接 |

修复后新增一致的（`refactor/session-lifecycle`）：

| 方面 | 对应修复 |
| --- | --- |
| 去重窗口与语义（重复的 ack 后忽略，太旧的重置 session，容器内逐条处理） | H0、H1、H2、L1 |
| 服务器时间差与 16/17 重发 | M1 |
| 请求跨连接保留（已 ack 的等结果，未 ack 的先查询状态） | H4、M3 |
| `msg_detailed_info`、`new_session_created` 补发与 getDifference | M2、M4 |
| bad_msg 32/33/64 后重发 | M5 |
| 存活检测"收到数据即存活"、网络变化触发重连 | M6、M7 |
| 出站打包容器 | L2 |
| -404 先验证再换 key | H3 |

## 修复结果

全部修复在分支 `refactor/session-lifecycle` 上完成，详细记录见 [CONNECTION_REFACTOR_PLAN.md](CONNECTION_REFACTOR_PLAN.md) 第 8、9 节。

| 结果 | 条目 |
| --- | --- |
| 已修复 | H0、H1、H2、H3、H4、M1、M2、M4、M5、M6、M7、L1、F1、F2 |
| 部分修复 | M3（断线场景已修复；连接内同 msg_id 重发是幂等的，不改）、L2（打包已完成；quick ack 没做） |
| 决定保持现状 | L4、L5 |
| 不做 | L3 剩下的细节（地址打分、IPv4 优先多等 2s、自动回退到 HTTP） |
| 暂缓 | 数据池连接跨重连保留 session（P5）：池按需建连，会导致等待中的请求挂起，而数据连接上的请求都可以安全重发 |

**仍然存在的差异：**

- 数据池连接每次重连都开新 session；断线时，已 ack 但还没拿到结果的请求会报错（不影响主连接）。
- 服务器时间差没有持久化：启动后第一个请求如果因时钟偏差被拒，会多一个往返。
- 没有 quick ack，也没有 tdesktop 那种按请求大小调整的接收超时。
