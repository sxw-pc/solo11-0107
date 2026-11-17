# 建仓需求补充 v3：来源交付与回补容量

本补充是仓库首版要求的版本化增量。原始建仓提示词保留历史，不更改 CDC 回放和主体授权的业务语义。

## 独立工作流

来源交付账先冻结分区交付槽位和独立生产者门槛。各生产者提交事件 ID、内容摘要、行数和采录时间，原始 JSON 字节与 SHA-256 持久化。同一生产者重复报告不能凑成两票；两个独立生产者对同一摘要达成门槛才可采信。不同摘要同时存在时，已采信槽位也会重开为争议，旧结论保存在 `feed_verdict`。这项决定回答“哪份上游交付可信”，不依赖 `replay_run`，也不改变 CDC 事务。

回补容量账独立登记带分区技能与单位容量的采集工人，再对缺失或争议槽位提交带优先级、资源单位和证据版本的回补请求。容量不足时低优先级任务进入等待；高优先级请求到达会重新安排占用。完成任务释放容量并提升等待者。新来源证据改变槽位修订后，旧任务成为 `stale`，必须用新请求重排；来源已被采信的任务转 `closed`。这项决定回答“哪项采集工作先获得有限资源”，有独立任务和事件台账。

二者的跨系统链路是：交付争议创建有依据的回补需求；晚到证据会使已分配资源失效，避免按过期理由继续采集；两名独立生产者达成一致则关闭不必要回补。报告从 `feed_observation`、`feed_verdict`、`recovery_job` 与 `recovery_event` 重建两个决定及其版本，不把尚未交付误判为 CDC 回放失败。

## 运行与反例

在仓库根目录运行：

```powershell
python -m cdcverdict feed-slot tests/fixtures/feed_slot.json --db supply.sqlite
python -m cdcverdict feed-slot tests/fixtures/feed_slot_east.json --db supply.sqlite
python -m cdcverdict recovery-worker tests/fixtures/recovery_worker.json --db supply.sqlite
python -m cdcverdict feed-observe tests/fixtures/feed_observation_a.json --db supply.sqlite
python -m cdcverdict recovery-request tests/fixtures/recovery_low.json --db supply.sqlite
python -m cdcverdict recovery-request tests/fixtures/recovery_high.json --db supply.sqlite
python -m cdcverdict feed-observe tests/fixtures/feed_observation_b.json --db supply.sqlite
python -m cdcverdict feed-observe tests/fixtures/feed_observation_conflict.json --db supply.sqlite
python -m cdcverdict supply-report --db supply.sqlite
python -m pytest -q
```

两个生产者一致后 `north-day` 曾为 `accepted`，第三个摘要不同会使它重开为 `disputed`。`recover-low` 初获资源，`recover-high` 到达后优先占用；完成后低优先级任务晋升。迟到证据使旧修订绑定的任务 `stale`，不能再报告其仍占用工人。测试使用临时库，不要求真实数据源。

本工作流不提供真实联网采集、生产者密码学认证或分布式调度；生产者身份由离线夹具声明，判定结果仅用于本地合成数据工程演示。
