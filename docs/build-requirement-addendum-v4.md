# 建仓需求补充 v4：回补执行租约与公平调度

v3 的来源交付裁定回答“是否需要回补”，原有 `recovery_job` 容量账回答“规划时哪项工作优先”。本增量回答执行阶段的两个不同问题：哪个 worker 现在拥有提交权，以及多个租户的待执行工作如何取得有限执行槽位。它不改变 CDC 行可见性和来源谱系判定。

## 持久化业务规则

`dispatch_worker` 登记执行 worker 的分区技能与并行容量。`dispatch_task` 把一个仍有效的 `recovery_job` 绑定到租户和租约时长；同一回补工作只能排入一个执行任务。`dispatch_tick` 在 SQLite 写事务中推进单调逻辑时钟，先取消已失效的回补任务、回收过期租约，再按每个分区持久化的 `dispatch_cursor` 在有待执行工作的租户之间轮转。每次领取产生递增 generation 与到期 tick，并写入 `dispatch_event`。容量约束按 worker 的当前活跃租约数计算；过期工作在有空闲替代 worker 时迁移。

worker 可在到期前用原 worker ID、任务 ID 和 generation 调用 `dispatch-renew`。完成时必须提供非空采集证据文件；服务计算 SHA-256 并把原始字节保存到 `dispatch_artifact`，随后原子地将执行任务与关联 `recovery_job` 标记为完成。过期、被替换、非当前 worker 或来源修订已变化的提交均拒绝，不写入完成证据。即使后来又分配给同一 worker，旧 generation 也不能复用。`dispatch-report` 展示租约、轮转游标、来源摘要和完整事件序列。

逻辑 tick 由调用方提供，不代表墙上时间；调度者必须单调推进。`BEGIN IMMEDIATE` 使多个本地进程对同一 SQLite 文件的领取、续租、提交串行化。此实现使用本地 SQLite，不宣称跨主机分布式共识或真实联网采集。

## 可运行链路

从仓库根目录运行以下命令。`dispatch_acquisition.txt` 是本地可复现的采集结果样本。

```powershell
python -m cdcverdict feed-slot tests/fixtures/feed_slot.json --db dispatch.sqlite
python -m cdcverdict feed-lineage tests/fixtures/feed_lineage_a.json --db dispatch.sqlite
python -m cdcverdict feed-observe tests/fixtures/feed_observation_a.json --db dispatch.sqlite
python -m cdcverdict recovery-worker tests/fixtures/recovery_worker.json --db dispatch.sqlite
python -m cdcverdict recovery-request tests/fixtures/recovery_low.json --db dispatch.sqlite
python -m cdcverdict dispatch-worker tests/fixtures/dispatch_worker_a.json --db dispatch.sqlite
python -m cdcverdict dispatch-queue tests/fixtures/dispatch_task_low.json --db dispatch.sqlite
python -m cdcverdict dispatch-tick --db dispatch.sqlite --tick 1
python -m cdcverdict dispatch-renew --db dispatch.sqlite --task dispatch-low --worker exec-a --generation 1 --tick 2
python -m cdcverdict dispatch-complete --db dispatch.sqlite --task dispatch-low --worker exec-a --generation 1 --tick 3 --artifact tests/fixtures/dispatch_acquisition.txt
python -m cdcverdict dispatch-report --db dispatch.sqlite
```

`tests/test_dispatch.py` 另复现三种不同结论：租约过期后从 `exec-a` 换到 `exec-b`，旧 generation 无法提交而新 worker 可以；持续续租使同一任务在原到期点不被夺走；容量为 1 时 `tenant-a` 连续排入两项、`tenant-b` 排入一项，实际领取次序为 A1、B1、A2。来源新观测使回补工作失效时，原执行任务取消且证据不能提交。`python -m pytest -q` 运行全仓测试。
