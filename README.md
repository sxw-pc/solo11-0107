# 变更流回放庭

面向迁移工程师的离线 CDC 可见性裁定。仅处理单表、单机合成快照和事务事件；不连接真实数据库。`python -m cdcverdict demo` 初始化、导入、回放并打印 JSON/Markdown 报告路径。`python -m pytest -q` 跑全部测试。

数据链：来源字节/哈希 → SnapshotRow/ChangeEvent → 事务完整性 → 键别名 → before 条件 → 可见行/墓碑 → Decision/Evidence → 报告。每次回放有独立 ReplayRun。事务事件状态为 begin→change→commit；运行状态为待接收→已校验→待核/已判定→已归档。身份冲突、缺 commit、before 矛盾和水位缺口不会被判为“已删除”。

错误码包括 snapshot_duplicate_key、watermark_regression、txn_incomplete、identity_conflict、before_mismatch、alias_cycle、alias_merge、stale_recreate。演示预期 A 在水位 3 的值为 `{"n":2}`。所有时间与序号均是逻辑水位，不含时区换算。初始化通过 SQLite `CREATE TABLE IF NOT EXISTS` 幂等执行；原始来源字节保留在 source_file.raw。报告失败时不会标记运行归档。

同一事务的 change 必须全部落在 begin 与 commit 之间。回放按提交顺序先在内存中验证整组变更，任一 before、键别名或删除/重建规则失败时，整组都不发布，所有受影响业务键均判为无法判定。这防止同一事务中部分键已更新、另一键失败却仍出具确定结论。`pytest` 固定从本仓 `src` 导入源码，避免误测系统中旧版安装包。

基线快照第一次导入后连同字节哈希、水位和 schema 版本一起冻结；相同字节可重跑，改动任一契约项会拒绝。同一事务即使冲突发生在没有业务键的 commit 事件，也会使该事务所有受影响键无法判定。报告中的问题和被排除事务按 ReplayRun 固定，后续回放不会改写旧报告的范围。

目标切换核对是独立的业务链：`target-import` 冻结目标副本的原始字节和水位，`cutover-audit` 将目标逐键状态与指定回放运行比较，持久记录缺失、额外或值不同的行，并输出 ready、lagging、divergent 或 unknown。归档的回放报告并不能替代目标副本一致性结论。

下游采用决策由 `consumer-contract`、`consumer-ack` 和 `release-gate` 执行。关键消费者的最低水位与对目标来源哈希的接受回执均须满足；目标在审计后变化会阻断切换。合同、回执和每次闸门的成员状态保存在 SQLite 中。`demo-decisions` 可运行一个目标一致且消费者已接受的完整样例。测试还覆盖目标落后、值分歧、缺失/拒绝回执、重复回执冲突和审计后目标变化。

目标有实际差异时，可用 `repair-plan` 导入逐键决议清单，然后以 `repair-apply` 生成写时复制的新目标快照。每个差异键必须明确选择 `restore_replay`、`accept_target` 或 `defer` 并给出理由；清单、逐键原值与期望值、结果快照均保存在 SQLite。接受目标差异或暂缓处理不会伪装成目标一致；只恢复部分键会留下可再次审计的剩余差异。审计后的目标被修改时修复拒绝应用。`repair-report` 展示决议与结果，`demo-repair` 演示分两次修复额外目标行并复核至 ready。新快照有新的来源哈希，旧消费者回执不能使它通过 release gate。

主体授权与导出追索是另一条业务链：`consent-import` 保存主体到业务键的固定归属、用途授权/撤销版本和原始证明；`export-create` 只从指定 `replay_run` 的可见值选取当前用途许可的数据，并持久化导出行与排除原因。迟到撤销不会改写旧导出字节，`export-audit` 为旧导出逐键创建待召回记录，`recall-ack` 仅接受原收件人的回执。`demo-consent` 展示授权导出、迟到撤销、旧导出召回、新导出排除和回执闭环。CLI 示例：

```text
python -m cdcverdict consent-import tests/fixtures/consent_grant.json --db data.db
python -m cdcverdict export-create --db data.db --run 1 --purpose research --recipient lab
python -m cdcverdict consent-import tests/fixtures/consent_withdraw_late.json --db data.db
python -m cdcverdict export-audit --db data.db --export 1
python -m cdcverdict export-report --db data.db --export 1
python -m cdcverdict recall-ack --db data.db --recall 1 --recipient lab --proof-sha receipt-proof-1
```

授权撤回的对象是数据主体与用途及其已发出的值副本；它不改变 CDC 事务订阅、消费者回执、目标修复或报告归档。具体建仓增量和边界记录在 `docs/build-requirement-addendum-v2.md`。

来源交付与回补容量是另一个独立工作流：`feed-slot` 登记交付期望，`feed-lineage` 为生产者登记带原文和版本的上游来源谱系，`feed-observe` 根据独立上游 origin 的摘要一致性给出采信或争议，`recovery-worker`、`recovery-request`、`recovery-complete` 维护有限采集资源的优先分配。同源转述不能凑 quorum；晚到冲突或谱系变更会重开已采信交付并使旧版本的回补任务失效。`supply-report` 从 SQLite 展示两套决定及历史证据。完整 CLI 和反例见 [建仓需求补充 v3](docs/build-requirement-addendum-v3.md)。

回补执行租约是与来源采信、规划容量不同的操作决定。`dispatch-worker` 登记实际执行池，`dispatch-queue` 将有效回补工作挂到租户，`dispatch-tick` 在持久化逻辑时钟上按租户轮转领取容量；`dispatch-renew` 延长当前租约，`dispatch-complete` 必须持当前 worker/generation 和采集证据文件才能完成。过期后可跨 worker 接管，旧 token 被 fencing；原始完成证据、SHA-256 和每次领取/过期/完成事件保存在 SQLite。命令与反例见 [建仓需求补充 v4](docs/build-requirement-addendum-v4.md)。

质量数据产品是另一条独立链：`quality-contract` 定义无环规则依赖图与逐产品要求，并按产品声明 `operational` 或 `audit` 交付合同；`quality-batch` 对离线订单事实逐层裁定并隔离受影响的产品。`quality-propose`、`quality-review`、`quality-withdraw` 和 `quality-apply` 对同一事实的竞争修复方案做独立审核与仲裁，保留每版事实及结论。`quality-publish` 只为当前 `ready` 产品导出并持久化带来源哈希的 manifest：操作型产品只含其声明字段、规则与当前有效值，审计型产品额外为每个投影事实给出从冻结源事实到当前值的完整修正链（逐次旧值/新值、revision、proposal、evidence 与独立 review proof，并标明被取代记录），链只从 SQLite 原始 proposal/review/arbitration 记录按 assessment 重建，任一当前值连不到唯一已采纳且独立复核的链即拒绝发布；修复一个规则可能仍无法放行下游产品。完整流程和反例见 [建仓需求补充 v5](docs/build-requirement-addendum-v5.md) 与 [v7](docs/build-requirement-addendum-v7.md)。

仓库计算用量的周期关账是独立账本：`period-contract`、`period-choose` 冻结有效合同及产品额度优先级，`period-import` 留存原始计量与更正，`period-close` 把产品专属额度、共享额度和应收逐行冻结。迟到更正只标记待重述，不改旧关账；`period-restate-request` 与 `period-restate-approve` 经独立授权后生成下一版和差额。它不计算 CDC 可见行，也不判断银行回款是否到账。命令、合同取舍及反例见 [建仓需求补充 v6](docs/build-requirement-addendum-v6.md)。
