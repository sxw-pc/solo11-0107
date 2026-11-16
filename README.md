# 变更流回放庭

面向迁移工程师的离线 CDC 可见性裁定。仅处理单表、单机合成快照和事务事件；不连接真实数据库。`python -m cdcverdict demo` 初始化、导入、回放并打印 JSON/Markdown 报告路径。`python -m pytest -q` 跑全部测试。

数据链：来源字节/哈希 → SnapshotRow/ChangeEvent → 事务完整性 → 键别名 → before 条件 → 可见行/墓碑 → Decision/Evidence → 报告。每次回放有独立 ReplayRun。事务事件状态为 begin→change→commit；运行状态为待接收→已校验→待核/已判定→已归档。身份冲突、缺 commit、before 矛盾和水位缺口不会被判为“已删除”。

错误码包括 snapshot_duplicate_key、watermark_regression、txn_incomplete、identity_conflict、before_mismatch、alias_cycle、alias_merge、stale_recreate。演示预期 A 在水位 3 的值为 `{"n":2}`。所有时间与序号均是逻辑水位，不含时区换算。初始化通过 SQLite `CREATE TABLE IF NOT EXISTS` 幂等执行；原始来源字节保留在 source_file.raw。报告失败时不会标记运行归档。

同一事务的 change 必须全部落在 begin 与 commit 之间。回放按提交顺序先在内存中验证整组变更，任一 before、键别名或删除/重建规则失败时，整组都不发布，所有受影响业务键均判为无法判定。这防止同一事务中部分键已更新、另一键失败却仍出具确定结论。`pytest` 固定从本仓 `src` 导入源码，避免误测系统中旧版安装包。

基线快照第一次导入后连同字节哈希、水位和 schema 版本一起冻结；相同字节可重跑，改动任一契约项会拒绝。同一事务即使冲突发生在没有业务键的 commit 事件，也会使该事务所有受影响键无法判定。报告中的问题和被排除事务按 ReplayRun 固定，后续回放不会改写旧报告的范围。

目标切换核对是独立的业务链：`target-import` 冻结目标副本的原始字节和水位，`cutover-audit` 将目标逐键状态与指定回放运行比较，持久记录缺失、额外或值不同的行，并输出 ready、lagging、divergent 或 unknown。归档的回放报告并不能替代目标副本一致性结论。

下游采用决策由 `consumer-contract`、`consumer-ack` 和 `release-gate` 执行。关键消费者的最低水位与对目标来源哈希的接受回执均须满足；目标在审计后变化会阻断切换。合同、回执和每次闸门的成员状态保存在 SQLite 中。`demo-decisions` 可运行一个目标一致且消费者已接受的完整样例。测试还覆盖目标落后、值分歧、缺失/拒绝回执、重复回执冲突和审计后目标变化。

目标有实际差异时，可用 `repair-plan` 导入逐键决议清单，然后以 `repair-apply` 生成写时复制的新目标快照。每个差异键必须明确选择 `restore_replay`、`accept_target` 或 `defer` 并给出理由；清单、逐键原值与期望值、结果快照均保存在 SQLite。接受目标差异或暂缓处理不会伪装成目标一致；只恢复部分键会留下可再次审计的剩余差异。审计后的目标被修改时修复拒绝应用。`repair-report` 展示决议与结果，`demo-repair` 演示分两次修复额外目标行并复核至 ready。新快照有新的来源哈希，旧消费者回执不能使它通过 release gate。

墓碑保留线由 `configure_retention` 一次性配置、不可更改：`full_history` 保留全部删除历史，任意旧水位都可重放，禁止压缩；`retention_line` 允许 `compact` 在水位 F 之前压缩事件与墓碑。压缩把 F、逐键最后删除水位、before 校验摘要、键别名谱系和每个旧 ReplayRun 的报告范围摘要永久写入 `retention_compaction`（附 SHA-256 摘要，只增不改），随后只删除未被证据、问题或别名引用的完整事务事件，绝不拆分事务、绝不改写旧运行的决策与证据。压缩后 W<F 的新查询一律判为“历史已过期”（运行与逐键状态），不会返回存在/已删除；W>=F 的回放从压缩摘要恢复 live/墓碑/别名状态，别名链、stale_recreate 与 before 校验继续生效，错误 before 不能复活已删除键。报告附带当前保留合同与压缩摘要，旧运行重新出报告时决策、证据、问题与排除范围逐项不变。
