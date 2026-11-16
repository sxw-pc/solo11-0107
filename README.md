# 变更流回放庭

面向迁移工程师的离线 CDC 可见性裁定。仅处理单表、单机合成快照和事务事件；不连接真实数据库。`python -m cdcverdict demo` 初始化、导入、回放并打印 JSON/Markdown 报告路径。`python -m pytest -q` 跑全部测试。

数据链：来源字节/哈希 → SnapshotRow/ChangeEvent → 事务完整性 → 键别名 → before 条件 → 可见行/墓碑 → Decision/Evidence → 报告。每次回放有独立 ReplayRun。事务事件状态为 begin→change→commit；运行状态为待接收→已校验→待核/已判定→已归档。身份冲突、缺 commit、before 矛盾和水位缺口不会被判为“已删除”。

错误码包括 snapshot_duplicate_key、watermark_regression、txn_incomplete、identity_conflict、before_mismatch、alias_cycle、alias_merge、stale_recreate。演示预期 A 在水位 3 的值为 `{"n":2}`。所有时间与序号均是逻辑水位，不含时区换算。初始化通过 SQLite `CREATE TABLE IF NOT EXISTS` 幂等执行；原始来源字节保留在 source_file.raw。报告失败时不会标记运行归档。

同一事务的 change 必须全部落在 begin 与 commit 之间。回放按提交顺序先在内存中验证整组变更，任一 before、键别名或删除/重建规则失败时，整组都不发布，所有受影响业务键均判为无法判定。这防止同一事务中部分键已更新、另一键失败却仍出具确定结论。`pytest` 固定从本仓 `src` 导入源码，避免误测系统中旧版安装包。

基线快照第一次导入后连同字节哈希、水位和 schema 版本一起冻结；相同字节可重跑，改动任一契约项会拒绝。同一事务即使冲突发生在没有业务键的 commit 事件，也会使该事务所有受影响键无法判定。报告中的问题和被排除事务按 ReplayRun 固定，后续回放不会改写旧报告的范围。
