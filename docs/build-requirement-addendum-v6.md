# 建仓需求补充 v6：仓库用量周期关账与受控重述

本增量处理仓库计算产品的**已计量用量、合同额度、关账应收与迟到更正**。它不读取 CDC 快照/事件/回放、目标切换、主体同意导出、来源 quorum、回补队列或质量数据产品表。独立账本由 `period_store.py` 创建，业务入口分别在 `period_usage.py` 和 `period_close.py`，通过 `python -m cdcverdict period-*` 命令操作。

## 两项改变业务结果的决定

**签约额度取舍。** 合同冻结租户、期间窗口、产品单价、产品专属额度、共享额度及共享额度使用优先级。归属签约人从候选合同中选定一份，选择后不能换约；同租户已选期间不能重叠。关账先用每个产品的专属额度，再按选定优先级分配共享额度，剩余单位按各产品单价计费。样例固定看板 6 单位、训练 5 单位，专属看板 2、共享 5：训练优先合同时应收 400 分；看板优先合同时应收 1600 分。总用量均为 11，普通分组求和无法得出这两个不同结论。每个产品的实际用量、两种额度使用量、计费单位和应收金额逐行入库。

**迟到更正与授权重述。** 原始计量文件、逐条事件、修订序号、更正原文及 SHA-256 保持不可变。期间关闭后新计量或更正只把当前期间标为 `needs_restatement`，不改写旧 `period_close` 与逐事件/逐产品快照。重述申请须指向当前关账版本并冻结候选来源摘要；仅合同列明、且不同于申请人的财务复核人可用证明批准。申请后又有新来源时，旧申请拒绝执行，必须重新申请。批准后原子创建下一版关账、记录旧新版差额并更新当前指针，旧版仍可查询。样例训练用量迟到更正 5→3 后，共享额度有 2 单位转给看板，应收从 400 分变为 200 分；变化发生在另一个产品，不能只对训练记录做局部减法。

## 离线运行

```powershell
python -m cdcverdict period-contract tests/fixtures/period_contract_train_first.json --db period.sqlite
python -m cdcverdict period-choose tests/fixtures/period_choice_train_first.json --db period.sqlite
python -m cdcverdict period-import tests/fixtures/period_usage.json --db period.sqlite
python -m cdcverdict period-close --db period.sqlite --tenant tenant-a --period 2026-09 --as-of 2026-10-02T00:00:00Z --actor ops-close
python -m cdcverdict period-import tests/fixtures/period_correction_late.json --db period.sqlite
python -m cdcverdict period-restate-request tests/fixtures/period_restatement_request.json --db period.sqlite
python -m cdcverdict period-restate-approve --db period.sqlite --request restate-2026-09-1 --reviewer finance-a --proof-sha finance-proof-1
python -m cdcverdict period-report --db period.sqlite --tenant tenant-a --period 2026-09
python -m pytest -q tests/test_period_close.py
```

反例覆盖未签约或换约、提前关账、无权关账、申请人自审或非授权复核、申请后来源再变化、跳号更正、事件 ID 跨类别复用，以及失败输入整批回滚。合同签约人、财务复核人和 `--as-of` 都是离线输入身份/时间；此项目不提供外部签名验真或可信时钟。关账应收并非银行实收/回款匹配，不能据此声称已付款。
