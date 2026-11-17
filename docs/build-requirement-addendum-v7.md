# 建仓需求补充 v7：期间合同签约修订与事件时间定价

本增量在 v6 的周期关账账本上加**期间内签约条款变更（amendment）**。它不读取 CDC 回放、目标切换、主体同意、来源 quorum、回补队列或质量数据产品表；表结构在 `period_store.py`，签署入口在 `period_usage.py`，定价与关账在 `period_close.py`，CLI 增加 `period-amend`。

## 两项改变业务结果的决定

**不可变签约修订。** amendment 由合同签约人签署并附证明，只可修改产品单价，绑定原合同 SHA-256、租户、期间、`effective_at` 与 `amendment_id`；同名字节可幂等重放，改动任一字段即拒绝。`effective_at` 必须落在合同期间窗口内，价格只能覆盖已签约产品且为非负整数分。修订文本进入 `source_digest`：已关账期间新增修订只把当前状态置为 `needs_restatement`，不改写旧 `period_close` 与 `period_line`。

**事件时间定价。** `prospective` 条款仅按用量事件的 `occurred_at` 判断是否采用新单价（`occurred_at >= effective_at` 才用新价）；`retroactive` 条款按签署文本把本期间全部有效用量追溯改价。更正事件的 `observed_at` 只表示更正何时被发现，绝不替代被更正用量的 `occurred_at`。同一产品多份修订按 (`effective_at`, amendment_id) 顺序折叠，后签署文本覆盖先签署文本。专属/共享额度与优先级仍只按原合同计算一次，并按事件发生先后吸收额度；迟到更正改变额度归属时，全产品行与逐事件计费快照一起重算。样例：看板 6/训练 5 单位关账 400 分，训练迟到更正 5→3 后看板有 2 个计费单位；同一 amendment 把看板单价 100 改为 300、`effective_at` 2026-09-20，而看板事件 `occurred_at` 2026-09-15、更正 `observed_at` 2026-10-03——prospective 仍为 200 分，retroactive 重算为 600 分。

## 受控重述

重述申请绑定当前旧关账与包含全部 amendment 的候选来源摘要；仅合同列明且不同于申请人的财务复核人可批准。申请后来源或 amendment 再变时旧申请拒绝执行，必须重新申请。批准原子生成下一版关账、逐产品记录旧新应收差额（`period_restatement_line`），并把该版本与生效 amendment 绑定（`period_close_amendment`）；旧版本与其逐事件定价快照保持可查询。

```powershell
python -m cdcverdict period-amend tests/fixtures/period_amendment_retroactive.json --db period.sqlite
python -m cdcverdict period-restate-request tests/fixtures/period_restatement_request.json --db period.sqlite
python -m cdcverdict period-restate-approve --db period.sqlite --request restate-2026-09-1 --reviewer finance-a --proof-sha finance-proof-1
python -m pytest -q tests/test_period_amendment.py
```

反例覆盖篡改/缺字段修订、非签约人、未知条款、未签约产品、负价、窗口外 `effective_at`、指向未选定合同、申请人自审、非授权复核、申请后修订再变化。签约人、复核人与时间均为离线输入；此项目不提供外部签名验真或可信时钟，关账应收并非银行实收。
