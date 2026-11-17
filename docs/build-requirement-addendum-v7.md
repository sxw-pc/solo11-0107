# 建仓需求补充 v7：质量产品的操作型/审计型交付合同与完整修正链

本增量不改变 v5 的规则依赖裁定、竞争修复仲裁和发布字节登记，只扩展质量产品合同与 `quality-publish` manifest。历史数据库与已发布 manifest 的原始字节保持不变；未声明交付方式的产品按 `operational` 处理。

## 两项改变业务结果的决定

**按产品声明交付合同。** 合同的每个产品可声明 `delivery` 为 `operational`（默认）或 `audit`，其它取值在 `quality-contract` 登记时即被拒绝。操作型产品的 manifest 只投影该产品所需规则闭包内的字段、当前有效事实和规则裁定，不附带任何修复凭据或无关字段；履约产品不会因财务字段的修正链而获得 `net` 等额外数据。审计型产品在同样的字段投影之外，必须为每个投影事实给出从冻结批次源事实到当前有效值的完整修正链。

**修正链只来自原始仲裁记录，且必须唯一可验证。** 链由 SQLite 中不可变的 `quality_proposal`、`quality_review`、`quality_arbitration` 行按每次 applied 仲裁绑定的 assessment 逐跳重建：每跳记录旧值/新值、revision、proposal_id、evidence_sha、独立 reviewer 与 review proof_sha，除最后一跳外均标记 `superseded: true`；从未修正的字段给出空链。重建过程校验相邻跳的值连续性（下一跳旧值等于上一跳新值，首跳旧值等于冻结源值）、每跳提案处于 applied 状态、存在与提案人不同的 approve 复核，并要求当前 `quality_correction` 行的 proposal/revision/值与链尾完全一致。任何投影字段连不到唯一已采纳且独立复核的链，审计型产品拒绝发布且不写文件；操作型产品不做链校验，仍按其声明字段发布。旧 `quality_publication` 的 raw、manifest_sha 与 assessment 绑定不可改写，后续修订产生新发布行，报告中旧发布标为非当前。

## 可复现流程

在仓库根目录运行：

```powershell
python -m cdcverdict quality-contract tests/fixtures/quality_contract_audit.json --db quality-audit.sqlite
python -m cdcverdict quality-batch tests/fixtures/quality_batch_audit.json --db quality-audit.sqlite
python -m cdcverdict quality-propose tests/fixtures/quality_proposal_audit_net_5.json --db quality-audit.sqlite
python -m cdcverdict quality-review tests/fixtures/quality_review_audit_net_5.json --db quality-audit.sqlite
python -m cdcverdict quality-apply --db quality-audit.sqlite --batch orders-audit
python -m cdcverdict quality-propose tests/fixtures/quality_proposal_audit_net_6.json --db quality-audit.sqlite
python -m cdcverdict quality-review tests/fixtures/quality_review_audit_net_6.json --db quality-audit.sqlite
python -m cdcverdict quality-apply --db quality-audit.sqlite --batch orders-audit
python -m cdcverdict quality-publish --db quality-audit.sqlite --batch orders-audit --product fulfillment --out fulfillment.json
python -m cdcverdict quality-publish --db quality-audit.sqlite --batch orders-audit --product accounting-ops --out accounting-ops.json
python -m cdcverdict quality-publish --db quality-audit.sqlite --batch orders-audit --product accounting-audit --out accounting-audit.json
python -m pytest -q tests/test_quality.py
```

样例中 `net` 经两次独立审核的修正从 -5 到 5 再到 6：`fulfillment` 与 `accounting-ops` 的 manifest 不含 `correction_chains` 或无关字段；`accounting-audit` 的 manifest 含 `-5→5`（superseded）与 `5→6` 两跳完整凭据，未修正的 `gross`、`paid`、`tax` 为空链。反例覆盖复核缺失或驳回（含被取代的旧跳）、当前 correction 被改绑或改值、无仲裁来源的孤儿 correction、跨产品字段/凭据泄漏、非法 delivery 取值，以及第三轮修正后旧 manifest 原始字节与 `quality_publication` 行保持不变。

审计型 manifest 只证明库内记录自洽且可逐跳核验；本项目不验证审核人身份签名，也不保证 SQLite 与输出文件之间的跨介质原子提交。
