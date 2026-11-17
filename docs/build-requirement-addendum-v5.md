# 建仓需求补充 v5：质量数据产品与竞争修复仲裁

本增量在同一仓库提供独立于 CDC 回放、来源交付、回补执行和主体授权的数据产品工作流。它处理离线订单事实，不读取 `replay_run`、`feed_slot`、`recovery_job` 或已有 `release_gate`，也不复用目标快照修复语义。

## 决定一：规则依赖影响与逐产品发布

`quality-contract` 冻结有向无环规则图和产品所需规则。规则可检查事实是否存在、非负、两事实相等或总额等于组成项之和；规则依赖失败时下游记录 `blocked`，而非假装其自身通过。`quality-batch` 冻结批次原文与 SHA-256，并持久化第 0 版有效事实、逐规则裁定、逐产品 `ready/blocked` 和阻断依据。例如 `net` 为负会使 `gross-balance`、`paid-match` 因依赖而阻断，会计与客户收据产品均不可发布；独立的履约产品仍可发布。

产品状态必须走实际的 `quality-publish`：它仅接受当前 assessment 下 `ready` 的产品，只投影该产品所需规则及其上游依赖引用的事实，导出包含该投影、规则裁定、批次/合同来源哈希和修订号的 JSON manifest，并在 `quality_publication` 保存同一原始字节、SHA-256、目标路径和 assessment ID。履约产品不会把仍有质量问题的财务事实顺带导出。`blocked` 产品拒绝导出，不创建文件。后续修订会产生新 assessment；旧发布物保留，但报告中标为非当前版本。

## 决定二：竞争修复提案与独立复核

`quality-propose` 为某个合同事实提交不可变的候选值、来源证据摘要、提案人和 `expected_revision`；旧修订提案不能覆盖新事实。`quality-review` 要求审核人与提案人不同，每项提案的审核结论及原文单独持久化。对同一事实的不同开放候选先判 `disputed`，即使其中一项已有审核通过也不会悄悄选胜者。败选方可用 `quality-withdraw` 留理由撤回。只有剩余唯一值获独立审核通过、且其它开放候选已被驳回或撤回时，`quality-apply` 才写入修正事实并重新运行整张依赖图。修正值被采纳不等于所有产品放行：修复总额后，付款额仍可能与新总额不等，会计继续阻断，收据可先发布。每次仲裁、原始事实、每版有效事实及裁定保存在 SQLite。

## 可复现流程

在仓库根目录运行：

```powershell
python -m cdcverdict quality-contract tests/fixtures/quality_contract.json --db quality.sqlite
python -m cdcverdict quality-batch tests/fixtures/quality_batch_bad_net.json --db quality.sqlite
python -m cdcverdict quality-propose tests/fixtures/quality_proposal_net_5.json --db quality.sqlite
python -m cdcverdict quality-propose tests/fixtures/quality_proposal_net_7.json --db quality.sqlite
python -m cdcverdict quality-apply --db quality.sqlite --batch orders-bad-net
python -m cdcverdict quality-withdraw --db quality.sqlite --proposal net-fix-7 --proposer ops-b --reason "invoice shows net 5"
python -m cdcverdict quality-review tests/fixtures/quality_review_net_5.json --db quality.sqlite
python -m cdcverdict quality-apply --db quality.sqlite --batch orders-bad-net
python -m cdcverdict quality-publish --db quality.sqlite --batch orders-bad-net --product accounting --out accounting.json
python -m cdcverdict quality-report --db quality.sqlite --batch orders-bad-net
python -m pytest -q
```

首次 `quality-apply` 返回 `disputed`、修订仍为 0；撤回并独立复核后返回 `applied`、修订为 1，会计产品变成 `ready`。`tests/test_quality.py` 还复现：上游 `net-nonnegative` 已通过但下游 `gross-balance` 失败；第一轮修正总额只放行收据，付款不匹配使会计继续阻断；第二轮修正付款后才可发布会计。测试验证阻断状态无导出文件、过期修订提案被拒、旧发布标为非当前，并通过 CLI 读取导出 manifest 的真实字节哈希。

审核人、提案人和来源摘要由离线样本声明；本项目未实现身份签名验证、实际支付结算或网络发布。SQLite 写入和输出文件写入不具备跨介质原子提交保证，故 manifest 只用于本地演示，不构成生产发布事务。
