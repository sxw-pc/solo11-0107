# 建仓需求补充 v7：来源路径独立性合同

v3 的来源交付裁定以谱系末端 origin 作为 quorum 计数单位，适合“只关心不同末端发布域”的业务合同。本增量增加一种更严格的合同 `path_disjoint`：每条证据必须与其余证据在完整来源路径上无任何节点交集。两种合同都是明确的业务保证等级；同一批观测在不同合同下可以得出不同资格，不能把一种模式当作另一种的近似。

## 持久化业务规则

`feed_slot` 增加不可变列 `independence_basis`，取值 `terminal_origin`（缺省，保持 v3 语义）或 `path_disjoint`。首次登记后合同不可更改；需要不同保证时登记新槽位。`feed-slot` 命令可通过 `--independence-basis` 指定，也可在槽位夹具中声明 `independence_basis`；两者冲突或取值未知时拒绝登记。已有数据库在连接时以 `terminal_origin` 安全补齐该列。

`terminal_origin` 继续只比较 `ancestry[-1]`（即 `root_origin`）：共享中间中继但末端不同的路径仍分别计票。

`path_disjoint` 使用 `feed_lineage_head` 对应的完整有序 `ancestry`：

- 同一 producer 对同一 digest 只投一票；其完整 ancestry 的全部节点都属于该票的来源依赖集合。
- 对每个 digest，求 producer 之间“完整路径两两无节点交集”的最大集合。只有该最大集合大小达到 `quorum` 时槽位才 `accepted`。
- 求解使用 bitset 冲突图的精确 branch-and-bound，不是按 producer 字典序贪心，也不把 root 数量或所有节点并集长度当作票数。producer 以排序顺序访问、先探索包含分支，因此 witness 与观测到达顺序无关。
- 超过明确搜索上限（默认 200000 个搜索节点，可用 `CDEVERDICT_PATH_SEARCH_LIMIT` 收紧或放宽）仍无法证明最大值时，槽位为 `indeterminate`、`selected_digest` 为空，绝不按贪心结果放行。重新裁定可恢复确定结论。

被采信的 slot 在 verdict 中保存 `selected_producers` 与 `path_witness`；后者逐 producer 记录完整 `ancestry`、`version`、`event_id` 和 `root_origin`。同时保存每个 digest 的候选完整路径。谱系新版本会按现有逻辑重算受影响槽位、递增 slot revision 并写入新 `feed_verdict`；旧 `recovery_job` 绑定旧 revision 而转为 `stale`，`accepted` 槽位的任务转 `closed`。`indeterminate` 与 `collecting` 一样不关闭回补。

## 反例

`quorum=2`，同一 digest-D：

- A 路径 `[relay-a, shared-warehouse, root-a]`，B 路径 `[relay-b, shared-warehouse, root-b]`。`terminal_origin` 因末端不同而 `accepted`；`path_disjoint` 发现共享 `shared-warehouse`，最大互斥集合为 1，保持 `collecting` 并保留回补；若 B 的新谱系去掉 `shared-warehouse`，则重算为 `accepted` 并记录 B 的 v2 完整路径。
- A=`[relay-a,r1,r2]`，B=`[relay-b,r1,r3]`，C=`[relay-c,r2,r4]`。按字典序先选 A 的贪心只得到 1 票；精确最大集合是 B+C，槽位 `accepted`，witness 为 B/C。

## 运行

```powershell
python -m cdcverdict feed-slot tests/fixtures/feed_slot_path.json --db supply.sqlite
python -m cdcverdict feed-lineage tests/fixtures/feed_lineage_pa.json --db supply.sqlite
python -m cdcverdict feed-lineage tests/fixtures/feed_lineage_pb.json --db supply.sqlite
python -m cdcverdict feed-observe tests/fixtures/feed_observation_a.json --db supply.sqlite
python -m cdcverdict feed-observe tests/fixtures/feed_observation_b.json --db supply.sqlite
python -m cdcverdict supply-report --db supply.sqlite
python -m pytest -q tests/test_supply.py tests/test_supply_path_independence.py
```

`tests/test_supply_path_independence.py` 覆盖 legacy 末端兼容、共享中间节点反例、贪心非最优反例、重复 producer 单票、确定性 witness、输入顺序置换、lineage 变更后的 verdict/job 修订以及求解限额 fail-closed。此实现仍是本地 SQLite 与离线夹具，不提供真实联网采集或密码学生产者认证。
