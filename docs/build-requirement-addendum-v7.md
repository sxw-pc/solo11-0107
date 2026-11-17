# 建仓需求补充 v7：用途处理方合同与冻结收件方链

本增量是主体授权工作流（v2）的版本化扩展，不改变 CDC 回放、目标切换、消费者闸门、来源交付、回补、质量数据产品或周期关账语义。它回答的是 v2 留下的一个独立业务问题：同一用途的同一份有效授权，**哪个处理方可以实际收到值**，以及迟到撤销后由谁负责召回。

## 两类合同

`processing_contract` 按用途冻结处理方模式，`consent_processor` 记录主体明确指定的处理方（来自授权原文）：

- `direct_only`：只有 `consent_processor` 为“该主体 + 该用途”明确列出的处理方可以直接收取。即使调用方指定了同一用途的另一收件方（例如 `agency`），导出也整体排除，不写 `export_row`、`export_chain` 或链成员。`direct_only` 不提供任何转交路径。
- `delegated_chain`：被允许的处理方可以针对**同一用途**把窄化的处理权限委托给子处理方。每条委托绑定父委托、主体、用途、有效水位区间与单调 revision。子委托的有效起点不得早于父链交集，到期水位不得晚于父链交集；环、被撤销的祖先或断裂的父链都不构成有效链。

旧的 owner/主体授权语义保留：没有处理合同的用途仍按 `consent_event` 的 grant/revoke/expires 决定可见性，导出链退化为单节点 `legacy`，历史测试与 `demo-consent` 不受影响。

## 冻结与追索

`export-create` 在写入任何 `export_row` 之前先为每个主体解析完整处理方链；任一环、超期、越权或祖先撤销都会让该主体整体被排除，绝不会先成功写入父收件方再发现子链无效。成功时保存 `export_chain`（主体、模式、合同、链摘要）与 `export_chain_member`（每一跳的处理方及委托 revision），导出值仍只来自指定 `ReplayRun` 的可见行与有效 `consent_event`。

迟到撤销仍通过 `export-audit`/`import_consent` 处理，但追索范围是该导出的冻结链：链上每个曾可能持有副本的处理方各自得到一条 `export_recall`。`recall-ack` 只接受该条召回对应的处理方；子处理方的回执不会关闭父处理方的另一份副本，重复审计与一致回执保持幂等。

重新 grant 只允许新的导出建立新的授权链，旧链的待召回副本保持 `recall_pending`，不会因 regrant 变回 `valid`；旧委托若已撤销，仅靠新 grant 也不能恢复委托，需要新的有效委托链。

## 命令与反例

```powershell
python -m cdcverdict consent-import tests/fixtures/consent_grant.json --db data.db
python -m cdcverdict processing-import tests/fixtures/consent_processing.json --db data.db
python -m cdcverdict export-create --db data.db --run 1 --purpose research --recipient agency --contract pc-delegated-research
python -m cdcverdict export-create --db data.db --run 1 --purpose research --recipient agency --contract pc-direct-research
python -m cdcverdict consent-import tests/fixtures/consent_withdraw_late.json --db data.db
python -m cdcverdict export-audit --db data.db --export 1
python -m cdcverdict export-report --db data.db --export 1
```

`tests/test_consent_recipient_chain.py` 覆盖：`direct_only` 拒绝未列名处理方、有效委托允许子处理方、子链超期/越权/祖先撤销/环拒绝、导出前原子阻断、迟到撤销为链上每个实际副本建立召回、单个 ACK 不关闭其他副本、迟到 revoke 后 regrant 不复活旧 export，以及完整重复流程的幂等。处理方与委托证据均由离线夹具声明，本项目不提供密码学签名或跨主机共识。
