# DemoLens S2

DemoLens S2 是一个可离线运行的 CS2 Source 2 `.dem` 提取器，用于把 demo 转成后续分析、复核和片段定位可直接消费的结构化数据。

## 已实现能力

- 解析 `demo header`
- 解析 `player list`
- 解析 `per-tick player state`
- 解析 `player_death / kill events`
- 输出明确诊断：`status`、`failure_stage`、`failure_reason`、stage 级统计
- 失败不会静默返回空结果
- 全程本地单机运行，不依赖数据库或 Web 后端

已验证可解析样本：

- `data/xifu.dem`
- `data/2026.5.1-1.dem`
- `data/2026.5.1-2.dem`

## 架构

项目采用分层设计：

- `pipeline`：编排解析阶段和诊断
- `backends`：具体 demo 解析后端
- `normalize`：把后端结果归一化成稳定 schema
- `exporters`：导出 `json / jsonl / parquet`
- `cli`：命令行入口

默认后端是 `CsDemoAnalyzerBackend`，并保留 `Demoparser2Backend` 作为兼容路径。

## 安装

```bash
python -m pip install -e .
```

## 使用

```bash
python -m demolens_s2 data/xifu.dem -o out
```

可选参数：

- `--ticks-format jsonl`
- `--ticks-format parquet`

如需指定 `cs-demo-analyzer` 可执行文件：

- 环境变量：`DEMOLENS_CSDA_PATH`
- 或在后端初始化时传入 `executable_path`

## 输出文件

默认输出以下文件：

- `demo_meta.json`
- `players.json`
- `ticks.jsonl` 或 `ticks.parquet`
- `kills.json`
- `diagnostics.json`

### 关键字段

`header`：

- `map_name`
- `demo_version_name`
- `demo_version_guid`
- `patch_version`
- `fullpackets_version`
- `server_name`

`players`：

- `steamid`
- `name`
- `team_number`

`ticks`：

- `tick`
- `steamid`
- `X`, `Y`, `Z`
- `pitch`, `yaw`
- `is_alive`
- `active_weapon`
- `health`

`kills`：

- `tick`
- `attacker_steamid`
- `victim_steamid`
- `attacker_name`
- `victim_name`
- `weapon`
- `headshot`
- `wallbang`
- `through_smoke`
- `round_num`
- `time_sec`
- `round_time_sec`

## 诊断语义

每次提取都会写入 stage 级诊断，字段包括：

- `status`
- `failure_stage`
- `failure_reason`
- `stages`
- `warnings`
- `notes`

常见失败分类：

- `unsupported_patch`
- `entity_not_found`
- `parse_failed`

CLI 在失败时返回非零退出码，便于上层流水线判断这次结果不可用。

## 数据约定

- JSON 序列化使用 `ensure_ascii=False`
- 所有数值保持为 Python 原生类型
- 概率、分数保留 6 位小数
- 失败不能伪装成正常空结果

## 测试

```bash
python -m pytest -q
```

当前仓库已通过真实 demo 回归和基础单测。

## 目录

- `demolens_s2/`：核心代码
- `tests/`：回归与单元测试
- `data/`：本地 demo 样本
- `docs/`：设计和开发记录

## 备注

如果后续要接入 review pipeline，建议直接消费 `diagnostics.json` 和标准化后的 `players / ticks / kills` 输出，不要依赖内部后端细节。
