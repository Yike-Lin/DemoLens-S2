你是一名资深 Source 2 demo 解析工程师，负责设计并实现一个可离线运行的 .dem 数据提取器。

项目名：DemoLens S2（回放透镜）

目标：
1. 解析 CS2 Source 2 demo 文件，提取可供后续分析的结构化数据。
2. 至少支持以下数据层：
   - demo header
   - player list
   - per-tick player state
   - player_death / kill events
3. 为每个解析阶段提供明确诊断：
   - status
   - failure_stage
   - failure_reason
   - stage-level stats
4. 不能静默返回空结果；失败必须告诉我失败在哪一步、为什么失败。
5. 输出必须可被后续反作弊/复核流水线直接消费。

核心约束：
- 不依赖数据库
- 不依赖 Web 后端
- 可本地单机运行
- JSON 序列化使用 ensure_ascii=False
- 所有数值使用 Python 原生类型
- 概率/分数保留 6 位小数
- 兼容 demo 版本变化；无法兼容时要明确提示 unsupported patch / entity not found / parse_failed

必须优先实现的数据字段：
- header: map_name, demo_version_name, demo_version_guid, patch_version, fullpackets_version, server_name
- players: steamid, name, team_number
- ticks: tick, steamid, X, Y, Z, pitch, yaw, is_alive, active_weapon, health
- kills: tick, attacker_steamid, victim_steamid, attacker_name, victim_name, weapon, headshot, wallbang, through_smoke, round_num, time_sec, round_time_sec

输出文件建议：
- demo_meta.json
- players.json
- ticks.parquet 或 ticks.jsonl
- kills.json
- diagnostics.json

实现要求：
1. 先设计 parser 接口，再实现具体后端。
2. 解析层和业务层分离，后续可替换后端。
3. 对 demo 兼容性做回归测试。
4. 先做最小可用版，再逐步补充事件类型和附加字段。
5. 不能把解析失败伪装成正常空结果。

建议开发阶段：
- Phase 1: header + player list
- Phase 2: per-tick player state
- Phase 3: kill events
- Phase 4: diagnostics / fallback / regression tests
- Phase 5: 接入现有 review pipeline

验收标准：
- 能对可解析 demo 输出完整 tick 流和 kill 事件
- 不能解析的 demo 会给出明确失败原因
- 输出格式稳定，能被后续聚合、HTML 报告、片段定位模块直接使用

请先输出：
1. 项目总体架构
2. 分阶段实施计划
3. 关键技术风险
4. 最小可行版本（MVP）定义
然后再开始写代码。
