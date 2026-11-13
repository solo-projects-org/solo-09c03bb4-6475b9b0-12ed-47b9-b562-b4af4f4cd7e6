# Delta Lake 销售订单表 · 金额非负约束启用台

退款在账务侧已**单独冲减销售额**，若再把负金额写进销售订单表，下游统计会
再扣一次。本工具让湖仓值班员在首页对一张**本地可写的 Delta Lake 样表**完成
体检，并在通过后以一次标准 Delta 提交启用约束：

```text
delta.constraints.nonnegative_amount = "amount_cents >= 0"
```

提交同时把 `minWriterVersion` 升到 **3**（读协议保持 1），其余元数据原样
保留。全程不依赖 Spark / Delta Lake 运行时，日志读写与 Parquet 扫描均由
Python + PyArrow 完成。

## 适用表范围（超出即拒绝并指出原因）

- 本地可写目录中的**单张表**，网页仅开放 `samples/` 白名单内样表；
- 版本 0 起 **JSON 提交连续无断档**（检查点只作加速基线，不能替代缺失提交）；
- 初始读写协议为 `minReaderVersion=1` / `minWriterVersion=2`；
- **无分区表**（`partitionColumns` 为空、add 动作 `partitionValues` 为空）；
- **无删除向量**（protocol 特性或任意 add/remove 带 `deletionVector` 即拒绝）；
- `order_id`、`amount_cents` 均为**非空 int64（long）**；
- Parquet 数据文件不改写、文件路径不复用。

## 体检与发布规则

1. 以 `_last_checkpoint` / 最新经典单文件 Parquet 检查点为基线，连续重放
   JSON 提交，重建**最新快照**的在用文件集合（add 覆盖、remove 退出）；
2. 扫描**不持有阻塞锁**，其他写入方可并发做只增删文件的提交；
3. 每次操作都重新以**最新快照**为准：新提交引入的新增文件同样扫描，已
   remove 退出的文件不再参与（即使物理文件缺失或含负金额也不报）；
4. 仍存在负金额（含空金额）时，列出 **order_id 与数据文件路径**；
   在用文件缺失或 Parquet 读不出时，指出**文件路径**；任一阻塞项存在都
   **不发布**任何提交；
5. 体检通过后，用**同一份**标准 Delta 提交同时写入 protocol（writer=3）与
   metaData（新增约束键，保留其余配置与元数据），提交内不含任何
   add/remove，不改写 Parquet；
6. 发布用 `O_CREAT|O_EXCL` 排他原子创建：撞上新版本提交则丢弃本次成果、
   以最新快照**整体重验后重试**，绝不覆盖已有提交；
7. 已启用约束的表再次操作直接返回**启用版号**，不再产生提交；页面展示
   启用版号。

## 目录结构

```
app/
  delta.py               # 核心：日志/检查点解析、快照重建、前置校验、扫描、原子提交
  web.py                 # Flask 路由：样表白名单、体检 API、启用 API
  templates/index.html   # 单页界面：选表 → 体检明细 → 一键启用
scripts/generate_samples.py  # 用 PyArrow 构造两套 Delta 销售订单样表
samples/                 # 本地可写样表（健康表 / 退款事故表）
tests/                   # pytest：核心逻辑与路由测试
Dockerfile / docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

首页两个按钮直接操作随附样表（挂载为**可写**目录，提交落在宿主机
`samples/<表>/_delta_log/` 下）：

| 样表 | 最新快照 | 预期体检结论 | 启用结果 |
|---|---|---|---|
| `healthy_table` | v3（含 v2 检查点） | 4 个在用文件全部可读、金额非负 | 发布 v4：protocol 1/3 + 约束元数据 |
| `incident_table` | v4（含 v2 检查点） | 负金额订单 `2004(-9900)`；`part-00003` 缺失；`part-00004` 损坏不可读 | 拦截，不产生任何提交 |

事故表中 `part-00001` 已在 v4 被 remove：其物理文件即便保留也不参与扫描。

## 本地开发（无 Docker 时）

```bash
pip install -r requirements.txt
python -I scripts/generate_samples.py
gunicorn --bind 0.0.0.0:8000 app.web:app
python -I -m pytest tests -q
```

## 设计取舍

- **连续性是硬约束**：期望最高版本取“最新 JSON 提交”与“最新检查点版本”
  的较大者，因此尾部提交丢失（检查点版本更高）和中间断档都能被发现；
- **检查点只加速、不兜底**：检查点里的 remove 是墓碑，快照以检查点
  add 列为基线，之后逐版重放 JSON 的 add/remove；
- **扫描与发布解耦**：扫描期间不阻塞并发提交，发布前再读一次最新快照
  比对版本号，配合排他创建实现乐观并发（OCI）；
- **元数据完整保留**：发布前对 metadata 深拷贝，仅向 `configuration`
  增加约束键并刷新 `lastUpdate`，`id` / `schemaString` / `createdTime` /
  `partitionColumns` 等全部原样回写；
- **路径白名单收敛**：网页层对表名做白名单 + `Path.resolve()` 前缀校验，
  阻断目录穿越与符号链接逃逸。
