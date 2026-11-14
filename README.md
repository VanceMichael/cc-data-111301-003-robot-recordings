# 机器人 MCAP 记录的通道世代与观测关联资料工具

这个 Python 项目用于业务来源资料的检查、原始字节归档与跨记录资料采集。单份资料预览
（`core.py`）和内容寻址归档（`archive.py`）保持不变；`store.py` 在归档之上增加一层
SQLite 索引，回答单份预览无法回答的跨记录问题：不同启动周期的检测与载荷如何按设备给出
的观测引用关联、重复片段如何去重、坏 chunk 的局部失败范围在哪里、如何按启动周期和时间段
索引化读取。不做轨迹解算，也不处理图像内容。

## 环境

Python 3.11.2。依赖及其间接依赖固定在 `requirements.txt`；空清单表示只使用标准库。安装只在环境准备阶段执行：

```sh
python3 -m pip install -r requirements.txt
```

## 单份资料命令（保留）

```sh
python3 -m robot_recordings inspect fixtures/part-0.mcap
python3 -m robot_recordings capture fixtures/part-0.mcap --archive .work/raw
```

归档返回 SHA-256、字节数及原文件名；文件名不参与内容身份。预览只说明单份来源的基本结构，
不代表关联资料已经齐全。输入不可识别时命令失败，不产生空的成功结果。

## 跨记录采集流程

资料库默认放在 `.work/store/`：内容寻址原始字节在 `raw/`，关系索引在 `index.sqlite3`。

```sh
# 1. 采集原始检测（此时载荷未到，观测处于 unlinked）
python3 -m robot_recordings collect fixtures/part-0.mcap fixtures/part-1.mcap --store .work/store

# 2. 保存补交前的观测关联快照
python3 -m robot_recordings snapshot --store .work/store --output .work/before.json

# 3. 只补交丢失的载荷片段，并对比快照看哪些观测从缺资料变为可用
python3 -m robot_recordings collect fixtures/part-2.mcap \
    --store .work/store --snapshot-before .work/before.json

# 4. 资料库清单：来源、局部失败范围、每个文件的 channel 世代（含同名 topic 的不同 schema）
python3 -m robot_recordings manifest --store .work/store

# 5. 观测关联查询，可按启动周期和状态过滤
python3 -m robot_recordings observations --store .work/store --boot boot-a --state linked

# 6. 按启动周期与时间段索引化读取，时间基准可选 sample_time/log_time/publish_time
python3 -m robot_recordings read --store .work/store --boot boot-a \
    --start 0 --end 200 --time-basis sample_time
```

### 关联状态（只按设备给出的引用，不按到达顺序配对）

| 状态 | 含义 |
| --- | --- |
| `linked` | 观测的 `payload_ref` 在同一 `boot` 内找到了载荷的 `observation_id` |
| `unlinked` | 观测带 `payload_ref`，但同 boot 内还没有对应载荷（后到或永久缺失） |
| `no_reference` | 观测没有 `payload_ref`（如 part-1） |
| `boot_mismatch` | 引用只在别的 boot 下存在载荷，不跨 boot 强行配对 |

载荷先到而观测未到时，载荷出现在 `orphan payloads` 里；观测补交后自动转为 `linked`。
`snapshot`/`diff` 与 `collect --snapshot-before` 的 `snapshot_diff.newly_available`
用于看清一次补交前后哪些观测从缺资料变成可用。

### 重复片段与局部失败

* 整份文件内容相同：采集报告标记 `duplicate`，不重复计数、不重复落归档。
* 片段重叠：消息按原始字节的 SHA-256 去重，消息身份只算一次；每次交付的出处
  （来源文件、channel 世代、sequence、log_time、publish_time）保留在 `message_sources`。
* 坏 chunk：解析以 chunk 为隔离单位，只登记该 chunk 的字节范围和 log_time 区间为局部失败，
  其余消息正常入索引，来源状态为 `partial`，清单不会声称整趟完整；`collect` 对局部失败
  返回退出码 3，输入完全无法识别返回非零且不写归档、不写索引。
* 有 chunk 索引的文件，`read` 先用 SQLite 索引定位来源与时间窗口，再通过 MCAP chunk 索引
  只解压相关 chunk，不必展开所有载荷；无索引文件回退为顺序读取。带局部失败的来源改走
  韧性解析，并在结果里标注 `partial_sources` 与失败区间。

## 身份与时间语义

容器内的 channel id、schema id 只在当前文件内有效，索引里以「来源 + 局部 id」登记为独立
channel 世代，保留 topic、消息编码、channel metadata（含单位）和 schema 原文；同名 topic
在下一文件换 schema 不会覆盖旧世代。消息内的 `boot`（设备启动身份）、传感器身份、
`sample_time`（设备时钟）与 MCAP 的 `log_time`、`publish_time` 分别建列、分别可查，不混用。

## 开发检查

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q robot_recordings tests
```

## 本地演示

```sh
python3 -m robot_recordings demo
```

演示实际读取来源资料，在临时目录跑通归档、三份资料采集、观测关联与清单，结束后自动清理。
所有示例身份与业务数值均为虚构，不需要外部账号。
