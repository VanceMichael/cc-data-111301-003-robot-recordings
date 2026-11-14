# 来源资料约定

来源使用 MCAP 容器及 JSON Schema 消息。

## 容器局部身份

文件中的 schema id 与 channel id 为容器局部编号，只在当前文件内有效。同名 topic 可以在下一
文件换 schema，因此采集索引以「来源文件 + 局部 channel id」登记 channel 世代，保留 topic、
消息编码、channel metadata（含单位）与 schema 原文，世代之间不互相覆盖。

## 时间与启动身份

* 消息内的 `boot` 是设备启动身份；不同 boot 的消息不会被关联到一起。
* 消息内的 `sample_time` 是设备时钟，与 MCAP 的 `log_time`、`publish_time` 含义不同，
  三者分别保留、分别可查。
* 传感器身份优先取消息体的 `sensor`，缺失时回退到 channel metadata 的 `sensor`。

## 观测与载荷的关联

观测消息（schema 名称 `Observation`）通过 `payload_ref` 引用同 boot 内的载荷身份；
载荷消息（schema 名称 `Payload`）以 `observation_id` 声明自己的身份。关联严格按
`(boot, 引用)` 匹配，不按到达顺序凑对：

* 载荷后到：观测先登记为 `unlinked`，载荷补交后转为 `linked`。
* 载荷先到：载荷先作为无主载荷（orphan payload）可查，观测补交后转为 `linked`。
* 永久缺失：观测一直保持 `unlinked`。
* 引用只在其它 boot 下存在：标记 `boot_mismatch`，不跨 boot 配对。
* 观测没有 `payload_ref`：状态为 `no_reference`。

## 演练资料

part-2 是补交载荷，而 part-0 的原始检测先于载荷交付；part-1 的观测没有载荷引用，属于
另一个启动周期。
