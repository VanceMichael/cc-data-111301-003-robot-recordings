# 来源资料约定

来源使用 MCAP 容器及 JSON Schema 消息。文件中的 schema id 与 channel id 为容器局部编号。消息内的 boot 是设备启动身份，sample_time 是设备时钟，不是 MCAP 的 log_time 或 publish_time。payload_ref 引用同一 boot 内的载荷身份。part-2 是补交载荷，而 part-0 的原始检测先于载荷交付。
