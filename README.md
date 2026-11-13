# 机器人 MCAP 记录的通道世代与观测关联资料工具

这个 Python 项目用于业务来源资料的检查与原始字节归档。当前提供单份资料预览和内容寻址保存，不生成业务查询视图。`robot_recordings/core.py` 处理现有资料预览，`archive.py` 保存原文件，`fixtures/` 保存本地业务演练资料，`tests/` 检查已有行为。

## 环境

Python 3.11.2。依赖及其间接依赖固定在 `requirements.txt`；空清单表示只使用标准库。安装只在环境准备阶段执行：

```sh
python3 -m pip install -r requirements.txt
```

## 运行

```sh
python3 -m robot_recordings inspect fixtures/part-0.mcap
python3 -m robot_recordings capture fixtures/part-0.mcap --archive .work/raw
```

归档返回 SHA-256、字节数及原文件名；文件名不参与内容身份。预览只说明单份来源的基本结构，不代表关联资料已经齐全。输入不可识别时命令失败，不产生空的成功结果。

## 开发检查

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q robot_recordings tests
```

## 本地演示

```sh
python3 -m robot_recordings demo
```

演示实际读取来源资料，打印原始归档摘要及结构预览，使用临时目录并自动清理。所有示例身份与业务数值均为虚构，不需要外部账号。

