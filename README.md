# dg_agent

## 概述
dg_agent 是一个Python数据治理工具，提供数据治理相关的自动化功能。
+ 数据模型架构查询
    通过问答接口查询 业务域、应用、数据实体的内容和关系。

## 安装

### 通过pip安装
```bash
pip install .
```

### 开发模式安装
```bash
pip install -e .
```

## 使用

### 命令行使用
安装后可以通过以下命令运行：
```bash
dg-agent
```

## 项目结构

```
dg_agent/
├── bot/
│   ├── agent/               # 代理核心模块
│   │   ├── __init__.py
│   │   ├── age_cypher_agent.py
│   │   └── data_gov_agent.py
│   ├── models/              # 模型模块
│   │   ├── __init__.py
│   │   └── deepseek.py
│   ├── chat_app.py          # 主程序入口
│   └── test.py              # 测试文件
├── setup.py                 # 项目配置
├── pyproject.toml           # 构建配置
└── run.bat                  # Windows启动脚本
```

## 依赖

- Python >= 3.8
- requests >= 2.31.0
- logfire >= 0.1.0

## 开发

### 构建
```bash
python setup.py build
```

### 运行测试
```bash
python -m pytest
```

### 打包
```bash
python setup.py sdist bdist_wheel
