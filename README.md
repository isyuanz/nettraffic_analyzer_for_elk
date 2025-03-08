# 节点出口流量分析

## 项目简介

这是一个用于分析网络节点出口流量的工具，支持 sFlow 数据采集和分析，可以帮助网络管理员监控和分析网络流量情况。

## 主要功能

- sFlow 数据采集和处理
- 流量数据存储到 Elasticsearch
- IP 地理位置解析
- 流量统计和分析
- XDB 数据库支持

## 项目结构

├── config/ # 配置文件目录
│ ├── config.json # 主配置文件
│ └── sflow.conf # sFlow 配置
├── nettraffic_analyzer/ # 核心代码目录
│ ├── es.py # Elasticsearch 操作模块
│ ├── resolver.py # IP 解析模块
│ └── xdbSearcher.py # XDB 查询模块
├── tests/ # 测试目录
└── run.py # 程序入口
