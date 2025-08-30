import datetime
import time
from elasticsearch import Elasticsearch
import subprocess
import logging
from datetime import timezone
from logging.handlers import TimedRotatingFileHandler
import os
import requests


dingtalk_webhook = "https://oapi.dingtalk.com/robot/send?access_token=6023ab04c155773b6889b01a0ca4ce691812bd359aaedefafd4f8644af35f111"


# 创建logs目录（如果不存在）
if not os.path.exists('logs'):
    os.makedirs('logs')

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# 创建文件处理器，每天轮换一次，保留7天的日志
file_handler = TimedRotatingFileHandler(
    filename='logs/es_check.log',
    when='midnight',
    interval=1,
    backupCount=7,
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# 添加处理器到logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def connect_elasticsearch():
    """
    连接到Elasticsearch服务器
    :return: Elasticsearch客户端实例
    """
    try:
        es = Elasticsearch(
            ["http://localhost:9200"],
            basic_auth=("nettraffic_analyzer", "nettraffic_analyzer")
        )
        if es.ping():
            logger.info("成功连接到ES")
            return es
        else:
            logger.error("无法连接到ES")
            return None
    except Exception as e:
        logger.error(f"连接ES时发生错误: {str(e)}")
        return None

def check_index_updates(es, index_name):
    """
    检查指定索引在最近一分钟内是否有更新
    :param es: Elasticsearch客户端实例
    :param index_name: 要检查的索引名称
    :return: 布尔值，表示是否有更新
    """
    try:
        now = datetime.datetime.utcnow()
        one_minute_ago = now - datetime.timedelta(minutes=1)

        # 构建查询，使用count API只获取数量
        query = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": one_minute_ago.isoformat(),
                        "lte": now.isoformat()
                    }
                }
            }
        }

        # 只获取文档数量
        result = es.count(index=index_name, body=query)
        
        # 如果有匹配的文档，说明有更新
        return result['count'] > 0
    except Exception as e:
        logger.error(f"检查索引更新时发生错误: {str(e)}")
        return False


def send_dingtalk_message(webhook, message):
    headers = {'Content-Type': 'application/json'}
    data = {
        "msgtype": "markdown",
        "markdown": {
            "title": "ELK监控告警",
            "text": message
        }
    }
    response = requests.post(webhook, json=data, headers=headers)
    if response.status_code == 200:
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " 钉钉消息发送成功")
    else:
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " 钉钉消息发送失败，正在重试...")
        # 重试发送钉钉消息
        retry_count = 10  # 设置重试次数
        for i in range(retry_count):
            response = requests.post(webhook, json=data, headers=headers)
            if response.status_code == 200:
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " 钉钉消息发送成功")
                break
            else:
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " 钉钉消息发送失败，正在重试...")
        else:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " 重试发送钉钉消息失败")


def main():
    """
    主函数，运行监控循环
    """
    es = connect_elasticsearch()
    if not es:
        logger.error("无法启动监控，ES连接失败")
        return

    while True:
        index_name = f"sflow-{datetime.datetime.now(timezone.utc).strftime('%Y.%m.%d')}"
        try:
            if not check_index_updates(es, index_name):
                logger.warning(f"索引 {index_name} 在最近一分钟内没有更新")
                send_dingtalk_message(dingtalk_webhook, f"【世捷通新ELK监控告警】索引 {index_name} 在最近一分钟内没有更新， 主机IP: 220.202.54.74")
            else:
                logger.info(f"索引 {index_name} 正常更新中")
            
            # 等待一分钟
            time.sleep(60)
        except Exception as e:
            logger.error(f"监控过程中发生错误: {str(e)}")
            time.sleep(60)  # 发生错误时也等待一分钟后继续

if __name__ == "__main__":
    main()
