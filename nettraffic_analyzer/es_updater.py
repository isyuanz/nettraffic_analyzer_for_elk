"""
@Author  : yuanz
@Time    : 2025/01/27 15:30
Website: https://www.yzgsa.com
Copyright (c) <yuanzigsa@gmail.com>
"""

import requests
import json
from requests.auth import HTTPBasicAuth

# Elasticsearch 的配置
ES_HOST = "http://localhost:9200"  # 替换为你的 Elasticsearch 地址
ES_USERNAME = "elastic"  # Elasticsearch 用户名
ES_PASSWORD = "1KBdEoghxWzvdZa=Vg=w"  # Elasticsearch 密码
INDEX = "sflow-2025.08.29"  # 索引名称
SCROLL_TIME = "1m"  # Scroll 滚动时间
BATCH_SIZE = 1000  # 每批次的文档数量

# 创建认证对象
auth = HTTPBasicAuth(ES_USERNAME, ES_PASSWORD)

# 初始化查询条件
query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"customer.keyword": "ACDN-腾讯-本网(80)-35G"}},
                {"term": {"host.ip.keyword": "220.202.8.254"}}
            ]
        }
    },
    "_source": ["_id"],
    "size": BATCH_SIZE
}

# 更新脚本
update_script = {
    "script": {
        "source": "ctx._source.node = 'YD-湖北武汉移动-出省5%'",
        "lang": "painless"
    },
    "conflicts": "proceed"
}

def search_scroll():
    """ 执行第一次查询，初始化 Scroll """
    url = f"{ES_HOST}/{INDEX}/_search?scroll={SCROLL_TIME}"
    response = requests.post(url, json=query, auth=auth)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error in search query: {response.text}")
        return None

def update_documents(ids):
    """ 执行批量更新操作 """
    update_body = update_script.copy()
    update_body["query"] = {"ids": {"values": ids}}
    url = f"{ES_HOST}/{INDEX}/_update_by_query"
    response = requests.post(url, json=update_body, auth=auth)
    if response.status_code == 200:
        print(f"Updated {len(ids)} documents successfully.")
    else:
        print(f"Error updating documents: {response.text}")

def process_scroll(scroll_id):
    """ 执行滚动查询并更新文档 """
    scroll_url = f"{ES_HOST}/_search/scroll"
    scroll_body = {
        "scroll": SCROLL_TIME,
        "scroll_id": scroll_id
    }

    while True:
        # 获取下一批数据
        response = requests.post(scroll_url, json=scroll_body, auth=auth)
        if response.status_code != 200:
            print(f"Error in scroll query: {response.text}")
            break

        data = response.json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            print("No more documents to update.")
            break

        # 获取当前批次的文档ID
        ids = [hit["_id"] for hit in hits]
        
        # 批量更新文档
        update_documents(ids)

        print(f"Updated {len(ids)} documents successfully.")
        # 更新 scroll_id 继续获取下一批数据
        scroll_id = data["_scroll_id"]
        scroll_body["scroll_id"] = scroll_id

def main():
    # 获取第一次查询的结果
    result = search_scroll()
    if result:
        # 获取第一个 scroll_id
        scroll_id = result["_scroll_id"]
        process_scroll(scroll_id)

if __name__ == "__main__":
    main()
