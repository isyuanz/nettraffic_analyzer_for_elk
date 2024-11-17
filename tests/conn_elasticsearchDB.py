# @Author  : yuanzi
# @Time    : 2024/11/2 10:12
# Website: https://www.yzgsa.com
# Copyright (c) <yuanzigsa@gmail.com>


from elasticsearch import Elasticsearch

# 创建 Elasticsearch 客户端
es = Elasticsearch(["http://localhost:9200"])

try:
    response = es.search(
        index="sflow*",
        body={
            "size": 100,  # 获取文档数量
            "query": {
                "match_all": {}
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "desc"  # 降序排列，获取最新的数据
                    }
                }
            ]
        }
    )
    # response = es.get(
    #     index="sflow",  # 替换为实际的索引名称
    #     id="CvMJNZMBBJr59vQEOWEj"  # 替换为文档的 _id
    # )
    # print(response)

    # 检查是否有返回结果
    if response['hits']['total']['value'] > 0:
        # 打印结果
        for hit in response['hits']['hits']:
            print(hit['_source'])
    else:
        print("No data found in the specified index.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # 关闭 Elasticsearch 连接
    es.transport.close()
