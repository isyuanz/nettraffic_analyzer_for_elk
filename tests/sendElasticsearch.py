import json
import random
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers

# 1. 配置Elasticsearch连接
es = Elasticsearch(["http://122.189.33.130:9200"])

# 检查连接是否成功
if not es.ping():
    raise ValueError("无法连接到Elasticsearch，请检查连接配置。")

# 2. 定义生成模拟数据的函数
def generate_mock_data(num_records=100):
    data = []
    for _ in range(num_records):
        # 生成随机时间，确保时间不是未来的
        timestamp = datetime.utcnow() - timedelta(minutes=random.randint(0, 1440))  # 最近一天内
        netflow_last_switched = timestamp - timedelta(seconds=random.randint(10, 300))
        netflow_first_switched = netflow_last_switched

        document = {
            "@timestamp": timestamp.isoformat() + "Z",
            "type": "netflow",
            "host": {
                "ip": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
            },
            "netflow": {
                "last_switched": netflow_last_switched.isoformat() + "Z",
                "protocol": random.choice([6, 17]),  # TCP or UDP
                "tcp_flags": random.randint(0, 32),
                "l4_dst_port": random.randint(1, 65535),
                "l4_src_port": random.randint(1, 65535),
                "ipv4_dst_addr": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
                "dst_as": random.randint(0, 65535),
                "bgp_ipv4_next_hop": f"{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
                "src_as": random.randint(0, 65535),
                "flowset_id": random.randint(0, 65535),
                "dst_mask": random.randint(0, 32),
                "ipv4_src_addr": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
                "sampling_algorithm": random.randint(0, 10),
                "input_snmp": random.randint(0, 1000),
                "version": 9,
                "first_switched": netflow_first_switched.isoformat() + "Z",
                "src_mask": random.randint(0, 32),
                "flow_seq_num": random.randint(0, 1000000),
                "src_tos": random.randint(0, 255),
                "ipv4_next_hop": f"{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
                "in_bytes": random.randint(0, 1000000),
                "output_snmp": random.randint(0, 1000),
                "sampling_interval": random.randint(1, 10000),
                "in_pkts": random.randint(0, 10000),
                "src_vlan": random.randint(0, 4094),
                "direction": random.choice([0, 1]),  # 例如：0 = 内部，1 = 外部
                "dst_vlan": random.randint(0, 4094)
            },
            "@version": "1"
        }

        action = {
            "_index": "sflow",  # 您可以根据需要更改索引名称
            "_source": document
        }
        data.append(action)
    return data

# 3. 生成模拟数据
num_records = 500  # 生成500条记录
mock_data = generate_mock_data(num_records=num_records)

# 4. 使用bulk方法批量插入数据
try:
    helpers.bulk(es, mock_data)
    print(f"成功插入 {num_records} 条记录到索引 'sflow'")
except Exception as e:
    print(f"插入数据时发生错误: {e}")

# 5. 验证数据 (可选)
# 您可以使用以下代码验证数据是否已成功插入
def verify_data(index, expected_count):
    query = {
        "size": 0,
        "aggs": {
            "total": {
                "value_count": {
                    "field": "@timestamp"
                }
            }
        }
    }
    response = es.search(index=index, body=query)
    total = response['aggregations']['total']['value']
    print(f"索引 '{index}' 中的文档总数: {total}")
    if total >= expected_count:
        print("验证成功，数据已成功插入。")
    else:
        print("验证失败，插入的数据量不足。")

# 调用验证函数
verify_data("sflow", num_records)

