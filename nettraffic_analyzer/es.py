# @Author  : yuanzi
# @Time    : 2024/11/17 14:01
# Website: https://www.yzgsa.com
# Copyright (c) <yuanzigsa@gmail.com>
import logging
from elasticsearch import Elasticsearch, helpers
from datetime import datetime, timedelta, timezone
import time
from dateutil import parser
from nettraffic_analyzer.resolver import Resolver

logger = logging.getLogger(__name__)


class Es:
    def __init__(self):
        # 配置 Elasticsearch 客户端
        self.es = Elasticsearch(["http://localhost:9200"])
        if self.es.ping():
            logger.info("成功连接到 Elasticsearch")
        else:
            logger.error("无法连接到 Elasticsearch")
            exit(1)
        self.resolver = Resolver()

    @ staticmethod
    def get_new_documents(es_client, index, timestamp_field, last_time):
        """
        获取时间戳大于 last_time 的所有新记录
        """
        query = {
            "query": {
                "range": {
                    timestamp_field: {
                        "gt": last_time.isoformat()
                    }
                }
            },
            "sort": [
                {timestamp_field: "asc"}
            ],
            "size": 10000  # 设置一个足够大的 size，根据你的需求调整
        }

        response = es_client.search(index=index, body=query, scroll='2m')
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        all_hits = hits.copy()

        while len(hits) > 0:
            response = es_client.scroll(scroll_id=scroll_id, scroll='2m')
            hits = response['hits']['hits']
            all_hits.extend(hits)

        return all_hits

    def prepare_bulk_update(self, docs):
        """
        根据记录中的字段值，准备 Bulk API 更新操作
        """
        new_docs = self.resolver.rewrite_docs(docs)
        actions = []
        for doc in new_docs:
            source = doc['_source']
            doc_id = doc['_id']
            new_field = {
                "flow_isp_type": source['flow_isp_type'],
                "flow_isp_info": source['flow_isp_info']
            }
            action = {
                "_op_type": "update",
                "_index": doc['_index'],
                "_id": doc_id,
                "doc": new_field
            }
            actions.append(action)
        return actions

    def run(self):
        timestamp_field = "@timestamp"
        check_interval = 10

        # 初始化最后一次检查的时间
        last_checked_time = datetime.now(timezone.utc) - timedelta(seconds=check_interval)

        while True:
            try:
                start = time.time()
                # 使用 UTC 时间
                index_name = f"sflow-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"

                # 获取新记录
                new_docs = self.get_new_documents(
                    es_client=self.es,
                    index=index_name,
                    timestamp_field=timestamp_field,
                    last_time=last_checked_time
                )

                if new_docs:
                    logger.info(f"找到 {len(new_docs)} 个新记录，正在处理...")

                    # 准备更新操作
                    bulk_actions = self.prepare_bulk_update(new_docs)

                    if bulk_actions:
                        # 执行批量更新
                        helpers.bulk(self.es, bulk_actions)
                        logger.info(f"成功更新 {len(bulk_actions)} 个记录。")
                    else:
                        logger.info("没有需要更新的记录。")

                    # 更新最后一次检查的时间为最新记录的时间
                    last_times = [doc['_source'][timestamp_field] for doc in new_docs]
                    latest_time_str = max(last_times)
                    last_checked_time = parser.isoparse(latest_time_str)

                else:
                    logger.info("没有新记录。")

                logger.info(f"更新完成，耗时：{round(time.time() - start, 2)}s")

            except Exception as e:
                logger.error(f"NettrafficAnalyzer_for_ELK运行发生错误: {e}")

            time.sleep(check_interval)
            