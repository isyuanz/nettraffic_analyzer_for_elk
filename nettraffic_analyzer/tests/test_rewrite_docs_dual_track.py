import os
import sys
import json
import logging
import tempfile
import unittest
from unittest.mock import MagicMock, patch

# 本机环境未安装 ip2region.util（resolver.py 在模块顶层 import），
# 但本测试只调用 @staticmethod，无需真实 ip2region。注入 MagicMock 桩绕过 import 错误。
for _mod in ('ip2region', 'ip2region.util', 'ip2region.searcher'):
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nettraffic_analyzer.resolver import Resolver


class RewriteDocsDualTrackTests(unittest.TestCase):
    def setUp(self):
        # 准备一个临时 customer_cidr.json
        self.tmpdir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.tmpdir, 'res'), exist_ok=True)
        with open(os.path.join(self.tmpdir, 'res', 'customer_cidr.json'), 'w') as f:
            json.dump([
                {'node': 'N1', 'customer': 'C1', 'cidr': '203.0.113.0/24', 'egress_ip': '120.92.10.5'},
                {'node': 'N1', 'customer': 'C2', 'cidr': '203.0.113.128/25', 'egress_ip': '120.92.10.5'},
            ], f)
        self.cwd = os.getcwd()
        os.chdir(self.tmpdir)

    def tearDown(self):
        os.chdir(self.cwd)

    def test_lookup_longest_prefix_v4(self):
        # Resolver.read_customer_cidr_data 是 @staticmethod，可直接通过类调用
        entries = Resolver.read_customer_cidr_data()
        self.assertEqual(Resolver._lookup_cidr('203.0.113.5', entries)['customer'], 'C1')
        self.assertEqual(Resolver._lookup_cidr('203.0.113.200', entries)['customer'], 'C2')

    def test_lookup_no_match_returns_empty(self):
        entries = Resolver.read_customer_cidr_data()
        self.assertEqual(Resolver._lookup_cidr('10.0.0.1', entries), {})

    def test_lookup_ipv6(self):
        # 当前配置无 IPv6 条目
        entries = Resolver.read_customer_cidr_data()
        self.assertEqual(Resolver._lookup_cidr('2001:db8::1', entries), {})


class RewriteDocsEndToEndTests(unittest.TestCase):
    """验证 rewrite_docs 在 IP 模式下完整跑通（C1 regression test）。"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.tmpdir, 'res'), exist_ok=True)
        with open(os.path.join(self.tmpdir, 'res', 'customer_cidr.json'), 'w') as f:
            json.dump([{
                'node': 'N1', 'customer': 'C1',
                'cidr': '203.0.113.0/24', 'egress_ip': '120.92.10.5',
            }], f)
        # 空列表占位，避免 read_config_data / read_sflow_cacti_data 抛错
        with open(os.path.join(self.tmpdir, 'res', 'config_data.json'), 'w') as f:
            json.dump([], f)
        with open(os.path.join(self.tmpdir, 'res', 'sflow_cacti_data.json'), 'w') as f:
            json.dump([], f)
        self.cwd = os.getcwd()
        os.chdir(self.tmpdir)

    def tearDown(self):
        os.chdir(self.cwd)

    def test_ip_mode_doc_enriched_without_keyerror(self):
        """IP 模式命中的文档应正常进入 new_docs，customer 字段被回写。"""
        doc = {
            '_id': 'abc123',
            '_index': 'sflow-2026.06.26',
            '_source': {
                '@timestamp': '2026-06-26T08:00:00.000Z',
                'host': {'ip': '10.255.0.1'},
                'src_ip': '203.0.113.5',
                'dst_ip': '8.8.8.8',
                'source_id_index': '999',
                'frame_length_times_sampling_rate': 1500,
            },
        }
        with patch.object(Resolver, 'resolve_country_info', return_value={}), \
                patch.object(Resolver, 'rewrite_ipinfo', side_effect=lambda ip, info: {
                    'isp': '中国移动', 'province': '广东', 'country': '中国',
                    'country_code': 'CN', 'city': '深圳',
                }):
            resolver = Resolver.__new__(Resolver)
            resolver.logger = logging.getLogger('test')
            new_docs = resolver.rewrite_docs([doc])
        # C1 修复前: KeyError 被第 461 行 except 吞掉，new_docs 为 []
        self.assertEqual(len(new_docs), 1, 'IP 模式文档应进入 new_docs，不应被 KeyError 吞掉')
        self.assertEqual(new_docs[0]['_source']['customer'], 'C1')
        self.assertEqual(new_docs[0]['_source']['node'], 'N1')

    def test_cidr_match_takes_priority_over_port_config(self):
        """同一文档同时命中端口配置和 CIDR 配置时，应以 CIDR 识别结果为准。"""
        with open(os.path.join(self.tmpdir, 'res', 'config_data.json'), 'w') as f:
            json.dump([{
                'host_ip': '10.255.0.1',
                'interface': '999',
                'agent_ip': '198.51.100.10',
                'node': 'PORT_NODE',
                'costumer': 'PORT_CUSTOMER',
                'switch': 'Gi1/0/1',
                'flow_direction': '出站',
                'relation_cacti_graph_id': 0,
            }], f)
        doc = {
            '_id': 'abc124',
            '_index': 'sflow-2026.06.26',
            '_source': {
                '@timestamp': '2026-06-26T08:00:00.000Z',
                'host': {'ip': '10.255.0.1'},
                'src_ip': '203.0.113.5',
                'dst_ip': '8.8.8.8',
                'source_id_index': '999',
                'frame_length_times_sampling_rate': 1500,
            },
        }
        with patch.object(Resolver, 'resolve_country_info', return_value={}), \
                patch.object(Resolver, 'rewrite_ipinfo', side_effect=lambda ip, info: {
                    'isp': '中国移动', 'province': '广东', 'country': '中国',
                    'country_code': 'CN', 'city': '深圳',
                }):
            resolver = Resolver.__new__(Resolver)
            resolver.logger = logging.getLogger('test')
            new_docs = resolver.rewrite_docs([doc])

        self.assertEqual(len(new_docs), 1)
        self.assertEqual(new_docs[0]['_source']['customer'], 'C1')
        self.assertEqual(new_docs[0]['_source']['node'], 'N1')
        self.assertEqual(new_docs[0]['_source']['sw_interface'], '')

    def test_empty_cidr_config_skips_cidr_lookup(self):
        """未配置 CIDR 条目时，不应对每条文档执行 CIDR 查询。"""
        with open(os.path.join(self.tmpdir, 'res', 'customer_cidr.json'), 'w') as f:
            json.dump([], f)
        with open(os.path.join(self.tmpdir, 'res', 'config_data.json'), 'w') as f:
            json.dump([{
                'host_ip': '10.255.0.1',
                'interface': '999',
                'agent_ip': '198.51.100.10',
                'node': 'PORT_NODE',
                'costumer': 'PORT_CUSTOMER',
                'switch': 'Gi1/0/1',
                'flow_direction': '出站',
                'relation_cacti_graph_id': 0,
            }], f)
        doc = {
            '_id': 'abc125',
            '_index': 'sflow-2026.06.26',
            '_source': {
                '@timestamp': '2026-06-26T08:00:00.000Z',
                'host': {'ip': '10.255.0.1'},
                'src_ip': '203.0.113.5',
                'dst_ip': '8.8.8.8',
                'source_id_index': '999',
                'frame_length_times_sampling_rate': 1500,
            },
        }
        with patch.object(Resolver, '_lookup_cidr', wraps=Resolver._lookup_cidr) as lookup, \
                patch.object(Resolver, 'resolve_country_info', return_value={}), \
                patch.object(Resolver, 'rewrite_ipinfo', side_effect=lambda ip, info: {
                    'isp': '中国移动', 'province': '广东', 'country': '中国',
                    'country_code': 'CN', 'city': '深圳',
                }):
            resolver = Resolver.__new__(Resolver)
            resolver.logger = logging.getLogger('test')
            new_docs = resolver.rewrite_docs([doc])

        self.assertEqual(len(new_docs), 1)
        self.assertEqual(new_docs[0]['_source']['customer'], 'PORT_CUSTOMER')
        lookup.assert_not_called()


if __name__ == '__main__':
    unittest.main()
