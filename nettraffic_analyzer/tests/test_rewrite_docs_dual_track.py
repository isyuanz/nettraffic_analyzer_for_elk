import os
import sys
import json
import tempfile
import unittest
from unittest.mock import MagicMock

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


if __name__ == '__main__':
    unittest.main()
