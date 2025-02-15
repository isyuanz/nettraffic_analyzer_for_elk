# @Author  : yuanzi
# @Time    : 2024/11/17 15:31
# Website: https://www.yzgsa.com
# Copyright (c) <yuanzigsa@gmail.com>
import json
from enum import Enum
import logging
import re
from nettraffic_analyzer.xdbSearcher import XdbSearcher
from nettraffic_analyzer.utils import setup_logger, ipv6_search

logger = logging.getLogger(__name__)


class Isp(Enum):
    CHINA_MOBILE = "中国移动"
    CHINA_UNICOM = "中国联通"
    CHINA_TELECOM = "中国电信"


class Resolver:
    def __init__(self):
        dbPath = "res/china.xdb"
        self.cb = XdbSearcher.loadContentFromFile(dbfile=dbPath)

    @staticmethod
    def resolve_ip_region(original_content, ipv6=False):
        """
        解析 xdb 原始查询内容，返回省份、城市、区县、运营商信息

        :param original_content: 原始查询内容，IPv4 为字符串，IPv6 为列表
        :param ipv6: 是否为 IPv6 查询
        :return: 包含省份、城市、区县、运营商信息的字典
        """
        # 默认返回值
        default_result = {
            'province': '未知',
            'city': '未知',
            'district': '未知',
            'isp': '未知',
        }

        # 处理 IPv6 查询结果
        if ipv6:
            if isinstance(original_content, (list, tuple)) and len(original_content) > 15:
                return {
                    'province': original_content[13] if original_content[13] else "未知",
                    'city': original_content[15] if original_content[15] else "未知",
                    # 'district': '未知',  # IPv6 结果中可能没有区县信息
                    'isp': original_content[6] if original_content[6] else "未知",
                }
            return default_result

        # 处理 IPv4 查询结果
        if isinstance(original_content, str) and original_content.strip():
            parts = original_content.split('|')
            if len(parts) > 9:
                return {
                    'province': parts[7] if parts[7] else "未知",
                    'city': parts[9] if parts[9] else "未知",
                    'district': parts[4] if parts[4] else "未知",
                    'isp': parts[0] if parts[0] else "未知",
                }
        return default_result

    @staticmethod
    def is_ipv4(ip):
        ipv4_pattern = re.compile(
            r'^(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])(\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])){3}$')
        return bool(ipv4_pattern.match(ip))

    @staticmethod
    def get_flow_detail(ip, interface, data):
        """
        获取节点和客户信息以及确定是入站流量还是出站流量
        :param ip: 源IP地址
        :param interface: 源接口
        :param data: 配置数据
        :return: 节点、客户、端口名、流量方向
        """
        try:
            lookup_key = f"{ip}_{interface}"
            flow_map = {f"{item['agent_ip']}_{item['interface']}": item for item in data}
            
            if lookup_key in flow_map:
                item = flow_map[lookup_key]
                flow_direction = "未知"
                
                if item['port_type'] == "up":
                    flow_direction = "入站" if item['direction'] == "in" else "出站"
                elif item['port_type'] == "down":
                    flow_direction = "出站" if item['direction'] == "in" else "入站"
                
                return item['node'], item['costumer'], item['switch'], flow_direction
                
            return "未知", "未知", "未知", "未知"
            
        except Exception as e:
            logger.error(f"Error in get_flow_detail: {e}")
            return "未知", "未知", "未知", "未知"

    @staticmethod
    def read_config_data():
        try:
            with open('res/config_data.json', 'r') as f:
                data = json.load(f)
                logger.info(f"当前配置：{data}")
                return data

        except Exception as e:
            logger.error(f"Error in read_config_data: {e}")
            return []

    @staticmethod
    def _get_agent_ip(data, host_ip, interface):
         for item in data:
             if item['host_ip'] == host_ip and item['interface'] == interface:
                 return item['agent_ip']

    @staticmethod
    def rewrite_ipinfo(ip, ipinfo, isv4=True):
        if isv4:
            ipinfo['isp'] = "中国联通" if ip and ip.startswith('120.72.50') else ipinfo['isp']

        return ipinfo

    def rewrite_docs(self, docs):
        """
        重写elasticsearch查询结果，添加IP归属地信息
            1. 同运营商省内比例-同网省内
            2. 同运营商出省比例-同网跨省
            3. 去往移动的比例-异网(移动)
            4. 去往联通的比例-异网(联通)
            5. 去往电信的比例-异网(电信)
        """
        searcher = XdbSearcher(contentBuff=self.cb)
        config_data = self.read_config_data()
        new_docs = []
        for doc in docs:
            source = doc['_source']
            # 默认情况下agent_ip和host_ip是一样的，但在三线情况下可能不同，所以以agent_ip为准
            host_ip = source['host'].get('ip')
            src_ip = source.get('src_ip')
            dst_ip = source.get('dst_ip')
            ifindex = source.get('input_interface_value')
            agent_ip = next((item['agent_ip'] for item in config_data if
                             item['host_ip'] == host_ip and item['interface'] == ifindex), None)
            if dst_ip is None or host_ip is None or agent_ip is None:
                continue

            # 查询agent_ip的归属地信息
            result = searcher.search(agent_ip)
            agent_ip_info = self.rewrite_ipinfo(agent_ip, self.resolve_ip_region(result))

            if self.is_ipv4(dst_ip):
                result = searcher.search(dst_ip)
                dst_ip_info = self.rewrite_ipinfo(dst_ip, self.resolve_ip_region(result))
                result = searcher.search(src_ip)
                src_ip_info = self.rewrite_ipinfo(src_ip, self.resolve_ip_region(result))
                source['ipType'] = "ipv4"
            else:
                # ipv6
                result = ipv6_search(dst_ip)
                dst_ip_info = self.resolve_ip_region(result, ipv6=True)
                result = ipv6_search(src_ip)
                src_ip_info = self.resolve_ip_region(result, ipv6=True)
                source['ipType'] = "ipv6"
            # 判断同网还是异网
            agent_isp = agent_ip_info.get('isp')
            dst_isp = dst_ip_info.get('isp')
            agent_province = agent_ip_info.get('province')
            dst_province = dst_ip_info.get('province')
            agent_isp = agent_isp.replace('中国', '')
            dst_isp = dst_isp.replace('中国', '')
            if agent_isp != "未知" and dst_isp != "未知" and agent_isp == dst_isp:
                # 同网
                if agent_province and dst_province and agent_province == dst_province:
                    source['flow_isp_type'] = '同网省内'
                else:
                    source['flow_isp_type'] = '同网跨省'
            else:
                # 异网
                if not dst_isp:
                    source['flow_isp_type'] = '异网(未知)'
                else:
                    source['flow_isp_type'] = f'异网({dst_isp})'

            source['flow_isp_info'] = dst_ip_info
            # 添加节点信息
            node, customer, sw_interface, flow_direction = self.get_flow_detail(agent_ip, ifindex, config_data)
            source['node'] = node
            source['customer'] = customer
            source['sw_interface'] = sw_interface
            source['src_ip_region'] = f"{src_ip} {src_ip_info.get('province', '')}{src_ip_info.get('city', '')}"
            source['dst_ip_region'] = f"{dst_ip} {dst_ip_info.get('province', '')}{dst_ip_info.get('city', '')}"
            source['flow_direction'] = flow_direction
            doc['_source'] = source
            new_docs.append(doc)
            # logger.info(f"IP:{ip.ljust(18)}归属：{province}-{city}-{isp}")

        # 关闭searcher
        searcher.close()
        return new_docs
