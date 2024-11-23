# @Author  : yuanzi
# @Time    : 2024/11/17 14:19
# Website: https://www.yzgsa.com
# Copyright (c) <yuanzigsa@gmail.com>

import os
import logging
import socket
import time
import mysql.connector
import psutil
import platform
import requests
from logging.handlers import TimedRotatingFileHandler
from colorlog import ColoredFormatter


def setup_logger():
    log_directory = 'log'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    log_file_path = os.path.join(log_directory, 'nettraffic_analyzer.log')

    # 配置控制台输出的日志格式和颜色
    console_formatter = ColoredFormatter(
        "%(log_color)s%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'ERROR': 'red',
            'WARNING': 'yellow',
            'CRITICAL': 'bold_red',
            'INFO': 'cyan',
            'DEBUG': 'white',
            'NOTSET': 'white'
        }
    )

    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)  # 设置为DEBUG级别以捕获所有级别的日志

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    file_handler = TimedRotatingFileHandler(
        filename=log_file_path, when='midnight', interval=1, backupCount=30, encoding='utf-8'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.WARNING)  # 文件日志默认记录INFO级别及以上的日志
    logger.addHandler(file_handler)

    return logger


def get_system_info():
    system_info = platform.platform()
    return system_info


def get_cpu_info():
    try:
        with open('/proc/cpuinfo', 'r') as file:
            for line in file:
                if line.strip().startswith("model name"):
                    cpu_model = line.split(":")[1].strip()
                    # cpu_info = f"Model: {cpu_model}"
                    return f"{cpu_model[0]} {cpu_model[1]}cores"
    except FileNotFoundError:
        return "CPU info not available"
        pass


def get_total_memory_gb():
    memory_info = psutil.virtual_memory()
    total_memory_bytes = memory_info.total
    total_memory_gb = round(total_memory_bytes / (1024 ** 3))
    return total_memory_gb


def get_ifname_by_ip(ip_address):
    for interface, addrs in psutil.net_if_addrs().items():
        for addr in addrs:
            if addr.family == socket.AF_INET and addr.address == ip_address:
                return interface
    return None


def get_elk_config():
    url = "http://localhost:8000/elk/config"
    while True:
        global config_data
        try:
            response = requests.get(url)
            if response.status_code == 200:
                config_data = response.json()
            else:
                pass
        except requests.exceptions.RequestException as e:
            pass
        time.sleep(10)


def ipv6_search(ipv6_address):
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='mspvAtxchJA2',
        database='ipv6'
    )
    cursor = connection.cursor()
    sql = """
        SELECT * 
        FROM ipv6_china_mainland
        WHERE 
            ip_dig_min_bin <= INET6_ATON(%s) 
            AND ip_dig_max_bin >= INET6_ATON(%s)
        ORDER BY ip_dig_min_bin DESC 
        LIMIT 1;
        """

    cursor.execute(sql, (ipv6_address, ipv6_address))

    result = cursor.fetchone()

    cursor.close()
    connection.close()

    return result


banner = f"""启动NettrafficAnalyzer_for_ELK程序...\n
 ████     ██ ██████████     ██             ████                       ████████ ██       ██   ██
░██░██   ░██░░░░░██░░░     ████           ░██░                       ░██░░░░░ ░██      ░██  ██ 
░██░░██  ░██    ░██       ██░░██         ██████  ██████  ██████      ░██      ░██      ░██ ██  
░██ ░░██ ░██    ░██      ██  ░░██       ░░░██░  ██░░░░██░░██░░█      ░███████ ░██      ░████   
░██  ░░██░██    ░██     ██████████        ░██  ░██   ░██ ░██ ░       ░██░░░░  ░██      ░██░██  
░██   ░░████    ░██    ░██░░░░░░██        ░██  ░██   ░██ ░██         ░██      ░██      ░██░░██ 
░██    ░░███    ░██    ░██     ░██ █████  ░██  ░░██████ ░███    █████░████████░████████░██ ░░██
░░      ░░░     ░░     ░░      ░░ ░░░░░   ░░    ░░░░░░  ░░░    ░░░░░ ░░░░░░░░ ░░░░░░░░ ░░   ░░ 

【程序版本】：v1.0   
【更新时间】：2024/11/17
【系统信息】：{get_system_info()}  
【CPU信息】：{get_cpu_info()}
【内存总量】：{get_total_memory_gb()}GB
【当前路径】：{os.getcwd()}
"""