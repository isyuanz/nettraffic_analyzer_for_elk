import socket
import struct
import time
import random
import sys

# sFlow默认的UDP端口
SFLOW_PORT = 2055
# 目标收集器IP
COLLECTOR_IP = '122.189.33.130'  # 修改为你的收集器IP


def build_sflow_header(sequence_number, uptime, agent_ip='127.0.0.1', agent_address_type=1, sub_agent_id=0):
    """
    构建sFlow Header
    """
    version = 5
    # 根据地址类型选择IPv4或IPv6
    if agent_address_type == 1:
        agent_address = socket.inet_pton(socket.AF_INET, agent_ip)
        # Padding agent_address to 16 bytes for uniformity (IPv4-mapped IPv6)
        agent_address_padded = agent_address + b'\x00' * 12
        header_format = '!II16sIII'
        packed_header = struct.pack(
            header_format,
            version,
            agent_address_type,
            agent_address_padded,
            sub_agent_id,
            sequence_number,
            uptime
        )
    elif agent_address_type == 16:
        agent_address = socket.inet_pton(socket.AF_INET6, agent_ip)
        header_format = '!II16sIII'
        packed_header = struct.pack(
            header_format,
            version,
            agent_address_type,
            agent_address,
            sub_agent_id,
            sequence_number,
            uptime
        )
    else:
        raise ValueError("Unsupported agent_address_type")

    return packed_header

def build_flow_sample(sequence_number):
    """
    构建Flow Sample记录
    """
    sample_type = 1  # FLOW_SAMPLE
    source_id = 0
    # Placeholder for sample_length
    flow_sample_header = struct.pack('!IIII', sequence_number, source_id, sample_type, 0)

    # Flow Sample内容（简化）
    # Enterprise = 0 (standard), Format = 1 (Flow Sample)
    enterprise = 0
    format_type = 1
    flow_sample_payload = struct.pack('!II', enterprise, format_type)

    # Sample Data (简化，实际sFlow Flow Sample包含更多字段)
    # Flow Sample Header: samples are structured as header + data records
    # For example purposes, we'll add a single flow record with minimal fields
    flow_record_format = 1  # Raw Packet Flow
    flow_record_length = 24  # Adjust according to actual data
    flow_record_header = struct.pack('!II', flow_record_format, flow_record_length)

    # Flow Record内容（简化）
    input_port = random.randint(1, 65535)
    output_port = random.randint(1, 65535)
    packet_count = random.randint(100, 10000)
    byte_count = random.randint(1000, 1000000)
    # 这里简化，仅包含这些字段
    flow_record_payload = struct.pack('!IIII', input_port, output_port, packet_count, byte_count)

    # 总Flow Sample内容
    flow_sample_content = flow_sample_payload + flow_record_header + flow_record_payload

    # 现在计算sample_length
    sample_length = len(flow_sample_header) + len(flow_sample_content)
    # 重建flow_sample_header with correct sample_length
    flow_sample_header = struct.pack('!IIII', sequence_number, source_id, sample_type, sample_length)

    # 完整Flow Sample
    flow_sample = flow_sample_header + flow_sample_content

    return flow_sample

def build_sflow_packet(sequence_number, uptime):
    """
    构建完整的sFlow数据包
    """
    # 构建sFlow Header
    sflow_header = build_sflow_header(sequence_number, uptime)

    # 构建Data Records
    data_records = build_flow_sample(sequence_number)

    # 记录数，这里只有一个Flow Sample
    num_data_records = 1
    data_records_count = struct.pack('!I', num_data_records)

    # 完整sFlow数据包
    sflow_packet = sflow_header + data_records_count + data_records

    return sflow_packet

def send_sflow_packet(collector_ip, collector_port, packet):
    """
    发送sFlow数据包到目标收集器
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(packet, (collector_ip, collector_port))
    sock.close()

def main():
    sequence_number = 1
    uptime = 0  # 毫秒
    try:
        while True:
            # 更新时间和序列号
            uptime += 100  # 假设每100ms发送一次
            packet = build_sflow_packet(sequence_number, uptime)
            send_sflow_packet(COLLECTOR_IP, SFLOW_PORT, packet)
            print(f"发送sFlow包，序列号: {sequence_number}, 上行时间: {uptime} ms")
            sequence_number += 1
            time.sleep(0.1)  # 100ms
    except KeyboardInterrupt:
        print("\n发送已停止。")
        sys.exit()
    except Exception as e:
        print(f"发生错误: {e}")
        sys.exit()

if __name__ == "__main__":
    main()
