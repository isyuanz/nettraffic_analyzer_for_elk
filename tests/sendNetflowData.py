import socket
import struct
import random
import time

# 配置参数
LOGSTASH_IP = '122.189.33.130'  # 替换为您的 ELK 服务器 IP
LOGSTASH_PORT = 2056  # Logstash 监听的端口

def create_netflow_v5_packet():
    # NetFlow v5 Header
    version = 5
    count = 1
    sys_uptime = 123456789
    unix_secs = int(time.time())
    unix_nsecs = 0
    flow_sequence = random.randint(1, 4294967295)
    engine_type = 0
    engine_id = 0
    sampling_interval = 0

    header = struct.pack('!HHLLLLBBH',
                         version,
                         count,
                         sys_uptime,
                         unix_secs,
                         unix_nsecs,
                         flow_sequence,
                         engine_type,
                         engine_id,
                         sampling_interval)

    # NetFlow v5 Flow Record
    src_addr = socket.inet_aton('192.168.1.100')
    dst_addr = socket.inet_aton('192.168.1.200')
    nexthop = socket.inet_aton('192.168.1.1')

    input_snmp = 2
    output_snmp = 3
    dpkts = 100
    dbytes = 12345
    first = int(time.time())
    last = int(time.time())
    src_port = 12345
    dst_port = 80
    pad1 = 0
    tcp_flags = 24
    prot = 6
    tos = 0
    src_as = 64512
    dst_as = 64513
    src_mask = 24
    dst_mask = 24
    pad2 = 0

    try:
        flow_record = struct.pack('!IIIHHIIIIHHBBBBHHBBH',
                                  struct.unpack('!I', src_addr)[0],
                                  struct.unpack('!I', dst_addr)[0],
                                  struct.unpack('!I', nexthop)[0],
                                  input_snmp,
                                  output_snmp,
                                  dpkts,
                                  dbytes,
                                  first,
                                  last,
                                  src_port,
                                  dst_port,
                                  pad1,
                                  tcp_flags,
                                  prot,
                                  tos,
                                  src_as,
                                  dst_as,
                                  src_mask,
                                  dst_mask,
                                  pad2)
    except struct.error as e:
        print(f"Flow Record struct.pack error: {e}")
        return None

    # 验证长度
    if len(flow_record) != 48:
        print(f"Flow Record length is {len(flow_record)} bytes, expected 48 bytes.")
        return None

    # 拼接 Header 和 Flow Record
    packet = header + flow_record

    # 验证整个数据包长度
    if len(packet) != 72:
        print(f"Packet length is {len(packet)} bytes, expected 72 bytes.")
        return None

    return packet

def send_netflow_packet(packet, ip, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(packet, (ip, port))
        sock.close()
        print(f"NetFlow v5 packet sent to {ip}:{port}")
    except socket.error as e:
        print(f"Socket error: {e}")

if __name__ == "__main__":
    while True:
        packet = create_netflow_v5_packet()
        if packet:
            print(f"Packet length: {len(packet)} bytes")
            send_netflow_packet(packet, LOGSTASH_IP, LOGSTASH_PORT)
            print("-------------------------------------------------------------------------")
        else:
            print("Failed to create NetFlow packet.")
        time.sleep(2)
