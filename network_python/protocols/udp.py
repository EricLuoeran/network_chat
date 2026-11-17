"""
udp —— 【第 1 层 传输层】UDP：User Datagram Protocol（用户数据报协议）

═══════════════════════════════════════════════════════════════════════
一、UDP 是什么
═══════════════════════════════════════════════════════════════════════

UDP 是一套**极简**的传输层规则，核心特点：
  · 无连接：不需要先"接通"，拿到对方地址直接发
  · 不可靠：发出去就不管了，不确认、不重传、不保证顺序
  · 有边界：一次 sendto 对应一个完整数据报，对端一次 recvfrom 收到完整一包
            （这一点和 TCP 的"字节流"截然不同）

比喻：拿大喇叭喊话 / 寄平邮。

【那 UDP 有什么用？】
正因为不管可靠性，它的开销极小、延迟极低。适合：
  · 实时音视频（丢几帧无所谓，重传反而造成卡顿）
  · 局域网"发现设备"（周期性喊一嗓子，这次丢了下次还有）
  · DNS 查询（短小、频繁，丢了重问一次即可）

本项目里 UDP 只干一件事：**在局域网里喊话找邻居**，绝不传输聊天内容。

═══════════════════════════════════════════════════════════════════════
二、UDP 报文格式（头部固定 8 字节）
═══════════════════════════════════════════════════════════════════════

     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |          源端口              |          目的端口             |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |            长度              |            校验和             |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                         数据 ...                              |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

总共只有 4 个字段，非常简洁（对比 TCP 头部最少 20 字节）。

注意：**这个头部由操作系统内核自动封装/解析**，应用程序用 socket API
发数据时只提供"数据部分"，收发都拿不到头部。下面代码里的 UDPHeader
类只是为了教学演示 —— 让你看清这 8 个字节长什么样。

═══════════════════════════════════════════════════════════════════════
三、三种发送方式
═══════════════════════════════════════════════════════════════════════

  单播（Unicast）  发给某一个具体的 IP
  广播（Broadcast） 发给整个网段，如 192.168.1.255，网段内所有设备都能收到
  组播（Multicast）发给某个"组地址"（224.0.0.0~239.255.255.255），
                   只有"加入了这个组"的设备才收得到，比广播更精准

本项目两种都用了：
  · broadcast.py  用 UDP 广播（端口 19877）
  · dns.py(mDNS)  用 UDP 组播（224.0.0.251:5353）
"""

import socket
import struct

from .common import pack_u16, unpack_u16, print_hex


# ===========================================================================
# 教学演示：UDP 头部长什么样（真实收发时由内核处理，应用层看不到）
# ===========================================================================

class UDPHeader:
    """
    UDP 头部：固定 8 字节。

    仅用于教学演示 —— 让你看清内核帮你封装的这 8 个字节。
    实际收发数据时，应用层拿到/发出的都只是"数据部分"。
    """

    SIZE = 8  # UDP 头部固定 8 字节

    def __init__(self, src_port: int, dst_port: int, length: int, checksum: int = 0):
        self.src_port = src_port
        self.dst_port = dst_port
        self.length = length      # 头部 + 数据的总字节数
        self.checksum = checksum  # 可选，IPv4 下可以是 0（表示不校验）

    def encode(self) -> bytes:
        """按上图的格式打包成 8 字节"""
        return struct.pack("!HHHH",
                           self.src_port,
                           self.dst_port,
                           self.length,
                           self.checksum)

    @classmethod
    def decode(cls, data: bytes) -> "UDPHeader":
        """从 8 字节解析出各字段"""
        if len(data) < cls.SIZE:
            raise ValueError(f"UDP 头部至少需要 {cls.SIZE} 字节，实际 {len(data)}")
        src_port, dst_port, length, checksum = struct.unpack_from("!HHHH", data, 0)
        return cls(src_port, dst_port, length, checksum)

    def __repr__(self) -> str:
        return (f"UDPHeader(src_port={self.src_port}, dst_port={self.dst_port}, "
                f"length={self.length}, checksum=0x{self.checksum:04x})")


# ===========================================================================
# UDP socket 封装
# ===========================================================================

class UDPSocket:
    """
    UDP socket 封装。

    关键 API 对比（体现"无连接"）：
        UDP：sendto(data, addr) / recvfrom(bufsize)  ← 每次都要指定地址
        TCP：connect(addr) 后 send(data) / recv(n)   ← 先连后发，不用带地址
    """

    def __init__(self, port: int = 0, bind_ip: str = "0.0.0.0"):
        """
        创建 UDP socket。

        :param port:    绑定端口，0 表示让系统自动分配
        :param bind_ip: 绑定 IP，'0.0.0.0' 表示本机所有网卡
        """
        # AF_INET   = IPv4
        # SOCK_DGRAM = 数据报套接字（UDP）；若是 SOCK_STREAM 则是 TCP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # SO_REUSEADDR：允许端口复用，避免程序重启时"地址已被占用"
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.sock.bind((bind_ip, port))
        self.port = self.sock.getsockname()[1]

    # --------------------------------------------------------------
    # 基础收发
    # --------------------------------------------------------------

    def sendto(self, data: bytes, ip: str, port: int) -> None:
        """向指定 IP:端口 发一个数据报（不保证到达、不等待回应）"""
        self.sock.sendto(data, (ip, port))

    def recvfrom(self, bufsize: int = 65535, timeout: float = None):
        """
        接收一个数据报。

        注意：UDP 保证"一次 recvfrom 拿到完整的一包"，
        不会像 TCP 那样出现半包/粘包 —— 这是数据报与字节流的本质区别。

        :return: (数据, (来源IP, 来源端口))，超时返回 (None, None)
        """
        self.sock.settimeout(timeout)
        try:
            data, addr = self.sock.recvfrom(bufsize)
            return data, addr
        except socket.timeout:
            return None, None

    # --------------------------------------------------------------
    # 广播：发给整个网段
    # --------------------------------------------------------------

    def enable_broadcast(self) -> None:
        """允许发送广播包（默认禁止，必须显式打开）"""
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def broadcast(self, data: bytes, port: int, bcast_ip: str = "255.255.255.255") -> None:
        """
        向网段的广播地址发送数据。

        广播地址举例：
            255.255.255.255  受限广播，只在本地网段传播，路由器不转发
            192.168.1.255    定向广播，发给 192.168.1.0/24 这个网段的所有主机

        本项目 discovery.py 用的就是这种方式（端口 19877）。
        """
        self.enable_broadcast()
        self.sock.sendto(data, (bcast_ip, port))

    # --------------------------------------------------------------
    # 组播：只发给"加入了组"的设备
    # --------------------------------------------------------------

    def join_multicast(self, group: str) -> None:
        """
        加入一个组播组。

        组播地址范围是 224.0.0.0 ~ 239.255.255.255。
        只有主动"加入"的设备才会收到该组的报文 —— 比广播精准，
        不会打扰网段内无关的设备（广播会打扰所有设备）。

        mDNS 用的组播地址是 224.0.0.251，端口 5353（见 dns.py）。
        """
        # 把组播地址转成 32 位二进制（网络字节序）
        mreq = socket.inet_aton(group) + socket.inet_aton("0.0.0.0")
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    def leave_multicast(self, group: str) -> None:
        """退出组播组"""
        mreq = socket.inet_aton(group) + socket.inet_aton("0.0.0.0")
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)

    # --------------------------------------------------------------

    def close(self) -> None:
        self.sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ===========================================================================
# 演示：直观感受 UDP 的"数据报边界"和"无连接"
# ===========================================================================

def demo():
    print("=" * 70)
    print("UDP 协议演示")
    print("=" * 70)

    # ---- 1. 看看 UDP 头部长什么样 ----
    print("\n【1】UDP 头部（8 字节，由内核封装，这里只是演示）")
    payload = b"hello"
    header = UDPHeader(src_port=54321, dst_port=19877,
                       length=UDPHeader.SIZE + len(payload))
    raw = header.encode() + payload
    print_hex(raw, "完整的 UDP 数据报（头部 + 数据）")
    print(f"解析回来：{UDPHeader.decode(raw)}\n")

    # ---- 2. 无连接收发 + 数据报边界 ----
    print("【2】无连接收发：一次 sendto = 一个完整数据报")
    receiver = UDPSocket(port=0)          # 端口 0 = 系统自动分配
    sender = UDPSocket(port=0)

    print(f"接收方监听端口：{receiver.port}")

    # 连发 3 个独立数据报
    for i in range(1, 4):
        sender.sendto(f"第{i}个数据报".encode("utf-8"), "127.0.0.1", receiver.port)

    # 收 3 次：每次拿到完整且独立的一包，不会粘在一起
    for i in range(1, 4):
        data, addr = receiver.recvfrom(timeout=2.0)
        print(f"  收到来自 {addr} 的数据报：{data.decode('utf-8')}")

    print("\n  ↑ 注意：3 次发送对应 3 次接收，边界清晰。"
          "\n     这正是 UDP 与 TCP 最大的区别（TCP 是字节流，会粘包）。")

    # ---- 3. 演示"不可靠" ----
    print("\n【3】不可靠性演示：发给一个没人监听的端口")
    print("  sendto() 调用本身会立刻返回 —— 不等待、不确认任何东西。")
    try:
        # 注意：Python3 里 b"中文" 是非法语法，必须写 "中文".encode("utf-8")
        sender.sendto("有人吗?".encode("utf-8"), "127.0.0.1", 59999)  # 该端口无服务
        data, addr = sender.recvfrom(timeout=0.5)
        print(f"  尝试接收回应：{data}")
    except ConnectionResetError:
        # ★ 这是 Windows 特有的行为，非常有教学价值：
        #   把数据报发往一个无人监听的端口时，对端主机会回一个
        #   ICMP「端口不可达」报文，Windows 会把它翻译成
        #   ConnectionResetError，在下一次 socket 操作时抛出来。
        print("  抛出 ConnectionResetError（远程主机强迫关闭了一个现有的连接）")
        print("  ↑ 这是 Windows 把对方的 ICMP『端口不可达』翻译成了异常，")
        print("     等于间接告诉你『那个端口没人』。")
        print("     ★ 但要注意：这只是操作系统的额外提示，")
        print("       **不是 UDP 协议本身的保证** —— 换成别的系统或网络，")
        print("       很可能完全静默，你根本无从得知对方收没收到。")
    print("  ↑ 无论如何，UDP 发出去就完事，不会重传。\n")

    receiver.close()
    sender.close()


if __name__ == "__main__":
    demo()
