"""
tcp —— 【第 1 层 传输层】TCP：Transmission Control Protocol（传输控制协议）

★ 本文件是网络协议讲解的"编程代码"素材，下面的注释与《TCP学习文档.md》逐块对应。
  阅读顺序建议：先读《TCP学习文档.md》的块 1~7，再回来对着代码看。

═══════════════════════════════════════════════════════════════════════
一、TCP 是什么                                        ← 对应【学习文档·块 1】
═══════════════════════════════════════════════════════════════════════

TCP 是一套"负责任"的传输层规则，核心特点：
  · 面向连接：通信前必须先"接通"（三次握手）
  · 可靠传输：丢包自动重传、按序到达、无重复
  · 字节流：数据像水流一样，没有"一包一包"的边界概念

比喻：打电话（先拨通、通话不丢字、连续地说）/ 寄挂号信。

【它在网络里的位置 —— 上下层支撑】
    应用层    ← 你的程序（HTTP / WebSocket / 文件传输）只管"说什么"
    传输层    ← TCP 在这里 ★本讲，负责"稳稳送到对方【程序】手里"
    网络层    ← IP 只管"送到哪台【电脑】"，不保证可靠、不保证顺序
    链路/物理层 ← 真正传比特

【代价】
为了做到可靠，TCP 头部最少 20 字节（UDP 只有 8 字节），
还要维护连接状态、拥塞控制等，开销明显大于 UDP。

本项目里 TCP 承载**全部**业务数据：HTTP、WebSocket、文件传输。

═══════════════════════════════════════════════════════════════════════
二、三次握手 / 四次挥手（连接是怎么建立和断开的）  ← 对应【学习文档·块 3、块 4】
═══════════════════════════════════════════════════════════════════════

【三次握手】建立连接 —— 双方确认彼此"能发也能收"：

    客户端                              服务端
      |  ---------- SYN ------------>   |   ① 我想连你，我的序号是 x
      |  <------ SYN + ACK ----------   |   ② 收到，我也想连你，我的序号是 y
      |  ---------- ACK ------------>   |   ③ 收到，开始通信

  · 比喻：打电话前的"喂，能听到吗？"——先拨通，再说话。
  · 为什么不能只两次？两次只让服务端确认了客户端能收发，
    但客户端还没确认"服务端能收"。第三次 ACK 就是补上这句确认。

下面这份代码里：客户端 connect()、服务端 accept() 内部就自动完成了这三步。

【四次挥手】断开连接 —— 因为连接是全双工的，两个方向要分别关闭：

    主动方                              被动方
      |  ---------- FIN ------------>   |   ① 我这边没数据了
      |  <--------- ACK -------------   |   ② 知道了
      |  <--------- FIN -------------   |   ③ 我这边也没数据了
      |  ---------- ACK ------------>   |   ④ 知道，断开

  · 比喻：挂电话要两边都说"拜拜"——你说完了，对方可能还有话，等他那边也
    说完了才能挂。因为双向，所以比握手多一次（共四次）。

【注意】这些握手过程由**操作系统内核**完成，应用程序感知不到，
       我们调用 connect()/close() 时内核默默把这事办了。

═══════════════════════════════════════════════════════════════════════
三、TCP 头部（最少 20 字节）                        ← 对应【学习文档·块 2】
═══════════════════════════════════════════════════════════════════════

比喻：头部就像**快递单**，寄的东西（数据）外面贴一张单子，
      写上"从哪来、到哪去、第几件、收到没"。

     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |          源端口              |          目的端口             |  ← 门牌号：找对方程序
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                         序号 seq                              |  ← 快递编号：本段数据第几字节
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                         确认号 ack                            |  ← 回执单：我已收到第 N 字节
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    | 首部长度 | 保留 |  标志位  |            窗口大小              |  ← 开关 / 我还能收多少
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |            校验和            |          紧急指针              |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |                    选项（可选，最多 40 字节）                  |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

标志位（Flags）各占 1 bit，是本文件要重点理解的：
    SYN  建立连接   ACK  确认      FIN  关闭连接
    RST  强制重置   PSH  立即推送  URG  紧急数据

【记忆重点】这些字段不是装饰，而是后面所有"可靠、连接"功能的**开关**（见第四节）。

═══════════════════════════════════════════════════════════════════════
四、★ 最重要：TCP 是"字节流"，会粘包               ← 对应【学习文档·块 5】
═══════════════════════════════════════════════════════════════════════

比喻：UDP 像寄信（一封一封，信封就是边界）；
      TCP 像接水管流水（你倒三次水，对方可能"哗"一桶全接住，分不清哪次是哪次）。

TCP 完全没有"消息边界"的概念。你分 3 次 send("A")("B")("C")，
对端一次 recv() 可能拿到 "ABC"，也可能拿到 "AB" 再拿到 "C"。

  · 粘包：多次发送的数据被一次收到（"A"+"B"+"C" → "ABC"）
  · 拆包：一次发送的数据被分多次收到（"ABC" → "AB" + "C"）

原因：TCP 只保证字节顺序正确，不保证"一次发送对应一次接收"。
三个"捣蛋鬼"：发送方有 Nagle 算法合并小包、接收方有缓冲区、网络还有 MTU 分片。

【一句话结论】TCP 只保证"字节顺序对"，绝不保证"你发几次 = 我收几次"。
             所以边界必须由应用层自己划 —— 这就是下面第五节分帧器存在的原因。

【怎么解决？应用层必须自己定义消息边界】常见三种方案：
  1. 长度前缀：先发 4 字节表示后面数据有多长（本文件采用，最通用）
  2. 分隔符：用 \r\n 或 \0 分隔（HTTP 头部用的就是 \r\n）
  3. 定长：每条消息固定长度（浪费空间，很少用）

本项目：
  · WebSocket 帧用"长度前缀"（见 websocket.py）
  · HTTP 头部用"分隔符 \r\n\r\n"（见 http.py）
"""

import socket
import struct
import threading

from .common import read_exact, print_hex


# ===========================================================================
# 教学演示：TCP 头部长什么样（真实收发时由内核处理）
# ===========================================================================

class TCPHeader:
    """
    TCP 头部：最少 20 字节。                    ← 对应【学习文档·块 2 与块 6】

    同样只是教学演示 —— 内核帮我们封装，应用层收发的是"数据部分"。
    这里只解析最关键的几个字段（端口 / seq / ack / 标志位 / 窗口）。

    这些字段就是"可靠传输的开关"（块 6 异常控制会用到）：
      · seq + ack → 按序、去重、超时重传
      · 窗口       → 流量控制（我还能收多少）
      · 标志位     → 管连接建立/释放/确认
    """

    MIN_SIZE = 20

    # 标志位掩码（位于第 13 字节）。每个标志只占 1 个 bit，
    # 用一个十六进制数表示"哪一个 bit 被点亮"。
    # 含义（配合握手/挥手理解）：
    FIN = 0x01   # 关闭连接（挥手第 1 步）
    SYN = 0x02   # 建立连接（握手第 1、2 步）
    RST = 0x04   # 强制重置（连接异常时直接掐断）
    PSH = 0x08   # 立即推送（别缓冲了，马上交给应用层）
    ACK = 0x10   # 确认收到（握手第 2、3 步、数据传输时几乎每个包都带）
    URG = 0x20   # 紧急数据（配合"紧急指针"字段，极少用）

    def __init__(self, src_port, dst_port, seq, ack, data_offset, flags, window):
        self.src_port = src_port
        self.dst_port = dst_port
        self.seq = seq
        self.ack = ack
        self.data_offset = data_offset  # 头部占多少个 32 位字（×4 = 字节数）
        self.flags = flags
        self.window = window

    @classmethod
    def decode(cls, data: bytes) -> "TCPHeader":
        """
        从 20 字节原始数据里，按"位置"把各字段抠出来。

        头部字段的排列（见本文件第三节的图）：
          偏移 0   : 源端口(2) + 目的端口(2)          → 共 4 字节
          偏移 4   : 序号 seq(4)
          偏移 8   : 确认号 ack(4)
          偏移 12  : 首部长度(高4位) + 保留(低4位) + 标志位(1字节) + 窗口(2字节)
          偏移 14  : 窗口(2)
        struct 格式串 "!HHII" 的含义：
          !  = 网络字节序（大端），网络上传输统一用大端
          H  = unsigned short（2 字节）→ 装端口
          I  = unsigned int  （4 字节）→ 装 seq / ack
        """
        src_port, dst_port, seq, ack = struct.unpack_from("!HHII", data, 0)
        # 第 12 字节：高 4 位是"首部长度"（单位是 32 位字，×4 才是字节数），低 4 位保留
        data_offset = (data[12] >> 4) & 0x0F
        # 第 13 字节：标志位（SYN/ACK/FIN… 就藏在它的各个 bit 里）
        flags = data[13]
        # 第 14 字节起 2 字节：窗口大小（我还能收多少字节）
        window = struct.unpack_from("!H", data, 14)[0]
        return cls(src_port, dst_port, seq, ack, data_offset, flags, window)

    def flags_str(self) -> str:
        """把标志位翻译成人能看懂的字符串，如 'SYN,ACK'"""
        names = []
        for bit, name in [(self.FIN, "FIN"), (self.SYN, "SYN"), (self.RST, "RST"),
                          (self.PSH, "PSH"), (self.ACK, "ACK"), (self.URG, "URG")]:
            if self.flags & bit:
                names.append(name)
        return ",".join(names) if names else "(无)"

    def __repr__(self) -> str:
        return (f"TCPHeader({self.src_port}->{self.dst_port}, "
                f"seq={self.seq}, ack={self.ack}, "
                f"flags={self.flags_str()}, window={self.window})")


# ===========================================================================
# ★ 解决粘包：长度前缀分帧器
# ===========================================================================

class LengthPrefixFramer:
    """
    用"4 字节长度前缀"给字节流划分消息边界。   ← 对应【学习文档·块 5 后半】

    ★ 这是解决粘包最通用、最核心的做法，也是本文件重点讲解的代码。

    思路：既然 TCP 不管边界，那我们就在每条消息前面"贴上标签"，
          先告诉对方"我这条有多长"，对方就能量着长度把水流切开。

    每条消息的格式：
        +------------------+--------------------+
        |  长度 (4 字节)   |   实际数据 ...     |
        +------------------+--------------------+
        大端序 uint32（!I）

    举例（发送 b"MSG-1" 时 len=5）：
        实际发出：[00 00 00 05][M S G - 1]
                  └─长度前缀─┘ └─数据体─┘

    有了它，无论 TCP 怎么粘包拆包，都能准确还原出一条条独立消息。
    这是几乎所有二进制协议（包括 WebSocket 帧）的基础做法；
    HTTP 头部则用"分隔符 \\r\\n\\r\\n"达到同样目的（本质都是应用层定界）。
    """

    PREFIX_SIZE = 4  # 长度字段占 4 字节

    def encode(self, payload: bytes) -> bytes:
        """
        给数据加上长度前缀。
        struct.pack("!I", len(payload))：把长度打包成 4 字节大端整数，
        再用 + 拼到数据前面。例如 b"MSG-1"(5 字节) → b'\\x00\\x00\\x00\\x05MSG-1'
        """
        return struct.pack("!I", len(payload)) + payload

    def send(self, sock, payload: bytes) -> None:
        """编码后一次发出。用 sendall 而不是 send：sendall 会一直发到全部发完
        （普通 send 可能只发出去一部分，剩下的要自己循环补发）。"""
        sock.sendall(self.encode(payload))

    def recv(self, sock) -> bytes:
        """
        从 socket 读一条完整消息。

        步骤（关键！）：
          1. 先【精确】读 4 字节，得到长度 N
          2. 再【精确】读 N 字节，得到数据体
        返回 b"" 表示连接已关闭。

        为什么必须"精确读"？因为 recv(n) 只是"最多读 n 字节"，
        可能读不满。所以要借助 common.read_exact 循环读，直到读够为止。
        """
        # 第一步：读长度前缀。必须精确读 4 字节，不能只靠一次 recv
        prefix = read_exact(sock, self.PREFIX_SIZE)
        if not prefix:
            return b""  # 对端关闭了连接
        length = struct.unpack("!I", prefix)[0]   # 解出"后面还有多少字节"

        # 第二步：按刚才得到的长度，精确读 N 字节数据体
        if length == 0:
            return b""  # 长度为 0，视为空/结束
        return read_exact(sock, length)


# ===========================================================================
# TCP 服务端 / 客户端
# ===========================================================================

class TCPServer:
    """
    TCP 服务端：监听端口，等待别人来连。       ← 对应【学习文档·块 3】

    对应本项目里的角色：每台电脑都跑着一个 TCPServer，
    这样别的电脑才能主动连过来（P2P 的"服务端"身份）。

    服务端一生做两件事：
      1. bind + listen —— 占好端口，竖起耳朵等
      2. accept        —— 接受一条已完成【三次握手】的连接
    """

    def __init__(self, port: int = 0, host: str = "0.0.0.0"):
        """
        创建 socket 并开始监听。

        SOCK_STREAM 就是"流式"套接字——正是 TCP（字节流）的体现，
        换成 SOCK_DGRAM 才是 UDP（数据报）。

        :param port: 监听端口，0 表示让系统自动分配（本例 demo 用 0，避免占用冲突）
        :param host: 绑定地址，'0.0.0.0' = 监听本机所有网卡
                     （若写成 '127.0.0.1'，则只有本机能连，外部设备连不进来！）
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR：程序重启后端口能立刻复用，否则要等一会儿（TIME_WAIT）
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))   # 占领这个"门牌号"
        self.sock.listen(128)          # 转为监听态；128 = 等待队列长度(backlog)
        self.host = host
        self.port = self.sock.getsockname()[1]  # 取出实际端口（port=0 时由系统分配）

    def accept(self, timeout: float = None):
        """
        接受一个连接，返回 (socket, (对方IP, 对方端口))。
        阻塞等待——直到有客户端完成三次握手连上来。
        """
        self.sock.settimeout(timeout)
        try:
            return self.sock.accept()
        except socket.timeout:
            return None, None

    def close(self) -> None:
        self.sock.close()


class TCPClient:
    """
    TCP 客户端：主动连接别人。

    对应本项目里的角色：connectToPeer() 主动拨号另一台电脑
    （P2P 的"客户端"身份）。
    """

    def __init__(self, sock):
        self.sock = sock

    @classmethod
    def connect(cls, host: str, port: int, timeout: float = 5.0) -> "TCPClient":
        """
        连接指定的 IP:端口。                      ← 对应【学习文档·块 3】

        这一步底层就是"三次握手" —— 由内核完成，我们只是调用。
        如果对方没开服务/被防火墙挡了，这里会超时或报错。
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))  # ← ★ 三次握手就发生在这一行内部（应用层看不到）
        return cls(sock)

    def send(self, data: bytes) -> None:
        """sendall 会一直发到全部发完（普通 send 可能只发一部分）。"""
        self.sock.sendall(data)

    def recv(self, bufsize: int = 4096) -> bytes:
        """注意：recv 只是"最多读 bufsize 字节"，可能读不满——
        这正是粘包/拆包的根源。所以本文件才需要 LengthPrefixFramer 来精确定界。"""
        return self.sock.recv(bufsize)

    def close(self) -> None:
        self.sock.close()


# ===========================================================================
# 演示：亲眼看见"粘包"，以及长度前缀如何解决它
# ===========================================================================

def demo():
    print("=" * 70)
    print("TCP 协议演示")
    print("=" * 70)

    # ---- 1. TCP 头部 ----
    print("\n【1】TCP 头部（20 字节，教学演示）")
    # 手工构造一个 SYN 包：端口 54321->8765, seq=0, ack=0, 标志位 SYN
    raw = struct.pack("!HHIIBBHHH",
                      54321, 8765,  # 源端口、目的端口
                      0, 0,         # seq、ack
                      0x50,         # 首部长度 5(×4=20字节) << 4
                      0x02,         # 标志位：SYN
                      8192,         # 窗口大小
                      0, 0)         # 校验和、紧急指针
    print_hex(raw, "一个 SYN 握手包")
    print(f"解析结果：{TCPHeader.decode(raw)}\n")

    # ---- 2. 粘包演示 ----
    print("【2】★ 粘包演示：分 3 次发送，可能被一次收到")
    server = TCPServer(port=0)
    result = {}

    def server_thread():
        conn, addr = server.accept(timeout=5)
        if conn is None:
            return
        import time
        time.sleep(0.3)  # 故意等一会儿，让 3 个包都到达缓冲区
        # 一次性读一大块 —— 结果是 3 条消息粘在一起
        data = conn.recv(4096)
        result["raw"] = data
        conn.close()

    t = threading.Thread(target=server_thread, daemon=True)
    t.start()

    client = TCPClient.connect("127.0.0.1", server.port)
    # 连续发 3 条，中间不停顿
    client.sock.sendall(b"MSG-1")
    client.sock.sendall(b"MSG-2")
    client.sock.sendall(b"MSG-3")
    t.join()
    print(f"  发送了 3 次：b'MSG-1', b'MSG-2', b'MSG-3'")
    print(f"  对端一次 recv() 收到：{result.get('raw')}")
    print("  ↑ 三条消息粘成一坨了！接收方根本不知道边界在哪。\n")
    client.close()

    # ---- 3. 用长度前缀解决 ----
    print("【3】★ 用长度前缀解决粘包")
    framer = LengthPrefixFramer()
    server2 = TCPServer(port=0)
    received = []

    def server_thread2():
        conn, addr = server2.accept(timeout=5)
        if conn is None:
            return
        # 即使对方连发，也能一条条完整读出
        for _ in range(3):
            msg = framer.recv(conn)
            if not msg:
                break
            received.append(msg)
        conn.close()

    t2 = threading.Thread(target=server_thread2, daemon=True)
    t2.start()

    client2 = TCPClient.connect("127.0.0.1", server2.port)
    for text in [b"MSG-1", b"MSG-2", b"MSG-3"]:
        framer.send(client2.sock, text)
    t2.join()
    print(f"  加了长度前缀后，对端收到：{received}")
    print("  ↑ 边界清清楚楚，粘包问题解决。\n")

    client2.close()
    server.close()
    server2.close()


if __name__ == "__main__":
    demo()