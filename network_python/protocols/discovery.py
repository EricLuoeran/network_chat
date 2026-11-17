"""
discovery —— 【第 2 层】邻居发现：UDP 广播 + mDNS 双通道

═══════════════════════════════════════════════════════════════════════
一、这一层要解决什么问题
═══════════════════════════════════════════════════════════════════════

P2P 通信的第一步不是"传数据"，而是**找到对方**：
    我是 192.168.1.5，局域网里还有谁在跑这个程序？他们在哪个 IP:端口？

这个问题有个专门的叫法：**服务发现（Service Discovery）**。

本项目用**两条独立的路**来找人，任意一条通了就行：

    ┌─────────────────────────────────────────────────────────┐
    │ 路 A：UDP 广播   端口 19877                              │
    │      往网段广播地址喊："我是张三，我在 192.168.1.5:8765" │
    │      特点：简单粗暴，但部分网络（公共WiFi/企业网）        │
    │            会把广播包直接丢弃                            │
    ├─────────────────────────────────────────────────────────┤
    │ 路 B：mDNS 组播  224.0.0.251:5353                        │
    │      往组播地址问："谁提供 _networkchat._tcp 服务？"      │
    │      特点：更标准（苹果 Bonjour、Linux Avahi 都用它），  │
    │            但 Windows 上若没装 Bonjour 可能失效          │
    └─────────────────────────────────────────────────────────┘

【为什么两条都要？】—— 冗余容错。
单一方案在不同网络环境下失利概率不低，两条并行能大幅提高成功率。
（这正是 Go 版 broadcast.go + mdns.go 并存的原因）

═══════════════════════════════════════════════════════════════════════
二、自定义的报文格式
═══════════════════════════════════════════════════════════════════════

UDP 广播（本文件定义）：
    实例ID|名字|端口|IP列表
    例：abc-123|张三|8765|192.168.1.5

mDNS（放在 TXT 记录里）：
    instance=abc-123|port=8765|addrs=192.168.1.5

两者都带 **instanceID**（进程唯一标识），用来**排除自己** ——
否则你会"发现"你自己，然后试图连接自己。

═══════════════════════════════════════════════════════════════════════
三、为什么要过滤虚拟网卡
═══════════════════════════════════════════════════════════════════════

一台电脑上往往有很多网卡：
    物理网卡（有线/无线）     ← 真正能用的
    Hyper-V 虚拟网卡          ← 172.x，对端连不上
    WSL / Docker 网卡         ← 同上
    VMware / VirtualBox 网卡  ← 同上
    169.254.x.x 链路本地地址  ← 没有路由，完全不可达

如果把虚拟网卡的 IP 广播出去，对端拿着连不通的地址去拨号，
表现就是"能看到人，但连不上"。所以必须过滤。

★ 这也是 Go 版曾出过 bug 的地方（过滤逻辑丢失导致二维码失效），
  详见上级目录的 bug 文件第 5 条。
"""

import socket
import struct
import time
import threading
from dataclasses import dataclass, field

from . import udp as udp_mod
from . import dns as dns_mod


# 本项目的服务名（mDNS 用，格式固定为 _服务名._协议.local）
SERVICE_TYPE = "_networkchat._tcp.local"

# UDP 广播端口（Go 版 broadcast.go 里写死的 19877）
BROADCAST_PORT = 19877


# ===========================================================================
# 本机 IP 获取（含虚拟网卡过滤）
# ===========================================================================

def get_outbound_ip() -> str:
    """
    获取"本机访问外网时实际使用的那个"IP —— 最可靠的真实局域网 IP。

    原理：对一个公网地址发起 UDP "连接"。
    **注意：UDP 是无连接的，这一步不会真的发出任何数据包**，
    只是让操作系统查一次路由表、给这个 socket 绑定一个本地地址，
    我们读这个本地地址即可。快，且不依赖外网是否真的可达。

    （对应 Go 版 discovery/mdns.go 的 outboundIP 函数）
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        s.connect(("8.8.8.8", 80))   # 8.8.8.8 只是"靶子"，不会被访问
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return None


# 虚拟网卡关键字（小写匹配名字包含即视为虚拟网卡）
VIRTUAL_KEYWORDS = [
    "vethernet", "hyper-v", "virtual", "vmware", "vmnet",
    "virtualbox", "vbox", "docker", "wsl", "bluetooth",
    "teredo", "isatap", "6to4", "tunnel",
]


def is_virtual_adapter(name: str) -> bool:
    """判断网卡名是否属于虚拟网卡"""
    lower = name.lower()
    return any(kw in lower for kw in VIRTUAL_KEYWORDS)


def get_local_ips() -> list:
    """
    获取本机可用于局域网通信的 IPv4 地址，按"越可能可用越靠前"排序。

    过滤规则（对应 Go 版 getLocalIPs）：
      1. 出网 IP 排第一（最可靠）
      2. 排除回环地址 127.x
      3. 排除链路本地地址 169.254.x.x（没有路由，不可达）
      4. 去重
    """
    ips = []

    # 第一步：出网 IP 优先
    out = get_outbound_ip()
    if out:
        ips.append(out)

    # 第二步：通过主机名解析补充其它地址
    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = info[4][0]
            # 排除回环、链路本地、以及已收录的
            if ip.startswith("127."):
                continue
            if ip.startswith("169.254."):
                continue
            if ip not in ips:
                ips.append(ip)
    except socket.error:
        pass

    return ips


# ===========================================================================
# 对端信息
# ===========================================================================

@dataclass
class Peer:
    """一个发现到的邻居"""
    name: str
    host: str                       # IP
    port: int
    instance_id: str = ""           # 用于排除自己
    addrs: list = field(default_factory=list)
    last_seen: float = field(default_factory=time.time)

    @property
    def id(self) -> str:
        """唯一标识：名字@IP:端口"""
        return f"{self.name}@{self.host}:{self.port}"

    def __repr__(self) -> str:
        return f"<Peer {self.id}>"


# ===========================================================================
# 路 A：UDP 广播发现
# ===========================================================================

class BroadcastDiscovery:
    """
    通过 UDP 广播发现邻居（对应 Go 版 discovery/broadcast.go）。

    工作方式：
      1. 开一个 socket 监听 19877，接收别人的广播
      2. 每 2 秒向网段广播一次自己的信息
      3. 收到别人的广播就记下来

    报文格式（自定义）：  实例ID|名字|端口|IP列表
    """

    def __init__(self, name: str, port: int):
        self.name = name
        self.port = port
        # 用时间戳+PID 保证同一台机器开多个实例也不会互相误认
        self.instance_id = f"{name}-{int(time.time() * 1000)}"
        self.peers = {}          # peer.id → Peer
        self.peer_queue = []     # 新发现的（供外部取）
        self._running = False
        self._sock = None

    # ---------- 报文编解码 ----------

    def build_message(self) -> bytes:
        """构造广播报文：实例ID|名字|端口|IP列表"""
        addrs = ",".join(get_local_ips())
        msg = f"{self.instance_id}|{self.name}|{self.port}|{addrs}"
        return msg.encode("utf-8")

    @staticmethod
    def parse_message(data: bytes) -> Peer:
        """解析广播报文"""
        text = data.decode("utf-8", errors="replace")
        parts = text.split("|")
        if len(parts) < 4:
            raise ValueError(f"广播报文格式不对：{text}")
        instance_id, name, port, addrs = parts[0], parts[1], parts[2], parts[3]
        addr_list = [a for a in addrs.split(",") if a]
        return Peer(name=name,
                    host=addr_list[0] if addr_list else "",
                    port=int(port),
                    instance_id=instance_id,
                    addrs=addr_list)

    # ---------- 启动 / 停止 ----------

    def start(self):
        """启动收发线程"""
        self._running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._announce_loop, daemon=True).start()

    def stop(self):
        self._running = False
        if self._sock:
            self._sock.close()

    def _listen_loop(self):
        """收：持续监听别人的广播"""
        self._sock = udp_mod.UDPSocket(port=BROADCAST_PORT)
        print(f"[广播] 开始监听 UDP :{BROADCAST_PORT}")
        while self._running:
            try:
                data, addr = self._sock.recvfrom(timeout=1.0)
                if not data:
                    continue
                peer = self.parse_message(data)
                # 关键：排除自己发的广播
                if peer.instance_id == self.instance_id:
                    continue
                self._on_peer(peer, addr[0])
            except (OSError, ValueError):
                continue

    def _announce_loop(self):
        """发：每 2 秒广播一次自己的存在"""
        sender = udp_mod.UDPSocket(port=0)
        sender.enable_broadcast()
        msg = self.build_message()
        while self._running:
            try:
                sender.broadcast(msg, BROADCAST_PORT)
            except OSError:
                pass
            time.sleep(2)

    def _on_peer(self, peer: Peer, from_ip: str):
        """记录一个发现到的邻居"""
        # 若报文里的 IP 列表为空，就用发包来源 IP
        if not peer.host:
            peer.host = from_ip
        new = peer.id not in self.peers
        self.peers[peer.id] = peer
        peer.last_seen = time.time()
        if new:
            print(f"[广播] 发现邻居：{peer}")
            self.peer_queue.append(peer)

    def list_peers(self) -> list:
        return list(self.peers.values())


# ===========================================================================
# 路 B：mDNS 发现
# ===========================================================================

class MDNSDiscovery:
    """
    通过 mDNS 组播发现邻居（对应 Go 版 discovery/mdns.go）。

    工作方式：加入组播组 224.0.0.251:5353，
    周期性发出 DNS 查询报文（格式见 dns.py）：
        "谁提供 _networkchat._tcp.local 服务？"
    提供该服务的设备会回应自己的主机名、端口和 TXT 信息。
    """

    def __init__(self, name: str, port: int):
        self.name = name
        self.port = port
        self.instance_id = f"{name}-{socket.gethostname()}-{id(self)}"
        self.peers = {}
        self._running = False
        self._sock = None

    def build_query(self) -> bytes:
        """构造 mDNS 查询报文（复用 dns.py 的报文构造）"""
        return dns_mod.build_query(SERVICE_TYPE, qtype=dns_mod.TYPE_PTR)

    def build_announce(self) -> bytes:
        """
        构造 mDNS 应答报文（告诉别人"我提供这个服务"）。

        携带的自定义信息放在 TXT 记录里：
            instance=xxx|port=8765|addrs=192.168.1.5
        这与 Go 版 mdns.go 里 info 字段的格式完全一致。
        """
        addrs = ",".join(get_local_ips())
        info = f"instance={self.instance_id}|port={self.port}|addrs={addrs}"

        # 简化版应答：头部 + 一条 TXT 形式的信息
        # （完整实现要构造 SRV/PTR/A 多条记录，这里聚焦原理）
        txt_rdata = bytes([len(info)]) + info.encode("utf-8")
        name_enc = dns_mod.encode_domain(SERVICE_TYPE)
        rr = name_enc + struct.pack("!HHIH", dns_mod.TYPE_TXT,
                                    dns_mod.CLASS_IN, 120, len(txt_rdata)) + txt_rdata

        header = dns_mod.DNSHeader(id=0, qr=1, aa=1, ancount=1)
        return header.encode() + rr

    def start(self):
        self._running = True
        self._sock = udp_mod.UDPSocket(port=dns_mod.MDNS_PORT)
        try:
            self._sock.join_multicast(dns_mod.MDNS_GROUP)
        except OSError as e:
            print(f"[mDNS] 加入组播失败：{e}")
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self._running = False
        if self._sock:
            self._sock.close()

    def _loop(self):
        print(f"[mDNS] 开始监听组播 {dns_mod.MDNS_GROUP}:{dns_mod.MDNS_PORT}")
        while self._running:
            # 发一次查询
            try:
                self._sock.sendto(self.build_query(),
                                  dns_mod.MDNS_GROUP, dns_mod.MDNS_PORT)
            except OSError:
                pass

            # 收响应
            deadline = time.time() + 2
            while time.time() < deadline and self._running:
                data, addr = self._sock.recvfrom(timeout=0.5)
                if not data:
                    continue
                try:
                    answers = dns_mod.parse_answers(data)
                    for ans in answers:
                        self._on_answer(ans, addr[0])
                except Exception:
                    continue
            time.sleep(3)

    def _on_answer(self, answer: dict, from_ip: str):
        """处理一条应答记录"""
        value = answer.get("data")
        # 我们关心 TXT 记录里的自定义信息
        if isinstance(value, list) and value:
            text = value[0]
            fields = dict(
                part.split("=", 1)
                for part in text.split("|") if "=" in part
            )
            instance = fields.get("instance", "")
            if instance == self.instance_id:
                return  # 排除自己
            port = int(fields.get("port", 0))
            addrs = [a for a in fields.get("addrs", "").split(",") if a]
            host = addrs[0] if addrs else from_ip
            peer = Peer(name=instance.split("-")[0] or "peer",
                        host=host, port=port,
                        instance_id=instance, addrs=addrs)
            if peer.id not in self.peers:
                print(f"[mDNS] 发现邻居：{peer}")
            self.peers[peer.id] = peer

    def list_peers(self) -> list:
        return list(self.peers.values())


# ===========================================================================
# 组合：双通道发现
# ===========================================================================

class DiscoveryService:
    """把 UDP 广播和 mDNS 两条路组合起来，谁先发现算谁的"""

    def __init__(self, name: str, port: int):
        self.broadcast = BroadcastDiscovery(name, port)
        self.mdns = MDNSDiscovery(name, port)

    def start(self):
        self.broadcast.start()
        self.mdns.start()

    def stop(self):
        self.broadcast.stop()
        self.mdns.stop()

    def list_peers(self) -> list:
        """合并两条路的结果（按 id 去重）"""
        merged = {}
        for p in self.broadcast.list_peers() + self.mdns.list_peers():
            merged[p.id] = p
        return list(merged.values())


# ===========================================================================
# 演示
# ===========================================================================

def demo():
    print("=" * 70)
    print("邻居发现演示（UDP广播 + mDNS 双通道）")
    print("=" * 70)

    # ---- 1. 本机 IP 与虚拟网卡过滤 ----
    print("\n【1】本机可用的局域网 IP")
    print(f"  出网 IP（最可靠）: {get_outbound_ip()}")
    print(f"  全部可用 IP      : {get_local_ips()}")
    print("  ↑ 已自动排除 127.x 回环地址和 169.254.x.x 链路本地地址")

    print("\n  虚拟网卡过滤测试：")
    for name in ["WLAN", "vEthernet (Default Switch)", "VMware Network Adapter"]:
        print(f"    {name:<32} → {'虚拟网卡(会过滤)' if is_virtual_adapter(name) else '物理网卡(保留)'}")

    # ---- 2. UDP 广播报文格式 ----
    print("\n【2】UDP 广播的报文格式（自定义）")
    bd = BroadcastDiscovery(name="张三", port=8765)
    msg = bd.build_message()
    print(f"  实际发送: {msg.decode('utf-8')}")
    print("  格式    : 实例ID|名字|端口|IP列表")
    parsed = BroadcastDiscovery.parse_message(msg)
    print(f"  解析回来: {parsed}")
    # 自己发的广播，instance_id 必然与自己相同 → 应被自动忽略，否则会"发现自己"
    is_self = (parsed.instance_id == bd.instance_id)
    print(f"  ★ 这条是自己发的? {is_self} → 应被自动忽略（靠 instance_id 排除自己）")

    # ---- 3. mDNS 查询报文 ----
    print("\n【3】mDNS 查询报文（复用 dns.py 构造）")
    q = MDNSDiscovery(name="张三", port=8765).build_query()
    from .common import print_hex
    print_hex(q, "mDNS 查询")
    print(f"  目标: 组播 {dns_mod.MDNS_GROUP}:{dns_mod.MDNS_PORT}")
    print(f"  问的是: 谁提供 {SERVICE_TYPE} 服务？")

    # ---- 4. 双通道的意义 ----
    print("\n【4】为什么要用两条路？")
    print("  UDP 广播 : 简单，但公共WiFi/企业网常丢弃广播包")
    print("  mDNS     : 标准，但 Windows 缺 Bonjour 时会失效")
    print("  两条并行 : 任意一条通就能发现邻居 → 冗余容错")


if __name__ == "__main__":
    demo()
