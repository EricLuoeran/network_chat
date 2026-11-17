"""
node —— 【第 2 层】P2P 节点：每台电脑同时是客户端和服务端

═══════════════════════════════════════════════════════════════════════
★ 什么是 P2P？（本文件的核心）
═══════════════════════════════════════════════════════════════════════

传统 C/S（客户端/服务器）架构里：
    · 服务器：一直开着，被动等别人来连
    · 客户端：主动去连服务器
    · 角色**固定不变**，且必须有中心服务器

P2P（Peer to Peer，点对点）里：
    · 每个节点**同时是客户端和服务端**
    · 既能被动接受别人的连接，也能主动拨号别人
    · **没有中心服务器**，任意两台都能直接通信

本项目正是这样：
    ┌────────────────────────────────────────────┐
    │  电脑 A                      电脑 B        │
    │  ├─ TCPServer 监听 8765  ←── 主动来连      │  ← A 是服务端
    │  └─ connect_to_peer()   ──→  B 的 8765    │  ← A 是客户端
    └────────────────────────────────────────────┘

代码中体现 P2P 的两个方法：
    _accept_loop()      ← 服务端身份：等着被连
    connect_to_peer()   ← 客户端身份：主动连别人

═══════════════════════════════════════════════════════════════════════
★ 为什么需要 hello 握手
═══════════════════════════════════════════════════════════════════════

TCP 连上之后，双方还不知道"对面是谁"。所以第一件事是互发 hello：
    {"type":"hello", "name":"张三", "port":8765, ...}

其中 **port 字段很关键**：
TCP 连接里能拿到的对端端口是"临时端口"（随机分配的），
不能用来标识对方。必须由对方在 hello 里主动告诉
"我平时监听哪个端口"，以后才知道该往哪回连。

【握手的顺序不能搞错】
如果双方都"先发 hello"，或者都"先等 hello"，就会死锁。
本项目的约定（和 Go 版一致）：
    主动拨号的一方 → 先发 hello，再收
    被动接受的一方 → 先收 hello，再发
这样一发一收正好错开，不会卡住。
"""

import threading
import time

from protocols.tcp import TCPServer, TCPClient
from protocols.websocket import WebSocketConnection, OPCODE_CLOSE
from protocols.http import read_request
from protocols import discovery as disc

from app.message import Message, hello_message, text_message


class PeerConn:
    """一条已建立的 P2P 连接"""

    def __init__(self, ws: WebSocketConnection, peer_id: str,
                 name: str = "", host: str = "", port: int = 0):
        self.ws = ws
        self.id = peer_id          # 唯一标识：name@host:port
        self.name = name
        self.host = host
        self.port = port
        self.connected = True

    def send(self, msg: Message) -> bool:
        """发一条消息，失败返回 False"""
        try:
            self.ws.send_text(msg.to_json())
            return True
        except OSError:
            self.connected = False
            return False

    def close(self):
        try:
            self.ws.close()
        except OSError:
            pass
        self.connected = False


class Node:
    """
    一个 P2P 节点。

    职责：
      1. 监听端口，接受别人的连接（服务端）
      2. 主动拨号别的节点（客户端）
      3. 通过 discovery 自动发现局域网邻居
      4. 维护连接表、消息记录
    """

    def __init__(self, name: str = "User", port: int = 0,
                 enable_discovery: bool = True):
        """
        :param enable_discovery: 是否开启邻居发现。
               单台机器跑多个节点做测试时要关掉 ——
               否则两个节点会争抢同一个 UDP 广播端口(19877)和 mDNS 端口(5353)。
        """
        self.name = name
        self.port = port          # 0 = 让系统自动分配
        self.enable_discovery = enable_discovery

        self.peers = {}           # peer_id → PeerConn
        self.peers_lock = threading.Lock()

        self.messages = {}        # peer_id → [消息字典, ...]
        self.messages_lock = threading.Lock()

        self.frontends = []       # 浏览器 WebSocket 连接（供 server.py 推消息）
        self.frontends_lock = threading.Lock()

        self.discovery = None
        self.tcp = None
        self.running = False

    # =====================================================================
    # 启动
    # =====================================================================

    def start(self):
        """
        启动节点：发现邻居 + 自动连接。

        ★ 注意：本节点**自己不监听端口**。
        整个程序只有 server.py 里那一个 TCP 监听（与 Go 版一致）：
        同一个端口上，靠 WebSocket 请求的 role 参数来区分
            role=frontend → 浏览器
            其它 / 无 role → 另一台电脑（P2P 对端）
        有对端连进来时，server 会把连接转交给 handle_peer_connection()。

        这样做的好处：对外只需要暴露一个端口，防火墙、二维码都好办。
        """
        self.running = True

        # 1. 启动邻居发现（UDP广播 + mDNS 双通道，见 discovery.py）
        if self.enable_discovery:
            self.discovery = disc.DiscoveryService(self.name, self.port)
            self.discovery.start()

        # 2. 周期性尝试连接发现到的邻居（P2P 的"客户端"身份）
        threading.Thread(target=self._connect_loop, daemon=True).start()

        print(f"[节点] {self.name} 已启动，端口 {self.port}")
        print(f"[节点] 本机 IP：{disc.get_local_ips()}")

    def stop(self):
        self.running = False
        if self.discovery:
            self.discovery.stop()
        if self.tcp:
            self.tcp.close()

    # =====================================================================
    # 【P2P 的"服务端"身份】被动接受连接
    # =====================================================================

    def handle_peer_connection(self, conn, request, addr):
        """
        【P2P 的"服务端"身份】处理一个对端连进来的 WebSocket。

        由 server.py 转交过来 —— server 解析出 role 不是 frontend，
        就说明这是另一台电脑，交给本方法处理。

        流程：WebSocket 握手 → 先收对方 hello → 再发自己 hello → 进入接收循环

        ★ 握手顺序（与 connect_to_peer 严格相反，这是为了避免死锁）：
            被动方（这里）  ：先收，后发
            主动方（拨号方）：先发，后收
        """
        try:
            # 完成 WebSocket 握手（回 101，算法见 websocket.py）
            ws = WebSocketConnection.accept(conn, request)

            # ---- 被动方：先收对方的 hello ----
            opcode, data = ws.recv()
            if opcode is None:
                return
            msg = Message.from_bytes(data)
            if msg.type != Message.TYPE_HELLO:
                return

            # ---- 再发自己的 hello ----
            ws.send_text(hello_message(self.name, self.port).to_json())

            # ---- 登记这个对端 ----
            peer_name = msg.get("name", "unknown")
            peer_port = msg.get("port", 0)
            peer_host = addr[0]
            peer_id = f"{peer_name}@{peer_host}:{peer_port}"

            peer = PeerConn(ws, peer_id, peer_name, peer_host, peer_port)
            self._register_peer(peer)

            print(f"[节点] {peer_name} 连进来了（{peer_id}）")

            # ---- 进入接收循环（阻塞在这里直到对方断开）----
            self._recv_loop(peer)

        except (OSError, ValueError) as e:
            print(f"[节点] 处理对端连接出错：{e}")
        finally:
            try:
                conn.close()
            except OSError:
                pass

    # =====================================================================
    # 【P2P 的"客户端"身份】主动拨号别人
    # =====================================================================

    def connect_to_peer(self, host: str, port: int, name: str = "peer") -> bool:
        """
        主动连接另一个节点。

        这就是 P2P 里"客户端"的那一半 —— 与 _accept_loop 合起来才是完整的 P2P。
        """
        peer_id_guess = f"{name}@{host}:{port}"
        # 已经连上了就跳过
        with self.peers_lock:
            if peer_id_guess in self.peers:
                return True

        try:
            # 1. 建立 TCP 连接（底层触发三次握手）
            client = TCPClient.connect(host, port, timeout=5)

            # 2. 发起 WebSocket 握手
            ws = WebSocketConnection.connect(client.sock, f"{host}:{port}", "/ws")

            # 3. 主动方：先发 hello，再收对方的（与被动方顺序相反，避免死锁）
            ws.send_text(hello_message(self.name, self.port).to_json())
            opcode, data = ws.recv()
            if opcode is None:
                return False
            msg = Message.from_bytes(data)
            if msg.type != Message.TYPE_HELLO:
                return False

            # 4. 登记
            peer_name = msg.get("name", name)
            peer_port = msg.get("port", port)
            peer_id = f"{peer_name}@{host}:{peer_port}"

            peer = PeerConn(ws, peer_id, peer_name, host, peer_port)
            self._register_peer(peer)

            print(f"[节点] 已连接到 {peer_name}（{peer_id}）")
            # 5. 进入接收循环
            threading.Thread(target=self._recv_loop, args=(peer,), daemon=True).start()
            return True

        except (OSError, ValueError, ConnectionError) as e:
            print(f"[节点] 连接 {host}:{port} 失败：{e}")
            return False

    def _connect_loop(self):
        """周期性检查发现到的邻居，没连上的就主动去连"""
        while self.running:
            time.sleep(3)
            if not self.discovery:
                continue
            for peer in self.discovery.list_peers():
                with self.peers_lock:
                    already = peer.id in self.peers
                if not already and peer.port:
                    # 在后台连，避免阻塞发现循环
                    threading.Thread(
                        target=self.connect_to_peer,
                        args=(peer.host, peer.port, peer.name),
                        daemon=True).start()

    # =====================================================================
    # 连接管理与消息处理
    # =====================================================================

    def _register_peer(self, peer: PeerConn):
        with self.peers_lock:
            self.peers[peer.id] = peer
        # 通知浏览器刷新联系人列表
        self.broadcast_to_frontends(Message(Message.TYPE_PEERS,
                                            data=self.list_peers()))

    def _recv_loop(self, peer: PeerConn):
        """持续接收某个对端的消息"""
        while self.running and peer.connected:
            try:
                opcode, data = peer.ws.recv()
                if opcode is None or opcode == OPCODE_CLOSE:
                    break
                if not data:
                    continue
                msg = Message.from_bytes(data)
                self._on_peer_message(peer, msg)
            except (OSError, ValueError):
                break

        # 连接断了
        peer.connected = False
        with self.peers_lock:
            self.peers.pop(peer.id, None)
        print(f"[节点] {peer.name} 断开连接")
        self.broadcast_to_frontends(Message(Message.TYPE_PEERS,
                                            data=self.list_peers()))

    def _on_peer_message(self, peer: PeerConn, msg: Message):
        """收到对端消息"""
        if msg.type == Message.TYPE_TEXT:
            text = msg.get("text", "")
            print(f"\n[{peer.name}] {text}\n> ", end="", flush=True)
            # 存起来
            with self.messages_lock:
                self.messages.setdefault(peer.id, []).append({
                    "id": f"{peer.id}-{msg.get('ts')}",
                    "peerId": peer.id,
                    "direction": "received",
                    "msgType": "text",
                    "content": text,
                    "timestamp": msg.get("ts"),
                })
            # 推给浏览器
            self.broadcast_to_frontends(Message(
                Message.TYPE_NEW_MESSAGE,
                peerId=peer.id,
                direction="received",
                msgType="text",
                content=text,
                timestamp=msg.get("ts"),
            ))

    # =====================================================================
    # 对外接口（供 server.py 和命令行使用）
    # =====================================================================

    def list_peers(self) -> list:
        """列出所有已知对端（已连接 + 已发现但未连接）"""
        result = []
        with self.peers_lock:
            for peer_id, peer in self.peers.items():
                result.append({
                    "name": peer.name, "host": peer.host,
                    "port": peer.port, "id": peer_id,
                    "connected": True, "online": True,
                })
        # 再加上"发现了但还没连上"的
        connected_ids = {p["id"] for p in result}
        if self.discovery:
            for peer in self.discovery.list_peers():
                if peer.id not in connected_ids:
                    result.append({
                        "name": peer.name, "host": peer.host,
                        "port": peer.port, "id": peer.id,
                        "connected": False, "online": True,
                    })
        return result

    def send_text(self, peer_id: str, text: str) -> bool:
        """给某个对端发文本"""
        with self.peers_lock:
            peer = self.peers.get(peer_id)
        if not peer:
            return False

        ok = peer.send(text_message(text, sender=self.name))
        if ok:
            # 也存一份到自己的记录里
            with self.messages_lock:
                self.messages.setdefault(peer_id, []).append({
                    "id": f"{peer_id}-{int(time.time() * 1000)}",
                    "peerId": peer_id,
                    "direction": "sent",
                    "msgType": "text",
                    "content": text,
                    "timestamp": int(time.time() * 1000),
                })
        return ok

    def get_messages(self, peer_id: str) -> list:
        with self.messages_lock:
            return list(self.messages.get(peer_id, []))

    # ---- 浏览器连接管理 ----

    def add_frontend(self, ws):
        with self.frontends_lock:
            self.frontends.append(ws)

    def broadcast_to_frontends(self, msg: Message):
        """把事件推给所有浏览器（对应 Go 版 broadcastEvents）"""
        payload = msg.to_json()
        with self.frontends_lock:
            dead = []
            for ws in self.frontends:
                try:
                    ws.send_text(payload)
                except OSError:
                    dead.append(ws)
            for ws in dead:
                self.frontends.remove(ws)
