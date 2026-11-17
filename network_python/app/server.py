"""
server —— 【第 2/3 层】给浏览器用的 HTTP + WebSocket 服务端

═══════════════════════════════════════════════════════════════════════
★ 这里的架构是 C/S，不是 P2P
═══════════════════════════════════════════════════════════════════════

注意区分两种关系（这是本项目最容易混淆的地方）：

    电脑 ↔ 电脑        → P2P（见 node.py），每台都能互相拨号
    浏览器 → 本机后端  → C/S（本文件），后端是唯一的 Server

浏览器**不会**去连别的电脑，它只连自己这一台机器上的后端。
至于"把消息转发给哪台电脑"，是后端的事（后端再用 P2P 发出去）。

所以本文件是标准的 C/S：
    Client = 浏览器
    Server = 本机这个 Python 程序

═══════════════════════════════════════════════════════════════════════
★ 一个连接，两种命运
═══════════════════════════════════════════════════════════════════════

浏览器发来的 HTTP 请求有两类，走完全不同的路：

    普通请求  GET /api/peers     → 按 HTTP 一问一答，返回 JSON
    升级请求  GET /ws（带 Upgrade: websocket）
                                 → 回 101，此后这条连接变成 WebSocket 长连接

判断依据就是请求头里有没有 Upgrade: websocket。
（这正是 http.py 里读到的头部，也是 websocket.py 握手的基础）

═══════════════════════════════════════════════════════════════════════
★ 状态管理（第 5 层）：本项目不用 Cookie
═══════════════════════════════════════════════════════════════════════

浏览器和后端之间是 WebSocket **长连接**，连接在就意味着"你还在"，
根本不需要 Cookie 来辨认身份。

所以第 5 层（Cookie）在本项目被完全跳过 ——
状态放在 node.peers / node.messages（内存）里，连接断开就从表里摘掉。
"""

import json
import threading
import mimetypes

from protocols.tcp import TCPServer
from protocols.websocket import WebSocketConnection, OPCODE_CLOSE
from protocols.http import read_request, HTTPResponse
from protocols import discovery as disc

from app.message import Message
from app.node import Node


# 网页文件所在的目录
WEB_DIR = "web"


def parse_query(path: str) -> dict:
    """
    从 URL 里取出查询参数。

        "/ws?role=frontend"  →  {"role": "frontend"}

    本项目靠这个 role 区分"浏览器"和"对端电脑"，是关键的一步。
    """
    from urllib.parse import urlparse, parse_qs
    query = urlparse(path).query
    return {k: v[0] for k, v in parse_qs(query).items()}


class ChatServer:
    """
    面向浏览器的 HTTP + WebSocket 服务端。

    它本身不实现 P2P，只是把浏览器的请求"翻译"成 node 的操作：
        浏览器说"发给张三" → server 调 node.send_text("张三@...", "...")
    """

    def __init__(self, node: Node, port: int = 0):
        self.node = node
        self.tcp = TCPServer(port=port)
        self.port = self.tcp.port

    # =====================================================================
    # 主循环
    # =====================================================================

    def serve_forever(self):
        print(f"[服务] 服务端启动：http://localhost:{self.port}")
        while True:
            conn, addr = self.tcp.accept()
            if conn is None:
                continue
            threading.Thread(target=self._handle_connection,
                             args=(conn, addr), daemon=True).start()

    def _handle_connection(self, conn, addr):
        """
        处理一个连接 —— **这里是整个程序的流量总入口**。

        同一个端口上会进来三种请求，分两层判断来区分：

            ① 普通 HTTP（GET / 、GET /api/...）
                                        → _handle_http（一问一答）
            ② WebSocket + role=frontend → 浏览器    → _handle_websocket（C/S）
            ③ WebSocket + 无 role       → 另一台电脑 → node.handle_peer_connection（P2P）

        第②③条的区分方式，与 Go 版 main.go 的 handleWebSocket 完全一致：
        看 URL 上的 role 参数。
        """
        try:
            request = read_request(conn)
            if request is None:
                conn.close()
                return

            # ★ 第一层判断：是不是要升级成 WebSocket？
            if request.get_header("upgrade", "").lower() == "websocket":
                # ★ 第二层判断：连过来的是浏览器，还是另一台电脑？
                role = parse_query(request.path).get("role", "")
                if role == "frontend":
                    self._handle_websocket(conn, request)               # C/S
                else:
                    self.node.handle_peer_connection(conn, request, addr)  # P2P
            else:
                self._handle_http(conn, request)

        except (OSError, ValueError) as e:
            print(f"[服务] 处理连接出错：{e}")
            try:
                conn.close()
            except OSError:
                pass

    # =====================================================================
    # 命运一：普通 HTTP 请求（一问一答）
    # =====================================================================

    def _handle_http(self, conn, request):
        path = request.query_path

        # ---- 静态资源 / 首页 ----
        if path == "/" or path == "/index.html":
            self._serve_static(conn, "index.html")
            return

        # ---- API：我是谁 ----
        if path == "/api/me":
            body = json.dumps({
                "name": self.node.name,
                "port": self.node.port,
                "ips": disc.get_local_ips(),
            }, ensure_ascii=False)
            conn.sendall(HTTPResponse.json(body).encode())
            return

        # ---- API：对端列表 ----
        if path == "/api/peers":
            body = json.dumps(self.node.list_peers(), ensure_ascii=False)
            conn.sendall(HTTPResponse.json(body).encode())
            return

        # ---- API：与某人的聊天记录 ----
        if path.startswith("/api/messages/"):
            peer_id = path[len("/api/messages/"):]
            # URL 里可能带转义，解一下
            from urllib.parse import unquote
            peer_id = unquote(peer_id)
            body = json.dumps(self.node.get_messages(peer_id), ensure_ascii=False)
            conn.sendall(HTTPResponse.json(body).encode())
            return

        # ---- API：发消息（POST）----
        if path == "/api/send" and request.method == "POST":
            data = json.loads(request.body.decode("utf-8") or "{}")
            peer_id = data.get("peerId", "")
            text = data.get("text", "")
            ok = self.node.send_text(peer_id, text)
            body = json.dumps({"status": "ok" if ok else "failed"},
                              ensure_ascii=False)
            conn.sendall(HTTPResponse.json(body).encode())
            return

        # ---- API：手动连接某个 IP（POST）----
        if path == "/api/connect" and request.method == "POST":
            data = json.loads(request.body.decode("utf-8") or "{}")
            host = data.get("host", "")
            port = int(data.get("port", 0))
            name = data.get("name", "peer")
            # 后台连接，避免HTTP请求卡住
            threading.Thread(target=self.node.connect_to_peer,
                             args=(host, port, name), daemon=True).start()
            body = json.dumps({"status": "connecting"}, ensure_ascii=False)
            conn.sendall(HTTPResponse.json(body).encode())
            return

        # ---- 其它：404 ----
        conn.sendall(HTTPResponse.not_found().encode())

    def _serve_static(self, conn, filename: str):
        """从 web/ 目录读文件返回给浏览器"""
        import os
        filepath = os.path.join(WEB_DIR, filename)
        try:
            with open(filepath, "rb") as f:
                content = f.read()
        except OSError:
            conn.sendall(HTTPResponse.not_found().encode())
            return

        ctype, _ = mimetypes.guess_type(filename)
        resp = HTTPResponse(
            status=200,
            headers={"Content-Type": ctype or "application/octet-stream"},
            body=content)
        conn.sendall(resp.encode())

    # =====================================================================
    # 命运二：升级成 WebSocket（长连接）
    # =====================================================================

    def _handle_websocket(self, conn, request):
        """
        完成 WebSocket 握手，此后这条连接就不再是 HTTP 了。

        浏览器连上来后，后端会把"新消息""对端列表变化"等事件
        主动推给它 —— 这正是 HTTP 做不到、必须用 WebSocket 的原因。
        """
        try:
            ws = WebSocketConnection.accept(conn, request)
        except ValueError as e:
            print(f"[服务] WebSocket 握手失败：{e}")
            conn.close()
            return

        # 登记到 node，这样 node 有事就能推给它
        self.node.add_frontend(ws)
        print("[服务] 浏览器已连接（WebSocket）")

        # 立刻推一次当前的对端列表
        try:
            ws.send_text(Message(Message.TYPE_PEERS,
                                 data=self.node.list_peers()).to_json())
        except OSError:
            return

        # 保持连接，读浏览器发来的帧（本项目浏览器主要靠 HTTP API 发消息，
        # 这里留着循环是为了能收到关闭帧并及时感知断开）
        try:
            while True:
                opcode, data = ws.recv()
                if opcode is None or opcode == OPCODE_CLOSE:
                    break
        except OSError:
            pass
        finally:
            with self.node.frontends_lock:
                if ws in self.node.frontends:
                    self.node.frontends.remove(ws)
            print("[服务] 浏览器断开连接")
