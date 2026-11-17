"""
http —— 【第 3 层 应用层】HTTP/1.1：手写报文解析与生成

═══════════════════════════════════════════════════════════════════════
一、HTTP 是什么
═══════════════════════════════════════════════════════════════════════

HTTP（HyperText Transfer Protocol，超文本传输协议）是**应用层协议**，
规定了"客户端怎么要数据、服务端怎么给数据"。

它运行在 TCP 之上（第 1 层），采用简单的"一问一答"模式：
    客户端发一个请求 → 服务端回一个响应 → （HTTP/1.1 可保持连接继续问答）

═══════════════════════════════════════════════════════════════════════
二、报文格式（纯文本！这是 HTTP 最好懂的地方）
═══════════════════════════════════════════════════════════════════════

【请求报文】
    GET /index.html HTTP/1.1\r\n        ← 请求行：方法 路径 版本
    Host: localhost:8765\r\n            ← 请求头（可有多行）
    User-Agent: Python\r\n
    Content-Length: 0\r\n
    \r\n                                ← 空行：头部结束的标志！
    [请求体 body]                        ← 可选

【响应报文】
    HTTP/1.1 200 OK\r\n                 ← 状态行：版本 状态码 原因短语
    Content-Type: text/html\r\n         ← 响应头
    Content-Length: 1234\r\n
    \r\n                                ← 空行
    <html>...</html>                    ← 响应体

【三个必须记住的细节】
  1. 换行是 \\r\\n（CRLF），不是 \\n。这是 RFC 强制规定的。
  2. 头部和 body 之间用**空行**（\\r\\n\\r\\n）分隔 —— 靠它判断头部读到哪结束。
  3. body 的长度由 Content-Length 头决定 —— 不靠"读到没数据为止"，
     因为 TCP 是长连接，读完 body 连接还不能关。

═══════════════════════════════════════════════════════════════════════
三、常见方法 / 状态码
═══════════════════════════════════════════════════════════════════════

方法：GET（取资源）POST（提交数据）PUT DELETE HEAD ...
状态码：
    200 OK              成功
    301/302             重定向
    400 Bad Request     请求格式错
    404 Not Found       资源不存在
    500 Internal Error  服务端出错
    101 Switching Protocols  ← 特殊！WebSocket 握手就靠它（见 websocket.py）
"""

import socket
from urllib.parse import unquote

from .common import read_exact


# ===========================================================================
# HTTP 请求
# ===========================================================================

class HTTPRequest:
    """表示一个 HTTP 请求"""

    def __init__(self, method="GET", path="/", version="HTTP/1.1",
                 headers=None, body=b""):
        self.method = method
        self.path = path            # 如 "/index.html"
        self.version = version
        self.headers = headers or {}  # 字典，key 不区分大小写（这里统一转小写）
        self.body = body

    # ---------- 解析：字节流 → 对象 ----------

    @classmethod
    def parse_head(cls, raw_head: bytes) -> "HTTPRequest":
        """
        解析请求头部（不含 body）。

        :param raw_head: 到 \\r\\n\\r\\n 为止的字节（不含最后的空行）
        """
        text = raw_head.decode("utf-8", errors="replace")
        lines = text.split("\r\n")

        # 第一行是请求行：GET /path HTTP/1.1
        method, path, version = lines[0].split(" ", 2)

        # 其余行是头部：Key: Value
        headers = {}
        for line in lines[1:]:
            if not line:
                continue
            if ":" in line:
                key, value = line.split(":", 1)
                # HTTP 头部名不区分大小写，统一存成小写方便查找
                headers[key.strip().lower()] = value.strip()

        return cls(method, path, version, headers)

    # ---------- 生成：对象 → 字节流 ----------

    def encode(self) -> bytes:
        """把请求序列化成字节（客户端用）"""
        lines = [f"{self.method} {self.path} {self.version}"]
        for k, v in self.headers.items():
            lines.append(f"{k}: {v}")
        head = "\r\n".join(lines) + "\r\n\r\n"   # 注意结尾的空行
        return head.encode("utf-8") + self.body

    # ---------- 便捷访问 ----------

    def get_header(self, name: str, default=None):
        return self.headers.get(name.lower(), default)

    @property
    def content_length(self) -> int:
        return int(self.get_header("content-length", 0))

    @property
    def query_path(self) -> str:
        """去掉 URL 中 ? 后面的查询参数，返回纯路径"""
        return unquote(self.path.split("?", 1)[0])

    def __repr__(self) -> str:
        return f"<HTTPRequest {self.method} {self.path}>"


# ===========================================================================
# HTTP 响应
# ===========================================================================

# 常见状态码的原因短语
REASON_PHRASES = {
    200: "OK",
    101: "Switching Protocols",
    204: "No Content",
    301: "Moved Permanently",
    302: "Found",
    400: "Bad Request",
    403: "Forbidden",
    404: "Not Found",
    405: "Method Not Allowed",
    500: "Internal Server Error",
}


class HTTPResponse:
    """表示一个 HTTP 响应"""

    def __init__(self, status=200, headers=None, body=b"", version="HTTP/1.1"):
        self.status = status
        self.headers = headers or {}
        self.body = body
        self.version = version

    def encode(self) -> bytes:
        """
        序列化成字节。

        注意：会自动补上 Content-Length。
        这是 HTTP 里非常关键的头 —— 没有它，客户端不知道该读多少 body。
        """
        reason = REASON_PHRASES.get(self.status, "Unknown")

        # 自动设置 Content-Length（除非调用方显式指定过，如 chunked 传输）
        headers = dict(self.headers)
        if "content-length" not in {k.lower() for k in headers}:
            headers["Content-Length"] = str(len(self.body))

        lines = [f"{self.version} {self.status} {reason}"]
        for k, v in headers.items():
            lines.append(f"{k}: {v}")
        head = "\r\n".join(lines) + "\r\n\r\n"
        return head.encode("utf-8") + self.body

    @classmethod
    def html(cls, body: str, status: int = 200) -> "HTTPResponse":
        """快捷构造一个 HTML 响应"""
        return cls(status=status,
                   headers={"Content-Type": "text/html; charset=utf-8"},
                   body=body.encode("utf-8"))

    @classmethod
    def json(cls, body: str, status: int = 200) -> "HTTPResponse":
        """快捷构造一个 JSON 响应"""
        return cls(status=status,
                   headers={"Content-Type": "application/json; charset=utf-8"},
                   body=body.encode("utf-8"))

    @classmethod
    def not_found(cls, text: str = "404 Not Found") -> "HTTPResponse":
        return cls.html(f"<h1>{text}</h1>", status=404)


# ===========================================================================
# 从 socket 读取完整请求（★ 这里最容易写错）
# ===========================================================================

def read_request(sock) -> HTTPRequest:
    """
    从 TCP 连接里读出一个完整的 HTTP 请求。

    难点：TCP 是字节流，你不知道头部有多长。
    做法：一字节一字节地读，直到凑出 \\r\\n\\r\\n（空行）为止；
          然后根据 Content-Length 读 body。

    （生产环境会用带缓冲的读取优化，这里用最直白的方式演示原理。）

    返回 None 表示连接已关闭。
    """
    buf = b""
    # ---- 第一步：读到头部结束标志 \r\n\r\n ----
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(1)   # 一次读 1 字节，慢但清晰
        if not chunk:
            return None        # 连接关闭
        buf += chunk

    # 拆出头部部分（不含结尾空行）
    head_part, _ = buf.split(b"\r\n\r\n", 1)
    request = HTTPRequest.parse_head(head_part)

    # ---- 第二步：按 Content-Length 读 body ----
    n = request.content_length
    if n > 0:
        request.body = read_exact(sock, n)

    return request


# ===========================================================================
# 极简 HTTP 服务器（手写，不用 http.server）
# ===========================================================================

class SimpleHTTPServer:
    """
    一个最小可用的 HTTP 服务器，用来演示协议本身。

    真实项目请用标准库 http.server 或框架；这里刻意手写，
    好让你看清"收到字节 → 解析 → 处理 → 生成响应字节"的全过程。
    """

    def __init__(self, port: int = 0, host: str = "0.0.0.0"):
        from .tcp import TCPServer
        self.tcp = TCPServer(port=port, host=host)
        self.port = self.tcp.port
        self.routes = {}   # 路径 → 处理函数

    def route(self, path: str):
        """装饰器：注册路由"""
        def decorator(func):
            self.routes[path] = func
            return func
        return decorator

    def handle_connection(self, conn, addr):
        """处理一个连接（HTTP/1.1 默认 keep-alive，可以循环处理多个请求）"""
        try:
            while True:
                request = read_request(conn)
                if request is None:
                    break

                # 查路由
                handler = self.routes.get(request.query_path)
                if handler:
                    response = handler(request)
                else:
                    response = HTTPResponse.not_found()

                conn.sendall(response.encode())

                # 若请求/响应要求关闭连接，则断开
                if request.get_header("connection", "").lower() == "close":
                    break
                if response.headers.get("Connection", "").lower() == "close":
                    break
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            conn.close()

    def serve_forever(self):
        """主循环：不断接受连接"""
        print(f"[HTTP] 手写服务器启动，监听 {self.tcp.host}:{self.port}")
        while True:
            conn, addr = self.tcp.accept()
            if conn is None:
                continue
            import threading
            threading.Thread(target=self.handle_connection,
                             args=(conn, addr), daemon=True).start()


# ===========================================================================
# 演示
# ===========================================================================

def demo():
    print("=" * 70)
    print("HTTP 协议演示")
    print("=" * 70)

    # ---- 1. 请求报文长什么样 ----
    print("\n【1】一个 GET 请求的真实字节")
    req = HTTPRequest("GET", "/index.html", headers={
        "Host": "localhost:8765",
        "User-Agent": "PythonHandmade/1.0",
    })
    raw_req = req.encode()
    print("```")
    print(raw_req.decode("utf-8").replace("\r\n", "\\r\\n\n"))
    print("```")
    print("  ↑ 注意每行的 \\r\\n，以及结尾那个额外的空行 \\r\\n")

    # ---- 2. 解析回来 ----
    print("\n【2】把上面的字节解析回对象")
    parsed = HTTPRequest.parse_head(raw_req.split(b"\r\n\r\n")[0])
    print(f"  方法   : {parsed.method}")
    print(f"  路径   : {parsed.path}")
    print(f"  版本   : {parsed.version}")
    print(f"  头部   : {parsed.headers}")

    # ---- 3. 响应报文 ----
    print("\n【3】一个响应报文")
    resp = HTTPResponse.html("<h1>你好，这是手写的 HTTP</h1>")
    raw_resp = resp.encode()
    print("```")
    print(raw_resp.decode("utf-8").replace("\r\n", "\\r\\n\n"))
    print("```")
    print("  ↑ Content-Length 是自动补上的，它告诉客户端 body 有多长\n")


if __name__ == "__main__":
    demo()
