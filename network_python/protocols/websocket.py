"""
websocket —— 【第 3 层 应用层】WebSocket（RFC 6455）：手写握手与帧编解码

═══════════════════════════════════════════════════════════════════════
一、WebSocket 是什么
═══════════════════════════════════════════════════════════════════════

HTTP 是"一问一答"，服务端不能主动推送。为了做实时聊天，需要一条
**建立之后长期保持、双向都能主动发**的连接 —— 这就是 WebSocket。

关键设计：**握手阶段借道 HTTP**。
先发一个特殊的 HTTP 请求，服务端回 101（Switching Protocols），
此后这条 TCP 连接就"升级"成 WebSocket，不再走 HTTP 那套问答。

    客户端                                    服务端
      |--- HTTP GET + Upgrade: websocket -----> |
      |<-- HTTP 101 + Sec-WebSocket-Accept ---- |   ← 握手完成，协议切换
      |<=========== 双向帧，随时互发 ==========>|

═══════════════════════════════════════════════════════════════════════
二、握手：那个 Accept 值是怎么算出来的？
═══════════════════════════════════════════════════════════════════════

客户端请求里带一个随机的 Base64 值：
    Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==

服务端这样算出响应值：
    1. 把这个 key 和一个**固定的 GUID** 字符串拼接
       GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
       （这是 RFC 规定死的魔法字符串）
    2. 对拼接结果做 SHA-1（得到 20 字节）
    3. 再 Base64 编码

    Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=

【为什么这么设计？】
不是加密，也没有安全性。只是为了**确认对方真的是 WebSocket 服务端** ——
如果对方是个普通 HTTP 服务，它不认识这个 GUID，就算不出正确的 Accept，
握手就会失败，从而避免误连。

═══════════════════════════════════════════════════════════════════════
三、帧格式（握手后，数据就按"帧"来传）
═══════════════════════════════════════════════════════════════════════

      0                   1                   2                   3
      0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     +-+-+-+-+-------+-+-------------+-------------------------------+
     |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
     |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
     |N|V|V|V|       |S|             |   (if payload len==126/127)   |
     | |1|2|3|       |K|             |                               |
     +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
     |     Extended payload length continued, if payload len == 127  |
     + - - - - - - - - - - - - - - - +-------------------------------+
     |                               |Masking-key, if MASK set to 1  |
     +-------------------------------+-------------------------------+
     | Masking-key (continued)       |          Payload Data         |
     +-------------------------------- - - - - - - - - - - - - - - - +
     :                     Payload Data continued ...                :
     + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
     |                     Payload Data continued ...                |
     +---------------------------------------------------------------+

字段说明：
    FIN      1 bit，是不是消息的最后一帧（大消息会拆成多帧）
    opcode   4 bit，帧类型
    MASK     1 bit，是否对载荷做了掩码（客户端→服务端**必须**为 1）
    Payload len  7 bit：
                 < 126        直接就是长度
                 == 126       后面 2 字节才是真实长度
                 == 127       后面 8 字节才是真实长度

opcode 取值：
    0x0  连续帧    0x1  文本    0x2  二进制
    0x8  关闭连接  0x9  Ping    0xA  Pong

═══════════════════════════════════════════════════════════════════════
四、★ 掩码（Mask）机制
═══════════════════════════════════════════════════════════════════════

RFC 强制：**客户端发给服务端的帧必须掩码**，服务端发给客户端的**不能**掩码。

做法：客户端生成一个随机的 4 字节 masking key，然后
    payload[i] ^= masking_key[i % 4]      （逐字节异或）

为什么要这么麻烦？
不是防窃听（异或根本不算加密）。真正目的是**防止缓存污染攻击**：
早期有人发现，攻击者可以让浏览器发出看似 HTTP 的数据，骗过中间的
代理服务器缓存。加上随机掩码后，数据变成不可预测的二进制，
中间设备就不会把它误判成 HTTP 请求了。
"""

import base64
import hashlib
import os
import struct

from .common import read_exact, print_hex


# RFC 6455 规定的固定 GUID，所有 WebSocket 实现都用这一个
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# 帧类型（opcode）
OPCODE_CONTINUATION = 0x0
OPCODE_TEXT = 0x1
OPCODE_BINARY = 0x2
OPCODE_CLOSE = 0x8
OPCODE_PING = 0x9
OPCODE_PONG = 0xA


# ===========================================================================
# 握手
# ===========================================================================

def compute_accept_key(client_key: str) -> str:
    """
    ★ 服务端核心算法：由客户端的 Sec-WebSocket-Key 算出 Sec-WebSocket-Accept。

        accept = base64( sha1( client_key + GUID ) )

    这是 WebSocket 握手唯一"有技术含量"的地方，其余都是拼字符串。
    """
    combined = client_key + WS_GUID
    sha1_digest = hashlib.sha1(combined.encode("utf-8")).digest()
    return base64.b64encode(sha1_digest).decode("utf-8")


def generate_client_key() -> str:
    """客户端生成随机的 Sec-WebSocket-Key（16 字节随机数做 Base64）"""
    return base64.b64encode(os.urandom(16)).decode("utf-8")


def build_handshake_response(client_key: str) -> bytes:
    """
    构造服务端的 101 响应。

    注意状态码是 **101 Switching Protocols**，不是 200。
    这告诉客户端："我同意升级协议，咱们从现在起不说 HTTP 了。"
    """
    accept = compute_accept_key(client_key)
    response = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        "\r\n"
    )
    return response.encode("utf-8")


def build_handshake_request(host: str, path: str = "/") -> tuple:
    """
    构造客户端的握手请求，返回 (完整请求字节, 本次用的 key)。

    它看起来就是个普通 HTTP GET，只是多了几个特殊头部：
        Upgrade: websocket              想升级协议
        Connection: Upgrade
        Sec-WebSocket-Key: <随机值>     给服务端算 Accept 用
        Sec-WebSocket-Version: 13       RFC 6455 的版本号
    """
    key = generate_client_key()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    return request.encode("utf-8"), key


# ===========================================================================
# 帧的编码 / 解码
# ===========================================================================

def mask_payload(payload: bytes, masking_key: bytes) -> bytes:
    """
    用 4 字节掩码对载荷逐字节异或。

    异或有个好性质：**做两次就还原**。
    所以掩码和解掩码是同一个函数 —— 这也是它不能当加密用的原因。
    """
    return bytes(b ^ masking_key[i % 4] for i, b in enumerate(payload))


def encode_frame(payload: bytes, opcode: int = OPCODE_TEXT,
                 fin: bool = True, mask: bool = False) -> bytes:
    """
    把数据打包成一个 WebSocket 帧。

    :param payload: 要发的数据
    :param opcode:  帧类型（文本/二进制/关闭/ping...）
    :param fin:     是否是最后一帧
    :param mask:    是否掩码（客户端→服务端必须 True，服务端→客户端必须 False）
    """
    # ---- 第 1 字节：FIN + opcode ----
    byte0 = (0x80 if fin else 0x00) | (opcode & 0x0F)

    # ---- 第 2 字节起：MASK + 载荷长度 ----
    length = len(payload)
    mask_bit = 0x80 if mask else 0x00

    if length < 126:
        # 长度直接放在 7 bit 里
        header = bytes([byte0, mask_bit | length])
    elif length < 65536:
        # 长度字段填 126，真实长度用后面 2 字节表示
        header = bytes([byte0, mask_bit | 126]) + struct.pack("!H", length)
    else:
        # 长度字段填 127，真实长度用后面 8 字节表示
        header = bytes([byte0, mask_bit | 127]) + struct.pack("!Q", length)

    # ---- 掩码键 + 掩码后的载荷 ----
    if mask:
        masking_key = os.urandom(4)
        return header + masking_key + mask_payload(payload, masking_key)
    else:
        return header + payload


def read_frame(sock) -> tuple:
    """
    从 socket 读出一个完整的 WebSocket 帧。

    :return: (opcode, payload, fin)；连接关闭时返回 (None, b"", True)
    """
    # --- 读前 2 字节 ---
    head = read_exact(sock, 2)
    if not head:
        return None, b"", True

    byte0, byte1 = head[0], head[1]
    fin = bool(byte0 & 0x80)
    opcode = byte0 & 0x0F
    masked = bool(byte1 & 0x80)
    length = byte1 & 0x7F

    # --- 处理扩展长度 ---
    if length == 126:
        length = struct.unpack("!H", read_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", read_exact(sock, 8))[0]

    # --- 读掩码键（如果有）---
    masking_key = b""
    if masked:
        masking_key = read_exact(sock, 4)

    # --- 读载荷 ---
    payload = read_exact(sock, length) if length > 0 else b""

    # --- 客户端发来的帧必须解掩码 ---
    if masked:
        payload = mask_payload(payload, masking_key)

    return opcode, payload, fin


# ===========================================================================
# 连接封装：把握手 + 帧收发串起来
# ===========================================================================

class WebSocketConnection:
    """封装一条已升级的 WebSocket 连接"""

    def __init__(self, sock, is_server: bool = False):
        self.sock = sock
        self.is_server = is_server  # 决定发帧时是否要掩码

    # ---------- 服务端侧握手 ----------
    @classmethod
    def accept(cls, sock, request) -> "WebSocketConnection":
        """
        服务端：收到握手请求后，回 101 完成升级。

        :param request: 已解析好的 HTTPRequest（见 http.py）
        """
        client_key = request.get_header("sec-websocket-key")
        if not client_key:
            raise ValueError("缺少 Sec-WebSocket-Key，不是合法的 WebSocket 握手")
        sock.sendall(build_handshake_response(client_key))
        return cls(sock, is_server=True)

    # ---------- 客户端侧握手 ----------
    @classmethod
    def connect(cls, sock, host: str, path: str = "/") -> "WebSocketConnection":
        """客户端：主动发握手请求并验证服务端返回的 Accept 值"""
        request_bytes, key = build_handshake_request(host, path)
        sock.sendall(request_bytes)

        # 读响应直到头部结束
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = sock.recv(1)
            if not chunk:
                raise ConnectionError("握手时连接被关闭")
            buf += chunk

        text = buf.decode("utf-8", errors="replace")
        if "101" not in text.split("\r\n")[0]:
            raise ConnectionError(f"握手失败，服务端返回：{text.splitlines()[0]}")

        # 校验 Accept 值，确认对面确实是 WebSocket 服务端
        expected = compute_accept_key(key)
        if f"Sec-WebSocket-Accept: {expected}" not in text:
            raise ConnectionError("Sec-WebSocket-Accept 校验失败")

        return cls(sock, is_server=False)

    # ---------- 收发 ----------
    def send_text(self, text: str) -> None:
        """发一条文本消息（服务端不掩码，客户端必须掩码）"""
        frame = encode_frame(text.encode("utf-8"),
                             opcode=OPCODE_TEXT,
                             fin=True,
                             mask=not self.is_server)
        self.sock.sendall(frame)

    def send_bytes(self, data: bytes) -> None:
        """发一条二进制消息"""
        frame = encode_frame(data, opcode=OPCODE_BINARY,
                             fin=True, mask=not self.is_server)
        self.sock.sendall(frame)

    def recv(self) -> tuple:
        """收一条消息，返回 (opcode, 数据)"""
        opcode, payload, _fin = read_frame(self.sock)
        return opcode, payload

    def recv_text(self) -> str:
        """收一条文本消息"""
        opcode, payload = self.recv()
        return payload.decode("utf-8", errors="replace")

    def ping(self) -> None:
        """发 Ping（心跳，用来探测对方是否还活着）"""
        self.sock.sendall(encode_frame(b"", opcode=OPCODE_PING,
                                       fin=True, mask=not self.is_server))

    def close(self) -> None:
        """发送关闭帧（opcode 0x8）"""
        try:
            self.sock.sendall(encode_frame(b"", opcode=OPCODE_CLOSE,
                                           fin=True, mask=not self.is_server))
        except OSError:
            pass
        self.sock.close()


# ===========================================================================
# 演示：用 RFC 官方示例验证握手算法，并看清帧的真实字节
# ===========================================================================

def demo():
    print("=" * 70)
    print("WebSocket 协议演示")
    print("=" * 70)

    # ---- 1. 握手算法（用 RFC 6455 官方示例验证）----
    print("\n【1】握手：Accept 值的计算（用 RFC 官方示例验证）")
    # RFC 6455 第 1.3 节给的示例：
    #   key    = "dGhlIHNhbXBsZSBub25jZQ=="
    #   accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    key = "dGhlIHNhbXBsZSBub25jZQ=="
    expected = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    actual = compute_accept_key(key)

    print(f"  客户端 Sec-WebSocket-Key : {key}")
    print(f"  固定 GUID               : {WS_GUID}")
    print(f"  拼接后                  : {key}{WS_GUID}")
    print(f"  SHA1 → Base64 后        : {actual}")
    print(f"  RFC 期望值              : {expected}")
    print(f"  结果：{'✅ 完全一致，算法正确' if actual == expected else '❌ 不一致'}")

    # ---- 2. 握手响应长什么样 ----
    print("\n【2】服务端返回的 101 响应")
    print("```")
    print(build_handshake_response(key).decode().replace("\r\n", "\\r\\n\n"))
    print("```")

    # ---- 3. 帧格式 ----
    print("【3】WebSocket 帧的真实字节")
    print("\n  3.1 服务端→客户端（不掩码，最简形式）")
    frame = encode_frame(b"Hi", opcode=OPCODE_TEXT, mask=False)
    print_hex(frame, "  文本帧 'Hi'")
    print(f"    第1字节 0x{frame[0]:02x} = FIN(1) + opcode(0x1 文本)")
    print(f"    第2字节 0x{frame[1]:02x} = MASK(0) + 长度(2)")
    print(f"    后面就是载荷 'Hi'\n")

    print("  3.2 客户端→服务端（必须掩码，多出 4 字节掩码键）")
    frame2 = encode_frame(b"Hi", opcode=OPCODE_TEXT, mask=True)
    print_hex(frame2, "  带掩码的文本帧 'Hi'")
    print(f"    第2字节 0x{frame2[1]:02x} = MASK(1) + 长度(2)")
    print(f"    第3~6字节是随机掩码键，载荷已被异或成乱码\n")

    # ---- 4. 编解码往返验证 ----
    print("【4】编解码往返验证")
    original = b"Hello, WebSocket!"
    encoded = encode_frame(original, opcode=OPCODE_TEXT, mask=True)
    # 手工解一遍：读头部、取掩码键、解掩码
    masked = bool(encoded[1] & 0x80)
    length = encoded[1] & 0x7F
    mk = encoded[2:6]
    body = encoded[6:6 + length]
    restored = mask_payload(body, mk)
    print(f"  原始数据 : {original}")
    print(f"  解码回来 : {restored}")
    print(f"  结果：{'✅ 一致（异或两次即还原）' if restored == original else '❌ 不一致'}")


if __name__ == "__main__":
    demo()
