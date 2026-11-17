"""
common —— 手写协议时用到的公共工具

【为什么需要"网络字节序"？】
不同 CPU 存储多字节整数的方式不同：
  - 大端序（Big Endian）：高位字节在前，如 0x1234 存为 12 34
  - 小端序（Little Endian）：低位字节在前，如 0x1234 存为 34 12

如果两台机器字节序不同却直接互发数字，读出来的值就完全错了。
所以网络协议统一规定：**多字节字段一律用大端序**，称为"网络字节序"。

Python 的 struct 模块里：
  '>'  大端序
  '<'  小端序
  '!'  网络序（等价于 '>'）

本文件还提供一个十六进制转储函数，用来把二进制报文打印成人能看懂的样子 ——
调试协议时这是最有用的工具，因为你必须"看见"真实的字节。
"""

import struct


def hex_dump(data: bytes, prefix: str = "") -> str:
    """
    把字节流转储成经典的 "偏移量 | 十六进制 | ASCII" 三栏格式。

    这是抓包工具（Wireshark、tcpdump）的显示方式，看协议必备。

        >>> hex_dump(b"HTTP/1.1")
        '00000000  48 54 54 50 2f 31 2e 31                           HTTP/1.1        '
    """
    lines = []
    for offset in range(0, len(data), 16):
        chunk = data[offset:offset + 16]
        # 左栏：偏移量（8 位十六进制）
        # 中栏：每个字节的十六进制
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        # 右栏：能打印的字符原样显示，不能打印的用 . 代替
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"{prefix}{offset:08x}  {hex_part:<47}  {ascii_part}")
    return "\n".join(lines)


def print_hex(data: bytes, title: str = "") -> None:
    """直接打印十六进制转储，调试协议时非常好用。"""
    if title:
        print(f"──── {title} （共 {len(data)} 字节） ────")
    print(hex_dump(data))
    print()


# ---------------------------------------------------------------------------
# 大端序（网络字节序）读写工具
# ---------------------------------------------------------------------------

def pack_u8(value: int) -> bytes:
    """打包 1 字节无符号整数"""
    return struct.pack("!B", value)


def pack_u16(value: int) -> bytes:
    """打包 2 字节无符号整数（网络字节序）"""
    return struct.pack("!H", value)


def pack_u32(value: int) -> bytes:
    """打包 4 字节无符号整数（网络字节序）"""
    return struct.pack("!I", value)


def unpack_u8(data: bytes, offset: int = 0) -> int:
    """从指定位置读 1 字节无符号整数"""
    return struct.unpack_from("!B", data, offset)[0]


def unpack_u16(data: bytes, offset: int = 0) -> int:
    """从指定位置读 2 字节无符号整数"""
    return struct.unpack_from("!H", data, offset)[0]


def unpack_u32(data: bytes, offset: int = 0) -> int:
    """从指定位置读 4 字节无符号整数"""
    return struct.unpack_from("!I", data, offset)[0]


def read_exact(sock, n: int) -> bytes:
    """
    从 socket 精确读取 n 个字节，不够就一直等。

    【为什么需要它？】
    socket.recv(n) 的语义是"最多读 n 个字节"，实际可能只读到 1 个字节就返回了。
    TCP 是字节流，没有消息边界，所以必须循环读满为止。
    这是所有二进制协议实现里最容易写错的地方之一。

    返回空 bytes 表示对端已关闭连接。
    """
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return b""  # 连接已断开
        buf += chunk
    return buf
