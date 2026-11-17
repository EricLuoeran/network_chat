"""
dns —— 【第 3 层 应用层】DNS / mDNS 报文：手写编码与解码

═══════════════════════════════════════════════════════════════════════
一、DNS 是什么
═══════════════════════════════════════════════════════════════════════

DNS（Domain Name System）把"名字"翻译成"IP 地址"。
它通常跑在 **UDP 53 端口**（少数情况用 TCP）。

**mDNS**（multicast DNS）是 DNS 的局域网变体：
  · 不需要 DNS 服务器，设备之间自己互相问
  · 组播地址 **224.0.0.251**，端口 **5353**
  · 名字统一以 **.local** 结尾，如 "MyPC.local"

这正是本项目"自动发现邻居"用的协议之一（另一条路是 UDP 广播）。

═══════════════════════════════════════════════════════════════════════
二、DNS 报文格式
═══════════════════════════════════════════════════════════════════════

【12 字节固定头部】
                                    1  1  1  1  1  1
      0  1  2  3  4  5  6  7  8  9  0  1  2  3  4  5
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                      ID                       |  事务 ID，请求和响应用它配对
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |QR|   Opcode  |AA|TC|RD|RA|   Z    |   RCODE   |  各种标志位
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    QDCOUNT                    |  问题数
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    ANCOUNT                    |  回答数
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    NSCOUNT                    |  授权记录数
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    |                    ARCOUNT                    |  附加记录数
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

标志位含义：
    QR     0=查询 1=响应
    Opcode 0=标准查询
    AA     权威回答
    TC     报文被截断（超过 UDP 512 字节）
    RD     期望递归
    RA     支持递归
    RCODE  响应码，0=无错误

═══════════════════════════════════════════════════════════════════════
三、★ 域名的特殊编码方式（DNS 最独特的设计）
═══════════════════════════════════════════════════════════════════════

域名不是写成 "www.example.com" 然后加个 \0，
而是**拆成一段段，每段前面加 1 字节长度**，最后以 0 结尾：

    www.example.com  →  3 w w w 7 e x a m p l e 3 c o m 0
                        ↑___________↑ ↑___________↑ ↑___↑  ↑
                            3字节        7字节       3字节   结束符

为什么这么麻烦？因为可以直接用二进制比较，也便于实现下面的"压缩指针"。

【压缩指针】
为了省空间，重复的域名可以用指针引用前面出现过的位置：
    若长度字节的高 2 位是 11（即 0xC0），说明这是一个指针，
    连同后面 1 字节共 14 位表示"相对于报文开头的偏移量"。

    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
    | 1  1|                OFFSET                   |
    +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

═══════════════════════════════════════════════════════════════════════
四、资源记录（RR）格式
═══════════════════════════════════════════════════════════════════════

    +-------------------------------------------------+
    |                    NAME (域名)                  |
    +-------------------------------------------------+
    |                    TYPE  (2 字节)               |  1=A  5=CNAME  12=PTR  16=TXT  33=SRV
    +-------------------------------------------------+
    |                    CLASS (2 字节)               |  1=IN(互联网)
    +-------------------------------------------------+
    |                    TTL   (4 字节)               |  缓存有效期（秒）
    +-------------------------------------------------+
    |                    RDLENGTH (2 字节)            |  后面数据的长度
    +-------------------------------------------------+
    |                    RDATA (变长)                 |  A记录=4字节IP；TXT=长度前缀+文本
    +-------------------------------------------------+
"""

import struct

from .common import print_hex


# 记录类型
TYPE_A = 1        # IPv4 地址
TYPE_CNAME = 5    # 别名
TYPE_PTR = 12     # 指针记录（mDNS 服务发现常用）
TYPE_TXT = 16     # 文本（本项目用它携带自定义 info）
TYPE_SRV = 33     # 服务位置（主机 + 端口）
TYPE_ANY = 255    # 查询所有类型

# 类
CLASS_IN = 1      # 互联网

# mDNS 组播地址与端口
MDNS_GROUP = "224.0.0.251"
MDNS_PORT = 5353


# ===========================================================================
# 域名编码 / 解码
# ===========================================================================

def encode_domain(name: str) -> bytes:
    """
    把域名编码成 DNS 格式：每段前加长度字节，以 0 结尾。

        "www.example.com" → b'\\x03www\\x07example\\x03com\\x00'
    """
    out = b""
    # 去掉结尾的点（"example.com." 和 "example.com" 等价）
    name = name.rstrip(".")
    if not name:
        return b"\x00"
    for label in name.split("."):
        encoded = label.encode("utf-8")
        if len(encoded) > 63:
            raise ValueError(f"域名每段不能超过 63 字节：{label}")
        out += bytes([len(encoded)]) + encoded
    return out + b"\x00"   # 0 表示域名结束


def decode_domain(data: bytes, offset: int) -> tuple:
    """
    从报文中解码域名，支持压缩指针。

    :return: (域名字符串, 解析结束后的新偏移量)
    """
    labels = []
    jumped = False          # 是否发生过指针跳转
    end_offset = offset     # 若发生跳转，这是"本字段"的结束位置

    while True:
        length = data[offset]

        # 情况 1：遇到 0，域名结束
        if length == 0:
            offset += 1
            if not jumped:
                end_offset = offset
            break

        # 情况 2：高 2 位是 11 → 压缩指针
        if (length & 0xC0) == 0xC0:
            # 取低 14 位作为偏移量
            ptr = struct.unpack_from("!H", data, offset)[0] & 0x3FFF
            if not jumped:
                end_offset = offset + 2   # 指针本身占 2 字节
            offset = ptr
            jumped = True
            continue

        # 情况 3：普通标签，前面 1 字节是长度
        offset += 1
        labels.append(data[offset:offset + length].decode("utf-8", errors="replace"))
        offset += length
        if not jumped:
            end_offset = offset

    return ".".join(labels), end_offset


# ===========================================================================
# 头部
# ===========================================================================

class DNSHeader:
    """DNS 报文头部，固定 12 字节"""

    SIZE = 12

    def __init__(self, id=0, qr=0, opcode=0, aa=0, tc=0, rd=0, ra=0,
                 z=0, rcode=0, qdcount=0, ancount=0, nscount=0, arcount=0):
        self.id = id
        self.qr = qr          # 0=查询，1=响应
        self.opcode = opcode
        self.aa = aa
        self.tc = tc
        self.rd = rd
        self.ra = ra
        self.z = z
        self.rcode = rcode
        self.qdcount = qdcount
        self.ancount = ancount
        self.nscount = nscount
        self.arcount = arcount

    @property
    def flags(self) -> int:
        """把各个标志位拼成一个 16 位整数"""
        return ((self.qr & 1) << 15 |
                (self.opcode & 0xF) << 11 |
                (self.aa & 1) << 10 |
                (self.tc & 1) << 9 |
                (self.rd & 1) << 8 |
                (self.ra & 1) << 7 |
                (self.z & 0x7) << 4 |
                (self.rcode & 0xF))

    def encode(self) -> bytes:
        return struct.pack("!HHHHHH",
                           self.id, self.flags,
                           self.qdcount, self.ancount,
                           self.nscount, self.arcount)

    @classmethod
    def decode(cls, data: bytes) -> "DNSHeader":
        id_, flags, qd, an, ns, ar = struct.unpack_from("!HHHHHH", data, 0)
        return cls(
            id=id_,
            qr=(flags >> 15) & 1,
            opcode=(flags >> 11) & 0xF,
            aa=(flags >> 10) & 1,
            tc=(flags >> 9) & 1,
            rd=(flags >> 8) & 1,
            ra=(flags >> 7) & 1,
            z=(flags >> 4) & 0x7,
            rcode=flags & 0xF,
            qdcount=qd, ancount=an, nscount=ns, arcount=ar,
        )

    def __repr__(self) -> str:
        kind = "响应" if self.qr else "查询"
        return (f"<DNSHeader id=0x{self.id:04x} {kind} "
                f"QD={self.qdcount} AN={self.ancount} "
                f"NS={self.nscount} AR={self.arcount}>")


# ===========================================================================
# 查询报文构造（mDNS 用）
# ===========================================================================

def build_query(name: str, qtype: int = TYPE_PTR, qclass: int = CLASS_IN,
                transaction_id: int = 0) -> bytes:
    """
    构造一个 DNS 查询报文（Question 部分）。

    mDNS 查询通常：
      · 名字是服务类型，如 "_networkchat._tcp.local"
      · 类型是 PTR(12)，用来"列出所有提供该服务的实例"
      · 事务 ID 一般为 0（mDNS 约定）
    """
    header = DNSHeader(id=transaction_id, qr=0, opcode=0,
                       qdcount=1, ancount=0, nscount=0, arcount=0)
    question = encode_domain(name) + struct.pack("!HH", qtype, qclass)
    return header.encode() + question


def parse_answers(data: bytes) -> list:
    """
    从响应报文中解析出所有回答记录（简化版：只取 名称/类型/数据）。

    完整实现还要处理 Question 段，这里聚焦"拿到对端信息"这个目的。
    """
    header = DNSHeader.decode(data)
    offset = DNSHeader.SIZE

    # 先跳过 Question 段
    for _ in range(header.qdcount):
        _, offset = decode_domain(data, offset)
        offset += 4   # QTYPE(2) + QCLASS(2)

    # 再解析 Answer 段
    answers = []
    for _ in range(header.ancount):
        name, offset = decode_domain(data, offset)
        rtype, rclass, ttl, rdlength = struct.unpack_from("!HHIH", data, offset)
        offset += 10
        rdata = data[offset:offset + rdlength]
        offset += rdlength

        # 按类型解释 RDATA
        if rtype == TYPE_A and rdlength == 4:
            # A 记录：4 字节就是 IPv4
            value = ".".join(str(b) for b in rdata)
        elif rtype == TYPE_SRV:
            # SRV 记录：优先级(2) 权重(2) 端口(2) 目标主机(域名)
            _prio, _weight, port = struct.unpack_from("!HHH", rdata, 0)
            target, _ = decode_domain(rdata, 6)
            value = {"port": port, "target": target}
        elif rtype == TYPE_TXT:
            # TXT 记录：一块块"长度+文本"，本项目用它传自定义信息
            value = parse_txt(rdata)
        else:
            value = rdata

        answers.append({
            "name": name,
            "type": rtype,
            "class": rclass,
            "ttl": ttl,
            "data": value,
        })

    return answers


def parse_txt(rdata: bytes) -> list:
    """
    解析 TXT 记录：格式为若干个"1字节长度 + 文本"块。

    本项目 Go 版塞进去的 info 就是放在 TXT 里的：
        "instance=xxx|port=8765|addrs=192.168.1.5"
    """
    blocks = []
    i = 0
    while i < len(rdata):
        length = rdata[i]
        i += 1
        if i + length > len(rdata):
            break
        blocks.append(rdata[i:i + length].decode("utf-8", errors="replace"))
        i += length
    return blocks


# ===========================================================================
# 演示
# ===========================================================================

def demo():
    print("=" * 70)
    print("DNS / mDNS 报文演示")
    print("=" * 70)

    # ---- 1. 域名编码 ----
    print("\n【1】域名的特殊编码")
    print("  人类写法 : www.example.com")
    raw = encode_domain("www.example.com")
    print(f"  DNS 编码 : {raw}")
    print("  解读     : 3www 7example 3com 0  （每段前加长度，0 结尾）")

    # 解码回来验证
    back, _ = decode_domain(raw, 0)
    print(f"  解码回来 : {back}\n")

    # ---- 2. 一个 mDNS 查询报文 ----
    print("【2】构造一个 mDNS 查询（本项目发现邻居时发的就是它）")
    query = build_query("_networkchat._tcp.local", qtype=TYPE_PTR)
    print_hex(query, "完整的 mDNS 查询报文")
    print(f"  头部解析 : {DNSHeader.decode(query)}")
    print(f"  目标     : 组播 {MDNS_GROUP}:{MDNS_PORT}")
    print(f"  含义     : 局域网里谁提供了 _networkchat._tcp 服务？\n")

    # ---- 3. 压缩指针（DNS 最巧妙的设计）----
    print("【3】压缩指针：重复的域名用指针引用，省字节")
    # 手工构造：先放一次域名，再用指针引用它
    packet = encode_domain("example.com") + b"\x00\x01\x00\x01"  # 域名 + type + class
    pointer = b"\xC0\x00"  # 0xC000 = 指针，指向偏移 0
    combined = packet + pointer + b"\x00\x01\x00\x01"
    print_hex(combined, "带压缩指针的报文")
    name, next_off = decode_domain(combined, len(packet))
    print(f"  指针 0xC000 解析出的域名: {name}")
    print(f"  只花了 2 字节就表示了 {len('example.com')} 字节的域名\n")

    # ---- 4. TXT 记录解析 ----
    print("【4】TXT 记录：本项目用它携带自定义信息")
    info = "instance=abc-123|port=8765|addrs=192.168.1.5"
    txt_rdata = bytes([len(info)]) + info.encode("utf-8")
    print(f"  原始数据: {info}")
    print(f"  解析出来: {parse_txt(txt_rdata)}")


if __name__ == "__main__":
    demo()
