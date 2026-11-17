"""
message —— 自定义的应用层消息格式

═══════════════════════════════════════════════════════════════════════
★ 这才是"你自己实现的协议"
═══════════════════════════════════════════════════════════════════════

前面 protocols/ 里写的 TCP/UDP/HTTP/WebSocket/DNS 都是**别人定好的标准协议**，
我们只是把它们实现出来（或者调用现成实现）。

但"一条聊天消息长什么样"、"对端收到后该干什么"，标准协议不管 ——
这属于**应用层**，必须你自己约定。这部分才是真正属于你的协议。

本项目约定：所有消息都是 JSON，靠 `type` 字段区分种类：

    ┌────────────┬──────────────────────────────────────────┐
    │ type       │ 含义 / 主要字段                          │
    ├────────────┼──────────────────────────────────────────┤
    │ hello      │ 握手：name, port, public_key             │
    │ text       │ 聊天文本：text, ts                       │
    │ file_meta  │ 文件开始：file_name, file_size           │
    │ file_end   │ 文件结束：file_id                        │
    │ peers      │ 对端列表（后端 → 浏览器）                │
    │ error      │ 错误提示                                 │
    └────────────┴──────────────────────────────────────────┘

对应 Go 版 main.go 里的 WSMsg 结构体。

【设计要点】
  1. 用 type 做"多路分发"，一个通道跑多种消息
  2. 字段都用 omitempty 风格（空值不序列化），减小体积
  3. 时间戳用毫秒（与前端 JS 的 Date.now() 一致，避免单位混乱）
"""

import json
import time


class Message:
    """
    一条应用层消息。

    本质上就是一个"带 type 字段的 JSON 对象"。
    """

    # ---- 消息类型常量 ----
    TYPE_HELLO = "hello"        # 握手：告诉对方我是谁
    TYPE_TEXT = "text"          # 聊天文本
    TYPE_FILE_META = "file_meta"  # 文件传输开始
    TYPE_FILE_END = "file_end"    # 文件传输结束
    TYPE_PEERS = "peers"        # 对端列表（后端推给浏览器）
    TYPE_NEW_MESSAGE = "new_message"  # 新消息（后端推给浏览器）
    TYPE_ERROR = "error"        # 错误

    def __init__(self, msg_type: str, **fields):
        self.type = msg_type
        self.fields = fields
        # 自动带上时间戳（前端 JS 的 Date.now() 也是毫秒，单位统一）
        if "ts" not in fields:
            fields["ts"] = int(time.time() * 1000)

    # ---------- 序列化 ----------

    def to_dict(self) -> dict:
        """转成字典（便于 JSON 序列化）"""
        return {"type": self.type, **self.fields}

    def to_json(self) -> str:
        """序列化成 JSON 字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def to_bytes(self) -> bytes:
        """序列化成字节（网络传输用）"""
        return self.to_json().encode("utf-8")

    # ---------- 反序列化 ----------

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        if "type" not in data:
            raise ValueError(f"缺少 type 字段，不是合法消息：{data}")
        msg_type = data.pop("type")
        return cls(msg_type, **data)

    @classmethod
    def from_json(cls, text: str) -> "Message":
        return cls.from_dict(json.loads(text))

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        return cls.from_json(data.decode("utf-8"))

    # ---------- 便捷访问 ----------

    def get(self, key, default=None):
        return self.fields.get(key, default)

    def __getitem__(self, key):
        return self.fields[key]

    def __repr__(self) -> str:
        return f"<Message type={self.type} {self.fields}>"


# ===========================================================================
# 常用消息的快捷构造
# ===========================================================================

def hello_message(name: str, port: int, public_key: str = "") -> Message:
    """
    构造握手消息。

    P2P 连接建立后第一件事就是互发 hello：
    告诉对方"我叫什么、我监听哪个端口、我的公钥是什么"。
    对方凭 port 才能知道该往哪个端口回连。
    """
    return Message(Message.TYPE_HELLO,
                   name=name, port=port, public_key=public_key)


def text_message(text: str, sender: str = "") -> Message:
    """构造聊天文本消息"""
    return Message(Message.TYPE_TEXT, text=text, from_name=sender)


def error_message(text: str) -> Message:
    """构造错误消息"""
    return Message(Message.TYPE_ERROR, text=text)


def demo():
    """演示：看清自定义协议的真面目"""
    print("=" * 70)
    print("自定义应用层消息格式演示")
    print("=" * 70)

    print("\n【1】构造一条聊天消息，看看它在网络上真正的样子")
    msg = text_message("你好，这是手写的 WebSocket！", sender="张三")
    raw = msg.to_bytes()
    print(f"  JSON 字符串: {raw.decode('utf-8')}")
    print(f"  字节长度   : {len(raw)} 字节")

    print("\n【2】序列化再反序列化，验证往返一致")
    restored = Message.from_bytes(raw)
    print(f"  原始  : type={msg.type}, text={msg.get('text')}")
    print(f"  还原后: type={restored.type}, text={restored.get('text')}")
    print(f"  结果  : {'✅ 一致' if restored.get('text') == msg.get('text') else '❌ 不一致'}")

    print("\n【3】握手消息（P2P 连接建立后第一条）")
    hello = hello_message("张三", 8765, "a1b2c3...")
    print(f"  {hello.to_json()}")

    print("\n【4】★ 为什么这是'你自己的协议'？")
    print("  TCP/UDP/HTTP/WebSocket 规定了'怎么把字节送过去'，")
    print("  但没规定'这些字节代表什么意思'。")
    print("  type=text 表示聊天、type=hello 表示握手 —— 这套约定是你定的，")
    print("  两端都遵守才能正常对话。这就是应用层协议。")


if __name__ == "__main__":
    demo()
