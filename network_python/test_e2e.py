"""
test_e2e —— 端到端测试：在本机启动两个节点，验证 P2P 通信真的能跑通

这是对整个项目最有力的验证 —— 它把前面所有手写的协议串起来跑一遍：

    TCP 建连 → WebSocket 握手 → 应用层 hello → 发消息 → 对端收到

运行：
    python test_e2e.py
"""

import threading
import time
import urllib.request

from app.node import Node
from app.server import ChatServer


def start_node(name: str):
    """
    启动一个节点 + 它自己的服务端。

    注意 enable_discovery=False：
    单台机器上跑两个节点时，若都开启发现会争抢同一个
    UDP 广播端口(19877)和 mDNS 端口(5353)，所以测试时关掉。
    """
    node = Node(name=name, enable_discovery=False)
    server = ChatServer(node, port=0)
    node.port = server.port
    node.start()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    time.sleep(0.3)
    return node, server


def http_get(url: str, timeout: int = 5):
    """简单的 HTTP GET，返回 (状态码, 响应体)"""
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read()


def main():
    print("=" * 62)
    print("端到端测试：两个节点的 P2P 通信")
    print("=" * 62)

    # ---- 启动两个节点 ----
    node_a, srv_a = start_node("Alice")
    node_b, srv_b = start_node("Bob")
    print(f"\n  节点 A（Alice）端口: {node_a.port}")
    print(f"  节点 B（Bob）  端口: {node_b.port}")

    # ---- 1. B 主动拨号 A（P2P 的"客户端"身份）----
    print("\n[1] B 主动连接 A")
    print("    底层依次发生：TCP三次握手 → WebSocket握手(101) → hello交换")
    ok = node_b.connect_to_peer("127.0.0.1", node_a.port, "Alice")
    print(f"    连接结果: {'✅ 成功' if ok else '❌ 失败'}")
    if not ok:
        print("\n连接失败，后续测试跳过")
        return 1
    time.sleep(0.6)

    # ---- 2. 双方是否都登记了对方 ----
    print("\n[2] 检查双方的连接表")
    peers_a = node_a.list_peers()
    peers_b = node_b.list_peers()
    print(f"    A 看到的对端: {[p['id'] for p in peers_a]}")
    print(f"    B 看到的对端: {[p['id'] for p in peers_b]}")
    linked = bool(peers_a) and bool(peers_b)

    # ---- 3. A 给 B 发消息 ----
    received = False
    if linked:
        print("\n[3] A 给 B 发一条文本消息")
        target_id = peers_a[0]["id"]
        sent = node_a.send_text(target_id, "你好 Bob，这是手写 WebSocket 发的消息！")
        print(f"    发送: {'✅ 成功' if sent else '❌ 失败'}")
        time.sleep(0.8)

        # ---- 4. B 是否收到 ----
        print("\n[4] 检查 B 是否收到")
        a_id = peers_b[0]["id"]
        msgs = node_b.get_messages(a_id)
        print(f"    B 与 A 的聊天记录: {len(msgs)} 条")
        for m in msgs:
            print(f"      [{m['direction']:>8}] {m['content']}")
        received = any(m["direction"] == "received" for m in msgs)
    else:
        print("\n[3][4] 跳过：连接未建立")

    # ---- 5. HTTP 接口 ----
    print("\n[5] 测试 HTTP 接口（手写 HTTP 报文解析）")
    api_ok = True
    for path in ["/api/me", "/api/peers"]:
        try:
            status, body = http_get(f"http://127.0.0.1:{node_a.port}{path}")
            print(f"    {path:<16} HTTP {status}  ({len(body)} 字节)")
            if status != 200:
                api_ok = False
        except Exception as e:
            print(f"    {path:<16} ❌ {e}")
            api_ok = False

    # ---- 6. 首页 ----
    print("\n[6] 测试网页首页")
    index_ok = False
    try:
        status, body = http_get(f"http://127.0.0.1:{node_a.port}/")
        print(f"    /  HTTP {status}  ({len(body)} 字节)")
        index_ok = (status == 200 and len(body) > 1000)
    except Exception as e:
        print(f"    /  ❌ {e}")

    # ---- 汇总 ----
    print("\n" + "=" * 62)
    print("测试结果")
    print("=" * 62)
    print(f"  P2P 连接建立（A↔B） : {'✅ 通过' if linked else '❌ 失败'}")
    print(f"  P2P 消息收发        : {'✅ 通过' if received else '❌ 失败'}")
    print(f"  HTTP 接口           : {'✅ 通过' if api_ok else '❌ 失败'}")
    print(f"  网页首页            : {'✅ 通过' if index_ok else '❌ 失败'}")

    all_ok = linked and received and api_ok and index_ok
    print(f"\n  总计：{'🎉 全部通过' if all_ok else '⚠️ 存在失败项'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
