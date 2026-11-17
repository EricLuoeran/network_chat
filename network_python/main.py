"""
main —— 程序入口

启动后会做三件事：
    1. 起一个 TCP 监听（**整个程序只有一个端口**）
    2. 开始邻居发现（UDP 广播 + mDNS 双通道）
    3. 循环接受连接，按 role 分发给浏览器或 P2P 对端

【为什么只有一个端口？】
Go 版就是这样设计的：同一个端口上，
    /ws?role=frontend  → 浏览器
    /ws（无 role）     → 另一台电脑
好处是对外只暴露一个端口，防火墙、二维码都简单。

运行：
    python main.py --name 张三
    python main.py --name 张三 --port 8765
"""

import argparse
import sys

from protocols import discovery
from app.node import Node
from app.server import ChatServer


def parse_args():
    parser = argparse.ArgumentParser(
        description="Network Chat — Python 协议教学版",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python main.py --name 张三              随机端口
  python main.py --name 张三 --port 8765  固定端口（推荐，方便对方连接）

学习协议：
  python run_demos.py              运行所有协议演示（强烈推荐先看这个）
  python run_demos.py websocket    只看某一个协议
        """)
    parser.add_argument("-n", "--name", default="User", help="你的显示名字")
    parser.add_argument("-p", "--port", type=int, default=0,
                        help="监听端口，默认 0 表示由系统随机分配")
    return parser.parse_args()


def main():
    args = parse_args()

    # ---- 1. 创建 P2P 节点（此时还不知道端口）----
    node = Node(name=args.name, port=args.port)

    # ---- 2. 创建服务端，它负责绑定端口 ----
    #     端口定下来后回填给 node，这样对外广播的才是正确端口
    server = ChatServer(node, port=args.port)
    node.port = server.port

    # ---- 3. 启动节点（发现邻居 + 自动连接）----
    node.start()

    # ---- 4. 打印连接信息 ----
    ips = discovery.get_local_ips()
    print()
    print("  ╔══════════════════════════════════════════════════╗")
    print(f"  ║  用户名 : {args.name:<38} ║")
    print(f"  ║  端口   : {server.port:<38} ║")
    print("  ╠══════════════════════════════════════════════════╣")
    for ip in ips:
        print(f"  ║  IP     : {ip:<38} ║")
    print("  ╠══════════════════════════════════════════════════╣")
    print("  ║  浏览器打开：                                    ║")
    print(f"  ║    http://localhost:{server.port:<28} ║")
    if ips:
        print(f"  ║  同一网络的其它设备可访问：                      ║")
        print(f"  ║    http://{ips[0]}:{server.port:<28} ║")
    print("  ╚══════════════════════════════════════════════════╝")
    print()
    print("  提示：公共网络（校园网/企业网）通常会开启『AP 隔离』，")
    print("        设备之间无法互访，此时手机扫码会打不开，属正常现象。")
    print()

    # ---- 5. 进入主循环（阻塞）----
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[退出] 正在关闭…")
        node.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()
