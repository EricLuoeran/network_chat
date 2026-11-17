"""
protocols —— 手写网络协议实现集合

本包的目标是**把协议本身讲清楚**，而不是调用现成库把功能跑通。

分层如下（自下而上）：

    第 1 层 传输层   udp.py / tcp.py
    第 2 层 组织架构 discovery.py（P2P 的"找人"阶段）
    第 3 层 应用层   http.py / websocket.py / dns.py

使用建议：
    想理解"数据怎么送过去"  → 先看 udp.py、tcp.py
    想理解"数据长什么样"    → 再看 http.py、websocket.py、dns.py
    想理解"设备怎么互相找到" → 最后看 discovery.py
"""

from . import common
from . import udp
from . import tcp
from . import http
from . import websocket
from . import dns
from . import discovery

__all__ = [
    "common",
    "udp",
    "tcp",
    "http",
    "websocket",
    "dns",
    "discovery",
]
