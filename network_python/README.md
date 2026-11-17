# Network Chat — Python 协议教学版

> 本目录是 `network_chat`（Go 版）的 Python 重写，**目的是把协议本身讲清楚**。
> Go 版大量调用现成库（`net/http`、`gorilla/websocket`、`hashicorp/mdns`），
> 协议细节被封装在库内部，看不见；本版本则**用 Python 手写每一层协议**，
> 让你能直接读到"一个 HTTP 报文长什么样""WebSocket 握手那串 key 怎么算出来的"。

**本项目不修改、不依赖上级目录的任何现有文件。**

---

## 一、网络五层模型与本项目的对应

```
┌──────────────────────────────────────────────────────────────────────┐
│ 第 5 层  状态管理                                                     │
│          ✗ 不用 Cookie                                                │
│          ✓ 改用：WebSocket 长连接 + 内存状态 + SQLite                 │
│          代码：app/node.py（内存状态）、app/storage.py（持久化）       │
├──────────────────────────────────────────────────────────────────────┤
│ 第 4 层  安全层                                                       │
│          ✗ 不用 TLS（全程明文 ws:// / http://）                       │
│          ✓ 改用：应用层自加密（ECDH 协商密钥）                        │
│          代码：app/crypto.py                                          │
├──────────────────────────────────────────────────────────────────────┤
│ 第 3 层  应用层协议                                                   │
│          ◆ HTTP      —— 手写报文   → protocols/http.py                │
│          ◆ WebSocket —— 手写帧     → protocols/websocket.py           │
│          ◆ mDNS/DNS  —— 手写报文   → protocols/dns.py                 │
├──────────────────────────────────────────────────────────────────────┤
│ 第 2 层  组织架构                                                     │
│          ◆ P2P：电脑 ↔ 电脑（每台既是客户端也是服务端）                │
│          ◆ C/S：浏览器 → 本机后端                                     │
│          代码：app/node.py、app/server.py                             │
├──────────────────────────────────────────────────────────────────────┤
│ 第 1 层  传输层                                                       │
│          ◆ TCP —— 可靠、面向连接、流式   → protocols/tcp.py           │
│          ◆ UDP —— 不可靠、无连接、数据报 → protocols/udp.py           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 二、文件导航

```
network_python/
├── README.md                  ← 你在这里
├── requirements.txt           ← 依赖（极少，核心协议零依赖）
│
├── protocols/                 ★ 核心：手写的协议实现
│   ├── __init__.py
│   ├── common.py              公共工具：十六进制转储、字节序
│   ├── udp.py         【第1层】UDP：无连接数据报、广播、组播
│   ├── tcp.py         【第1层】TCP：面向连接、流式、粘包与定界
│   ├── http.py        【第3层】HTTP/1.1：手写请求/响应报文
│   ├── websocket.py   【第3层】WebSocket(RFC6455)：手写握手+帧编解码
│   ├── dns.py         【第3层】DNS/mDNS：手写报文、域名压缩、资源记录
│   └── discovery.py   【第2层】邻居发现：UDP广播 + mDNS 双通道
│
├── app/                       应用层：把协议组装成聊天程序
│   ├── __init__.py
│   ├── message.py     自定义应用层消息格式（对应 Go 版 WSMsg）
│   ├── crypto.py      应用层加密（ECDH 密钥协商）
│   ├── storage.py     SQLite 持久化（聊天记录、联系人）
│   ├── node.py        P2P 节点：同时是客户端和服务端
│   └── server.py      HTTP + WebSocket 服务端
│
├── web/
│   └── index.html     简易网页客户端
│
└── main.py            程序入口
```

---

## 三、各文件要回答的核心问题

| 文件 | 学完你能回答 |
|---|---|
| `udp.py` | 为什么 UDP 发出去就不管了？广播和组播有什么区别？ |
| `tcp.py` | 为什么 TCP 会"粘包"？应用层为什么要自己定消息边界？ |
| `http.py` | 一个 HTTP 请求报文到底长什么样？换行符为什么是 `\r\n`？ |
| `websocket.py` | WebSocket 握手那串 `Sec-WebSocket-Accept` 是怎么算出来的？帧格式如何？ |
| `dns.py` | mDNS 凭什么能在局域网里"喊一嗓子就找到人"？域名压缩是什么？ |
| `discovery.py` | 设备是怎么自动发现彼此的？为什么要广播和 mDNS 两条路？ |

---

## 四、运行

```bash
# 核心协议层零依赖，标准库即可
python main.py --name 你的名字 --port 8765
```

浏览器打开 `http://localhost:8765` 即可。

---

## 五、与 Go 版的行为对照

功能保持一致，但**内部实现完全展开**：

| 功能 | Go 版 | Python 版 |
|---|---|---|
| 监听 | `net.Listen("tcp", ...)` | `protocols/tcp.py` 的 `TCPServer` |
| HTTP 服务 | `http.Serve()` | `protocols/http.py` 手写报文解析 |
| WebSocket | `gorilla/websocket` | `protocols/websocket.py` 手写帧 |
| 邻居发现 | `hashicorp/mdns` + UDP 广播 | `protocols/dns.py` 手写 DNS 报文 |
| 消息格式 | `WSMsg` 结构体 | `app/message.py` |
| 加密 | `crypto.SharedKey` | `app/crypto.py` |
