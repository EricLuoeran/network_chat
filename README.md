# Network Chat - 局域网聊天工具

纯局域网通信工具，零服务器、零外部依赖、端到端加密。

## 下载

**不想编译？** 直接下载打包好的可执行文件：

[![Download](https://img.shields.io/github/v/release/EricLuoeran/network_chat?label=download&style=flat-square)](https://github.com/EricLuoeran/network_chat/releases/latest)

前往 [Releases](https://github.com/EricLuoeran/network_chat/releases/latest) 下载 `network_chat.exe`，双击即可运行（Windows 10/11 64 位）。

首次运行如果弹出 Windows 防火墙提示，请**同时勾选「专用网络」和「公用网络」**，否则对方无法连接到你。

## 功能

- 🔍 **自动发现** — mDNS 自动发现局域网内的同事
- 💬 **即时消息** — 端到端加密文本聊天
- 🖼️ **图片发送** — 支持 JPG/PNG/GIF/WebP 等
- 📎 **文件传输** — 任意文件类型，分块加密传输
- 📁 **文件夹传输** — 整个文件夹打包发送
- 🔒 **NaCl 加密** — Curve25519 + XSalsa20-Poly1305
- 📱 **全平台** — 编译一次，到处运行

## 快速开始

### 启动

```bash
# 默认设置（随机端口，用户名 "User"）
./network_chat

# 自定义
./network_chat -n 张三 -p 8765

# 查看帮助
./network_chat -h
```

启动后自动打开浏览器界面。局域网内的其他同事也启动相同程序，自动互相发现。

### 使用

1. 左侧列表显示局域网内所有在线的同事
2. 点击联系人开始聊天
3. 底部按钮发送文件/文件夹
4. 首次连接时双方显示对方指纹，建议口头核对确保安全

## 从源码编译

### 前置条件

- Go 1.21+
- 网络连接（仅首次，用于下载 Go 依赖）

### 编译

```bash
cd network_chat
go mod tidy
go build -o network_chat .

# Windows 直接双击 build.bat
```

### 交叉编译

```bash
# Windows
GOOS=windows GOARCH=amd64 go build -o network_chat.exe .

# macOS
GOOS=darwin GOARCH=amd64 go build -o network_chat_mac .

# Linux
GOOS=linux GOARCH=amd64 go build -o network_chat_linux .
```

## 安全

- 所有消息经 NaCl secretbox 加密（XSalsa20 + Poly1305）
- 密钥交换通过 Curve25519 ECDH
- 首次连接 TOFU（Trust On First Use），后续连接验证指纹是否变化
- 服务器不可见明文，因为根本没有服务器
- 私钥存储在 `data/identity.json`，权限 0600

## 数据存储

所有数据保存在 `data/` 目录：

| 文件 | 内容 |
|------|------|
| `identity.json` | 你的密钥对 |
| `chat.db` | 聊天记录（SQLite） |
| `downloads/` | 接收的文件 |

## 技术栈

| 层次 | 技术 |
|------|------|
| 语言 | Go |
| 前端 | Vanilla HTML/CSS/JS |
| 网络发现 | mDNS (hashicorp/mdns) |
| P2P 通信 | WebSocket |
| 加密 | NaCl (golang.org/x/crypto) |
| 存储 | SQLite (modernc.org/sqlite) |

## 项目结构

```
network_chat/
├── main.go              主程序：HTTP 服务、P2P 连接、消息路由
├── qr.go                二维码生成（手机端接入）
├── build.bat            Windows 一键编译脚本
│
├── crypto/              端到端加密（Curve25519 + XSalsa20-Poly1305）
├── discovery/           局域网发现
│   ├── broadcast.go       UDP 广播（端口 19877）
│   └── mdns.go            mDNS 组播（端口 5353）
├── storage/             SQLite 持久化（聊天记录、联系人）
├── transfer/            文件分块传输
│
├── web/                 前端界面（原生 HTML/CSS/JS）
│   ├── index.html         桌面端
│   ├── mobile.html        手机端
│   └── app.js             WebSocket 客户端
│
└── network_python/      ★ Python 教学版：手写协议实现
    ├── README.md          五层模型与文件导航
    ├── run_demos.py       一键运行所有协议演示
    ├── test_e2e.py        端到端 P2P 测试
    ├── protocols/         手写的协议实现（非调用库）
    │   ├── udp.py           UDP 数据报 / 广播 / 组播
    │   ├── tcp.py           TCP 头部 / 粘包 / 长度前缀
    │   ├── http.py          HTTP 报文解析与生成
    │   ├── websocket.py     RFC 6455 握手 + 帧编解码
    │   ├── dns.py           DNS 报文 / 域名压缩指针
    │   └── discovery.py     UDP 广播 + mDNS 双通道发现
    └── app/               应用层组装（消息格式、P2P 节点、服务端）
```

## 网络架构

本项目是 **P2P 与 C/S 混合架构**：

```
发现阶段：UDP 广播(19877) + mDNS 组播(5353)    ← 只负责找邻居
通信阶段：WebSocket(ws://) 架在 TCP 上          ← 承载全部业务数据
```

- **电脑 ↔ 电脑**：真 P2P，双方既主动拨号也接受连接，无中心服务器
- **浏览器 / 手机 → 本机后端**：标准 C/S，后端是唯一服务方
- **传输层**：TCP 承载所有业务数据；UDP 仅用于发现，不传业务数据

## Python 教学版

`network_python/` 是一个**独立可运行**的子项目，用 Python 从零手写协议实现，
适合想理解"协议到底是怎么跑起来的"的人——Go 版调用 `gorilla/websocket`、
`hashicorp/mdns` 等库，协议细节被封装；Python 版则握手、帧编解码、DNS 报文
全部自己实现，每个报文都能打印出真实字节。

```bash
cd network_python
python run_demos.py    # 看每个协议的字节级实现
python test_e2e.py     # 验证两个节点能否真正通信
```

零第三方依赖，仅需 Python 3.8+。

## 已知限制

- 仅支持 IPv4 局域网（IPv6 地址在发现阶段会被过滤）
- 公共 WiFi / 校园网若开启 AP 隔离（客户端隔离），设备间无法互通
- 未启用 TLS（`ws://` 明文传输），安全性由应用层 NaCl 加密保证

## 协议

MIT — 随意使用、修改、分发。详见 [LICENSE](LICENSE)。
