"""
app —— 应用层：把手写的协议组装成一个真正能跑的局域网聊天程序

protocols/ 里是"协议的零件"，这里是把零件装成"车"的地方。

    app/message.py   自定义的应用层消息格式（对应 Go 版 WSMsg）
    app/storage.py   SQLite 持久化（聊天记录）
    app/node.py      P2P 节点：同时是客户端和服务端
    app/server.py    HTTP + WebSocket 服务端（给浏览器用）
"""
