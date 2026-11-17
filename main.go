package main

// =============================================================================
// Network Chat —— 架构分层总览（自下而上，对应"网络五层模型"）
// =============================================================================
//
//  ┌────────────────────────────────────────────────────────────────────────┐
//  │ 第 5 层 │ 状态管理                                                     │
//  │         │ ✗ 本程序【没有】用 Cookie                                    │
//  │         │ ✓ 改用：WebSocket 长连接 + 内存状态 + SQLite 持久化           │
//  │         │ 代码：App.peers / App.frontends / App.devices / storage.DB   │
//  ├────────────────────────────────────────────────────────────────────────┤
//  │ 第 4 层 │ 安全层                                                       │
//  │         │ ✗ 本程序【没有】HTTPS/TLS（全程 http:// 与 ws:// 明文）      │
//  │         │ ✓ 改用：应用层自己加密（ECDH 协商出的 crypto.SharedKey）      │
//  │         │ 代码：identity.ComputeShared(peerPub)                        │
//  ├────────────────────────────────────────────────────────────────────────┤
//  │ 第 3 层 │ 应用层协议 HTTP                                              │
//  │         │ 路由分发 + 静态资源 + 升级成 WebSocket                       │
//  │         │ 代码：http.NewServeMux / http.Serve / serveWeb /             │
//  │         │       upgrader.Upgrade（HTTP 101 → WebSocket）               │
//  ├────────────────────────────────────────────────────────────────────────┤
//  │ 第 2 层 │ 宏观组织架构（本项目是 P2P 与 C/S 的混合体）                 │
//  │         │ ◆ P2P：电脑 ↔ 电脑，每台既是客户端也是服务端                 │
//  │         │   代码：connectToPeer（主动拨号）/ handlePeerWS（被动接收）   │
//  │         │ ◆ C/S：浏览器、手机 → 本机后端，后端是唯一的 Server          │
//  │         │   代码：handleFrontend（浏览器）/ handleMobile（手机）        │
//  ├────────────────────────────────────────────────────────────────────────┤
//  │ 第 1 层 │ 传输层                                                       │
//  │         │ ◆ TCP（可靠快递）：承载 HTTP / WebSocket / 文件与文件夹传输   │
//  │         │   代码：net.Listen("tcp", addr)                              │
//  │         │ ◆ UDP（极速平邮）：只用于"发现邻居"，不传任何业务数据        │
//  │         │   代码：discovery/broadcast.go 的 net.ListenUDP（端口 19877）│
//  │         │        discovery/mdns.go 的 mDNS 组播（端口 5353）           │
//  └────────────────────────────────────────────────────────────────────────┘
//
// 一句话串起来：
//   两台电脑之间是 P2P 架构（第 2 层），跑在 TCP 之上（第 1 层），
//   用 HTTP 协议沟通（第 3 层）；
//   第 4 层没上 TLS，改为在应用层自己加密；
//   第 5 层没用 Cookie，改为用 WebSocket 长连接 + 数据库来维持状态。
// =============================================================================

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"network_chat/crypto"
	"network_chat/discovery"
	"network_chat/storage"
	"network_chat/transfer"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

//go:embed web/*
var webFS embed.FS

// =============================================================================
// Config
// =============================================================================

type Config struct {
	Name    string
	Port    int
	DataDir string
}

// =============================================================================
// App
// =============================================================================

// 【第 5 层 - 状态管理：本程序没有使用 Cookie】
//
// Cookie 的意义是给"健忘"的 HTTP 打补丁 —— HTTP 天生无状态，
// 每次请求都是独立的，服务端只能靠 Cookie 里的标识认出"还是刚才那个客户端"。
//
// 但 WebSocket 一旦建立，就是一条长期保持的连接，天然知道对面是谁，
// 压根不需要 Cookie 来"认人"。所以本程序改用下面这三样来维持状态：
//
//   · 长连接本身：frontends / devices / peers 这几个 map 直接持有连接对象，
//     连接在，状态就在；连接断开就从中删除。
//   · 内存状态：peersByPK（按公钥索引对端）、folderUploads（文件夹上传进度）等。
//   · 数据库持久化：db（SQLite）保存聊天记录与联系人，
//     保证程序重启后历史消息仍在 —— 这恰恰是 Cookie 做不到的事。
//
// 一句话：Cookie 是"给无状态协议打的状态补丁"，
// 而 WebSocket 自身就是有状态的，于是第 5 层在本程序里被整个跳过了。
type App struct {
	cfg      Config
	identity *crypto.Identity
	db       *storage.DB
	// discMu 保护 discovery / bcast 这两个字段的"指针替换"。
	// 改名、随机端口重绑时会把它们整体换成新实例，而 discoveryPollLoop、
	// getContacts 等协程同时可能正在读取旧实例 —— 不加锁就是数据竞争。
	// 注意：这里保护的是**字段指针**，不是实例内部状态（那由各自内部锁负责）。
	discMu    sync.RWMutex
	discovery *discovery.Service
	bcast     *discovery.BroadcastDiscovery
	files     *transfer.Manager

	// Folder packing state
	folderUploads   map[string]*folderUpload // folderName → pending files
	folderUploadsMu sync.Mutex

	// Mobile device hub (phone browsers connected to this computer)
	devices   map[string]*MobileClient
	devicesMu sync.Mutex

	peers     map[string]*PeerConn // "name@host:port"
	peersByPK map[string]*PeerConn // publicKey hex → *PeerConn
	peersMu   sync.RWMutex

	// Frontend connections (browser → local backend)
	frontends   map[*FrontendConn]bool
	frontendsMu sync.Mutex

	// Event bus: messages received from peers → push to frontends
	events chan UIEvent

	upgrader websocket.Upgrader
}

// PeerConn is an established P2P WebSocket connection.
type PeerConn struct {
	ID        string
	Name      string
	Host      string
	Port      int
	PublicKey [32]byte
	SharedKey crypto.SharedKey
	Verified  bool
	Conn      *websocket.Conn
	mu        sync.Mutex
}

func (pc *PeerConn) SendJSON(v interface{}) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.Conn.WriteJSON(v)
}

// registerPeer adds or replaces a peer connection, deduplicating by public key.
// If a connection with the same public key exists, the one with the smaller
// public key lexicographically keeps the connection (P2P tiebreaker).
func (a *App) registerPeer(pc *PeerConn) {
	pkHex := crypto.HexEncode(pc.PublicKey[:])

	a.peersMu.Lock()
	defer a.peersMu.Unlock()

	// Check for existing connection with same public key (different host/port)
	if existing, ok := a.peersByPK[pkHex]; ok {
		ourPK := crypto.HexEncode(a.identity.PublicKey[:])
		if ourPK < pkHex {
			// We have the smaller key, keep our connection
			existing.Conn.Close()
		} else {
			// Peer has smaller key, close our new connection and keep theirs
			pc.Conn.Close()
			return
		}
	}

	// Close any old connection with the same peerID
	if old, ok := a.peers[pc.ID]; ok {
		old.Conn.Close()
	}

	a.peers[pc.ID] = pc
	a.peersByPK[pkHex] = pc
}

// unregisterPeer removes a peer connection from both maps.
func (a *App) unregisterPeer(pc *PeerConn) {
	pkHex := crypto.HexEncode(pc.PublicKey[:])
	a.peersMu.Lock()
	// Only delete if this exact pointer is still registered.
	// (registerPeer may have replaced it with a new connection that shares the same peerID.)
	if cur, ok := a.peers[pc.ID]; ok && cur == pc {
		delete(a.peers, pc.ID)
	}
	if cur, ok := a.peersByPK[pkHex]; ok && cur == pc {
		delete(a.peersByPK, pkHex)
	}
	a.peersMu.Unlock()
}

// FrontendConn is the browser's WebSocket to its own backend.
type FrontendConn struct {
	Conn *websocket.Conn
	mu   sync.Mutex
}

func (fc *FrontendConn) SendJSON(v interface{}) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.Conn.WriteJSON(v)
}

// UIEvent is pushed from backend to the frontend (browser).
type UIEvent struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type folderUpload struct {
	name  string
	files map[string][]byte // relativePath → content
}

// =============================================================================
// WS message types
// =============================================================================

type WSMsg struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
	Ts   int64  `json:"ts,omitempty"`

	// Hello
	Name       string `json:"name,omitempty"`
	PublicKey  string `json:"publicKey,omitempty"`
	ListenPort int    `json:"listenPort,omitempty"`
	Port      int    `json:"port,omitempty"`

	// Text
	Ciphertext string `json:"ciphertext,omitempty"`
	Nonce      string `json:"nonce,omitempty"`

	// File
	FileName    string `json:"fileName,omitempty"`
	FileSize    int64  `json:"fileSize,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
	TotalChunks int    `json:"totalChunks,omitempty"`
	ChunkIndex  int    `json:"chunkIndex,omitempty"`
	ChunkData   string `json:"chunkData,omitempty"`

	// Folder
	FolderName string `json:"folderName,omitempty"`
	FileCount  int    `json:"fileCount,omitempty"`
	TotalSize  int64  `json:"totalSize,omitempty"`

	Message string `json:"message,omitempty"`
}

// MobileClient is a phone/tablet browser connected to this computer.
type MobileClient struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Conn     *websocket.Conn `json:"-"`
	mu       sync.Mutex

	// Chunked file receive state
	pendingFileID   string
	pendingFileName string
	pendingFileSize int64
	pendingFileBuf  []byte
}

func (mc *MobileClient) SendJSON(v interface{}) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.Conn.WriteJSON(v)
}

// broadcastDevices pushes the device list to all frontends.
func (a *App) broadcastDevices() {
	a.devicesMu.Lock()
	list := make([]map[string]string, 0, len(a.devices))
	for _, d := range a.devices {
		list = append(list, map[string]string{
			"id": d.ID, "name": d.Name,
		})
	}
	a.devicesMu.Unlock()
	a.pushToFrontends("devices", list)
}

// =============================================================================
// Frontend → Backend message (control channel)
type FrontendMsg struct {
	Action     string `json:"action"` // "send_text", "send_file", "connect_peer", "pack_and_send"
	PeerID     string `json:"peerId,omitempty"`
	Text       string `json:"text,omitempty"`
	FilePath   string `json:"filePath,omitempty"`
	FolderName string `json:"folderName,omitempty"`
}

// =============================================================================
// Constructor
// =============================================================================

func NewApp(cfg Config) (*App, error) {
	os.MkdirAll(cfg.DataDir, 0755)

	idPath := filepath.Join(cfg.DataDir, "identity.json")
	identity, err := crypto.LoadIdentity(idPath, cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("identity: %w", err)
	}

	dbPath := filepath.Join(cfg.DataDir, "chat.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("database: %w", err)
	}

	files := transfer.NewManager(filepath.Join(cfg.DataDir, "downloads"))

	return &App{
		cfg:       cfg,
		identity:  identity,
		db:        db,
		files:     files,
		peers:     make(map[string]*PeerConn),
		peersByPK: make(map[string]*PeerConn),
		devices:        make(map[string]*MobileClient),
		folderUploads:   make(map[string]*folderUpload),
		frontends: make(map[*FrontendConn]bool),
		events:    make(chan UIEvent, 128),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}, nil
}

// =============================================================================
// Start
// =============================================================================

// getDisc 安全地取出当前的发现服务实例（加读锁）。
// 所有"只想用一下"的地方都走这里，避免直接读字段时撞上改名导致的指针替换。
func (a *App) getDisc() (*discovery.Service, *discovery.BroadcastDiscovery) {
	a.discMu.RLock()
	defer a.discMu.RUnlock()
	return a.discovery, a.bcast
}

// setDisc 在写锁保护下，用新实例替换旧的发现服务。
// 调用方负责先把旧实例 Stop() 掉（见 restartDiscovery）。
func (a *App) setDisc(d *discovery.Service, b *discovery.BroadcastDiscovery) {
	a.discMu.Lock()
	a.discovery = d
	a.bcast = b
	a.discMu.Unlock()
}

// startDisc 创建并启动一对新的发现服务，然后原子替换旧字段。
// 这是"端口确定后重绑"和"改名"两处共用的逻辑。
func (a *App) startDisc() {
	d := discovery.New(a.identity.Name, a.cfg.Port)
	b := discovery.NewBroadcast(a.identity.Name, a.cfg.Port)
	// 先替换字段再启动：新实例的内部状态与字段保持一致，
	// 其它协程立刻读到的就是这一对，不会短暂读到"新 d + 旧 b"的混合状态。
	a.setDisc(d, b)
	d.Start()
	b.Start()
}

func (a *App) Start() error {
	// Start discovery first (mDNS + UDP broadcast)
	a.startDisc()
	go a.discoveryPollLoop()

	// Event broadcaster
	go a.broadcastEvents()

	// 【第 3 层 - 应用层协议：HTTP】
	// ServeMux 就是 HTTP 的"路由表"——规定了什么路径交给谁处理，
	// 也就是"怎么要数据、怎么给数据"这套规则本身。
	// 这些 HTTP 流量全部由第 1 层那条 TCP 监听来承载。
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", a.handleWebSocket)
	mux.HandleFunc("/api/me", a.handleMe)
	mux.HandleFunc("/api/peers", a.handlePeers)
	mux.HandleFunc("/api/contacts", a.handleContacts)
	mux.HandleFunc("/api/contacts/verify", a.handleVerifyContact)
	mux.HandleFunc("/api/messages/", a.handleMessages)
	mux.HandleFunc("/api/download/", a.handleDownload)
	mux.HandleFunc("/api/upload", a.handleUpload)
	mux.HandleFunc("/api/connect", a.handleManualConnect)
	mux.HandleFunc("/api/name", a.handleChangeName)
	mux.HandleFunc("/api/devices", a.handleDevices)
	mux.HandleFunc("/qr.png", a.handleQR)
	mux.HandleFunc("/api/send-to-mobile", a.handleSendToMobile)
	// Serve web UI for everything else
	mux.HandleFunc("/", a.serveWeb)

	log.Printf("")
	log.Printf("  ╔══════════════════════════════════════════════╗")
	log.Printf("  ║     Network Chat - 局域网聊天工具             ║")
	log.Printf("  ╠══════════════════════════════════════════════╣")
	log.Printf("  ║  用户名: %-34s ║", a.identity.Name)
	log.Printf("  ║  指纹:   %-34s ║", crypto.ShortFingerprint(a.identity.PublicKey))
	log.Printf("  ║  端口:   %-34d ║", a.cfg.Port)
	log.Printf("  ╠══════════════════════════════════════════════╣")
	for _, ip := range getLocalIPs() {
		log.Printf("  ║  局域网 IP: %-31s ║", ip.String())
	}
	log.Printf("  ╠══════════════════════════════════════════════╣")
	log.Printf("  ║  💡 让对方在浏览器点 ➕ 手动连接              ║")
	log.Printf("  ║     输入你的 IP 和端口 %-5d 即可              ║", a.cfg.Port)
	log.Printf("  ╚══════════════════════════════════════════════╝")
	log.Printf("")

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("[app] shutting down...")
		curD, curB := a.getDisc()
		curD.Stop()
		curB.Stop()
		a.db.Close()
		os.Exit(0)
	}()

	// 【第 1 层 - 传输层：TCP（可靠快递）】
	// 这里创建的 TCP 监听是整个程序的网络底座：
	//   第 3 层的 HTTP、升级后的 WebSocket、以及文件/文件夹传输，全部跑在这条 TCP 连接之上。
	// 监听地址写作 ":端口"（省略 IP），等价于绑定 0.0.0.0，
	// 即本机所有网卡都能接受连接 —— 所以对端电脑、手机才能连进来。
	// 若写成 "127.0.0.1:端口" 就只有本机能连，外部设备一律被拒。
	addr := fmt.Sprintf(":%d", a.cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if a.cfg.Port == 0 {
		// 端口是随机分配的，此刻才拿到真实端口 —— 要用它重建发现服务，
		// 否则广播/mDNS 里通告的还是端口 0，别人拿到也连不上。
		a.cfg.Port = listener.Addr().(*net.TCPAddr).Port
		oldD, oldB := a.getDisc()
		oldD.Stop()
		oldB.Stop()
		a.startDisc()
		go a.discoveryPollLoop()
	}
	go openBrowser(fmt.Sprintf("http://localhost:%d", a.cfg.Port))
	return http.Serve(listener, mux)
}

// =============================================================================
// HTTP: serve web UI
// =============================================================================

func (a *App) serveWeb(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" || path == "m" {
		path = "web/index.html"
	}
	if r.URL.Path == "/m" {
		path = "web/mobile.html"
	}
	content, err := webFS.ReadFile(path)
	if err != nil {
		content, _ = webFS.ReadFile("web/index.html")
		if content == nil {
			http.Error(w, "Not Found", 404)
			return
		}
	}
	ext := filepath.Ext(path)
	ct := map[string]string{
		".html": "text/html; charset=utf-8",
		".css":  "text/css; charset=utf-8",
		".js":   "application/javascript; charset=utf-8",
	}[ext]
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Write(content)
}

// =============================================================================
// HTTP: API handlers
// =============================================================================

func (a *App) handleMe(w http.ResponseWriter, r *http.Request) {
	ips := []string{}
	for _, ip := range getLocalIPs() {
		ips = append(ips, ip.String())
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"name":             a.identity.Name,
		"fingerprint":      crypto.Fingerprint(a.identity.PublicKey),
		"shortFingerprint": crypto.ShortFingerprint(a.identity.PublicKey),
		"publicKey":        crypto.HexEncode(a.identity.PublicKey[:]),
		"port":             a.cfg.Port,
		"ips":              ips,
	})
}

func (a *App) handlePeers(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(a.collectPeers())
}

func (a *App) handleContacts(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		contacts, _ := a.db.GetContacts()
		if contacts == nil {
			contacts = []*storage.Contact{}
		}
		json.NewEncoder(w).Encode(contacts)
		return
	}
	http.Error(w, "Method not allowed", 405)
}

func (a *App) handleVerifyContact(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var body struct{ ID string }
	json.NewDecoder(r.Body).Decode(&body)
	a.db.VerifyContact(body.ID)
	a.peersMu.Lock()
	if pc, ok := a.peers[body.ID]; ok {
		pc.Verified = true
	}
	a.peersMu.Unlock()
	a.pushPeersToFrontend()
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *App) handleMessages(w http.ResponseWriter, r *http.Request) {
	peerID := strings.TrimPrefix(r.URL.Path, "/api/messages/")
	if peerID == "" || peerID == "/api/messages" {
		msgs, _ := a.db.GetRecentChats()
		if msgs == nil {
			msgs = []*storage.Message{}
		}
		json.NewEncoder(w).Encode(msgs)
		return
	}
	msgs, _ := a.db.GetMessages(peerID, 200, 0)
	if msgs == nil {
		msgs = []*storage.Message{}
	}
	json.NewEncoder(w).Encode(msgs)
}

func (a *App) handleDownload(w http.ResponseWriter, r *http.Request) {
	relPath := strings.TrimPrefix(r.URL.Path, "/api/download/")
	data, err := a.files.ReadFile(relPath)
	if err != nil {
		http.Error(w, "Not found", 404)
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, filepath.Base(relPath)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(data)
}

func (a *App) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 500<<20) // 500MB limit
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, fmt.Sprintf("Parse error: %v", err), 400)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, fmt.Sprintf("File error: %v", err), 400)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, fmt.Sprintf("Read error: %v", err), 500)
		return
	}

	folderName := r.FormValue("folderName")
	relativePath := r.FormValue("relativePath")

	if folderName != "" {
		// Keyed by peer+folder to avoid colliding with concurrent uploads
		peerID := r.FormValue("peerId")
		key := peerID + "\x00" + folderName
		a.folderUploadsMu.Lock()
		fu, ok := a.folderUploads[key]
		if !ok {
			fu = &folderUpload{name: folderName, files: make(map[string][]byte)}
			a.folderUploads[key] = fu
		}
		fu.files[relativePath] = data
		a.folderUploadsMu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"fileName": header.Filename,
			"fileSize": len(data),
			"folder":   folderName,
		})
		return
	}

	savedPath, err := a.files.SaveFile(header.Filename, data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Save error: %v", err), 500)
		return
	}

	relPath, _ := filepath.Rel(a.files.BaseDir(), savedPath)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"fileName": header.Filename,
		"fileSize": len(data),
		"path":     relPath,
	})
}

// POST /api/connect — manual connection by IP:port and name
func (a *App) handleManualConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var body struct {
		Host string `json:"host"`
		Port int    `json:"port"`
		Name string `json:"name"` // expected peer name
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Bad request", 400)
		return
	}
	if body.Host == "" || body.Port == 0 {
		http.Error(w, "host and port required", 400)
		return
	}

	peerID := fmt.Sprintf("%s@%s:%d", body.Name, body.Host, body.Port)
	log.Printf("[manual] connecting to %s...", peerID)
	go a.connectToPeer(peerID, body.Name, body.Host, body.Port)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "connecting",
		"peerId": peerID,
	})
}

// POST /api/name — change display name
func (a *App) handleChangeName(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var body struct{ Name string }
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		http.Error(w, "Bad request", 400)
		return
	}

	oldName := a.identity.Name
	a.identity.Name = body.Name
	idPath := filepath.Join(a.cfg.DataDir, "identity.json")
	if err := a.identity.Save(idPath); err != nil {
		// 持久化失败就回滚内存里的名字，保证"内存与磁盘一致"，
		// 并明确告诉前端失败原因（返回 JSON，避免前端解析纯文本报错）
		a.identity.Name = oldName
		http.Error(w, "save identity failed: "+err.Error(), 500)
		return
	}

	// 【改名为什么要异步重建发现服务？】
	// 发现服务里广播的"名字"是创建时写死的，改名必须整体重建一套。
	// 但 Stop() 内部会等注册协程就绪，最长阻塞 2 秒；而本函数是同步处理
	// HTTP 请求的，若在这里同步重建，浏览器很可能等不到响应就把连接断了 ——
	// 表现就是前端弹"改名失败"，但其实名字早已改好。
	// 因此：名字立即改（上方已完成），发现服务的重建丢到后台协程去做。
	go func(newName string) {
		oldD, oldB := a.getDisc()
		oldD.Stop()
		oldB.Stop()
		a.startDisc()
		log.Printf("[disc] discovery restarted under name %q", newName)
	}(body.Name)

	log.Printf("[app] name changed: %s → %s", oldName, body.Name)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"name":   a.identity.Name,
	})
}

// =============================================================================
// Mobile device support (phone browsers)
// =============================================================================

// GET/POST /api/devices — list or send to mobile devices
func (a *App) handleDevices(w http.ResponseWriter, r *http.Request) {
	a.devicesMu.Lock()
	list := make([]map[string]string, 0, len(a.devices))
	for _, d := range a.devices {
		list = append(list, map[string]string{"id": d.ID, "name": d.Name})
	}
	a.devicesMu.Unlock()
	json.NewEncoder(w).Encode(list)
}

// GET /qr.png — QR code for mobile devices to scan
func (a *App) handleQR(w http.ResponseWriter, r *http.Request) {
	// Get the best LAN IP
	ips := getLocalIPs()
	if len(ips) == 0 {
		http.Error(w, "no network", 500)
		return
	}
	url := fmt.Sprintf("http://%s:%d/m", ips[0], a.cfg.Port)
	png, err := generateQR(url)
	if err != nil {
		http.Error(w, "QR not available: "+err.Error(), 404)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write(png)
}

// WebSocket handler for mobile (phone browser) connections
func (a *App) handleMobile(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	var hello struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}
	if err := conn.ReadJSON(&hello); err != nil {
		conn.Close()
		return
	}

	client := &MobileClient{
		ID:   "mobile-" + uuid.New().String()[:8],
		Name: hello.Name,
		Conn: conn,
	}
	if client.Name == "" {
		client.Name = "手机"
	}

	a.devicesMu.Lock()
	a.devices[client.ID] = client
	a.devicesMu.Unlock()

	log.Printf("[mobile] device connected: %s (%s)", client.Name, client.ID)
	a.broadcastDevices()

	// Send welcome
	myIPs := getLocalIPs()
	lanIP := "localhost"
	if len(myIPs) > 0 {
		lanIP = myIPs[0].String()
	}
	client.SendJSON(map[string]interface{}{
		"type":  "welcome",
		"id":    client.ID,
		"name":  client.Name,
		"lanIP": lanIP,
		"port":  a.cfg.Port,
	})

	defer func() {
		a.devicesMu.Lock()
		delete(a.devices, client.ID)
		a.devicesMu.Unlock()
		conn.Close()
		log.Printf("[mobile] device disconnected: %s", client.Name)
		a.broadcastDevices()
	}()

	// Read messages from mobile (text JSON + binary file chunks)
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if mt == websocket.BinaryMessage {
			a.processMobileBinary(client, data)
		} else if mt == websocket.TextMessage {
			var msg map[string]interface{}
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			a.processMobileMsg(client, msg)
		}
	}
}

func (a *App) processMobileMsg(from *MobileClient, msg map[string]interface{}) {
	msgType, _ := msg["type"].(string)

	switch msgType {
	case "text":
		text, _ := msg["text"].(string)
		toID, _ := msg["to"].(string)
		if toID == "" || toID == "computer" {
			a.pushToFrontends("mobile_message", map[string]interface{}{
				"from":     from.ID,
				"fromName": from.Name,
				"text":     text,
				"ts":       time.Now().UnixMilli(),
			})
		} else {
			a.devicesMu.Lock()
			target, ok := a.devices[toID]
			a.devicesMu.Unlock()
			if ok {
				target.SendJSON(map[string]interface{}{
					"type": "text", "from": from.ID,
					"fromName": from.Name, "text": text,
				})
			}
		}

	case "file_meta":
		// Start chunked file receive
		fileID, _ := msg["id"].(string)
		fileName, _ := msg["name"].(string)
		fileSize, _ := msg["size"].(float64)

		from.pendingFileID = fileID
		from.pendingFileName = fileName
		from.pendingFileSize = int64(fileSize)
		from.pendingFileBuf = nil // reset

		log.Printf("[mobile] receiving file: %s (%.0f bytes) via chunks", fileName, fileSize)
		// Tell mobile we're ready to receive chunks
		from.SendJSON(map[string]interface{}{
			"type": "file_ack", "id": fileID, "status": "ready",
		})

	case "file_end":
		// Finalize and save
		fileID, _ := msg["id"].(string)
		if fileID != from.pendingFileID {
			return
		}
		fileName := from.pendingFileName
		fileData := from.pendingFileBuf

		from.pendingFileID = ""
		from.pendingFileName = ""
		from.pendingFileBuf = nil

		if len(fileData) == 0 {
			return
		}

		savedPath, err := a.files.SaveFile(fileName, fileData)
		if err != nil {
			log.Printf("[mobile] save file error: %v", err)
			return
		}

		log.Printf("[mobile] received file: %s (%d bytes)", fileName, len(fileData))
		evType := "mobile_file"
		if isImageExt(filepath.Ext(fileName)) {
			evType = "mobile_image"
		}
		relPath, _ := filepath.Rel(a.files.BaseDir(), savedPath)
		a.pushToFrontends(evType, map[string]interface{}{
			"from":     from.ID,
			"fromName": from.Name,
			"fileName": fileName,
			"fileSize": len(fileData),
			"filePath": relPath,
			"ts":       time.Now().UnixMilli(),
		})

	default:
		a.pushToFrontends("mobile_event", msg)
	}
}

func (a *App) processMobileBinary(from *MobileClient, data []byte) {
	if from.pendingFileID == "" {
		return
	}
	from.pendingFileBuf = append(from.pendingFileBuf, data...)
}

// POST /api/send-to-mobile — frontend sends text/file path to a mobile device
func (a *App) handleSendToMobile(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var body struct {
		DeviceID string `json:"deviceId"`
		Text     string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	a.devicesMu.Lock()
	device, ok := a.devices[body.DeviceID]
	a.devicesMu.Unlock()
	if !ok {
		http.Error(w, "Device not connected", 404)
		return
	}

	if body.Text != "" {
		device.SendJSON(map[string]interface{}{
			"type":     "text",
			"from":     "computer",
			"fromName": a.identity.Name,
			"text":     body.Text,
		})
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// =============================================================================
// WebSocket: dispatcher
// =============================================================================

// 【第 2 层 - 宏观组织架构：这里同时存在 C/S 与 P2P 两种架构】
//
// 同样是访问 /ws 这一个地址，却按 URL 上的 role 参数进入三种截然不同的关系：
//
//   role=frontend → handleFrontend   【C/S 架构】浏览器(Client) → 本机后端(Server)
//   role=mobile   → handleMobile     【C/S 架构】手机(Client)   → 本机后端(Server)
//   role 为空     → handlePeerWS     【P2P 架构】另一台电脑连进来，双方对等
//
// 为什么电脑之间算 P2P？
//   因为每台电脑都同时身兼两种身份：
//     · 服务端：在 handlePeerWS 里被动接受别人的连接；
//     · 客户端：在 connectToPeer 里主动拨号 ws://对方IP:端口/ws。
//   没有中心服务器，谁都能主动连谁、也都能被连 —— 这就是 P2P。
//
// 为什么浏览器和手机算 C/S？
//   它们只连自己这一台电脑的后端，不会去连别的电脑，
//   后端对它们而言是唯一的服务提供方，是标准的 Client/Server 关系。
//
// 【与第 3 层的衔接】这些连接都是先以 HTTP 请求进来，
// 再由 upgrader.Upgrade() 把协议从 HTTP "升级"成 WebSocket（服务端回 101 状态码）。
// 升级完成后，这条 TCP 连接就脱离了 HTTP 的"一问一答"模式，变成双向长连接。
func (a *App) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	role := r.URL.Query().Get("role")
	switch role {
	case "frontend":
		a.handleFrontend(w, r)
	case "mobile":
		a.handleMobile(w, r)
	default:
		a.handlePeerWS(w, r)
	}
}

// =============================================================================
// WebSocket: frontend (browser ↔ local backend)
// =============================================================================

func (a *App) handleFrontend(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	fc := &FrontendConn{Conn: conn}
	a.frontendsMu.Lock()
	a.frontends[fc] = true
	a.frontendsMu.Unlock()

	log.Printf("[frontend] browser connected")

	// Push current peer list immediately
	a.pushPeersToFrontend()

	defer func() {
		a.frontendsMu.Lock()
		delete(a.frontends, fc)
		a.frontendsMu.Unlock()
		conn.Close()
		log.Printf("[frontend] browser disconnected")
	}()

	for {
		var msg FrontendMsg
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		switch msg.Action {
		case "send_text":
			if msg.PeerID != "" && msg.Text != "" {
				go func() {
					if err := a.sendTextMessage(msg.PeerID, msg.Text); err != nil {
						a.sendToFrontend(UIEvent{Type: "error", Data: err.Error()})
					}
				}()
			}
		case "send_file":
			if msg.PeerID != "" && msg.FilePath != "" {
				go func() {
					if err := a.sendFileToPeer(msg.PeerID, msg.FilePath); err != nil {
						a.sendToFrontend(UIEvent{Type: "error", Data: err.Error()})
					}
				}()
			}
		case "connect_peer":
			if msg.PeerID != "" {
				go a.manualConnect(msg.PeerID)
			}
		case "pack_and_send":
			if msg.PeerID != "" && msg.FolderName != "" {
				go a.packAndSendFolder(msg.PeerID, msg.FolderName)
			}
		}
	}
}

func (a *App) manualConnect(peerID string) {
	// Parse "name@host:port"
	parts := strings.Split(peerID, "@")
	if len(parts) != 2 {
		return
	}
	name := parts[0]
	hostPort := strings.Split(parts[1], ":")
	if len(hostPort) != 2 {
		return
	}
	host := hostPort[0]
	port := 0
	fmt.Sscanf(hostPort[1], "%d", &port)
	if port == 0 {
		return
	}
	a.connectToPeer(fmt.Sprintf("%s@%s:%d", name, host, port), name, host, port)
}

// hasLivePeerByName reports whether we currently have a live P2P connection to
// any peer with the given display name. Used to avoid duplicate dials when
// discovery reports the same peer under several of its IPs.
func (a *App) hasLivePeerByName(name string) bool {
	a.peersMu.RLock()
	defer a.peersMu.RUnlock()
	for _, pc := range a.peers {
		if pc.Name == name {
			return true
		}
	}
	return false
}

// =============================================================================
// WebSocket: peer (other chat clients connecting to us)
// =============================================================================

func (a *App) handlePeerWS(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	var hello WSMsg
	if err := conn.ReadJSON(&hello); err != nil || hello.Type != "hello" {
		conn.Close()
		return
	}

	peerPub, err := crypto.PubKeyFromHex(hello.PublicKey)
	if err != nil {
		conn.Close()
		return
	}

	sharedKey := a.identity.ComputeShared(peerPub)
	remoteAddr := conn.RemoteAddr().String()
	host, _, _ := net.SplitHostPort(remoteAddr)
	host = strings.Trim(host, "[]")
	// Use the chat port the peer advertised in its hello. The TCP source port
	// is ephemeral and would not match the peer's discoverable ID.
	port := hello.Port
	peerID := fmt.Sprintf("%s@%s:%d", hello.Name, host, port)

	peerConn := &PeerConn{
		ID:        peerID,
		Name:      hello.Name,
		Host:      host,
		Port:      port,
		PublicKey: peerPub,
		SharedKey: sharedKey,
		Conn:      conn,
	}

	// Send our hello back
	ourHello := WSMsg{
		Type:      "hello",
		Name:      a.identity.Name,
		PublicKey: crypto.HexEncode(a.identity.PublicKey[:]),
		Port:      a.cfg.Port,
	}
	if err := peerConn.SendJSON(ourHello); err != nil {
		conn.Close()
		return
	}

	// Register (deduplicates by public key)
	a.registerPeer(peerConn)

	// Save contact
	fp := crypto.Fingerprint(peerPub)
	contact := &storage.Contact{
		ID:          peerID,
		Name:        hello.Name,
		Host:        host,
		Port:        port,
		PublicKey:   hello.PublicKey,
		Fingerprint: fp,
		LastSeen:    time.Now().UnixMilli(),
	}
	if existing, _ := a.db.GetContact(peerID); existing != nil && existing.PublicKey != hello.PublicKey {
		log.Printf("[SECURITY] WARNING: key changed for %s!", peerID)
		peerConn.Verified = false
	} else if existing != nil {
		peerConn.Verified = existing.Verified
	}
	a.db.SaveContact(contact)

	// Notify frontend
	a.pushPeersToFrontend()

	log.Printf("[peer] connected: %s", peerID)
	a.readPeerMessages(peerConn)

	// Cleanup
	a.unregisterPeer(peerConn)
	a.pushPeersToFrontend()
}

// =============================================================================
// Read loop for peer messages
// =============================================================================

func (a *App) readPeerMessages(pc *PeerConn) {
	defer pc.Conn.Close()
	for {
		var msg WSMsg
		if err := pc.Conn.ReadJSON(&msg); err != nil {
			return
		}
		a.processPeerMsg(pc, &msg)
	}
}

func (a *App) processPeerMsg(pc *PeerConn, msg *WSMsg) {
	now := time.Now().UnixMilli()
	if msg.Ts == 0 {
		msg.Ts = now
	}

	switch msg.Type {
	case "text":
		decrypted, err := a.decryptPayload(pc.SharedKey, msg.Nonce, msg.Ciphertext)
		if err != nil {
			log.Printf("[decrypt] %v", err)
			return
		}
		dbMsg := &storage.Message{
			ID: msg.ID, PeerID: pc.ID, Direction: "received",
			MsgType: "text", Content: decrypted, Timestamp: msg.Ts,
		}
		a.db.SaveMessage(dbMsg)
		a.pushToFrontends("new_message", dbMsg)

	case "file_start":
		a.files.StartChunked(msg.ID, msg.FileName, msg.FileSize, msg.TotalChunks)

	case "file_chunk":
		chunk, err := a.decryptPayload(pc.SharedKey, msg.Nonce, msg.ChunkData)
		if err != nil {
			return
		}
		a.files.ReceiveChunk(msg.ID, msg.ChunkIndex, chunk)

	case "file_end":
		savedPath, err := a.files.FinishChunked(msg.ID)
		if err != nil {
			log.Printf("[file] finish: %v", err)
			return
		}
		fi, _ := os.Stat(savedPath)
		var size int64
		if fi != nil {
			size = fi.Size()
		}
		fileName := filepath.Base(savedPath)
		msgType := "file"
		if isImageExt(filepath.Ext(fileName)) {
			msgType = "image"
		}
		// Auto-unpack .tar.gz folder archives
		if strings.HasSuffix(strings.ToLower(fileName), ".tar.gz") {
			unpackedDir, err := a.files.UnpackFolder(savedPath)
			if err == nil {
				msgType = "folder"
				// Count files in the unpacked directory
				fc, ts := countDir(unpackedDir)
				fileName = filepath.Base(unpackedDir)
				size = ts
				// Replace content with folder metadata for correct bubble display
				dbMsg := &storage.Message{
					ID: msg.ID, PeerID: pc.ID, Direction: "received",
					MsgType: "folder",
					Content: []byte(fmt.Sprintf(`{"folderName":"%s","fileCount":%d,"totalSize":%d}`, fileName, fc, ts)),
					Timestamp: msg.Ts, FileName: fileName, FileSize: ts, FilePath: unpackedDir,
				}
				a.db.SaveMessage(dbMsg)
				a.pushToFrontends("new_message", dbMsg)
				os.Remove(savedPath) // clean up the .tar.gz
				log.Printf("[file] auto-unpacked folder: %s (%d files, %d bytes)", unpackedDir, fc, ts)
				return // skip the generic file message below
			}
		}
		dbMsg := &storage.Message{
			ID: msg.ID, PeerID: pc.ID, Direction: "received",
			MsgType: msgType, Content: []byte(fmt.Sprintf(`{"fileName":"%s","fileSize":%d}`, fileName, size)),
			Timestamp: msg.Ts, FileName: fileName, FileSize: size, FilePath: savedPath,
		}
		a.db.SaveMessage(dbMsg)
		a.pushToFrontends("new_message", dbMsg)
	}
}

func (a *App) decryptPayload(shared crypto.SharedKey, nonceHex, ctHex string) ([]byte, error) {
	nonceBytes, _ := crypto.HexDecode(nonceHex)
	ctBytes, _ := crypto.HexDecode(ctHex)
	if len(nonceBytes) < 24 {
		return nil, fmt.Errorf("bad nonce")
	}
	combined := make([]byte, 24+len(ctBytes))
	copy(combined[:24], nonceBytes[:24])
	copy(combined[24:], ctBytes)
	return crypto.Decrypt(shared, combined)
}

// =============================================================================
// Outgoing: send text
// =============================================================================

func (a *App) sendTextMessage(peerID, text string) error {
	a.peersMu.RLock()
	pc, ok := a.peers[peerID]
	a.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("peer not connected: %s", peerID)
	}

	ciphertext, err := crypto.Encrypt(pc.SharedKey, []byte(text))
	if err != nil {
		return err
	}

	nonce := ciphertext[:24]
	ct := ciphertext[24:]
	msgID := uuid.New().String()
	now := time.Now().UnixMilli()

	wsMsg := WSMsg{
		Type:       "text",
		ID:         msgID,
		Ts:         now,
		Ciphertext: crypto.HexEncode(ct),
		Nonce:      crypto.HexEncode(nonce),
	}

	if err := pc.SendJSON(wsMsg); err != nil {
		return err
	}

	dbMsg := &storage.Message{
		ID: msgID, PeerID: peerID, Direction: "sent",
		MsgType: "text", Content: []byte(text), Timestamp: now,
	}
	a.db.SaveMessage(dbMsg)
	a.pushToFrontends("new_message", dbMsg)
	return nil
}

// =============================================================================
// Outgoing: send file
// =============================================================================

func (a *App) sendFileToPeer(peerID, filePath string) error {
	a.peersMu.RLock()
	pc, ok := a.peers[peerID]
	a.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("peer not connected: %s", peerID)
	}

	// Resolve path: try as-is first, then relative to files base dir
	fullPath := filePath
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		fullPath = filepath.Join(a.files.BaseDir(), filePath)
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return err
	}

	fileName := filepath.Base(filePath)
	chunkSize := 64 * 1024
	totalChunks := (len(data) + chunkSize - 1) / chunkSize
	msgID := uuid.New().String()
	now := time.Now().UnixMilli()

	// file_start
	if err := pc.SendJSON(WSMsg{
		Type: "file_start", ID: msgID, Ts: now,
		FileName: fileName, FileSize: int64(len(data)),
		TotalChunks: totalChunks, MimeType: http.DetectContentType(data),
	}); err != nil {
		return err
	}

	// chunks
	for i := 0; i < totalChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		enc, err := crypto.Encrypt(pc.SharedKey, data[start:end])
		if err != nil {
			return err
		}
		if err := pc.SendJSON(WSMsg{
			Type: "file_chunk", ID: msgID, ChunkIndex: i,
			ChunkData: crypto.HexEncode(enc[24:]),
			Nonce:     crypto.HexEncode(enc[:24]),
		}); err != nil {
			return err
		}
	}

	// file_end
	if err := pc.SendJSON(WSMsg{Type: "file_end", ID: msgID}); err != nil {
		return err
	}

	msgType := "file"
	if isImageExt(filepath.Ext(fileName)) {
		msgType = "image"
	}
	dbMsg := &storage.Message{
		ID: msgID, PeerID: peerID, Direction: "sent", MsgType: msgType,
		Content: []byte(fmt.Sprintf(`{"fileName":"%s","fileSize":%d}`, fileName, len(data))),
		Timestamp: now, FileName: fileName, FileSize: int64(len(data)),
	}
	a.db.SaveMessage(dbMsg)
	a.pushToFrontends("new_message", dbMsg)
	log.Printf("[file] sent %s to %s (%d bytes)", fileName, pc.Name, len(data))
	return nil
}

// =============================================================================
// Connect to discovered peers
// =============================================================================

// packAndSendFolder packs the accumulated folder files into a .tar.gz and sends to peer.
func (a *App) packAndSendFolder(peerID, folderName string) {
	// Use a unique key: peer+folder to avoid collisions between concurrent uploads.
	key := peerID + "\x00" + folderName
	a.folderUploadsMu.Lock()
	fu, ok := a.folderUploads[key]
	if !ok {
		a.folderUploadsMu.Unlock()
		a.sendToFrontend(UIEvent{Type: "error", Data: "文件夹数据不完整，请重新发送"})
		return
	}
	delete(a.folderUploads, key)
	a.folderUploadsMu.Unlock()

	// Create temp dir and write files with folder structure
	tmpDir, err := os.MkdirTemp("", "ncfolder-*")
	if err != nil {
		a.sendToFrontend(UIEvent{Type: "error", Data: "创建临时目录失败"})
		return
	}
	defer os.RemoveAll(tmpDir)

	absTmp, _ := filepath.Abs(tmpDir)
	destDir := filepath.Join(tmpDir, sanitizeName(folderName))
	fileCount := 0
	var totalSize int64

	for relPath, data := range fu.files {
		// Sanitize each path segment to prevent traversal
		clean := sanitizeRelPath(relPath)
		dest := filepath.Join(destDir, filepath.FromSlash(clean))
		absDest, _ := filepath.Abs(dest)
		if !strings.HasPrefix(absDest, absTmp) {
			log.Printf("[folder] SKIP traversal attempt: %s", relPath)
			continue
		}
		os.MkdirAll(filepath.Dir(dest), 0755)
		os.WriteFile(dest, data, 0644)
		fileCount++
		totalSize += int64(len(data))
	}

	// Pack the folder
	archivePath, err := a.files.PackFolder(destDir)
	if err != nil {
		a.sendToFrontend(UIEvent{Type: "error", Data: "打包文件夹失败: " + err.Error()})
		return
	}
	defer os.Remove(archivePath) // clean up .tar.gz after sending

	// Send to peer
	log.Printf("[folder] sending %s (%d files, %d bytes) to %s", folderName, fileCount, totalSize, peerID)
	if err := a.sendFileToPeer(peerID, archivePath); err != nil {
		a.sendToFrontend(UIEvent{Type: "error", Data: "发送文件夹失败: " + err.Error()})
		return
	}
	a.sendToFrontend(UIEvent{Type: "mobile_message", Data: map[string]interface{}{
		"text": fmt.Sprintf("📁 %s (%d 个文件) 已发送", folderName, fileCount),
	}})
}

// sanitizeRelPath cleans a relative path, removing .. and leading slashes.
func sanitizeRelPath(p string) string {
	// Replace backslashes, remove leading slashes
	p = strings.TrimLeft(strings.ReplaceAll(p, "\\", "/"), "/")
	// Split and filter out ".." segments
	parts := strings.Split(p, "/")
	var clean []string
	for _, part := range parts {
		if part == ".." || part == "." || part == "" {
			continue
		}
		clean = append(clean, part)
	}
	return strings.Join(clean, "/")
}

// sanitizeName returns a safe filename (no path separators).
// countDir recursively counts files and total size in a directory.
func countDir(dir string) (fileCount int, totalSize int64) {
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		fileCount++
		totalSize += info.Size()
		return nil
	})
	return
}

func sanitizeName(name string) string {
	name = strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|', 0:
			return '_'
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." {
		return "folder"
	}
	return name
}

// discoveryPollLoop reads discovery events from channels (instant) and also
// polls every 30s as a safety net for when discovery is restarted.
func (a *App) discoveryPollLoop() {
	// Feed mDNS + broadcast events into a single channel
	// 启动时先取一次当前实例；改名会替换字段，但下面 ticker 里的轮询
	// 每次都重新取，所以改名后能自动跟上新实例。
	merged := make(chan discovery.Peer, 64)
	initD, initB := a.getDisc()
	go func() {
		for p := range initD.Peers() {
			merged <- p
		}
	}()
	go func() {
		for p := range initB.Peers() {
			merged <- p
		}
	}()

	seen := make(map[string]time.Time)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	dial := func(peer discovery.Peer) {
		if a.hasLivePeerByName(peer.Name) {
			return
		}
		retryKey := fmt.Sprintf("%s@%d", peer.Name, peer.Port)
		if last, ok := seen[retryKey]; ok && time.Since(last) < 30*time.Second {
			return
		}
		seen[retryKey] = time.Now()
		peerID := fmt.Sprintf("%s@%s:%d", peer.Name, peer.Host, peer.Port)
	go a.connectToPeer(peerID, peer.Name, peer.Host, peer.Port)
	}

	for {
		select {
		case peer := <-merged:
			dial(peer)
		case <-ticker.C:
			// 每轮都重新取实例：改名替换字段后，这里自然读到的是新实例
			curD, curB := a.getDisc()
			for _, p := range curD.ListPeers() {
				dial(p)
			}
			for _, p := range curB.ListPeers() {
				dial(p)
			}
		}
	}
}

// connectToPeer dials a discovered peer, trying each candidate address in
// order until one completes the WebSocket handshake. Discovery may advertise
// several IPs (physical + virtual adapters); the first can be unreachable, so
// we fall back to the others instead of giving up on the first failure.
func (a *App) connectToPeer(peerID, name, host string, port int) {
	// 【第 2 层 - P2P 的"客户端"身份】主动拨号另一台电脑。
	// 对方机器上跑着同样的程序、也监听着同样的端口，所以能接住这个连接。
	// 本程序既有这段"主动拨号"、又有前面的 handlePeerWS"被动接听"，合起来才是完整的 P2P。
	//
	// 【第 4 层 - 安全层缺失】注意这里是 ws:// 而不是 wss://，
	// 即 WebSocket 没有套上 TLS 加密壳，链路本身是明文的。
	// 本程序把安全性放到应用层自己做：连接建立后通过 ECDH 协商出 SharedKey，
	// 由业务代码负责加解密，而不依赖传输层的 HTTPS/TLS。
	// 代价是：若前端页面用 HTTPS 打开，浏览器会禁止去连 ws://（混合内容被拦截）。
	//
	// ⚠️ 历史 bug（本次修复）：原先写作 fmt.Sprintf("ws://%s:%d/ws", host, port)，
	// 当 host 是 IPv6（例如 2001:f90:... 或 fe80::...）时，地址里本身带冒号，
	// 拼出来会变成 ws://2001:f90:...:52056/ws，Go 分不清哪个冒号是端口分隔符，
	// 于是狂报 "dial tcp: address ...: too many colons in address"。
	// 正确写法是用 net.JoinHostPort —— 它会自动给 IPv6 加上方括号：
	//     [2001:f90:...]:52056
	// 这样即使将来真有 IPv6 对端，URL 也不会拼错。
	endpoint := net.JoinHostPort(host, strconv.Itoa(port))
	url := fmt.Sprintf("ws://%s/ws", endpoint)
	log.Printf("[app] dialing %s...", url)
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		log.Printf("[app] connect to %s failed: %v", url, err)
		a.sendToFrontend(UIEvent{Type: "error", Data: fmt.Sprintf("连接 %s 失败：%v", url, err)})
		return
	}

	// Hello
	hello := WSMsg{
		Type: "hello", Name: a.identity.Name,
		PublicKey: crypto.HexEncode(a.identity.PublicKey[:]),
		Port:      a.cfg.Port,
	}
	if err := conn.WriteJSON(hello); err != nil {
		conn.Close()
		a.sendToFrontend(UIEvent{Type: "error", Data: fmt.Sprintf("发送握手到 %s:%d 失败", host, port)})
		return
	}

	var resp WSMsg
	if err := conn.ReadJSON(&resp); err != nil || resp.Type != "hello" {
		log.Printf("[app] hello response from %s failed: err=%v type=%s", url, err, resp.Type)
		conn.Close()
		a.sendToFrontend(UIEvent{Type: "error", Data: fmt.Sprintf("%s:%d 握手响应异常（对方可能未启动或端口不对）", host, port)})
		return
	}

	peerPub, err := crypto.PubKeyFromHex(resp.PublicKey)
	if err != nil {
		log.Printf("[app] bad pubkey from %s: %v", url, err)
		conn.Close()
		a.sendToFrontend(UIEvent{Type: "error", Data: "对方公钥无效"})
		return
	}

	sharedKey := a.identity.ComputeShared(peerPub)
	pc := &PeerConn{
		ID: peerID, Name: name, Host: host, Port: port,
		PublicKey: peerPub, SharedKey: sharedKey, Conn: conn,
	}

	// Check contact history
	if contact, _ := a.db.GetContact(peerID); contact != nil {
		if contact.PublicKey != resp.PublicKey {
			log.Printf("[SECURITY] key changed for %s!", peerID)
			pc.Verified = false
		} else {
			pc.Verified = contact.Verified
		}
	}

	// Register
	a.registerPeer(pc)

	fp := crypto.Fingerprint(peerPub)
	a.db.SaveContact(&storage.Contact{
		ID: peerID, Name: name, Host: host, Port: port,
		PublicKey: resp.PublicKey, Fingerprint: fp,
		LastSeen: time.Now().UnixMilli(), Verified: pc.Verified,
	})

	log.Printf("[peer] connected: %s", peerID)
	a.pushPeersToFrontend()
	a.readPeerMessages(pc)

	a.unregisterPeer(pc)
	a.pushPeersToFrontend()
}

// =============================================================================
// Peer list (merge discovery + live connections)
// =============================================================================

// peerView is the JSON shape sent to the frontend for each known peer.
type peerView struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Connected bool   `json:"connected"`
	Verified  bool   `json:"verified"`
	Online    bool   `json:"online"`
}

// collectPeers returns every peer we know about: live P2P connections (which
// include manually-connected peers not present in any discovery list) plus
// discovered peers from mDNS and UDP broadcast.
func (a *App) collectPeers() []peerView {
	seenID := make(map[string]bool)
	result := make([]peerView, 0)

	// Collect names that are already live-connected — to avoid showing
	// their ghost IPs from discovery.
	connectedNames := make(map[string]bool)
	a.peersMu.RLock()
	for _, pc := range a.peers {
		connectedNames[pc.Name] = true
	}
	a.peersMu.RUnlock()

	// Live connections always shown (they're real P2P sessions with verified keys).
	a.peersMu.RLock()
	for _, pc := range a.peers {
		id := fmt.Sprintf("%s@%s:%d", pc.Name, pc.Host, pc.Port)
		if seenID[id] {
			continue
		}
		seenID[id] = true
		verified := false
		if c, _ := a.db.GetContact(id); c != nil {
			verified = c.Verified
		}
		result = append(result, peerView{
			Name: pc.Name, Host: pc.Host, Port: pc.Port,
			Connected: true, Verified: verified, Online: true,
		})
	}
	a.peersMu.RUnlock()

	// Discovered peers — skip if that name already has a live connection,
	// otherwise show (dedup by full ID only, NOT by name alone).
	add := func(name, host string, port int, online bool) {
		// If already connected to someone with this name, skip ghost IPs
		if connectedNames[name] {
			return
		}
		id := fmt.Sprintf("%s@%s:%d", name, host, port)
		if seenID[id] {
			return
		}
		seenID[id] = true
		a.peersMu.RLock()
		_, connected := a.peers[id]
		a.peersMu.RUnlock()
		verified := false
		if c, _ := a.db.GetContact(id); c != nil {
			verified = c.Verified
		}
		result = append(result, peerView{
			Name: name, Host: host, Port: port,
			Connected: connected, Verified: verified, Online: online,
		})
	}
	curD, curB := a.getDisc()
	for _, p := range curD.ListPeers() {
		add(p.Name, p.Host, p.Port, p.Online)
	}
	for _, p := range curB.ListPeers() {
		add(p.Name, p.Host, p.Port, p.Online)
	}
	return result
}

// =============================================================================
// Frontend event broadcasting
// =============================================================================

func (a *App) pushToFrontends(eventType string, data interface{}) {
	a.events <- UIEvent{Type: eventType, Data: data}
}

func (a *App) sendToFrontend(ev UIEvent) {
	a.events <- ev
}

func (a *App) broadcastEvents() {
	for ev := range a.events {
		a.frontendsMu.Lock()
		for fc := range a.frontends {
			go func(fc *FrontendConn, ev UIEvent) {
				fc.mu.Lock()
				defer fc.mu.Unlock()
				fc.Conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				fc.Conn.WriteJSON(ev)
			}(fc, ev)
		}
		a.frontendsMu.Unlock()
	}
}

func (a *App) pushPeersToFrontend() {
	a.pushToFrontends("peers", a.collectPeers())
}

// =============================================================================
// Helpers
// =============================================================================

func isImageExt(ext string) bool {
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg", ".ico":
		return true
	}
	return false
}

func getLocalIPs() []net.IP {
	return discovery.GetLocalIPs()
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if cmd != nil {
		cmd.Start()
	}
}

// =============================================================================
// Main
// =============================================================================

func main() {
	log.SetFlags(log.Ltime)

	name := "User"
	port := 0
	dataDir := "./data"

	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--name", "-n":
			if i+1 < len(os.Args) {
				i++
				name = os.Args[i]
			}
		case "--port", "-p":
			if i+1 < len(os.Args) {
				i++
				fmt.Sscanf(os.Args[i], "%d", &port)
			}
		case "--data", "-d":
			if i+1 < len(os.Args) {
				i++
				dataDir = os.Args[i]
			}
		case "--help", "-h":
			fmt.Println("Network Chat - 局域网聊天工具")
			fmt.Println("")
			fmt.Println("Usage: network_chat [options]")
			fmt.Println("  -n, --name <name>   Display name (default: User)")
			fmt.Println("  -p, --port <port>   Listen port (default: random)")
			fmt.Println("  -d, --data <dir>    Data directory (default: ./data)")
			fmt.Println("  -h, --help          Show this help")
			os.Exit(0)
		}
	}

	app, err := NewApp(Config{Name: name, Port: port, DataDir: dataDir})
	if err != nil {
		log.Fatalf("init: %v", err)
	}
	if err := app.Start(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("start: %v", err)
	}
}
