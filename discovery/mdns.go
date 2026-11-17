package discovery

// =============================================================================
// 本文件在网络五层模型中的位置
// =============================================================================
//
// mDNS（组播 DNS）属于【第 1 层 - 传输层】的 UDP 应用。
// 它和同包的 broadcast.go 一样，只负责"在局域网里找到邻居"，不传任何业务数据。
//
// 两者的差别仅仅是"喊话方式"不同：
//   · broadcast.go —— UDP 广播：往 255.255.255.255 这类广播地址发，端口 19877；
//   · mdns.go      —— UDP 组播：往 224.0.0.251 这个组播地址发，端口 5353。
//
// 为什么两种方式都要保留？
//   因为各自的可靠性依赖环境：
//     · UDP 广播在部分网络（尤其公共 WiFi、企业网）会被路由器直接丢掉；
//     · mDNS 在 Windows 上又常因系统缺少 Bonjour 服务而失效。
//   两条路并行，任意一条通了就能发现邻居 —— 这是典型的"冗余容错"设计。
//
// 【与第 2 层的关系】
//   发现只是 P2P 的"第一步：找到人"。
//   找到之后真正去建立连接，交回 main.go 的 connectToPeer 完成（那才是 P2P 的握手与通信）。
// =============================================================================

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/mdns"
)

// Peer represents a discovered peer on the local network.
type Peer struct {
	Name     string   `json:"name"`
	Host     string   `json:"host"`
	Port     int      `json:"port"`
	Addrs    []string `json:"addrs"`    // IP addresses
	LastSeen time.Time `json:"lastSeen"`
	Online   bool     `json:"online"`
}

// Service handles mDNS registration and discovery.
type Service struct {
	peerCh     chan Peer
	peers      map[string]*Peer // key: "name@host"
	peersMu    sync.RWMutex
	stopCh     chan struct{}
	stopOnce   sync.Once
	server     *mdns.Server
	// serverMu 保护 server 字段：它在 runRegistration 协程里被写入，
	// 又在 Stop()（跑在另一个协程，如处理改名请求的 HTTP 协程）里被读取，
	// 不加锁就构成数据竞争。
	serverMu sync.Mutex
	// readyCh 在注册协程"完成初始化"时被关闭（成功建好 server、或明确失败，都会关），
	// 让 Stop() 能等 server 就绪后再关闭它，避免漏关而残留幽灵服务。
	// readyOnce 保证 close 只发生一次，防止重复关闭同一个 channel 引发 panic。
	readyOnce sync.Once
	readyCh   chan struct{}

	ourName    string
	ourPort    int
	instanceID string // unique per process, avoids self-discovery
}

// New creates a new mDNS service.
func New(ourName string, ourPort int) *Service {
	hostname, _ := os.Hostname()
	return &Service{
		peerCh:     make(chan Peer, 32),
		peers:      make(map[string]*Peer),
		stopCh:     make(chan struct{}),
		readyCh:    make(chan struct{}),
		ourName:    ourName,
		ourPort:    ourPort,
		instanceID: fmt.Sprintf("%s-%s-%d", ourName, hostname, os.Getpid()),
	}
}

// Peers returns the channel of discovered peers.
func (s *Service) Peers() <-chan Peer {
	return s.peerCh
}

// ListPeers returns a snapshot of currently known peers.
func (s *Service) ListPeers() []Peer {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	result := make([]Peer, 0, len(s.peers))
	for _, p := range s.peers {
		result = append(result, *p)
	}
	return result
}

// Start begins both advertising our service and discovering peers.
func (s *Service) Start() error {
	go s.runDiscovery()
	go s.runRegistration()
	// Give it a moment to initialize
	time.Sleep(100 * time.Millisecond)
	return nil
}

// Stop shuts down the mDNS service.
func (s *Service) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)

		// 等注册协程完成初始化再动手，否则会漏关 server：
		// 若 Stop() 抢先跑，此时 s.server 还是 nil，就会直接跳过 Shutdown；
		// 随后 runRegistration 才把 server 赋上，可 stopCh 已关闭、它随即返回，
		// 于是这个 mDNS 服务永远留在后台继续广播旧名字（改名时表现为幽灵服务）。
		//
		// 这里必须带超时兜底：万一 Start() 从未被调用过，readyCh 永远不会关闭，
		// 无超时的话 Stop() 就会永久卡死 —— 宁可等 2 秒，也不能死锁。
		select {
		case <-s.readyCh:
		case <-time.After(2 * time.Second):
		}

		// 加锁取走 server 并置空，保证与 runRegistration 的写入互斥，
		// 且只会有这一个地方真正执行 Shutdown（置空后别人再取就是 nil）。
		s.serverMu.Lock()
		srv := s.server
		s.server = nil
		s.serverMu.Unlock()

		if srv != nil {
			srv.Shutdown()
		}
	})
}

// =============================================================================
// Registration (advertise ourselves)
// =============================================================================

// 【第 1 层 - UDP 组播：注册自己（对外宣告"我在这儿"）】
// 把本机信息打包成一条 mDNS 服务记录，周期性发往组播地址 224.0.0.251:5353。
// 局域网里其它跑着本程序的电脑收到后，就知道"有个叫这名字的家伙在 IP:端口 上"。
//
// 下面的 info 是自定义载荷，塞了三样东西，对端在 parseEntry 里按同样格式解析：
//   · instance —— 本进程唯一 ID，用来避免"自己发现自己"（收到相同 instance 就忽略）；
//   · port     —— 我们的聊天端口；
//   · addrs    —— 我们可用的局域网 IP 列表。
//
// ✅ 已修复（原隐患：数据竞争 + 幽灵服务残留）
// 原先本函数在自己的 goroutine 里给 s.server 赋值，而 Stop() 在另一个协程读取它，
// 既没加锁（数据竞争），又存在漏关风险 —— 若 Stop() 抢先执行，s.server 仍是 nil，
// 就会跳过 Shutdown，导致该服务永远留在后台广播旧名字（改名时表现为幽灵设备）。
//
// 现在的做法：
//   1. s.server 的读写统一放到 serverMu 保护之下，消除数据竞争；
//   2. 用 readyCh 让 Stop() 等到 server 就绪后再关闭，
//      并配 2 秒超时，防止 Start() 从未被调用时 Stop() 永久卡死；
//   3. 本函数结尾再兜底关一次（Stop 已取走时这里拿到 nil，不会重复关闭）。
func (s *Service) runRegistration() {
	hostname, _ := os.Hostname()
	addrs := getLocalIPs()
	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}

	info := fmt.Sprintf("instance=%s|port=%d|addrs=%s",
		s.instanceID, s.ourPort, strings.Join(addrStrs, ","))

	service, err := mdns.NewMDNSService(
		s.ourName,
		"_networkchat._tcp",
		"",
		hostname+".",
		s.ourPort,
		nil,
		[]string{info},
	)
	if err != nil {
		log.Printf("[mDNS] create service: %v", err)
		// 失败也要通知 Stop()，否则它会一直等 readyCh，白等到超时才返回
		s.readyOnce.Do(func() { close(s.readyCh) })
		return
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		log.Printf("[mDNS] create server: %v", err)
		s.readyOnce.Do(func() { close(s.readyCh) })
		return
	}

	// 加锁写入 server，与 Stop() 里的读取配对，消除数据竞争
	s.serverMu.Lock()
	s.server = server
	s.serverMu.Unlock()
	// 通知 Stop()：server 已就绪，可以安全关闭了
	s.readyOnce.Do(func() { close(s.readyCh) })
	log.Printf("[mDNS] advertising as %q on port %d (IPs: %s)", s.ourName, s.ourPort, strings.Join(addrStrs, ", "))

	// 阻塞在这里，直到 Stop() 关闭 stopCh
	<-s.stopCh

	// 兜底收尾：正常情况下 Stop() 已经取走并关闭了 server（取走时已置 nil），
	// 这里再取只会拿到 nil，不会重复 Shutdown；
	// 万一 Stop() 因超时没拿到，这里也能补上，确保绝不残留幽灵服务。
	s.serverMu.Lock()
	srv := s.server
	s.server = nil
	s.serverMu.Unlock()
	if srv != nil {
		srv.Shutdown()
	}
}

// =============================================================================
// Discovery (find peers)
// =============================================================================

// 【第 1 层 - UDP 组播：发现别人（主动问"谁在"）】
// 反复向组播地址查询 _networkchat._tcp 这个服务名，收集应答里的服务记录，
// 解析成 Peer 交给 addOrUpdatePeer，再由 peerCh 送给 main.go 去拨号连接。
//
// 节奏是：查一轮（5 秒超时）→ 休息 3 秒 → 再查，构成持续的发现循环。
// 之所以要循环而不是查一次，是为了能发现"后上线的"对端。
func (s *Service) runDiscovery() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		entriesCh := make(chan *mdns.ServiceEntry, 32)
		go func() {
			// This blocks until there's a result or we stop
			params := &mdns.QueryParam{
				Service: "_networkchat._tcp",
				Domain:  "local",
				Timeout: 5 * time.Second,
				Entries: entriesCh,
			}
			if err := mdns.Query(params); err != nil {
				log.Printf("[mDNS] query error: %v", err)
			}
			close(entriesCh)
		}()

		// Collect results
		for entry := range entriesCh {
			peer := s.parseEntry(entry)
			if peer == nil {
				continue // self
			}
			s.addOrUpdatePeer(peer)
		}

		// Wait before next scan
		select {
		case <-s.stopCh:
			return
		case <-time.After(3 * time.Second):
		}
	}
}

// parseEntry 把一条 mDNS 查询结果解析成 Peer；凡是"不可能连上"的记录一律返回 nil。
//
// ⚠️ 历史 bug（本次修复）：原先这里"来者不拒"——不管对方是不是本程序、
// 也不管地址是 IPv4 还是 IPv6，抓到就直接当对端收下。结果局域网里
// Meta Quest 头显等设备广播的 _oculusal_sp 服务、以及只带 IPv6 地址
// （2001:... 公网 / fe80::... 链路本地）的记录，全被塞进了联系人列表，
// 表现为"能发现、但每 30 秒重试一次连接、界面上永远刷红字"：
//
//     dial tcp: address 2001:f90:...:52056: too many colons in address
//
// 根因有二：
//   1. mDNS 库把解析结果分别放进 entry.AddrV4 与 entry.AddrV6，
//      而旧代码读的是已废弃的 entry.Addr（库会优先填 IPv6），于是拿到 IPv6；
//   2. connectToPeer 拼 ws:// 时没给 IPv6 加方括号，Go 分不清哪个冒号是端口。
//
// 现在这里加三道关卡，从源头把假记录挡在外面（第 2 个原因在 main.go 修复）：
//   ① 必须带本程序的自定义载荷（instance/addrs），别的设备不会带 → 天然筛掉；
//   ② 地址只取 IPv4 且必须可路由，IPv6 / 链路本地 / 回环一律丢弃；
//   ③ 没有可用地址、或端口为 0 的记录丢弃。
func (s *Service) parseEntry(entry *mdns.ServiceEntry) *Peer {
	var instanceID string
	var port int
	var addrStrs []string

	for _, field := range entry.InfoFields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 {
			continue
		}
		switch parts[0] {
		case "instance":
			instanceID = parts[1]
		case "port":
			fmt.Sscanf(parts[1], "%d", &port)
		case "addrs":
			addrStrs = strings.Split(parts[1], ",")
		}
	}

	// 【关卡 ①】必须有本程序的自定义载荷（instance / addrs）。
	// 这条判断等于"只认自己的服务"：别的设备（例如 Meta Quest 的
	// _oculusal_sp）虽然也会响应 mDNS，但它们不会带我们这套 payload，
	// 于是 instanceID 为空 —— 直接丢弃，绝不让它进联系人列表。
	// （mdns.ServiceEntry 结构体本身没有可用的服务名字段，
	//   所以改用"是否带我们的自定义字段"来甄别，更可靠。）
	if instanceID == "" {
		return nil
	}

	// 自己广播的记录，靠 instanceID 排除，否则会"发现自己"
	if instanceID == s.instanceID {
		return nil
	}

	name := entry.Name
	if name == "" {
		name = entry.Host
	}

	// 【关卡 ④】裁掉 mDNS 服务后缀，避免同一个对端显示两遍。
	// mDNS 返回的实例名形如 "User._networkchat._tcp.local."，
	// 而 UDP 广播发现给出的名字是干净的 "User" —— 两者对不上，去重就会失效，
	// 于是同一个对端在联系人列表里出现两条（一条"已连接"、一条"在线"）。
	// 这里把服务后缀切掉，让它回归 "User"，与广播那条自然合并。
	if idx := strings.Index(name, "._"); idx > 0 {
		name = name[:idx]
	}
	name = strings.TrimSuffix(name, ".")

	// 收集候选地址：优先用我们自己载荷里的 addrs（注册时只放 IPv4），
	// 只有在载荷缺失时才退回 mDNS 库解析出的地址 —— 且**只取 AddrV4**。
	//
	// ⚠️ 这是"假连接"的根源所在：
	// mDNS 库会把解析结果分别放进 AddrV4 和 AddrV6 两个字段，
	// 旧代码用的是已废弃的 entry.Addr（库优先填 IPv6），
	// 于是 2001:f90:...（公网 IPv6）和 fe80::...（链路本地）被当成了对端地址，
	// 拨号时直接报 "too many colons in address"。
	// 现在只认 AddrV4，IPv6 一律不进候选列表。
	var raw []string
	if len(addrStrs) > 0 {
		raw = addrStrs
	} else if entry.AddrV4 != nil {
		raw = []string{entry.AddrV4.String()}
	} else {
		// 只有 IPv6、没有 IPv4 → 本程序的对端连接只支持 IPv4，直接丢弃
		return nil
	}

	// 【关卡 ②】只保留"可路由的 IPv4"，把 IPv6 / 链路本地 / 回环全部剔除。
	var goodAddrs []string
	seen := map[string]bool{}
	for _, a := range raw {
		a = strings.TrimSpace(a)
		ip := net.ParseIP(a)
		if ip == nil {
			continue // 不是合法 IP
		}
		v4 := ip.To4()
		if v4 == nil {
			continue // 是 IPv6，丢弃
		}
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue // 127.0.0.1 / 169.254.x.x
		}
		s4 := v4.String()
		if seen[s4] {
			continue // 去重
		}
		seen[s4] = true
		goodAddrs = append(goodAddrs, s4)
	}

	// 【关卡 ③】筛选后一个可用地址都没有 → 这条记录毫无意义，丢弃。
	if len(goodAddrs) == 0 {
		return nil
	}

	if port == 0 {
		port = entry.Port
	}
	if port == 0 {
		return nil // 端口都没有，无法连接
	}

	return &Peer{
		Name:     name,
		Host:     goodAddrs[0],
		Port:     port,
		Addrs:    goodAddrs,
		LastSeen: time.Now(),
		Online:   true,
	}
}

// 记录（或刷新）一个发现到的对端。
//   首次出现 → 存入 peers 表，并推给 peerCh，让 main.go 拿去拨号连接；
//   已存在   → 只刷新 LastSeen 时间戳和 Online 标记，不再重复推送。
//
// ⚠️ 已知隐患（对应 bug 清单第 9 条）：条目只增不减、且从不标记离线。
// 一旦某台电脑下线，它的记录会永远留在 s.peers 里，在 UI 上就是个永不消失的"幽灵在线"。
// 合理做法是定期扫描，把 LastSeen 超过一定时长（例如 30 秒）的条目标记为离线或清除。
func (s *Service) addOrUpdatePeer(peer *Peer) {
	key := fmt.Sprintf("%s@%s:%d", peer.Name, peer.Host, peer.Port)
	s.peersMu.Lock()
	existing, ok := s.peers[key]
	if !ok {
		s.peers[key] = peer
		s.peersMu.Unlock()
		log.Printf("[mDNS] found peer: %s at %s:%d", peer.Name, peer.Host, peer.Port)
		select {
		case s.peerCh <- *peer:
		default:
		}
		return
	}
	existing.LastSeen = time.Now()
	existing.Online = true
	s.peersMu.Unlock()
}

// =============================================================================
// 辅助函数：本机局域网 IP 地址获取
// =============================================================================
//
// 这组函数要解决的核心问题：一台电脑上往往同时存在很多网卡（物理网卡、Hyper-V 虚拟
// 网卡、WSL 网卡、Docker 网卡、VMware 网卡、VPN 网卡、蓝牙网卡……），其中绝大多数 IP
// 局域网里的另一台电脑根本访问不到。一旦把这些 IP 通过 mDNS / UDP 广播通告出去，
// 或写进二维码，对端就会拿着一个连不通的地址去拨号。
//
// 表现出来的现象就是：能看到对端（发现阶段正常），但点连接一直失败/超时；
// 手机扫码后连不上。
//
// 因此这里用「三道过滤 + 一道去重」保证只返回真正可被对端访问的局域网 IPv4：
//   1. outboundIP()          —— 优先取系统出网使用的那块网卡的 IP（最可靠）
//   2. isVirtualInterface()  —— 按网卡名跳过虚拟网卡
//   3. IsLinkLocalUnicast()  —— 排除 169.254.x.x 这类链路本地地址
//   4. seen 去重             —— 同一个 IP 只收录一次
//
// ⚠️ 历史 bug（2026/7/19 引入，本次修复）：
// 之前重写 getLocalIPs() 时，上面三道过滤被整体丢掉了，只剩下「跳过回环 + 只取 IPv4」。
// 结果 isVirtualInterface() 和 outboundIP() 变成没有任何调用点的死代码，
// 169.254.x.x 与各类虚拟网卡 IP 被直接广播给对端，导致"能发现但连不上"。
// 本次修复把三道过滤全部恢复。

// GetLocalIPs 对外暴露的入口，返回本机的局域网 IPv4 地址列表。
// 返回结果按「越可能可用越靠前」排序，调用方直接取 ips[0] 通常就是最佳地址。
//
// 调用点遍布全局，任何一处拿到错误 IP 都会引发连接问题：
//   - main.go                  启动横幅、/api/me、二维码
//   - discovery/mdns.go        mDNS 对外注册的地址
//   - discovery/broadcast.go   UDP 广播对外通告的地址
func GetLocalIPs() []net.IP {
	return getLocalIPs()
}

// isVirtualInterface 判断网卡名字是否属于虚拟/非物理网卡。
// 判断依据是网卡名中是否包含特定关键字（不区分大小写），命中任意一个即视为虚拟网卡。
//
// 这里刻意用「包含匹配」而不是「精确匹配」：各家虚拟化方案命名并不统一，
// 例如 Hyper-V 的网卡常叫 "vEthernet (Default Switch)"、
// VMware 叫 "VMware Network Adapter VMnet1"，
// 只有包含匹配才能把它们一并覆盖。
func isVirtualInterface(name string) bool {
	lower := strings.ToLower(name)
	// 覆盖：Hyper-V、WSL、Docker、VMware、VirtualBox、VPN 隧道、蓝牙、Teredo、ISATAP、6to4
	skip := []string{
		"vethernet", "hyper-v", "virtual", "vmware", "vmnet",
		"virtualbox", "vbox", "docker", "wsl", "bluetooth",
		"teredo", "isatap", "6to4", "tunnel",
	}
	for _, s := range skip {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}

// outboundIP 返回「本机访问公网时实际使用的那个」IPv4 地址，
// 也就是系统路由表选中的出口网卡的 IP。这是挑选真实局域网 IP 最可靠的方式：
//
//   1. 不依赖网卡名字判断 —— 即使 Hyper-V / WSL 那块 vEthernet 网卡真的在承载流量，
//      只要它是出网口，就不会被误判成"虚拟网卡"而丢掉；
//   2. 天然排除没有默认路由的网卡 —— 链路本地地址(169.254.x.x)和仅在容器内部互通的
//      网卡都不会成为出网口，因此自动被过滤掉；
//   3. 一台机器可能同时插着网线和连着 Wi-Fi，路由表选中的那个通常就是"真正在用"的。
//
// 实现原理：对一个公网地址做 UDP "连接"。注意这里**不会真的发出任何数据包**，
// UDP 本身是无连接的，Dial 只是让操作系统查一次路由表、为这个 socket 绑定一个本地
// 地址，我们随后读 LocalAddr() 拿到的就是出网 IP。
// 因此该调用很快，也不依赖外网是否真的可达。
//
// 失败时（例如机器完全没有网络）返回 nil，调用方会回退到遍历网卡的方式。
func outboundIP() net.IP {
	// 8.8.8.8:80 只是用来触发路由表查询的"靶子"，不会被真正访问，也不会发包
	conn, err := net.DialTimeout("udp", "8.8.8.8:80", 1*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()
	// LocalAddr 即系统为本次出网选中的本机地址
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		if ip := addr.IP.To4(); ip != nil {
			return ip
		}
	}
	return nil
}

// getLocalIPs 收集本机所有「可用于局域网通信」的 IPv4 地址，并按可用优先级排序：
// 出网 IP 排在第一位，其余物理网卡的 IP 随后。
//
// 每个地址都要经过以下关卡才会被收录：
//   - 网卡处于启用状态(FlagUp)，且不是回环网卡(lo / 127.0.0.1)
//   - 网卡名字未命中虚拟网卡关键字
//   - 是 IPv4 地址（本项目只支持 IPv4 局域网，To4() 对 IPv6 返回 nil）
//   - 不是回环地址，也不是链路本地地址 169.254.x.x
//   - 同一个 IP 不重复收录
func getLocalIPs() []net.IP {
	seen := make(map[string]bool) // 已收录的 IP 字符串，用于去重
	var ips []net.IP

	// 第一步：优先放入出网 IP。它是最可能真正连通的那个，
	// 保证调用方取 ips[0] 时就能拿到最佳地址。
	if out := outboundIP(); out != nil {
		ips = append(ips, out)
		seen[out.String()] = true
	}

	// 第二步：遍历其余网卡，补充其它可用的局域网 IP
	// （例如机器同时插着网线和连着 Wi-Fi 的情况）。
	ifaces, err := net.Interfaces()
	if err != nil {
		return ips
	}
	for _, iface := range ifaces {
		// 跳过未启用的网卡与回环网卡
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		// 跳过虚拟网卡：Hyper-V / WSL / Docker / VMware / VirtualBox / VPN / 蓝牙等
		if isVirtualInterface(iface.Name) {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			// 网卡地址可能是 *net.IPNet（带掩码）或 *net.IPAddr（不带掩码），分别取值
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			// 排除空值、回环地址(127.0.0.1)以及链路本地地址(169.254.x.x)
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			// 只要 IPv4：To4() 遇到 IPv6 地址会返回 nil
			v4 := ip.To4()
			if v4 == nil {
				continue
			}
			// 去重（例如出网 IP 已在第一步收录，这里就不会再加一次）
			if seen[v4.String()] {
				continue
			}
			seen[v4.String()] = true
			ips = append(ips, v4)
		}
	}
	return ips
}
