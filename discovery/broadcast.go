package discovery

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// BroadcastDiscovery uses simple UDP broadcast for peer discovery.
// Much more reliable than mDNS on Windows.
type BroadcastDiscovery struct {
	peerCh     chan Peer
	peers      map[string]*Peer
	peersMu    sync.RWMutex
	stopCh     chan struct{}
	ourName    string
	ourPort    int
	instanceID string

	broadcastPort int
}

func NewBroadcast(ourName string, ourPort int) *BroadcastDiscovery {
	return &BroadcastDiscovery{
		peerCh:        make(chan Peer, 64),
		peers:         make(map[string]*Peer),
		stopCh:        make(chan struct{}),
		ourName:       ourName,
		ourPort:       ourPort,
		instanceID:    fmt.Sprintf("%s-%d-%d", ourName, ourPort, time.Now().UnixNano()),
		broadcastPort: 19877,
	}
}

func (b *BroadcastDiscovery) Peers() <-chan Peer { return b.peerCh }

func (b *BroadcastDiscovery) ListPeers() []Peer {
	b.peersMu.RLock()
	defer b.peersMu.RUnlock()
	result := make([]Peer, 0, len(b.peers))
	for _, p := range b.peers {
		result = append(result, *p)
	}
	return result
}

func (b *BroadcastDiscovery) Start() error {
	// Ensure we can listen before starting broadcast (avoids data race on broadcastPort).
	b.tryListen()
	go b.broadcastLoop()
	go b.listenLoop()
	log.Printf("[udp] broadcast discovery started (port %d)", b.broadcastPort)
	return nil
}

// 【第 1 层 - 传输层：UDP（极速平邮）】
// 整个程序里 UDP 只负责一件事：在局域网里"喊话找邻居"。
// 聊天文本、文件、文件夹这些真正的业务数据，一律走 TCP，UDP 一个字节都不传。
// 选 UDP 是因为"发现"只需要往广播地址丢一个小包，对方收没收到无所谓，
// 不需要 TCP 那套三次握手与可靠送达，开销更低也更简单。
func (b *BroadcastDiscovery) tryListen() {
	addr := &net.UDPAddr{Port: b.broadcastPort}
	// 默认广播端口 19877；若被占用则顺延尝试 19878（见下方回退逻辑）
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		addr.Port = 19878
		conn, err = net.ListenUDP("udp4", addr)
		if err != nil {
			log.Printf("[udp] cannot listen on discovery ports: %v", err)
			return
		}
		b.broadcastPort = addr.Port
		log.Printf("[udp] using alternate port %d", b.broadcastPort)
	}
	conn.Close() // Close test listen; real listen happens in listenLoop
}

func (b *BroadcastDiscovery) Stop() {
	close(b.stopCh)
}

// =============================================================================
// Send — use both subnet-directed and 255.255.255.255 broadcast
// =============================================================================

func (b *BroadcastDiscovery) broadcastLoop() {
	addrs := getLocalIPs()
	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}

	// Collect all broadcast targets
	var targets []string
	targets = append(targets, "255.255.255.255") // limited broadcast

	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		ifaceAddrs, _ := iface.Addrs()
		for _, addr := range ifaceAddrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP.To4() == nil || ipNet.IP.IsLoopback() {
				continue
			}
			// Calculate subnet broadcast
			ip := ipNet.IP.To4()
			mask := ipNet.Mask
			bcast := make(net.IP, 4)
			for i := 0; i < 4; i++ {
				bcast[i] = ip[i] | ^mask[i]
			}
			targets = append(targets, bcast.String())
		}
	}

	// Deduplicate targets
	seen := make(map[string]bool)
	var unique []string
	for _, t := range targets {
		if !seen[t] {
			seen[t] = true
			unique = append(unique, t)
		}
	}

	msg := fmt.Sprintf("%s|%s|%d|%s", b.instanceID, b.ourName, b.ourPort, strings.Join(addrStrs, ","))

	for {
		select {
		case <-b.stopCh:
			return
		case <-time.After(2 * time.Second):
		}

		for _, target := range unique {
			b.sendOne(target, msg)
		}
	}
}

func (b *BroadcastDiscovery) sendOne(target, msg string) {
	dst := &net.UDPAddr{IP: net.ParseIP(target), Port: b.broadcastPort}

	// Prefer binding to our best LAN IP so the source isn't 172.x Docker/WSL.
	bestIP := bestLocalIP()
	var conn *net.UDPConn
	var err error
	if bestIP != nil {
		conn, err = net.ListenUDP("udp4", &net.UDPAddr{IP: bestIP, Port: 0})
	}
	if conn == nil {
		conn, err = net.ListenUDP("udp4", nil) // fallback: let OS pick
		if err != nil {
			return
		}
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	if _, err := conn.WriteToUDP([]byte(msg), dst); err != nil {
		log.Printf("[udp] send to %s:%d failed: %v", target, b.broadcastPort, err)
	}
}

// bestLocalIP returns the top-ranked LAN IPv4 address (192.168 > 10.x > other > 172.x).
func bestLocalIP() net.IP {
	ips := getLocalIPs()
	if len(ips) > 0 {
		return ips[0]
	}
	return nil
}

// =============================================================================
// Listen
// =============================================================================

func (b *BroadcastDiscovery) listenLoop() {
	addr := &net.UDPAddr{Port: b.broadcastPort}
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Printf("[udp] listen on :%d failed: %v", b.broadcastPort, err)
		return
	}
	defer conn.Close()

	// Set receive buffer
	conn.SetReadBuffer(65536)

	buf := make([]byte, 2048)
	log.Printf("[udp] listening on :%d for broadcasts", b.broadcastPort)

	for {
		select {
		case <-b.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if strings.Contains(err.Error(), "closed") || strings.Contains(err.Error(), "use of closed") {
				return
			}
			log.Printf("[udp] read error: %v", err)
			continue
		}

		b.processPacket(string(buf[:n]), remote.IP)
	}
}

func (b *BroadcastDiscovery) processPacket(data string, remoteIP net.IP) {
	parts := strings.SplitN(data, "|", 4)
	if len(parts) < 4 {
		return
	}
	instanceID := parts[0]
	name := parts[1]
	port := 0
	fmt.Sscanf(parts[2], "%d", &port)
	addrStrs := strings.Split(parts[3], ",")

	// Skip self
	if instanceID == b.instanceID {
		return
	}

	// Use the FIRST IP from the packet's address list (sorted best-first by
	// getLocalIPs), not remoteIP. remoteIP can be 172.x from Docker/WSL
	// virtual adapters that inject themselves into the routing table.
	host := remoteIP.String()
	for _, a := range addrStrs {
		if ip := net.ParseIP(a); ip != nil && ip.To4() != nil && !ip.IsLinkLocalUnicast() {
			host = a
			break
		}
	}

	peer := &Peer{
		Name:     name,
		Host:     host,
		Port:     port,
		Addrs:    addrStrs,
		LastSeen: time.Now(),
		Online:   true,
	}

	key := fmt.Sprintf("%s@%s:%d", name, host, port)
	b.peersMu.Lock()
	if _, exists := b.peers[key]; !exists {
		b.peers[key] = peer
		b.peersMu.Unlock()
		log.Printf("[udp] FOUND peer: %s at %s:%d", name, host, port)
		select {
		case b.peerCh <- *peer:
		default:
		}
	} else {
		b.peers[key].LastSeen = time.Now()
		b.peersMu.Unlock()
	}
}
