// =============================================================================
// Network Chat - Frontend Application
// =============================================================================

// 【第 5 层 - 前端状态管理：这里同样不用 Cookie】
// 后端靠 WebSocket 长连接维持状态（详见 main.go 中 App 结构的注释），
// 前端则用一个全局 STATE 对象充当"唯一数据源"：
// 页面上的一切（联系人列表、聊天记录、当前会话、待发文件）都从这里读，
// 数据一变就重新渲染对应的 DOM。
//
// 为什么前端也不需要 Cookie？
//   因为浏览器与本机后端之间同样是一条 WebSocket 长连接（见下方 setupWebSocket），
//   而且这个后端只服务于本机的这一个浏览器，压根不需要 Cookie 去辨认"你是谁"。
//   真正的持久化放在后端 SQLite 里，前端只保留本次会话的临时状态。
const STATE = {
  me: null,
  peers: [],
  contacts: [],
  devices: [],      // [{id, name}]
  activePeerId: null,
  activeDeviceId: null,
  messages: {},    // peerId → [msg, ...]
  deviceMsgs: {},  // deviceId → [msg, ...]
  pendingFiles: [], // File objects to send
  ws: null,
};

// =============================================================================
// Init
// =============================================================================

async function init() {
  await loadMyInfo();
  await loadContacts();
  setupWebSocket();
  setupEventListeners();
  pollPeers();
}

async function loadMyInfo() {
  try {
    const r = await fetch('/api/me');
    STATE.me = await r.json();
    renderMyInfo();
    // Show our IP:port for others to connect
    const addrEl = document.getElementById('my-address');
    if (addrEl && STATE.me.ips && STATE.me.ips.length > 0) {
      addrEl.innerHTML = '📍 ' + STATE.me.ips.map(ip => `${ip}:${STATE.me.port}`).join('<br>📍 ');
    }
  } catch (e) {
    console.error(e);
  }
}

function renderMyInfo() {
  document.getElementById('my-name').textContent = STATE.me.name;
  document.getElementById('my-fingerprint').textContent = STATE.me.shortFingerprint;
  document.getElementById('my-avatar').textContent = (STATE.me.name || '?')[0].toUpperCase();
  document.title = `${STATE.me.name} - Network Chat`;
}

async function loadContacts() {
  try {
    const r = await fetch('/api/contacts');
    STATE.contacts = await r.json();
  } catch (e) {
    console.error(e);
  }
}

// =============================================================================
// WebSocket to backend
// =============================================================================

// 【第 2 层 - C/S 架构：浏览器(Client) 连 本机后端(Server)】
// 浏览器只连自己这一台电脑上的后端，不会去连别的电脑 —— 这是标准的 C/S 关系。
// （电脑与电脑之间的 P2P 由后端 Go 代码完成，前端不参与，对前端而言后端就是唯一的服务方。）
//
// 【第 3 层 - HTTP 升级为 WebSocket】
// WebSocket 的握手用的还是 HTTP：浏览器先发一个带 Upgrade 头的 HTTP 请求，
// 后端 upgrader.Upgrade() 同意后返回 101 状态码；
// 此后这条 TCP 连接就变成双向长连接，脱离 HTTP 的"一问一答"模式。
//
// 注意 proto 的判断：页面走 https 就用 wss，走 http 就用 ws。
// 本项目后端是明文 HTTP，所以实际走的是 ws://（对应第 4 层未启用 TLS）。
function setupWebSocket() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${proto}//${location.host}/ws?role=frontend`;

  function connect() {
    const ws = new WebSocket(url);
    STATE.ws = ws;

    ws.onopen = () => {
      document.getElementById('status-text').textContent = '已连接';
      pollPeers();
    };

    ws.onmessage = (e) => {
      try {
        handleEvent(JSON.parse(e.data));
      } catch (err) {
        console.error(err);
      }
    };

    ws.onclose = () => {
      document.getElementById('status-text').textContent = '重连中...';
      setTimeout(connect, 2000);
    };
  }

  connect();
}

function handleEvent(ev) {
  switch (ev.type) {
    case 'peers':
      STATE.peers = ev.data;
      renderPeerList();
      break;
    case 'devices':
      STATE.devices = ev.data;
      renderDeviceList();
      break;
    case 'new_message':
      onNewMessage(ev.data);
      break;
    case 'mobile_message':
      onMobileMessage(ev.data);
      break;
    case 'mobile_image':
      onMobileFile(ev.data, 'image');
      break;
    case 'mobile_file':
      onMobileFile(ev.data, 'file');
      break;
    case 'error':
      toast(ev.data, 'error');
      break;
  }
}

function sendCmd(obj) {
  if (STATE.ws && STATE.ws.readyState === WebSocket.OPEN) {
    STATE.ws.send(JSON.stringify(obj));
  }
}

// =============================================================================
// Peer list
// =============================================================================

function renderPeerList() {
  const list = document.getElementById('peer-list');
  const search = (document.getElementById('search-input')?.value || '').toLowerCase();
  let peers = STATE.peers.filter(p => !search || p.name.toLowerCase().includes(search));

  if (!peers.length) {
    list.innerHTML = `<div style="padding:40px;text-align:center;color:#999;">
      <div style="font-size:48px;margin-bottom:12px;">🔍</div>
      <div>${search ? '没有匹配的联系人' : '搜索局域网内的同事...'}</div>
      <div style="font-size:13px;margin-top:4px;">同一网络自动发现</div>
    </div>`;
    return;
  }

  list.innerHTML = peers.map(p => {
    const id = pid(p);
    const active = STATE.activePeerId === id;
    const dotCls = p.connected ? 'online' : (p.online ? 'online' : 'offline');
    const status = p.connected ? '已连接' : (p.online ? '在线' : '离线');
    return `
      <div class="peer-item ${active ? 'active' : ''}" data-peer-id="${esc(id)}">
        <div class="avatar">${(p.name||'?')[0].toUpperCase()}</div>
        <div>
          <div class="peer-name">${esc(p.name)}${p.verified ? ' ✅' : ''}</div>
          <div class="peer-status"><span class="status-dot ${dotCls}"></span> ${status}</div>
        </div>
        <div class="peer-meta"><div>${esc(p.host)}</div></div>
      </div>`;
  }).join('');

  list.querySelectorAll('.peer-item').forEach(el => {
    el.addEventListener('click', () => selectPeer(el.dataset.peerId));
  });
}

function pid(p) { return `${p.name}@${p.host}:${p.port}`; }

function findPeer(peerId) {
  return STATE.peers.find(p => pid(p) === peerId);
}

// =============================================================================
// Mobile devices
// =============================================================================

function renderDeviceList() {
  const list = document.getElementById('device-list');
  if (!STATE.devices || STATE.devices.length === 0) {
    list.innerHTML = '<div style="padding:4px 16px;font-size:12px;color:#bbb;">暂无连接设备</div>';
    return;
  }
  list.innerHTML = STATE.devices.map(d => {
    const active = STATE.activeDeviceId === d.id;
    return `<div class="peer-item ${active ? 'active' : ''}" data-device-id="${esc(d.id)}">
      <div class="avatar" style="background:#52c41a;">📱</div>
      <div>
        <div class="peer-name">${esc(d.name)}</div>
        <div class="peer-status"><span class="status-dot online"></span> 已连接</div>
      </div>
    </div>`;
  }).join('');

  list.querySelectorAll('.peer-item').forEach(el => {
    el.addEventListener('click', () => selectDevice(el.dataset.deviceId));
  });
}

function selectDevice(deviceId) {
  STATE.activeDeviceId = deviceId;
  STATE.activePeerId = null;
  document.getElementById('chat-empty').className = 'chat-empty-hide';
  document.getElementById('chat-messages').style.display = 'block';
  const d = STATE.devices.find(d => d.id === deviceId);
  document.getElementById('chat-peer-name').textContent = d?.name || deviceId;
  document.getElementById('chat-peer-status').textContent = '📱 本机直连';
  renderDeviceMessages(deviceId);
  renderPeerList();
  renderDeviceList();
}

function onMobileMessage(data) {
  const key = data.from;
  if (!STATE.deviceMsgs[key]) STATE.deviceMsgs[key] = [];
  STATE.deviceMsgs[key].push({
    id: 'm' + Date.now(),
    direction: 'received',
    msgType: 'text',
    content: data.text,
    timestamp: data.ts || Date.now(),
    fileName: null, fileSize: null, filePath: null, peerId: key,
  });
  if (STATE.activeDeviceId === key) renderDeviceMessages(key);
}

function onMobileFile(data, msgType) {
  const key = data.from;
  if (!STATE.deviceMsgs[key]) STATE.deviceMsgs[key] = [];
  const content = JSON.stringify({ fileName: data.fileName, fileSize: data.fileSize });
  STATE.deviceMsgs[key].push({
    id: 'm' + Date.now(),
    direction: 'received',
    msgType: msgType, // 'image' or 'file'
    content: content,
    timestamp: data.ts || Date.now(),
    fileName: data.fileName,
    fileSize: data.fileSize,
    filePath: data.filePath,
    peerId: key,
  });
  if (STATE.activeDeviceId === key) renderDeviceMessages(key);
}

function renderDeviceMessages(deviceId) {
  const container = document.getElementById('chat-messages');
  const msgs = STATE.deviceMsgs[deviceId] || [];
  if (!msgs.length) {
    container.innerHTML = '<div class="msg-system"><span>和手机互传 — 不用加密，直接发送</span></div>';
  } else {
    container.innerHTML = msgs.map(m => msgHTML(m)).join('');
  }
  container.scrollTop = container.scrollHeight;
}

async function sendToMobile() {
  const input = document.getElementById('msg-input');
  const text = input.value.trim();
  if (!text || !STATE.activeDeviceId) return;

  try {
    await fetch('/api/send-to-mobile', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ deviceId: STATE.activeDeviceId, text }),
    });
    // Add to local messages
    const key = STATE.activeDeviceId;
    if (!STATE.deviceMsgs[key]) STATE.deviceMsgs[key] = [];
    STATE.deviceMsgs[key].push({
      id: 'm' + Date.now(),
      direction: 'sent',
      msgType: 'text',
      content: text,
      timestamp: Date.now(),
      fileName: null, fileSize: null, filePath: null, peerId: key,
    });
    renderDeviceMessages(key);
    // ✅ 已修复：只有确认发送成功才清空输入框。
    // 原先清空动作写在 try/catch 之外，发送失败时也会执行，
    // 用户刚敲的字一瞬间就没了，想重发还得重新打一遍。
    // 现在失败时保留内容，方便直接修改后重发。
    input.value = '';
  } catch (e) {
    toast('发送失败', 'error');
  }
  input.focus();
}

// =============================================================================
// Peer selection
// =============================================================================

function selectPeer(peerId) {
  STATE.activePeerId = peerId;
  STATE.activeDeviceId = null;
  renderPeerList();
  document.getElementById('chat-empty').className = 'chat-empty-hide';
  document.getElementById('chat-messages').style.display = 'block';
  const p = findPeer(peerId);
  document.getElementById('chat-peer-name').textContent = p?.name || peerId;
  document.getElementById('chat-peer-status').textContent = p?.connected ? '🔒 加密连接已建立' : '正在建立加密连接...';
  loadMessages(peerId);
  // Auto-connect if not connected
  if (p && !p.connected) {
    sendCmd({ action: 'connect_peer', peerId });
  }
}

// =============================================================================
// Messages
// =============================================================================

async function loadMessages(peerId) {
  try {
    const r = await fetch(`/api/messages/${encodeURIComponent(peerId)}`);
    STATE.messages[peerId] = await r.json();
    renderMessages(peerId);
  } catch (e) {
    console.error(e);
  }
}

function onNewMessage(msg) {
  const key = msg.peerId;
  if (!STATE.messages[key]) STATE.messages[key] = [];
  if (!STATE.messages[key].find(m => m.id === msg.id)) {
    STATE.messages[key].push(msg);
  }
  if (STATE.activePeerId === key) renderMessages(key);
  renderPeerList();
}

function renderMessages(peerId) {
  const container = document.getElementById('chat-messages');
  const msgs = STATE.messages[peerId] || [];
  if (!msgs.length) {
    container.innerHTML = `<div class="msg-system"><span>开始聊天 — 端到端加密</span></div>`;
  } else {
    container.innerHTML = msgs.map(m => msgHTML(m)).join('');
  }
  container.scrollTop = container.scrollHeight;
}

function msgHTML(m) {
  const isSelf = m.direction === 'sent';
  const cls = isSelf ? 'self' : 'peer';
  const time = new Date(m.timestamp).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });

  switch (m.msgType) {
    case 'text': {
      const text = esc(decodeBytes(m.content));
      return `<div class="msg-row ${cls}"><div><div class="msg-bubble">${text}</div><div class="msg-time">${time}</div></div></div>`;
    }
    case 'image': {
      const url = fileDownloadUrl(m);
      const info = parseMeta(m.content);
      return `<div class="msg-row ${cls}"><div>
        ${url ? `<img class="img-bubble" src="${url}" alt="${esc(info?.fileName||'image')}" onclick="window.open('${url}')" loading="lazy" />`
              : `<div class="msg-bubble">🖼️ ${esc(info?.fileName||'图片')}</div>`}
        <div class="msg-time">${time}</div>
      </div></div>`;
    }
    case 'file': {
      const url = fileDownloadUrl(m);
      const info = parseMeta(m.content);
      return `<div class="msg-row ${cls}"><div><div class="msg-bubble" style="padding:6px 10px;">
        <div class="file-bubble" ${url ? `onclick="window.open('${url}')"` : ''}>
          <div class="file-icon">${fileIcon(info?.fileName||'')}</div>
          <div class="file-info">
            <div class="file-name">${esc(info?.fileName||'file')}</div>
            <div class="file-size">${fmtSize(info?.fileSize||0)}</div>
          </div>
        </div>
      </div><div class="msg-time">${time}</div></div></div>`;
    }
    case 'folder': {
      const info = parseMeta(m.content);
      return `<div class="msg-row ${cls}"><div><div class="msg-bubble" style="padding:6px 10px;">
        <div class="folder-bubble">
          <div class="file-icon">📁</div>
          <div class="file-info">
            <div class="file-name">${esc(info?.folderName||'文件夹')}</div>
            <div class="file-size">${info?.fileCount||0} 个文件 · ${fmtSize(info?.totalSize||0)}</div>
          </div>
        </div>
      </div><div class="msg-time">${time}</div></div></div>`;
    }
    default:
      return `<div class="msg-system"><span>${m.msgType} · ${time}</span></div>`;
  }
}

// =============================================================================
// Content decoding
// =============================================================================

// Go's json.Marshal on []byte produces base64 string in JSON.
// We decode base64 → UTF-8 bytes → string.
function decodeBytes(content) {
  if (!content) return '';
  if (typeof content !== 'string') {
    // Might be an array from JSON
    if (Array.isArray(content)) {
      return new TextDecoder().decode(new Uint8Array(content));
    }
    return String(content);
  }
  try {
    // Try base64 decode
    const bin = atob(content);
    // Check if it's valid UTF-8 (for text messages) or JSON (for file metadata)
    // Try UTF-8 first
    try {
      const bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
      return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    } catch {
      // Not valid UTF-8, return the base64 string itself
      return content;
    }
  } catch {
    return content;
  }
}

function parseMeta(content) {
  const str = decodeBytes(content);
  try { return JSON.parse(str); } catch { return {}; }
}

function fileDownloadUrl(m) {
  if (!m.filePath) return '';
  const path = m.filePath.replace(/\\/g, '/');
  // Strip any downloads/ prefix to get relative path
  const idx = path.indexOf('downloads/');
  const rel = idx >= 0 ? path.substring(idx + 'downloads/'.length) : path;
  return '/api/download/' + encodeURIComponent(rel);
}

// =============================================================================
// Send
// =============================================================================

function sendMessage() {
  const input = document.getElementById('msg-input');
  const text = input.value.trim();
  if (!text) return;

  if (STATE.activeDeviceId) {
    sendToMobile();
  } else if (STATE.activePeerId) {
    sendCmd({ action: 'send_text', peerId: STATE.activePeerId, text });
    input.value = '';
    input.focus();
  }
}

// =============================================================================
// File handling
// =============================================================================

function setupEventListeners() {
  document.getElementById('btn-send').addEventListener('click', sendMessage);
  document.getElementById('msg-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
  });
  document.getElementById('btn-file').addEventListener('click', () => {
    document.getElementById('file-input').click();
  });
  document.getElementById('btn-folder').addEventListener('click', () => {
    document.getElementById('folder-input').click();
  });
  document.getElementById('btn-refresh').addEventListener('click', pollPeers);
  document.getElementById('btn-manual-connect').addEventListener('click', showManualConnect);
  document.getElementById('my-name').addEventListener('click', changeMyName);
  document.getElementById('btn-qr').addEventListener('click', showQR);
  document.getElementById('file-input').addEventListener('change', onFileSelect);
  document.getElementById('folder-input').addEventListener('change', onFolderSelect);
  document.getElementById('search-input').addEventListener('input', renderPeerList);
}

function onFileSelect(e) {
  STATE.pendingFiles.push(...Array.from(e.target.files));
  e.target.value = '';
  updateFilePreview();
  if (STATE.activePeerId) sendPendingFiles();
}

function onFolderSelect(e) {
  const files = Array.from(e.target.files);
  if (!files.length) return;
  // Get folder name from first file's relative path
  // 注意顺序：先把 folderName 算出来，再去清空 input。
  // webkitRelativePath 挂在 File 对象上，虽然主流浏览器在清空 input 后仍能读到，
  // 但这属于实现细节而非规范保证；先取值再清空更稳妥。
  const folderName = files[0].webkitRelativePath?.split('/')[0] || 'folder';
  e.target.value = '';

  if (STATE.activePeerId) {
    sendFolderToPeer(folderName, files);
  } else {
    // ✅ 已修复：原先这里会把文件夹的文件塞进 pendingFiles 等着稍后发送，
    // 但后续 sendPendingFiles() 是按"单个文件"发的、不带 relativePath，
    // 于是目录结构会全部丢失 —— 对方收到的是一堆平铺的散文件，
    // 名为"发文件夹"，实际根本不是文件夹。
    // 与其悄悄发出一个假文件夹，不如明确提示用户先选联系人。
    toast('请先选择一位联系人，再发送文件夹', 'error');
    return;
  }
}

// 【第 3 层 - HTTP】发送文件夹：与发单文件同样的"两段式"，难点在于要保住目录结构。
//
// 关键就在 relativePath 这个字段：它取自 file.webkitRelativePath，
// 形如 "我的文件夹/子目录/a.txt"，后端凭它把每个文件摆回原本的目录层次，
// 再整体打包成 .tar.gz 发给对端。少了它，所有文件会被平铺到同一层，文件夹结构就丢了。
//
// uploadKey 用 "当前对端ID + 分隔符 + 文件夹名" 作为键，
// 这样两个不同对端的同名文件夹不会互相覆盖。
//
// ✅ 已修复（原隐患：静默发出残缺文件夹）
// 原先循环里的 fetch 完全没检查响应（未判断 r.ok），
// 只要没抛网络异常，即便后端对每个文件都返回错误，循环也会照常走完，
// 最后照样执行 pack_and_send —— 对端收到残缺文件夹，而发送方毫无察觉。
// 现在改为统计失败个数，只要有一个失败就中止并提示，绝不发出半成品。
async function sendFolderToPeer(folderName, files) {
  // Upload each file with folder path info
  toast(`正在准备文件夹: ${folderName}...`, 'info');
  const uploadKey = STATE.activePeerId + '\x00' + folderName;
  // 统计上传失败的个数：只要有一个文件没传上去，就中止发送。
  // 否则 pack_and_send 会把残缺的一批文件打包发出去，
  // 对端收到一个打不开/少文件的文件夹，而发送方这边还显示"已发送"。
  let failed = 0;
  for (const file of files) {
    const fd = new FormData();
    fd.append('file', file);
    fd.append('folderName', folderName);
    fd.append('peerId', STATE.activePeerId);
    fd.append('relativePath', file.webkitRelativePath || (folderName + '/' + file.name));
    try {
      const r = await fetch('/api/upload', { method: 'POST', body: fd });
      if (!r.ok) failed++;
    } catch (e) {
      failed++;
    }
  }
  if (failed > 0) {
    toast(`文件夹「${folderName}」有 ${failed} 个文件上传失败，已取消发送`, 'error');
    return;
  }
  // Tell backend to pack and send
  sendCmd({ action: 'pack_and_send', peerId: STATE.activePeerId, folderName });
}

function updateFilePreview() {
  const preview = document.getElementById('file-preview');
  preview.innerHTML = STATE.pendingFiles.map((f, i) => `
    <div class="file-preview-item">
      <span>${fileIcon(f.name)} ${esc(f.name)} (${fmtSize(f.size)})</span>
      <span class="remove-preview" data-idx="${i}">✕</span>
    </div>
  `).join('');
  preview.querySelectorAll('.remove-preview').forEach(el => {
    el.addEventListener('click', () => {
      STATE.pendingFiles.splice(parseInt(el.dataset.idx), 1);
      updateFilePreview();
    });
  });
}

// 【第 3 层 - HTTP：文件上传走 HTTP，而不是 WebSocket】
// 为什么文件不走 WebSocket？
//   因为本项目里的 WebSocket 通道专门用来传"控制指令"（send_text / send_file 这类小 JSON），
//   而文件体积大、又适合用 multipart/form-data 提交，所以单独走 HTTP POST。
//
// 于是"发一个文件给对端"实际是两段路：
//   ① 浏览器 --HTTP POST /api/upload--> 本机后端（文件先存盘，后端返回路径）
//   ② 本机后端 --WebSocket(P2P)--> 对端（sendCmd 只发一条带路径的指令，由后端读出文件并转发）
//
// 一句话总结：HTTP 负责"浏览器 → 本机后端"这一段，
//           P2P 的 WebSocket 负责"本机 → 对端电脑"那一段。
//
// ✅ 已修复（原隐患：静默失败）
// 原先只判断 result.status === 'ok' 才提示成功，其它情况一律不吭声：
// 后端返回 500、或返回体不是合法 JSON 时，文件既没发出去、界面上也没有任何提示，
// 用户会误以为发送成功。
// 现在改为：先用 r.ok 检查 HTTP 状态码，再检查业务状态，任一不满足都抛错，
// 由下面的 catch 统一弹出"发送失败"提示。
async function sendPendingFiles() {
  if (!STATE.activePeerId) return;

  for (const file of [...STATE.pendingFiles]) {
    try {
      toast(`发送中: ${file.name}...`, 'info');
      const fd = new FormData();
      fd.append('file', file);
      fd.append('peerId', STATE.activePeerId);
      const r = await fetch('/api/upload', { method: 'POST', body: fd });
      // 显式检查 HTTP 状态码：后端若返回 500 等错误，r.ok 为 false，
      // 此时 response 体通常不是合法 JSON，取 .status 会无意义，直接按失败处理。
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const result = await r.json();
      if (result.status === 'ok') {
        // Tell backend to forward to peer
        sendCmd({ action: 'send_file', peerId: STATE.activePeerId, filePath: result.path });
        toast(`${file.name} 已发送`, 'success');
      } else {
        // 后端返回了 JSON 但状态不是 ok —— 同样视为失败，必须让用户看见
        throw new Error(result.message || '后端未返回 ok');
      }
    } catch (e) {
      toast(`发送失败: ${file.name}`, 'error');
    }
  }
  STATE.pendingFiles = [];
  updateFilePreview();
}

// =============================================================================
// Poll
// =============================================================================

// 【第 3 层 - HTTP】定期轮询对端列表。
// 其实 WebSocket 也会主动推送 peers 事件（见 handleEvent 的 case 'peers'），
// 这里仍保留 5 秒一次的轮询作为兜底：万一推送丢失、或后端中途重启过，
// 页面也能靠轮询自行恢复一致，不至于一直显示过期的列表。
function pollPeers() {
  fetch('/api/peers').then(r => r.json()).then(peers => {
    STATE.peers = peers;
    renderPeerList();
  }).catch(() => {});
}

setInterval(pollPeers, 5000);

// =============================================================================
// Change name
// =============================================================================

async function changeMyName() {
  const newName = prompt('输入新名字:', STATE.me?.name || '');
  if (!newName || newName.trim() === '') return;
  try {
    const r = await fetch('/api/name', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newName.trim() }),
    });
    // ✅ 已修复：原先不检查 HTTP 状态码、也不判断业务状态，
    // 后端一旦返回 400/500（http.Error 返回的是纯文本），
    // 下面的 r.json() 会解析纯文本直接抛错，用户只看到笼统一句"改名失败"，
    // 根本不知道是名字非法、还是保存失败。
    // 现在：先看状态码，再看业务状态，并把**真实原因**带进提示。
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const result = await r.json();
    if (result.status !== 'ok') {
      throw new Error(result.message || '后端未返回 ok');
    }
    STATE.me.name = result.name;
    renderMyInfo();
    toast(`名字已改为: ${result.name}`, 'success');
  } catch (e) {
    toast(`改名失败：${e.message}`, 'error');
  }
}

// =============================================================================
// QR code
// =============================================================================

// 标记"关闭弹窗"的监听器是否已绑定过（详见 showQR 内的说明）
let qrListenerBound = false;

function showQR() {
  const modal = document.getElementById('qr-modal');
  const ips = STATE.me?.ips || [];
  const ip = ips[0] || 'localhost';
  const port = STATE.me?.port || '';
  document.getElementById('qr-url').textContent = `http://${ip}:${port}/m`;
  document.getElementById('qr-img').src = '/qr.png?' + Date.now();
  modal.style.display = 'flex';
  // ✅ 已修复：用 qrListenerBound 标志保证只绑定一次。
  // 原先每调用一次 showQR() 就 addEventListener 一次，点 N 次按钮会累积 N 个相同的回调。
  // 这里刻意没有把绑定挪到函数外面直接执行 —— 因为那样要求脚本运行时 DOM 已就绪，
  // 若 <script> 放在 <head> 里，getElementById 会拿到 null 而直接报错。
  // 用标志位做惰性绑定，无论脚本位置如何都安全。
  if (!qrListenerBound) {
    qrListenerBound = true;
    modal.addEventListener('click', function (e) {
      if (e.target === modal) modal.style.display = 'none';
    });
  }
}

// =============================================================================
// Manual connect
// =============================================================================

function showManualConnect() {
  const host = prompt('输入对方 IP 地址:\n(例: 192.168.1.100)', '');
  if (!host) return;
  const portStr = prompt('输入对方端口号:\n(对方启动时显示的端口)', '');
  if (!portStr) return;
  const port = parseInt(portStr);
  if (!port) return toast('端口无效', 'error');
  const name = prompt('对方用户名:\n(可留空)', 'peer') || 'peer';

  const peerId = `${name}@${host}:${port}`;
  toast(`正在连接 ${peerId}...`, 'info');
  fetch('/api/connect', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ host, port, name }),
  }).then(r => r.json()).then(data => {
    toast(data.status === 'connecting' ? `已发起连接: ${data.peerId}` : '连接失败', data.status === 'connecting' ? 'success' : 'error');
    setTimeout(pollPeers, 1000);
  }).catch(() => toast('连接请求失败', 'error'));
}

// =============================================================================
// Helpers
// =============================================================================

function esc(s) {
  if (!s) return '';
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function fmtSize(bytes) {
  if (!bytes) return '0 B';
  const u = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + u[Math.min(i, u.length - 1)];
}

function fileIcon(name) {
  const ext = (name || '').split('.').pop()?.toLowerCase();
  const m = {
    pdf: '📕', doc: '📘', docx: '📘', xls: '📗', xlsx: '📗', ppt: '📙', pptx: '📙',
    txt: '📄', md: '📝', csv: '📊',
    zip: '🗜️', rar: '🗜️', '7z': '🗜️', tar: '🗜️', gz: '🗜️',
    jpg: '🖼️', jpeg: '🖼️', png: '🖼️', gif: '🖼️', webp: '🖼️', svg: '🖼️', bmp: '🖼️',
    mp3: '🎵', wav: '🎵', flac: '🎵', aac: '🎵',
    mp4: '🎬', avi: '🎬', mkv: '🎬', mov: '🎬', webm: '🎬',
    exe: '⚙️', msi: '⚙️', apk: '📱', dmg: '💿',
    html: '🌐', css: '🎨', js: '📜', py: '🐍', go: '🔵', rs: '🦀', java: '☕',
  };
  return m[ext] || '📎';
}

function toast(msg, type) {
  let c = document.getElementById('toast-container');
  if (!c) { c = document.createElement('div'); c.id = 'toast-container'; document.body.appendChild(c); }
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => t.remove(), 3500);
}

// Start
init();
