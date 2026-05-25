/* ============================================================
   KalpanaOS Dashboard — app.js
   Full SPA connected to all backend services via APIs
   ============================================================ */

// ─── Config ──────────────────────────────────────────────────
const API = {
  SIL:  '/api/sil',
  COL:  '/api/col',
  SSI:  '/api/ssi',
  AICP: '/api/aicp',
  AAF:  '/api/aaf',
  ORCH: '/api/orchestrator',
  CBAL: '/api/cbal',
};

// ─── State ───────────────────────────────────────────────────
let state = {
  token: null,
  user: null,
  sessionId: null,
  currentPendingCmd: null,
  refreshInterval: null,
};

// ─── Helpers ──────────────────────────────────────────────────
function $(id) { return document.getElementById(id); }

async function api(service, path, opts = {}) {
  const url = `${API[service]}${path}`;
  const headers = { 'Content-Type': 'application/json' };
  if (state.token) headers['Authorization'] = `Bearer ${state.token}`;
  if (opts.headers) Object.assign(headers, opts.headers);
  try {
    const res = await fetch(url, { ...opts, headers });
    if (res.status === 401) { logout(); return null; }
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return await res.json();
    return null;
  } catch (e) {
    console.warn(`API ${service} ${path} error:`, e.message);
    return null;
  }
}

function toast(msg, type = 'info') {
  const container = $('toast-container');
  const el = document.createElement('div');
  el.className = `toast toast-${type}`;
  const icon = type === 'success' ? '✓' : type === 'error' ? '✕' : 'ℹ';
  el.innerHTML = `<span>${icon}</span><span>${msg}</span>`;
  container.appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; el.style.transform = 'translateX(20px)'; el.style.transition = '0.3s'; setTimeout(() => el.remove(), 300); }, 3500);
}

function formatTime(ts) {
  if (!ts) return '—';
  const d = new Date(ts);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatDate(ts) {
  if (!ts) return '—';
  const d = new Date(ts);
  return `${d.toLocaleDateString()} ${d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
}

function relativeTime(ts) {
  if (!ts) return '—';
  const diff = Date.now() - new Date(ts).getTime();
  if (diff < 60000) return `${Math.floor(diff/1000)}s ago`;
  if (diff < 3600000) return `${Math.floor(diff/60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff/3600000)}h ago`;
  return `${Math.floor(diff/86400000)}d ago`;
}

// ─── Auth ─────────────────────────────────────────────────────
async function login(email, password) {
  const btn = $('login-btn');
  const err = $('login-error');
  btn.querySelector('.btn-text').textContent = 'Authenticating…';
  btn.disabled = true;
  err.classList.add('hidden');

  try {
    const res = await fetch(`${API.SIL}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Authentication failed');

    state.token = data.access_token;
    state.user = data.user;
    localStorage.setItem('kalpana_token', data.access_token);
    localStorage.setItem('kalpana_refresh', data.refresh_token);

    showApp();
    updateInferenceBadge();
  } catch (e) {
    err.textContent = e.message;
    err.classList.remove('hidden');
  } finally {
    btn.querySelector('.btn-text').textContent = 'Authenticate';
    btn.disabled = false;
  }
}

async function tryAutoLogin() {
  const token = localStorage.getItem('kalpana_token');
  if (!token) return false;
  state.token = token;
  const data = await api('SIL', '/auth/me');
  if (!data || data.error) {
    await tryRefresh();
    return !!state.token;
  }
  state.user = data;
  return true;
}

async function tryRefresh() {
  const refresh = localStorage.getItem('kalpana_refresh');
  if (!refresh) return;
  try {
    const res = await fetch(`${API.SIL}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: refresh }),
    });
    if (!res.ok) { logout(); return; }
    const data = await res.json();
    state.token = data.access_token;
    localStorage.setItem('kalpana_token', data.access_token);
    localStorage.setItem('kalpana_refresh', data.refresh_token);
  } catch (e) { logout(); }
}

function logout() {
  state.token = null; state.user = null;
  localStorage.removeItem('kalpana_token');
  localStorage.removeItem('kalpana_refresh');
  if (state.refreshInterval) clearInterval(state.refreshInterval);
  $('app').classList.add('hidden');
  $('login-screen').classList.remove('hidden');
}

// ─── Navigation ───────────────────────────────────────────────
function showApp() {
  $('login-screen').classList.add('hidden');
  $('app').classList.remove('hidden');
  if (state.user) {
    $('sidebar-username').textContent = state.user.email?.split('@')[0] || 'Operator';
  }
  navigateTo('dashboard');
  startAutoRefresh();
}

function navigateTo(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  const pageEl = $(`page-${page}`);
  const navEl = $(`nav-${page}`);
  if (pageEl) pageEl.classList.add('active');
  if (navEl) navEl.classList.add('active');

  switch (page) {
    case 'dashboard': loadDashboard(); break;
    case 'services':  loadServices(); break;
    case 'agents':    loadAgents(); loadTasks(); break;
    case 'search':    break;
    case 'observe':   loadObservability(); break;
    case 'chat':      break;
    // Phase 2
    case 'anomalies': loadAnomalies(); break;
    case 'memory':    loadMemory(''); break;
    // Phase 3
    case 'topology':  loadTopologyAgents(); break;
    // Phase 6
    case 'analytics': loadAnalytics(); break;
    // App Compiler
    case 'apps':      loadAppsPage(); break;
  }
}

// ─── Dashboard ────────────────────────────────────────────────
async function loadDashboard() {
  const [services, agents, pending, node] = await Promise.all([
    api('COL', '/services'),
    api('AAF', '/agents'),
    api('AICP', '/pending'),
    api('COL', '/nodes'),
  ]);

  // Stats
  $('stat-services').textContent = services?.length ?? '—';
  $('stat-agents').textContent = agents?.length ?? '—';
  $('stat-pending').textContent = pending?.length ?? '—';

  // Try to count collections
  const cols = await api('SSI', '/collections');
  const colCount = cols?.result?.collections?.length ?? 0;
  $('stat-vectors').textContent = colCount;

  // Node info
  if (node) {
    $('node-hostname').textContent = node.hostname || '—';
    $('node-cpu').textContent = node.cpu_count || '—';
    $('node-mem').textContent = node.total_memory || '—';
    $('node-docker').textContent = node.docker_version || '—';
    $('node-uptime').textContent = node.uptime || '—';
  }

  // Service health list
  const healthList = $('service-health-list');
  if (services && services.length) {
    healthList.innerHTML = services.map(s => `
      <div class="service-health-item">
        <span class="status-dot ${s.status === 'running' ? 'online' : 'offline'}"></span>
        <span class="service-name">${s.name || s.Names?.[0]?.replace('/','') || '?'}</span>
        <span class="service-image">${(s.image || s.Image || '').split(':')[0]}</span>
        <span class="badge badge-${s.status === 'running' ? 'green' : 'red'}">${s.status || '?'}</span>
      </div>`).join('');
  } else {
    healthList.innerHTML = '<p style="color:var(--text-muted);font-size:0.85rem;padding:10px 0">No services found</p>';
  }

  // Audit log
  const audit = await api('COL', '/audit');
  const tbody = $('audit-log-body');
  if (audit && audit.length) {
    tbody.innerHTML = audit.slice(0,20).map(a => `
      <tr>
        <td>${formatTime(a.ts)}</td>
        <td>${a.operator || '—'}</td>
        <td><code style="font-family:'JetBrains Mono',monospace;font-size:0.78rem">${a.action || '—'}</code></td>
        <td>${a.resource || '—'}</td>
        <td style="color:var(--text-muted);font-size:0.78rem">${a.detail || ''}</td>
      </tr>`).join('');
  } else {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-row">No audit events yet</td></tr>';
  }
}

// ─── Services ─────────────────────────────────────────────────
async function loadServices() {
  const grid = $('services-grid');
  const services = await api('COL', '/services');
  if (!services || services.length === 0) {
    grid.innerHTML = `<div class="card glass" style="grid-column:1/-1;text-align:center;padding:48px;color:var(--text-muted)"><p style="font-size:2rem;margin-bottom:12px">◫</p><p>No services deployed yet</p><button class="btn btn-primary" style="margin-top:16px" onclick="openDeployModal()">+ Deploy Your First Service</button></div>`;
    return;
  }
  grid.innerHTML = services.map(s => {
    const name = s.name || s.Names?.[0]?.replace('/','') || '?';
    const image = s.image || s.Image || '—';
    const status = s.status || 'unknown';
    const statusColor = status === 'running' ? 'green' : status === 'exited' ? 'red' : 'amber';
    return `<div class="service-card glass">
      <div class="service-card-header">
        <span class="status-dot ${status === 'running' ? 'online' : 'offline'}"></span>
        <span class="service-card-name">${name}</span>
        <span class="badge badge-${statusColor}">${status}</span>
      </div>
      <div class="service-card-body">
        <div><span>Image</span><span style="font-family:'JetBrains Mono',monospace;font-size:0.78rem">${image}</span></div>
        <div><span>Created</span><span>${formatDate(s.created_at || s.Created)}</span></div>
        ${s.ports ? `<div><span>Ports</span><span>${JSON.stringify(s.ports)}</span></div>` : ''}
      </div>
      <div class="service-card-actions">
        <button class="btn btn-ghost btn-sm" onclick="viewService('${name}')">Details</button>
        <button class="btn btn-danger btn-sm" onclick="stopService('${name}')">✕ Stop</button>
      </div>
    </div>`;
  }).join('');
}

async function stopService(name) {
  if (!confirm(`Stop service "${name}"? This will be logged.`)) return;
  const res = await api('COL', `/services/${name}`, { method: 'DELETE' });
  if (res !== null) { toast(`Service "${name}" stopped`, 'success'); loadServices(); loadDashboard(); }
  else toast('Failed to stop service', 'error');
}

async function viewService(name) {
  const data = await api('COL', `/services/${name}`);
  if (data) alert(JSON.stringify(data, null, 2));
}

function openDeployModal() {
  $('deploy-modal').classList.remove('hidden');
}

// ─── Deploy Form ──────────────────────────────────────────────
async function deployService(e) {
  e.preventDefault();
  const name = $('deploy-name').value.trim();
  const image = $('deploy-image').value.trim();
  const ports = $('deploy-ports').value.trim();
  const envRaw = $('deploy-env').value.trim();
  const mem = $('deploy-mem').value.trim();

  const env = envRaw ? envRaw.split('\n').map(l => l.trim()).filter(Boolean) : [];
  const portList = ports ? [ports] : [];

  const res = await api('COL', '/services/deploy', {
    method: 'POST',
    body: JSON.stringify({ name, image, ports: portList, env, mem_limit: mem }),
  });

  if (res && !res.error) {
    toast(`Service "${name}" deployed!`, 'success');
    $('deploy-modal').classList.add('hidden');
    $('deploy-form').reset();
    loadServices();
  } else {
    toast(res?.error || 'Deploy failed', 'error');
  }
}

// ─── Agents ───────────────────────────────────────────────────
async function loadAgents() {
  const agents = await api('AAF', '/agents');
  const list = $('agents-list');
  if (!agents || agents.length === 0) {
    list.innerHTML = '<p style="color:var(--text-muted);font-size:0.85rem">No agents registered</p>';
    return;
  }
  list.innerHTML = agents.map(a => {
    const caps = (a.capabilities || '').split(',').filter(Boolean);
    return `<div class="agent-item" onclick="runAgent('${a.id}', '${a.name}')">
      <div class="agent-icon">◎</div>
      <div>
        <div class="agent-name">${a.name}</div>
        <div class="agent-desc">${a.description || ''}</div>
        <div class="agent-caps">${caps.map(c => `<span class="cap-badge">${c}</span>`).join('')}</div>
      </div>
    </div>`;
  }).join('');
}

async function loadTasks() {
  const tasks = await api('AAF', '/tasks');
  const list = $('tasks-list');
  if (!tasks || tasks.length === 0) {
    list.innerHTML = '<p style="color:var(--text-muted);font-size:0.85rem">No tasks yet. Click an agent to run it.</p>';
    return;
  }
  list.innerHTML = tasks.slice(0,20).map(t => `
    <div class="task-item" onclick="viewTask('${t.id}')">
      <div class="task-header">
        <span class="task-agent">${t.agent_id || '—'}</span>
        <span class="task-status-badge task-status-${t.status || 'pending'}">${t.status || 'pending'}</span>
      </div>
      <div class="task-time">${relativeTime(t.created_at)} · Click for output</div>
    </div>`).join('');
}

async function runAgent(agentId, agentName) {
  const input = prompt(`Run ${agentName}:\nEnter task input (or leave blank for default):`);
  if (input === null) return;
  const res = await api('AAF', '/tasks', {
    method: 'POST',
    body: JSON.stringify({ agent_id: agentId, input: input || `Run ${agentName}` }),
  });
  if (res && res.task_id) {
    toast(`Agent task started: ${res.task_id}`, 'success');
    loadTasks();
  } else {
    toast('Failed to start agent task', 'error');
  }
}

async function viewTask(taskId) {
  const task = await api('AAF', `/tasks/${taskId}`);
  if (task) {
    const output = task.output ? `\n\n--- OUTPUT ---\n${task.output}` : '';
    const error = task.error ? `\n\n--- ERROR ---\n${task.error}` : '';
    alert(`Task: ${task.id}\nAgent: ${task.agent_id}\nStatus: ${task.status}${output}${error}`);
  }
}

// ─── Knowledge Search ─────────────────────────────────────────
async function doSearch() {
  const q = $('search-input').value.trim();
  if (!q) return;
  const results = $('search-results');
  results.innerHTML = '<div class="loading-pulse"></div>';
  const data = await api('SSI', '/search', {
    method: 'POST',
    body: JSON.stringify({ query: q, top_k: 8 }),
  });
  if (!data || !data.results || data.results.length === 0) {
    results.innerHTML = '<div class="search-empty"><span class="search-empty-icon">◍</span><p>No results found. Try ingesting some documents first.</p></div>';
    return;
  }
  results.innerHTML = data.results.map(r => `
    <div class="search-result-card">
      <div class="result-header">
        <span class="result-source">📄 ${r.source || 'unknown'}</span>
        <span class="result-score">Score: ${(r.score || 0).toFixed(3)}</span>
      </div>
      <div class="result-text">${(r.text || '').substring(0, 400)}${r.text?.length > 400 ? '…' : ''}</div>
    </div>`).join('');
}

async function ingestText() {
  const text = $('ingest-text').value.trim();
  const source = $('ingest-source').value.trim() || 'manual-input';
  if (!text) { toast('Please enter text to ingest', 'error'); return; }

  const btn = $('ingest-btn');
  btn.textContent = 'Ingesting…';
  btn.disabled = true;

  const res = await api('SSI', '/ingest/text', {
    method: 'POST',
    body: JSON.stringify({ text, source }),
  });

  btn.textContent = '+ Ingest to Knowledge Base';
  btn.disabled = false;

  if (res && !res.error) {
    toast(`Ingested ${res.chunks || '?'} chunks from "${source}"`, 'success');
    $('ingest-text').value = '';
    $('ingest-source').value = '';
  } else {
    toast(res?.error || 'Ingestion failed', 'error');
  }
}

// ─── AI Chat ──────────────────────────────────────────────────
function appendMessage(role, content) {
  const container = $('chat-messages');
  const welcome = container.querySelector('.chat-welcome');
  if (welcome) welcome.remove();

  const msg = document.createElement('div');
  msg.className = `message ${role}`;
  const avatarChar = role === 'user' ? 'U' : '⚛';
  msg.innerHTML = `
    <div class="msg-avatar">${avatarChar}</div>
    <div class="msg-bubble">${formatMessageContent(content)}</div>`;
  container.appendChild(msg);
  container.scrollTop = container.scrollHeight;
  return msg;
}

function formatMessageContent(text) {
  // Simple markdown-ish formatting
  return text
    .replace(/```([\s\S]*?)```/g, '<pre><code>$1</code></pre>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\n/g, '<br>');
}

function showThinking() {
  const container = $('chat-messages');
  const msg = document.createElement('div');
  msg.className = 'message assistant';
  msg.id = 'thinking-bubble';
  msg.innerHTML = `<div class="msg-avatar">⚛</div><div class="msg-bubble"><div class="msg-thinking"><div class="thinking-dot"></div><div class="thinking-dot"></div><div class="thinking-dot"></div></div></div>`;
  container.appendChild(msg);
  container.scrollTop = container.scrollHeight;
}

function removeThinking() {
  const el = $('thinking-bubble');
  if (el) el.remove();
}

async function sendChat() {
  const input = $('chat-input');
  const msg = input.value.trim();
  if (!msg) return;

  input.value = '';
  input.style.height = '';
  appendMessage('user', msg);
  showThinking();

  const data = await api('AICP', '/chat', {
    method: 'POST',
    body: JSON.stringify({ message: msg, session_id: state.sessionId }),
  });

  removeThinking();

  if (!data) {
    appendMessage('assistant', '⚠ Could not reach KalpanaAI. Check that the AICP service is running.');
    return;
  }

  state.sessionId = data.session_id;
  appendMessage('assistant', data.reply || 'No response');

  // Handle pending confirmation
  if (data.pending_command) {
    state.currentPendingCmd = data.pending_command;
    const banner = $('pending-confirmation');
    $('pending-desc').textContent = `Confirm: ${data.pending_command.description || 'destructive operation'}`;
    banner.classList.remove('hidden');
  }
}

async function confirmPending() {
  if (!state.currentPendingCmd) return;
  const res = await api('AICP', `/pending/${state.currentPendingCmd.id}/confirm`, { method: 'POST' });
  $('pending-confirmation').classList.add('hidden');
  if (res && !res.error) {
    appendMessage('assistant', `✅ Command executed: ${state.currentPendingCmd.description}`);
    toast('Command executed', 'success');
  } else {
    toast(res?.error || 'Execution failed', 'error');
  }
  state.currentPendingCmd = null;
}

async function rejectPending() {
  if (!state.currentPendingCmd) return;
  await api('AICP', `/pending/${state.currentPendingCmd.id}/reject`, { method: 'POST' });
  $('pending-confirmation').classList.add('hidden');
  appendMessage('assistant', `❌ Command rejected: ${state.currentPendingCmd.description}`);
  state.currentPendingCmd = null;
}

// ─── Observability ────────────────────────────────────────────
async function loadObservability() {
  const services = [
    { name: 'sil', label: 'Sovereign Identity Layer', url: `${API.SIL}/health` },
    { name: 'col', label: 'Cloud Operating Layer', url: `${API.COL}/health` },
    { name: 'ssi', label: 'Semantic Search', url: `${API.SSI}/health` },
    { name: 'aicp', label: 'AI Control Plane', url: `${API.AICP}/health` },
    { name: 'aaf', label: 'Agent Framework', url: `${API.AAF}/health` },
  ];

  const checks = await Promise.all(services.map(async s => {
    try {
      const res = await fetch(s.url, { headers: { 'Authorization': `Bearer ${state.token}` } });
      const ok = res.ok;
      const data = await res.json().catch(() => ({}));
      return { ...s, ok, status: data.status || (ok ? 'ok' : 'error') };
    } catch { return { ...s, ok: false, status: 'unreachable' }; }
  }));

  const healthEl = $('observe-health');
  healthEl.innerHTML = checks.map(c => `
    <div class="health-item">
      <span class="status-dot ${c.ok ? 'online' : 'offline'}"></span>
      <span class="health-item-name">${c.label}</span>
      <span class="health-item-status" style="color:${c.ok ? 'var(--green)' : 'var(--red)'}">${c.status}</span>
    </div>`).join('');

  const iframe = $('grafana-iframe');
  const fallback = $('grafana-fallback-link');
  if (iframe && !iframe.src) {
    const host = window.location.hostname;
    iframe.src = `http://${host}:3001/d/kalpana-overview/kalpanaos-overview?orgId=1&refresh=10s&kiosk`;
    if (fallback) fallback.href = `http://${host}:3001`;
  }
}

// ─── Inference Badge ──────────────────────────────────────────
async function updateInferenceBadge() {
  const badge = $('chat-model-badge');
  if (!badge) return;
  try {
    const res = await api('AICP', '/status');
    if (res && res.nvidia_api) {
      if (res.nvidia_api.startsWith('error')) {
        badge.innerHTML = `🛡️ Sovereign Mode (Local LLaMA 3.2)`;
        badge.style.background = 'rgba(0, 0, 0, 0.02)';
        badge.style.color = 'var(--text-secondary)';
        badge.style.borderColor = 'var(--border)';
      } else {
        badge.innerHTML = `⚡ Cloud Mode (${res.model || 'NVIDIA NIM'})`;
        badge.style.background = 'var(--accent)';
        badge.style.color = '#ffffff';
        badge.style.borderColor = 'var(--accent)';
      }
    }
  } catch (err) {
    console.error('Failed to fetch inference status', err);
  }
}

// ─── Auto Refresh ─────────────────────────────────────────────
function startAutoRefresh() {
  state.refreshInterval = setInterval(() => {
    const activePage = document.querySelector('.page.active');
    if (!activePage) return;
    if (activePage.id === 'page-dashboard') loadDashboard();
    if (activePage.id === 'page-agents') loadTasks();
    if (activePage.id === 'page-anomalies') loadAnomalies();
    if (activePage.id === 'page-topology') loadTopologyAgents();
    if (activePage.id === 'page-analytics') loadAnalytics();
    if (activePage.id === 'page-apps') loadAppsPage();
  }, 15000);
}

// ─── Phase 2: Anomaly Detection ───────────────────────────────
const SEVERITY_CONFIG = {
  CRITICAL: { color: '#ef4444', bg: 'rgba(239,68,68,0.15)', border: 'rgba(239,68,68,0.4)', pulse: true },
  HIGH:     { color: '#f97316', bg: 'rgba(249,115,22,0.15)', border: 'rgba(249,115,22,0.4)', pulse: false },
  MEDIUM:   { color: '#f59e0b', bg: 'rgba(245,158,11,0.15)', border: 'rgba(245,158,11,0.4)', pulse: false },
  LOW:      { color: '#3b82f6', bg: 'rgba(59,130,246,0.15)', border: 'rgba(59,130,246,0.3)', pulse: false },
};

async function loadAnomalies() {
  const list = $('anomalies-list');
  list.innerHTML = '<div class="loading-pulse"></div>';
  const data = await api('AICP', '/anomalies?resolved=false');
  if (!data || !data.anomalies || data.anomalies.length === 0) {
    list.innerHTML = `<div class="card glass" style="text-align:center;padding:3rem;color:var(--text-muted)">
      <div style="font-size:3rem;margin-bottom:1rem">✓</div>
      <h3 style="color:var(--text-secondary)">No Active Anomalies</h3>
      <p>Your infrastructure looks healthy. Run a scan to check for new issues.</p>
    </div>`;
    return;
  }
  list.innerHTML = data.anomalies.map(a => renderAnomalyCard(a)).join('');
}

function renderAnomalyCard(a) {
  const sev = SEVERITY_CONFIG[a.severity] || SEVERITY_CONFIG.LOW;
  const services = (a.services_affected || []).map(s => `<span class="cap-badge">${s}</span>`).join('');
  return `<div class="anomaly-card glass" style="border:1px solid ${sev.border};background:${sev.bg};margin-bottom:1rem;padding:1.25rem 1.5rem;border-radius:12px">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:.75rem">
      <div style="display:flex;align-items:center;gap:.75rem">
        <span style="background:${sev.color};color:#fff;padding:2px 10px;border-radius:999px;font-size:0.7rem;font-weight:700;letter-spacing:.05em">${a.severity}</span>
        <strong style="color:var(--text-primary);font-size:1rem">${a.title || 'Unnamed Anomaly'}</strong>
      </div>
      <div style="display:flex;align-items:center;gap:.5rem">
        <span style="color:var(--text-muted);font-size:0.78rem">${relativeTime(a.detected_at)}</span>
        <button class="btn btn-ghost btn-sm" onclick="resolveAnomaly('${a.id}')">✓ Resolve</button>
      </div>
    </div>
    <p style="color:var(--text-secondary);font-size:0.875rem;margin:.5rem 0">${a.description || ''}</p>
    ${services ? `<div style="display:flex;gap:.5rem;flex-wrap:wrap;margin-top:.5rem">${services}</div>` : ''}
  </div>`;
}

async function resolveAnomaly(id) {
  const note = prompt('Resolution note (optional):') ?? '';
  const res = await api('AICP', `/anomalies/${id}/resolve`, {
    method: 'POST',
    body: JSON.stringify({ note }),
  });
  if (res && !res.error) {
    toast('Anomaly resolved', 'success');
    loadAnomalies();
  } else {
    toast(res?.error || 'Failed to resolve', 'error');
  }
}

async function scanAnomalies() {
  const btn = $('scan-anomalies-btn');
  const output = $('anomaly-scan-output');
  btn.textContent = '⟳ Scanning…';
  btn.disabled = true;
  output.classList.add('hidden');

  const data = await api('AICP', '/diagnose', {
    method: 'POST',
    body: JSON.stringify({ window_minutes: 30 }),
  });

  btn.textContent = '⚡ Run Diagnostic Scan';
  btn.disabled = false;

  if (!data) {
    toast('Scan failed — check AICP service', 'error');
    return;
  }

  // Show scan time badge
  const scanTimeEl = $('anomaly-scan-time');
  if (data.scanned_at) {
    scanTimeEl.textContent = `Last scan: ${formatTime(data.scanned_at)}`;
    scanTimeEl.style.display = '';
  }

  // Show report
  output.innerHTML = formatMessageContent(data.report || 'No report generated');
  output.classList.remove('hidden');

  if (data.anomalies_detected > 0) {
    toast(`${data.anomalies_detected} anomalies detected!`, 'error');
  } else {
    toast('Scan complete — no anomalies detected', 'success');
  }

  // Refresh list
  loadAnomalies();
}

// ─── Phase 2: Episodic Memory ─────────────────────────────────
const MEMORY_TYPE_ICONS = {
  chat_interaction: '💬',
  agent_output:     '🤖',
  anomaly:          '⚠',
  infra_event:      '🔧',
};

let currentMemoryFilter = '';

async function loadMemory(type) {
  currentMemoryFilter = type;
  const list = $('memory-list');
  list.innerHTML = '<div class="loading-pulse"></div>';

  const params = type ? `?type=${encodeURIComponent(type)}&limit=100` : '?limit=100';
  const data = await api('AICP', `/memory${params}`);

  if (!data || !data.memories || data.memories.length === 0) {
    list.innerHTML = `<div class="card glass" style="text-align:center;padding:3rem;color:var(--text-muted)">
      <div style="font-size:3rem;margin-bottom:1rem">◈</div>
      <h3 style="color:var(--text-secondary)">No Memories Yet</h3>
      <p>Start chatting with KalpanaAI or run agents — memories will appear here automatically.</p>
    </div>`;
    return;
  }

  // Sort by created_at desc
  const sorted = [...data.memories].sort((a,b) => new Date(b.created_at) - new Date(a.created_at));

  list.innerHTML = sorted.map(m => {
    const icon = MEMORY_TYPE_ICONS[m.type] || '📝';
    const tags = (m.tags || []).map(t => `<span class="cap-badge" style="font-size:.7rem">${t}</span>`).join('');
    return `<div class="card glass" style="margin-bottom:.75rem;padding:1rem 1.25rem">
      <div style="display:flex;align-items:flex-start;gap:.75rem">
        <div style="font-size:1.5rem;flex-shrink:0;padding-top:2px">${icon}</div>
        <div style="flex:1">
          <div style="display:flex;align-items:center;gap:.5rem;margin-bottom:.4rem;flex-wrap:wrap">
            <span style="background:rgba(99,102,241,.2);color:#818cf8;padding:1px 8px;border-radius:999px;font-size:.7rem;font-weight:600">${(m.type||'').replace('_',' ')}</span>
            <span style="color:var(--text-muted);font-size:.75rem">${relativeTime(m.created_at)}</span>
            ${tags}
          </div>
          <p style="color:var(--text-secondary);font-size:.875rem;line-height:1.5;white-space:pre-wrap">${(m.content||'').substring(0,400)}${(m.content||'').length > 400 ? '…' : ''}</p>
        </div>
        <button class="btn btn-ghost btn-sm" style="flex-shrink:0" onclick="deleteMemory('${m.id}')">✕</button>
      </div>
    </div>`;
  }).join('');
}

async function deleteMemory(id) {
  if (!confirm('Forget this memory?')) return;
  const res = await api('AICP', `/memory/${id}`, { method: 'DELETE' });
  if (res && !res.error) {
    toast('Memory forgotten', 'success');
    loadMemory(currentMemoryFilter);
  } else {
    toast('Failed to delete memory', 'error');
  }
}

// ─── Event Listeners ──────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  // Try auto-login
  const ok = await tryAutoLogin();
  if (ok) {
    showApp();
    updateInferenceBadge();
  }

  // Login form
  $('login-form').addEventListener('submit', e => {
    e.preventDefault();
    login($('email').value, $('password').value);
  });

  // Nav
  document.querySelectorAll('.nav-item').forEach(btn => {
    btn.addEventListener('click', () => navigateTo(btn.dataset.page));
  });

  // Logout
  $('logout-btn').addEventListener('click', () => {
    if (confirm('Logout?')) logout();
  });

  // Refresh dashboard
  $('refresh-dashboard').addEventListener('click', loadDashboard);

  // Chat
  $('chat-send').addEventListener('click', sendChat);
  $('chat-input').addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat(); }
  });
  $('chat-input').addEventListener('input', function() {
    this.style.height = 'auto';
    this.style.height = Math.min(this.scrollHeight, 120) + 'px';
  });

  // Quick prompts
  document.querySelectorAll('.quick-prompt').forEach(btn => {
    btn.addEventListener('click', () => {
      $('chat-input').value = btn.dataset.prompt;
      sendChat();
    });
  });

  // New session
  $('new-session-btn').addEventListener('click', () => {
    state.sessionId = null;
    $('chat-messages').innerHTML = `<div class="chat-welcome">
      <div class="welcome-icon">⚛</div>
      <h3>New session started</h3>
      <p>What can I help you with?</p>
    </div>`;
  });

  // Pending confirmation
  $('confirm-cmd-btn').addEventListener('click', confirmPending);
  $('reject-cmd-btn').addEventListener('click', rejectPending);

  // Deploy modal
  $('open-deploy-modal').addEventListener('click', openDeployModal);
  $('deploy-service-btn').addEventListener('click', openDeployModal);
  $('close-deploy-modal').addEventListener('click', () => $('deploy-modal').classList.add('hidden'));
  $('cancel-deploy').addEventListener('click', () => $('deploy-modal').classList.add('hidden'));
  $('deploy-form').addEventListener('submit', deployService);
  $('deploy-modal').addEventListener('click', e => { if (e.target === $('deploy-modal')) $('deploy-modal').classList.add('hidden'); });

  // Search
  $('search-btn').addEventListener('click', doSearch);
  $('search-input').addEventListener('keydown', e => { if (e.key === 'Enter') doSearch(); });
  $('ingest-btn').addEventListener('click', ingestText);

  // Agents
  $('run-agent-btn').addEventListener('click', () => {
    // Focus agents list
    const first = document.querySelector('.agent-item');
    if (first) first.click();
    else toast('No agents registered yet', 'info');
  });
  $('refresh-tasks').addEventListener('click', loadTasks);

  // Phase 2: Anomaly Detection
  $('scan-anomalies-btn').addEventListener('click', scanAnomalies);
  $('refresh-anomalies-btn').addEventListener('click', loadAnomalies);

  // Phase 2: Episodic Memory
  $('refresh-memory-btn').addEventListener('click', () => loadMemory(currentMemoryFilter));
  document.querySelectorAll('.memory-filter').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.memory-filter').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      loadMemory(btn.dataset.type);
    });
  });

  // Phase 3: Agent Topology
  $('refresh-topology-btn').addEventListener('click', loadTopologyAgents);
  $('run-orchestration-btn').addEventListener('click', runOrchestration);

  // Phase 3: Orchestrations
  $('refresh-plans-btn').addEventListener('click', loadPlans);

  // Phase 3: Live Events
  $('clear-events-btn').addEventListener('click', () => {
    $('events-feed').innerHTML = '';
  });

  // Phase 4: Federation
  $('add-peer-btn')?.addEventListener('click', () => {
    $('peer-modal').classList.remove('hidden');
  });
  $('close-peer-modal')?.addEventListener('click', () => {
    $('peer-modal').classList.add('hidden');
  });
  $('cancel-peer')?.addEventListener('click', () => {
    $('peer-modal').classList.add('hidden');
  });
  $('peer-form')?.addEventListener('submit', addPeer);
  // Optional if modal bg click needs closing
  $('peer-modal')?.addEventListener('click', e => {
    if (e.target === $('peer-modal')) $('peer-modal').classList.add('hidden');
  });

  // Phase 3: Init SSE stream (always active)
  initSSEStream();

  // Phase 6: CBAL Analytics
  $('run-sql-btn')?.addEventListener('click', runAnalyticsSQL);
  $('trigger-compression-btn')?.addEventListener('click', triggerCBALCompression);
  $('refresh-analytics-btn')?.addEventListener('click', loadAnalytics);
  document.querySelectorAll('.sql-template-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const q = btn.dataset.query;
      const input = $('sql-query-input');
      if (input) {
        input.value = q;
        runAnalyticsSQL();
      }
    });
  });

  $('btn-do-import-url')?.addEventListener('click', async () => {
    const table = $('import-url-table').value.trim();
    const url = $('import-url-path').value.trim();

    if (!table || !url) {
      showImportStatus('Table name and URL are required', 'danger');
      return;
    }

    showImportStatus('Importing dataset...', 'info');

    try {
      const res = await api('CBAL', '/datasets/import', {
        method: 'POST',
        body: JSON.stringify({
          table_name: table,
          source_type: 'url',
          url: url
        })
      });

      if (res && res.success) {
        showImportStatus(res.message, 'success');
        $('import-url-table').value = '';
        $('import-url-path').value = '';
        loadDatasets();
      } else {
        showImportStatus(res.error || 'Failed to import dataset', 'danger');
      }
    } catch (e) {
      showImportStatus(`Error: ${e.message}`, 'danger');
    }
  });

  $('btn-do-import-paste')?.addEventListener('click', async () => {
    const table = $('import-paste-table').value.trim();
    const csvData = $('import-paste-data').value.trim();

    if (!table || !csvData) {
      showImportStatus('Table name and CSV content are required', 'danger');
      return;
    }

    showImportStatus('Importing dataset...', 'info');

    try {
      const res = await api('CBAL', '/datasets/import', {
        method: 'POST',
        body: JSON.stringify({
          table_name: table,
          source_type: 'paste',
          csv_data: csvData
        })
      });

      if (res && res.success) {
        showImportStatus(res.message, 'success');
        $('import-paste-table').value = '';
        $('import-paste-data').value = '';
        loadDatasets();
      } else {
        showImportStatus(res.error || 'Failed to import dataset', 'danger');
      }
    } catch (e) {
      showImportStatus(`Error: ${e.message}`, 'danger');
    }
  });

  // App Compiler UI events
  $('open-register-app-modal')?.addEventListener('click', () => {
    $('app-modal').classList.remove('hidden');
  });
  $('close-app-modal')?.addEventListener('click', () => {
    $('app-modal').classList.add('hidden');
  });
  $('cancel-app')?.addEventListener('click', () => {
    $('app-modal').classList.add('hidden');
  });
  $('refresh-apps')?.addEventListener('click', loadAppsPage);
  $('close-logs-btn')?.addEventListener('click', () => {
    $('build-logs-container').classList.add('hidden');
  });
  $('app-form')?.addEventListener('submit', registerNewApp);
  $('app-modal')?.addEventListener('click', e => {
    if (e.target === $('app-modal')) $('app-modal').classList.add('hidden');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: Agent Topology
// ─────────────────────────────────────────────────────────────────────────────

async function loadTopologyAgents() {
  const grid = $('agents-grid');
  if (!grid) return;
  grid.innerHTML = '<div class="loading-pulse"></div>';
  try {
    const data = await api('ORCH', '/agents');
    const agents = data.agents || [];
    if (agents.length === 0) {
      grid.innerHTML = '<p style="color:var(--text-muted)">No agents registered yet. Start the stack to register agents.</p>';
      return;
    }
    grid.innerHTML = agents.map(renderAgentCard).join('');
  } catch (e) {
    grid.innerHTML = `<p style="color:var(--danger)">Failed to load agents: ${e.message}</p>`;
  }
}

function renderAgentCard(agent) {
  const statusClass = { active: 'dot-active', inactive: 'dot-inactive', busy: 'dot-busy' }[agent.status] || 'dot-inactive';
  const caps = (agent.capabilities || []).map(c => `<span class="cap-tag">${c}</span>`).join('');
  const hb = agent.last_heartbeat ? new Date(agent.last_heartbeat).toLocaleTimeString() : 'Never';
  return `
    <div class="agent-card">
      <div class="agent-card-name">
        <span class="agent-status-dot ${statusClass}"></span>${agent.name || agent.id}
      </div>
      <div class="agent-card-desc">${agent.description || ''}</div>
      <div class="agent-card-caps">${caps}</div>
      <div style="font-size:.7rem;color:var(--text-muted)">
        Load: ${agent.current_load || 0}/${agent.max_concurrency || '?'} &nbsp;·&nbsp; HB: ${hb}
      </div>
    </div>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: Orchestrations
// ─────────────────────────────────────────────────────────────────────────────

async function loadPlans() {
  const list = $('plans-list');
  if (!list) return;
  list.innerHTML = '<div class="loading-pulse"></div>';
  try {
    const data = await api('ORCH', '/plans');
    const plans = data.plans || [];
    if (plans.length === 0) {
      list.innerHTML = '<div class="card glass"><p style="color:var(--text-muted)">No orchestration plans yet. Use Quick Orchestrate on the Agent Topology page.</p></div>';
      return;
    }
    list.innerHTML = plans.map(renderPlanCard).join('');
  } catch (e) {
    list.innerHTML = `<p style="color:var(--danger)">Failed to load plans: ${e.message}</p>`;
  }
}

function renderPlanCard(plan) {
  const statusColors = { completed: '#22c55e', failed: '#ef4444', running: '#f59e0b', pending: '#94a3b8' };
  const color = statusColors[plan.status] || '#94a3b8';
  const steps = (plan.steps || []).map(s =>
    `<span class="step-chip ${s.status}">${stepEmoji(s.status)} ${s.agent_id}</span>`
  ).join('');
  const output = plan.final_output
    ? `<div class="plan-output">${escapeHTML(plan.final_output.slice(0, 600))}${plan.final_output.length > 600 ? '…' : ''}</div>`
    : '';
  const ts = plan.created_at ? new Date(plan.created_at).toLocaleString() : '';
  return `
    <div class="plan-card">
      <div class="plan-header">
        <div class="plan-goal">${escapeHTML(plan.goal || '')}</div>
        <span class="badge" style="background:${color}22;color:${color};border:1px solid ${color}44">${plan.status}</span>
      </div>
      <div class="plan-meta">Plan ID: ${plan.id} &nbsp;·&nbsp; ${ts}</div>
      <div class="plan-steps">${steps}</div>
      ${output}
    </div>`;
}

function stepEmoji(status) {
  return { completed: '✅', failed: '❌', running: '⏳', pending: '⬜' }[status] || '⬜';
}

async function runOrchestration() {
  const goal = $('orch-goal')?.value?.trim();
  if (!goal) { toast('Enter a goal first', 'warning'); return; }

  const btn = $('run-orchestration-btn');
  const statusDiv = $('orch-status');
  btn.disabled = true;
  if (statusDiv) statusDiv.style.display = 'block';

  try {
    const result = await api('ORCH', '/orchestrate', { method: 'POST', body: JSON.stringify({ goal }) });
    toast(`Plan ${result.status}: ${(result.final_output || '').slice(0, 80)}...`, 'success');
    navigateTo('orchestrations');
    loadPlans();
  } catch (e) {
    toast(`Orchestration failed: ${e.message}`, 'error');
  } finally {
    btn.disabled = false;
    if (statusDiv) statusDiv.style.display = 'none';
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: Live Event Stream (SSE from Orchestrator)
// ─────────────────────────────────────────────────────────────────────────────

let sseConnection = null;

function initSSEStream() {
  if (sseConnection) return; // already connected

  const ORCH_BASE = window.location.hostname === 'localhost' || window.location.hostname === ''
    ? 'http://localhost:8006'
    : `${window.location.protocol}//${window.location.hostname}:8006`;

  try {
    sseConnection = new EventSource(`${ORCH_BASE}/events`);

    sseConnection.onmessage = (e) => {
      try {
        const event = JSON.parse(e.data);
        appendEvent(event);
      } catch (_) {}
    };

    sseConnection.onopen = () => {
      const badge = $('sse-status-badge');
      if (badge) { badge.textContent = '🟢 Connected'; badge.className = 'badge badge-green'; }
    };

    sseConnection.onerror = () => {
      const badge = $('sse-status-badge');
      if (badge) { badge.textContent = '🔴 Disconnected'; badge.className = 'badge badge-red'; }
      sseConnection = null;
      // Reconnect after 10 seconds
      setTimeout(initSSEStream, 10000);
    };
  } catch (_) {
    // SSE not available (e.g. not on the orchestrator page yet)
  }
}

function appendEvent(event) {
  const feed = $('events-feed');
  if (!feed) return;

  const MAX_EVENTS = 200;
  while (feed.children.length >= MAX_EVENTS) {
    feed.removeChild(feed.lastChild);
  }

  const time = event.timestamp ? new Date(event.timestamp).toLocaleTimeString() : new Date().toLocaleTimeString();
  const type = event.type || 'unknown';
  const from = event.from ? `from: ${event.from}` : '';
  const to = event.to ? ` → ${event.to}` : '';

  const row = document.createElement('div');
  row.className = 'event-row';
  row.innerHTML = `
    <span class="event-time">${time}</span>
    <span class="event-type ${escapeHTML(type)}">${escapeHTML(type)}</span>
    <span class="event-body">${escapeHTML(from + to)} ${event.trace_id ? `<span style="opacity:.5;font-size:.68rem">${event.trace_id.slice(0, 16)}</span>` : ''}</span>`;
  feed.prepend(row);
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: Wire up page navigation for new pages
// ─────────────────────────────────────────────────────────────────────────────

// Patch navigateTo to load Phase 3 page data on navigation
const _origNavigateTo = typeof navigateTo === 'function' ? navigateTo : null;
window.addEventListener('DOMContentLoaded', () => {
  // Intercept nav clicks for Phase 3 pages
  document.addEventListener('click', e => {
    const btn = e.target.closest('[data-page]');
    if (!btn) return;
    const page = btn.dataset.page;
    if (page === 'topology') loadTopologyAgents();
    if (page === 'orchestrations') loadPlans();
    if (page === 'events') initSSEStream();
    if (page === 'federation') loadFederation();
    if (page === 'analytics') loadAnalytics();
  });
});

function escapeHTML(str) {
  if (!str) return '';
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 4: Federation & Edge
// ─────────────────────────────────────────────────────────────────────────────

async function loadFederation() {
  loadPeers();
  loadRemediationLog();
}

async function loadPeers() {
  const tbody = $('peers-list-tbody');
  const marketTbody = $('federated-marketplace-tbody');
  if (!tbody) return;
  tbody.innerHTML = '<tr><td colspan="5" class="empty-row"><div class="loading-pulse"></div></td></tr>';
  
  try {
    const data = await api('ORCH', '/peers');
    const peers = data.peers || [];
    
    if (peers.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" class="empty-row">No trusted peers connected. Add one to form a federation.</td></tr>';
      if (marketTbody) marketTbody.innerHTML = '<tr><td colspan="5" class="empty-row">No peers available</td></tr>';
      return;
    }
    
    tbody.innerHTML = peers.map(p => {
      const statusClass = p.status === 'online' ? 'online' : 'offline';
      return `
        <tr>
          <td><span class="status-dot ${statusClass}"></span> ${p.status || 'unknown'}</td>
          <td><strong>${escapeHTML(p.name)}</strong></td>
          <td><span style="font-family:'JetBrains Mono',monospace;font-size:.8rem">${escapeHTML(p.url)}</span></td>
          <td><span style="font-family:'JetBrains Mono',monospace;font-size:.8rem">${escapeHTML(p.did)}</span></td>
          <td>
            <button class="btn btn-ghost btn-sm" onclick="removePeer('${p.did}')">Remove</button>
          </td>
        </tr>
      `;
    }).join('');

    if (marketTbody) {
      let marketHtml = '';
      peers.forEach(p => {
        if (p.advertised_agents && p.advertised_agents.length > 0) {
          marketHtml += p.advertised_agents.map(a => `
            <tr>
              <td><strong>${escapeHTML(a.name)}</strong></td>
              <td>${escapeHTML(p.name)}</td>
              <td>${escapeHTML(a.description || '')}</td>
              <td>${(a.capabilities || []).map(c => `<span class="cap-badge">${escapeHTML(c)}</span>`).join('')}</td>
              <td>
                <button class="btn btn-primary btn-sm" onclick="delegateToPeer('${p.did}', '${a.name}')">Delegate Task</button>
              </td>
            </tr>
          `).join('');
        }
      });
      marketTbody.innerHTML = marketHtml || '<tr><td colspan="5" class="empty-row">No agents advertised by connected peers.</td></tr>';
    }

  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="5" class="empty-row" style="color:var(--danger)">Error: ${e.message}</td></tr>`;
  }
}

async function addPeer(e) {
  e.preventDefault();
  const name = $('peer-name').value.trim();
  const did = $('peer-did').value.trim();
  const url = $('peer-url').value.trim();
  
  const res = await api('ORCH', '/peers', {
    method: 'POST',
    body: JSON.stringify({ name, did, url })
  });
  
  if (res && !res.error) {
    toast(`Peer "${name}" trust established`, 'success');
    $('peer-modal').classList.add('hidden');
    $('peer-form').reset();
    loadPeers();
  } else {
    toast(res?.error || 'Failed to establish trust with peer', 'error');
  }
}

async function removePeer(did) {
  if (!confirm(`Remove peer ${did}? This will sever trust.`)) return;
  toast('Not implemented in prototype', 'info');
}

async function delegateToPeer(peerDid, agentName) {
  const input = prompt(`Delegate task to remote agent ${agentName}:\nEnter task details:`);
  if (!input) return;
  
  const res = await api('ORCH', '/peers/delegate', {
    method: 'POST',
    body: JSON.stringify({ peer_did: peerDid, agent_name: agentName, input })
  });
  
  if (res && res.task_id) {
    toast(`Task delegated securely. ID: ${res.task_id}`, 'success');
  } else {
    toast(res?.error || 'Delegation failed', 'error');
  }
}

async function loadRemediationLog() {
  const tl = $('remediation-audit-log');
  if (!tl) return;
  
  tl.innerHTML = '<div class="loading-pulse"></div>';
  
  const audit = await api('COL', '/audit');
  if (!audit) {
    tl.innerHTML = '<p style="color:var(--danger)">Failed to fetch audit log</p>';
    return;
  }
  
  // Filter for remediation events
  const recoveries = audit.filter(a => a.operator === 'remediation_agent' || (a.action && a.action.startsWith('recovery:')));
  
  if (recoveries.length === 0) {
    tl.innerHTML = '<div class="card glass"><p style="color:var(--text-muted);text-align:center;padding:1rem">No self-healing events recorded yet. Infrastructure is stable.</p></div>';
    return;
  }
  
  tl.innerHTML = recoveries.slice(0, 15).map(r => {
    const time = new Date(r.ts).toLocaleString();
    return `
      <div style="display:flex; gap:1rem; margin-bottom:1rem">
        <div style="display:flex; flex-direction:column; align-items:center; min-width:20px">
          <div style="width:12px; height:12px; border-radius:50%; background:var(--green); margin-top:4px"></div>
          <div style="flex:1; width:2px; background:rgba(255,255,255,0.1); margin-top:4px; margin-bottom:4px"></div>
        </div>
        <div class="card glass" style="flex:1; padding:0.8rem 1rem">
          <div style="display:flex; justify-content:space-between; margin-bottom:0.4rem">
            <strong style="color:var(--green); font-family:'JetBrains Mono',monospace; font-size:0.9rem">${escapeHTML(r.action)}</strong>
            <span style="font-size:0.75rem; color:var(--text-muted)">${time}</span>
          </div>
          <div style="font-size:0.85rem">Resource: <code style="padding:0.2rem 0.4rem; background:rgba(0,0,0,0.2); border-radius:4px">${escapeHTML(r.resource)}</code></div>
          <div style="font-size:0.8rem; color:var(--text-secondary); margin-top:0.4rem">${escapeHTML(r.detail || '')}</div>
        </div>
      </div>
    `;
  }).join('');
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 6: CBAL Analytics
// ─────────────────────────────────────────────────────────────────────────────

async function loadAnalytics() {
  loadDatasets();
  const risksEl = $('cbal-node-risks');
  if (!risksEl) return;

  risksEl.innerHTML = '<div class="loading-pulse"></div>';

  try {
    const data = await api('CBAL', '/insights');
    if (!data) {
      risksEl.innerHTML = '<p style="color:var(--text-muted)">CBAL service unreachable or returned no data.</p>';
      return;
    }

    // Render risk scores
    let html = '';
    const scores = data.topology_risk_scores || {};
    const anomalies = data.unresolved_anomalies || [];

    if (Object.keys(scores).length === 0 && anomalies.length === 0) {
      html = '<div style="padding:1rem;text-align:center;color:var(--text-muted)">No active anomalies or risks detected on nodes.</div>';
    } else {
      if (Object.keys(scores).length > 0) {
        html += '<h4 style="margin-bottom:0.5rem;color:var(--text-primary)">Node Risk Scores</h4>';
        for (const [node, score] of Object.entries(scores)) {
          const percentage = Math.round(score * 100);
          const color = score > 0.7 ? 'var(--red)' : score > 0.4 ? 'var(--amber)' : 'var(--green)';
          html += `
            <div style="margin-bottom:0.75rem">
              <div style="display:flex;justify-content:space-between;font-size:0.8rem;margin-bottom:0.25rem">
                <span>Node: <strong>${escapeHTML(node)}</strong></span>
                <span style="color:${color};font-weight:700">${percentage}% Risk</span>
              </div>
              <div style="height:6px;background:rgba(255,255,255,0.05);border-radius:3px;overflow:hidden">
                <div style="height:100%;width:${percentage}%;background:${color};border-radius:3px"></div>
              </div>
            </div>`;
        }
      }

      if (anomalies.length > 0) {
        html += '<h4 style="margin-top:1rem;margin-bottom:0.5rem;color:var(--text-primary)">Recent Unresolved Anomalies</h4>';
        html += anomalies.map(a => `
          <div style="padding:0.5rem 0.75rem;background:rgba(239,68,68,0.05);border:1px solid rgba(239,68,68,0.15);border-radius:6px;margin-bottom:0.5rem;font-size:0.8rem">
            <div style="display:flex;justify-content:space-between;margin-bottom:0.25rem">
              <strong style="color:#ef4444">${escapeHTML(a.severity)}: ${escapeHTML(a.type)}</strong>
              <span style="color:var(--text-muted);font-size:0.75rem">${relativeTime(a.timestamp)}</span>
            </div>
            <div style="color:var(--text-secondary)">${escapeHTML(a.description)}</div>
            <div style="font-size:0.75rem;color:var(--text-muted);margin-top:0.25rem">Node: ${escapeHTML(a.node_id)}</div>
          </div>
        `).join('');
      }
    }

    risksEl.innerHTML = html;
  } catch (e) {
    risksEl.innerHTML = `<p style="color:var(--danger)">Failed to load CBAL insights: ${e.message}</p>`;
  }
}

async function runAnalyticsSQL() {
  const queryInput = $('sql-query-input');
  const errorEl = $('sql-query-error');
  const thead = $('sql-results-thead');
  const tbody = $('sql-results-tbody');
  const runBtn = $('run-sql-btn');

  if (!queryInput || !tbody) return;

  const query = queryInput.value.trim();
  if (!query) { toast('Enter a query first', 'warning'); return; }

  runBtn.disabled = true;
  runBtn.textContent = 'Running...';
  errorEl.classList.add('hidden');
  tbody.innerHTML = '<tr><td class="empty-row"><div class="loading-pulse"></div></td></tr>';

  try {
    const data = await api('CBAL', '/query', {
      method: 'POST',
      body: JSON.stringify({ query })
    });

    if (!data) {
      throw new Error('No response from CBAL service');
    }
    if (data.error) {
      throw new Error(data.error);
    }

    const cols = data.columns || [];
    const rows = data.rows || [];

    // Render headers
    if (cols.length === 0) {
      thead.innerHTML = '<tr><th>Result</th></tr>';
    } else {
      thead.innerHTML = `<tr>${cols.map(c => `<th>${escapeHTML(c)}</th>`).join('')}</tr>`;
    }

    // Render rows
    if (rows.length === 0) {
      tbody.innerHTML = `<tr><td colspan="${cols.length || 1}" class="empty-row">Query executed successfully. 0 rows returned.</td></tr>`;
    } else {
      tbody.innerHTML = rows.map(row => `
        <tr>
          ${cols.map(c => {
            const val = row[c];
            const strVal = val === null || val === undefined ? 'NULL' : typeof val === 'object' ? JSON.stringify(val) : String(val);
            return `<td>${escapeHTML(strVal)}</td>`;
          }).join('')}
        </tr>
      `).join('');
    }
  } catch (e) {
    errorEl.textContent = e.message;
    errorEl.classList.remove('hidden');
    thead.innerHTML = '<tr><th>Error</th></tr>';
    tbody.innerHTML = `<tr><td class="empty-row" style="color:var(--danger)">Query failed. Check the error message above.</td></tr>`;
  } finally {
    runBtn.disabled = false;
    runBtn.textContent = '▶ Run Query';
  }
}

async function triggerCBALCompression() {
  const btn = $('trigger-compression-btn');
  const statusEl = $('cbal-compression-status');
  if (!btn || !statusEl) return;

  btn.disabled = true;
  btn.textContent = 'Compressing...';
  statusEl.innerHTML = '<div class="loading-pulse"></div><p style="text-align:center;margin-top:0.5rem;font-size:0.8rem">Executing CBAL summarization and pruning...</p>';

  try {
    const data = await api('CBAL', '/compress', { method: 'POST' });
    if (data && data.compressed) {
      statusEl.innerHTML = `
        <div style="color:var(--green);font-weight:600;margin-bottom:0.5rem">✓ Compression Cycle Completed Successfully</div>
        <div style="font-size:0.8rem;color:var(--text-muted);margin-bottom:0.5rem">Pruned: ${escapeHTML(data.records_pruned)}</div>
        <div style="padding:0.75rem;background:rgba(255,255,255,0.02);border-radius:6px;border:1px solid rgba(255,255,255,0.05)">
          <strong>Semantic Memory:</strong><br/>
          <span style="font-style:italic;color:var(--text-secondary)">"${escapeHTML(data.semantic_memory)}"</span>
        </div>
      `;
      toast('Compression cycle complete', 'success');
      loadAnalytics(); // Refresh insights/anomalies (pruned records will disappear)
    } else {
      throw new Error(data?.error || 'Failed to compress');
    }
  } catch (e) {
    statusEl.innerHTML = `<div style="color:var(--red);font-weight:600">✕ Compression Failed</div><div style="margin-top:0.5rem;font-size:0.8rem;color:var(--text-muted)">${escapeHTML(e.message)}</div>`;
    toast('Compression failed', 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = '⚡ Compress Telemetry';
  }
}

// ─── App Compiler & Deployer ──────────────────────────────────
async function loadAppsPage() {
  const appsGrid = $('apps-list-grid');
  const buildsTbody = $('builds-history-tbody');
  if (!appsGrid || !buildsTbody) return;

  const [apps, builds] = await Promise.all([
    api('COL', '/apps/deployments'),
    api('COL', '/apps/builds'),
  ]);

  // 1. Render Apps Grid
  if (!apps || apps.length === 0) {
    appsGrid.innerHTML = '<div class="search-empty">No applications registered. Click "+ Register Application" to start.</div>';
  } else {
    appsGrid.innerHTML = apps.map(app => {
      let badgeClass = 'badge-blue';
      if (app.platform === 'android') badgeClass = 'badge-amber';
      if (app.platform === 'ios') badgeClass = 'badge-purple';

      let statusBadge = '';
      if (app.status === 'building') statusBadge = '<span class="status-dot unknown" style="animation:thinking 1.2s infinite"></span> Building';
      else if (app.status === 'success') statusBadge = '<span class="status-dot online"></span> Operational';
      else if (app.status === 'failed') statusBadge = '<span class="status-dot offline"></span> Build Failed';
      else statusBadge = '<span class="status-dot neutral"></span> Idle';

      const repoName = app.git_repo.split('/').pop().replace('.git', '');

      // Resolve Web App URL
      let webAppURL = `/api/col/apps/web/${app.id}/`;
      let domainInfo = '';
      if (app.platform === 'web') {
        const hostIP = window.location.hostname;
        const hostPort = window.location.port ? `:${window.location.port}` : '';
        if (app.domain) {
          webAppURL = `http://${app.domain}${hostPort}`;
          domainInfo = `<div><span>Domain:</span> <span style="font-family:monospace;font-size:0.8rem">${app.domain}</span></div>`;
        } else if (hostIP !== 'localhost' && hostIP !== '127.0.0.1') {
          webAppURL = `http://${app.id}.${hostIP}.nip.io${hostPort}`;
          domainInfo = `<div><span>Domain:</span> <span style="font-family:monospace;font-size:0.8rem">${app.id}.${hostIP}.nip.io</span></div>`;
        } else {
          domainInfo = `<div><span>Domain:</span> <span style="font-family:monospace;font-size:0.8rem">${app.id}.[IP].nip.io</span></div>`;
        }
      }

      // Open Web App button if platform is web and status is success
      const actionButton = app.platform === 'web' && app.status === 'success'
        ? `<a href="${webAppURL}" target="_blank" class="btn btn-ghost btn-sm" style="margin-top:10px;display:inline-flex">🚀 Open Web App</a>`
        : '';

      return `
        <div class="service-card glass">
          <div class="service-card-header">
            <span class="service-card-name">${app.name}</span>
            <span class="badge ${badgeClass}">${app.platform}</span>
          </div>
          <div class="service-card-body" style="margin-bottom:12px">
            <div><span>Repo:</span> <span>${repoName} (${app.branch})</span></div>
            <div><span>Status:</span> <span>${statusBadge}</span></div>
            ${domainInfo}
          </div>
          <div class="service-card-actions">
            <button class="btn btn-primary btn-sm" onclick="triggerAppBuild('${app.id}')" ${app.status === 'building' ? 'disabled' : ''}>
              ⚡ Compile & Deploy
            </button>
            ${actionButton}
          </div>
        </div>
      `;
    }).join('');
  }

  // 2. Render Builds History
  if (!builds || builds.length === 0) {
    buildsTbody.innerHTML = '<tr><td colspan="7" class="empty-row">No compilation builds recorded.</td></tr>';
  } else {
    buildsTbody.innerHTML = builds.map(b => {
      let statusClass = 'task-status-pending';
      if (b.status === 'building') statusClass = 'task-status-running';
      else if (b.status === 'success') statusClass = 'task-status-completed';
      else if (b.status === 'failed') statusClass = 'task-status-failed';

      // Download link if success
      const downloadLink = b.status === 'success' && b.artifact_file
        ? `<a href="/api/col/apps/downloads/${b.id}/${b.artifact_file}" class="btn btn-ghost btn-sm" style="padding:4px 8px;font-size:0.75rem">📥 Download ${b.artifact_file.split('.').pop().toUpperCase()}</a>`
        : '—';

      return `
        <tr>
          <td><span style="font-family:monospace;font-size:0.75rem">${b.id.substring(0, 16)}</span></td>
          <td><strong>${b.app_name}</strong></td>
          <td><span class="badge ${b.platform === 'web' ? 'badge-blue' : b.platform === 'android' ? 'badge-amber' : 'badge-purple'}">${b.platform}</span></td>
          <td><span class="task-status-badge ${statusClass}">${b.status}</span></td>
          <td>${formatDate(b.created_at)}</td>
          <td>${downloadLink}</td>
          <td>
            <button class="btn btn-ghost btn-sm" onclick="viewBuildLogs('${b.id}', '${b.app_name}')" style="padding:4px 8px;font-size:0.75rem">
              📝 Logs
            </button>
          </td>
        </tr>
      `;
    }).join('');
  }
}

// Trigger Build API
async function triggerAppBuild(appID) {
  toast('Triggering compilation build...', 'info');
  const res = await api('COL', '/apps/build', {
    method: 'POST',
    body: JSON.stringify({ app_id: appID }),
  });
  if (res && res.build_id) {
    toast('Build pipeline started!', 'success');
    loadAppsPage();
    // Open logs terminal automatically
    viewBuildLogs(res.build_id, 'Build Triggered');
  } else {
    toast('Failed to trigger build pipeline', 'error');
  }
}

// View Build Logs
async function viewBuildLogs(buildID, appName) {
  const container = $('build-logs-container');
  const title = $('build-logs-title');
  const pre = $('build-logs-pre');
  if (!container || !title || !pre) return;

  container.classList.remove('hidden');
  title.textContent = `Streaming Build Output: ${appName} (${buildID.substring(0,16)})`;
  pre.textContent = 'Loading live terminal compilation output stream...';

  // Perform initial fetch
  const fetchLogs = async () => {
    const res = await fetch(`/api/col/apps/builds/${buildID}/logs`, {
      headers: state.token ? { 'Authorization': `Bearer ${state.token}` } : {},
    });
    if (res.ok) {
      const logs = await res.text();
      pre.textContent = logs;
      pre.scrollTop = pre.scrollHeight; // Auto-scroll
      if (logs.includes('[BUILDING LIVE]') || !logs.includes('Build finished')) {
        // Continue polling if still building
        setTimeout(fetchLogs, 1500);
      }
    }
  };
  
  fetchLogs();
}

// Register App Submit handler
async function registerNewApp(e) {
  e.preventDefault();
  const name = $('app-name-input').value;
  const platform = $('app-platform-input').value;
  const repo = $('app-repo-input').value;
  const branch = $('app-branch-input').value;
  const domain = $('app-domain-input').value;

  const res = await api('COL', '/apps/register', {
    method: 'POST',
    body: JSON.stringify({ name, platform, git_repo: repo, branch, domain }),
  });

  if (res && res.id) {
    toast('Application registered successfully!', 'success');
    $('app-modal').classList.add('hidden');
    $('app-form').reset();
    loadAppsPage();
  } else {
    toast('Failed to register application', 'error');
  }
}

// Expose globals for inline HTML triggers
window.triggerAppBuild = triggerAppBuild;
window.viewBuildLogs = viewBuildLogs;

async function loadDatasets() {
  const listEl = $('datasets-explorer-list');
  const countBadge = $('datasets-count-badge');
  if (!listEl) return;

  try {
    const data = await api('CBAL', '/datasets');
    if (!data || data.error) {
      listEl.innerHTML = '<p style="color:var(--danger);font-size:0.75rem">Failed to load datasets.</p>';
      return;
    }

    countBadge.textContent = data.length;

    if (data.length === 0) {
      listEl.innerHTML = '<div style="padding:1rem;text-align:center;color:var(--text-muted);font-size:0.8rem">No datasets available.</div>';
      return;
    }

    listEl.innerHTML = data.map(db => {
      const isSystem = ['metric_history', 'orchestration_history', 'anomaly_history'].includes(db.table_name);
      
      const columnsList = db.columns.map(c => `
        <div style="display:flex;justify-content:space-between;padding:2px 0;font-size:0.75rem;color:var(--text-muted)">
          <span style="font-family:monospace">${escapeHTML(c.name)}</span>
          <span style="opacity:0.7;font-style:italic">${escapeHTML(c.type.toLowerCase())}</span>
        </div>
      `).join('');

      const deleteBtn = isSystem ? '' : `
        <button class="btn btn-ghost btn-sm" style="padding:2px 6px;color:#ef4444" onclick="event.stopPropagation(); deleteDataset('${escapeHTML(db.table_name)}')">
          🗑️
        </button>
      `;

      return `
        <div class="dataset-item" style="border:1px solid var(--border);border-radius:6px;background:rgba(0,0,0,0.01);margin-bottom:0.4rem;cursor:pointer" onclick="toggleDatasetDetails('${escapeHTML(db.table_name)}')">
          <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 10px">
            <div style="display:flex;flex-direction:column;gap:2px">
              <span style="font-weight:600;font-size:0.8rem;font-family:monospace">${escapeHTML(db.table_name)}</span>
              <span style="font-size:0.7rem;color:var(--text-muted)">${db.row_count} rows</span>
            </div>
            <div style="display:flex;gap:4px">
              <button class="btn btn-ghost btn-sm" style="padding:2px 6px" title="Query table" onclick="event.stopPropagation(); selectDatasetForQuery('${escapeHTML(db.table_name)}')">
                🔎
              </button>
              ${deleteBtn}
            </div>
          </div>
          <div id="dataset-details-${escapeHTML(db.table_name)}" class="hidden" style="padding:0 10px 10px 10px;border-top:1px solid rgba(0,0,0,0.03);background:rgba(0,0,0,0.01)">
            <div style="font-size:0.7rem;font-weight:600;margin:6px 0 4px 0;color:var(--text-secondary)">Columns</div>
            ${columnsList}
          </div>
        </div>
      `;
    }).join('');

  } catch (e) {
    listEl.innerHTML = `<p style="color:var(--danger);font-size:0.75rem">Error: ${e.message}</p>`;
  }
}

function showImportStatus(msg, type) {
  const el = $('import-status-msg');
  if (!el) return;
  el.textContent = msg;
  el.className = 'import-status-msg';
  el.classList.remove('hidden');
  if (type === 'danger') {
    el.style.background = 'rgba(239, 68, 68, 0.1)';
    el.style.border = '1px solid rgba(239, 68, 68, 0.2)';
    el.style.color = '#ef4444';
  } else if (type === 'success') {
    el.style.background = 'rgba(34, 197, 94, 0.1)';
    el.style.border = '1px solid rgba(34, 197, 94, 0.2)';
    el.style.color = '#22c55e';
  } else {
    el.style.background = 'rgba(9, 9, 11, 0.05)';
    el.style.border = '1px solid var(--border)';
    el.style.color = 'var(--text-primary)';
  }
}

window.toggleDatasetDetails = function(name) {
  const el = document.getElementById(`dataset-details-${name}`);
  if (el) el.classList.toggle('hidden');
};

window.selectDatasetForQuery = function(name) {
  const input = $('sql-query-input');
  if (input) {
    input.value = `SELECT * FROM ${name} LIMIT 10`;
    runAnalyticsSQL();
  }
};

window.deleteDataset = async function(name) {
  if (!confirm(`Are you sure you want to delete the dataset '${name}'?`)) return;

  try {
    const res = await api('CBAL', `/datasets/${name}`, {
      method: 'DELETE'
    });
    if (res && res.success) {
      toast(`Dataset '${name}' deleted successfully`, 'success');
      loadDatasets();
    } else {
      toast(res.error || 'Failed to delete dataset', 'error');
    }
  } catch (e) {
    toast(`Error: ${e.message}`, 'error');
  }
};

window.switchImportTab = function(tab) {
  if (tab === 'url') {
    $('tab-import-url').classList.add('active');
    $('tab-import-url').style.borderBottom = '2px solid var(--text-primary)';
    $('tab-import-paste').classList.remove('active');
    $('tab-import-paste').style.borderBottom = 'none';
    $('import-form-url').classList.remove('hidden');
    $('import-form-paste').classList.add('hidden');
  } else {
    $('tab-import-paste').classList.add('active');
    $('tab-import-paste').style.borderBottom = '2px solid var(--text-primary)';
    $('tab-import-url').classList.remove('active');
    $('tab-import-url').style.borderBottom = 'none';
    $('import-form-paste').classList.remove('hidden');
    $('import-form-url').classList.add('hidden');
  }
  const msgEl = $('import-status-msg');
  if (msgEl) msgEl.classList.add('hidden');
};
