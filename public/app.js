/**
 * easyup client.
 *
 * Files are sliced into fixed-size chunks and pushed with a small pool of
 * parallel PUTs. Every chunk is independent, so a failed or aborted chunk is
 * simply re-sent, and an upload can be resumed after a reload by asking the
 * server which chunks it already holds.
 */

const $ = (id) => document.getElementById(id);
const MiB = 1024 * 1024;

const state = {
  config: { chunkSize: 8 * MiB, maxFileSize: 50 * 1024 ** 3, concurrency: 4, authRequired: false },
  token: localStorage.getItem('easyup.token') || '',
  queue: [],
};

/* ----------------------------------------------------------------- format */

function formatBytes(bytes, digits) {
  if (!Number.isFinite(bytes)) return '—';
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const precision = digits ?? (value < 10 ? 1 : 0);
  return `${value.toFixed(precision)} ${units[unit]}`;
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return '—';
  if (seconds < 60) return `${Math.ceil(seconds)}s`;
  const mins = Math.floor(seconds / 60);
  if (mins < 60) return `${mins}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(mins / 60)}h ${mins % 60}m`;
}

function formatDate(iso) {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

function iconFor(name, type = '') {
  const ext = (name.split('.').pop() || '').toLowerCase();
  if (type.startsWith('video/') || ['mp4', 'mkv', 'mov', 'avi', 'webm'].includes(ext)) return '🎬';
  if (type.startsWith('image/') || ['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'heic'].includes(ext)) return '🖼️';
  if (type.startsWith('audio/') || ['mp3', 'wav', 'flac', 'aac', 'ogg'].includes(ext)) return '🎵';
  if (['zip', 'tar', 'gz', 'bz2', 'xz', '7z', 'rar', 'iso', 'dmg'].includes(ext)) return '📦';
  if (ext === 'pdf') return '📕';
  if (['csv', 'xlsx', 'xls', 'parquet'].includes(ext)) return '📊';
  if (['txt', 'md', 'log', 'json', 'xml', 'yaml', 'yml'].includes(ext)) return '📄';
  return '💾';
}

/* ------------------------------------------------------------------ toast */

function toast(message, kind = '') {
  const node = document.createElement('div');
  node.className = `toast ${kind}`.trim();
  node.textContent = message;
  $('toasts').append(node);
  setTimeout(() => {
    node.classList.add('leaving');
    node.addEventListener('animationend', () => node.remove(), { once: true });
  }, kind === 'error' ? 6000 : 3200);
}

/* -------------------------------------------------------------------- api */

function authHeaders(extra = {}) {
  return state.token ? { ...extra, authorization: `Bearer ${state.token}` } : extra;
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    ...options,
    headers: authHeaders(options.body ? { 'content-type': 'application/json', ...options.headers } : options.headers),
  });
  const text = await res.text();
  const body = text ? JSON.parse(text) : null;
  if (!res.ok) {
    const err = new Error(body?.message || `Request failed (${res.status})`);
    err.status = res.status;
    err.code = body?.error;
    throw err;
  }
  return body;
}

/** Adds the token to a plain browser link (download anchors cannot set headers). */
function withToken(url) {
  if (!state.token) return url;
  return `${url}${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(state.token)}`;
}

/* --------------------------------------------------------- resume registry */

/**
 * Maps a locally-chosen file to a server-side upload id, so re-picking the same
 * file after a reload continues where it stopped instead of restarting.
 */
const registry = {
  key: 'easyup.resume',
  read() {
    try {
      return JSON.parse(localStorage.getItem(this.key) || '{}');
    } catch {
      return {};
    }
  },
  write(map) {
    try {
      localStorage.setItem(this.key, JSON.stringify(map));
    } catch { /* storage may be unavailable; resume is best-effort */ }
  },
  fingerprint: (file) => `${file.name}|${file.size}|${file.lastModified}`,
  get(file) { return this.read()[this.fingerprint(file)]; },
  set(file, id) {
    const map = this.read();
    map[this.fingerprint(file)] = { id, at: Date.now() };
    // Keep the registry small and free of stale entries (> 7 days).
    const cutoff = Date.now() - 7 * 24 * 3600 * 1000;
    for (const [key, value] of Object.entries(map)) {
      if (!value?.at || value.at < cutoff) delete map[key];
    }
    this.write(map);
  },
  clear(file) {
    const map = this.read();
    delete map[this.fingerprint(file)];
    this.write(map);
  },
};

/* ----------------------------------------------------------------- upload */

const RETRYABLE_STATUS = new Set([0, 408, 423, 425, 429, 500, 502, 503, 504, 507]);
const MAX_ATTEMPTS = 5;

class Upload {
  constructor(file) {
    this.file = file;
    this.id = null;
    this.chunkSize = state.config.chunkSize;
    this.chunkCount = 1;
    this.done = new Set();       // chunk indices confirmed by the server
    this.pending = [];           // chunk indices still to send
    this.inflight = new Map();   // index -> bytes sent so far (for live progress)
    this.xhrs = new Map();
    this.state = 'pending';
    this.resumed = false;
    this.error = '';
    this.doneBytes = 0;
    this.speed = 0;
    this.lastSample = { at: performance.now(), bytes: 0 };
    this.node = null;
    this.buildNode();
  }

  get sizeOf() {
    return (index) => Math.min(this.chunkSize, this.file.size - index * this.chunkSize);
  }

  /** Confirmed bytes plus bytes currently in flight, for a smooth bar. */
  get sentBytes() {
    let total = this.doneBytes;
    for (const loaded of this.inflight.values()) total += loaded;
    return Math.min(total, this.file.size);
  }

  get percent() {
    if (this.file.size === 0) return this.state === 'done' ? 100 : 0;
    return Math.min(100, (this.sentBytes / this.file.size) * 100);
  }

  /* ---------------------------------------------------------------- setup */

  async start() {
    if (this.file.size > state.config.maxFileSize) {
      this.fail(`Too large — the limit is ${formatBytes(state.config.maxFileSize)}.`);
      return;
    }
    this.setState('uploading');
    try {
      await this.reserve();
    } catch (err) {
      this.fail(err.message);
      return;
    }
    this.render();
    await this.run();
  }

  /** Resumes a matching server-side upload if there is one, else creates it. */
  async reserve() {
    const saved = registry.get(this.file);
    if (saved?.id) {
      const status = await api(`/api/uploads/${encodeURIComponent(saved.id)}`).catch(() => null);
      if (status && status.size === this.file.size) {
        this.adopt(status);
        const bitmap = Uint8Array.from(atob(status.bitmap), (c) => c.charCodeAt(0));
        for (let i = 0; i < this.chunkCount; i += 1) {
          if (bitmap[i]) {
            this.done.add(i);
            this.doneBytes += this.sizeOf(i);
          }
        }
        this.resumed = this.done.size > 0;
        this.queueRemaining();
        if (this.resumed) toast(`Resuming ${this.file.name} at ${Math.round(this.percent)}%`);
        return;
      }
      registry.clear(this.file);
    }

    const upload = await api('/api/uploads', {
      method: 'POST',
      body: JSON.stringify({
        filename: this.file.name,
        size: this.file.size,
        contentType: this.file.type || 'application/octet-stream',
      }),
    });
    this.adopt(upload);
    registry.set(this.file, upload.id);
    this.queueRemaining();
  }

  adopt(upload) {
    this.id = upload.id;
    this.chunkSize = upload.chunkSize;
    this.chunkCount = upload.chunkCount;
  }

  queueRemaining() {
    this.pending = [];
    for (let i = 0; i < this.chunkCount; i += 1) {
      if (!this.done.has(i)) this.pending.push(i);
    }
  }

  /* -------------------------------------------------------------- workers */

  async run() {
    const workers = Math.min(state.config.concurrency, Math.max(1, this.pending.length));
    await Promise.all(Array.from({ length: workers }, () => this.worker()));

    // A graceful pause ends here, once every in-flight chunk has drained.
    if (this.state === 'pausing') {
      this.setState('paused');
      this.note = '';
      this.render();
      return;
    }
    if (this.state !== 'uploading') return;
    if (this.done.size < this.chunkCount) {
      if (!this.error) this.fail('Upload stalled with chunks missing.');
      return;
    }
    await this.finish();
  }

  async worker() {
    while (this.state === 'uploading') {
      const index = this.pending.shift();
      if (index === undefined) return;
      try {
        await this.sendChunk(index);
        this.done.add(index);
        this.doneBytes += this.sizeOf(index);
      } catch (err) {
        this.inflight.delete(index);
        if (err.name === 'AbortError') {
          this.pending.unshift(index); // paused or cancelled: keep it for later
          return;
        }
        if (err.code === 'upload_not_found') {
          registry.clear(this.file);
          this.fail('The server no longer has this upload. Add the file again to restart.');
          return;
        }
        this.fail(err.message);
        return;
      }
      this.render();
    }
  }

  /** Sends one chunk, retrying transient failures with exponential backoff. */
  async sendChunk(index) {
    let attempt = 0;
    for (;;) {
      attempt += 1;
      try {
        await this.putChunk(index);
        return;
      } catch (err) {
        this.inflight.delete(index);
        const retryable = RETRYABLE_STATUS.has(err.status ?? 0);
        if (err.name === 'AbortError' || !retryable || attempt >= MAX_ATTEMPTS) throw err;
        const backoff = Math.min(8000, 400 * 2 ** (attempt - 1)) * (0.7 + Math.random() * 0.6);
        this.note = `retrying chunk ${index + 1} (${attempt}/${MAX_ATTEMPTS})`;
        this.render();
        await new Promise((resolve) => setTimeout(resolve, backoff));
        if (this.state !== 'uploading') throw Object.assign(new Error('aborted'), { name: 'AbortError' });
      }
    }
  }

  /** XHR rather than fetch: it reports upload progress, which fetch cannot. */
  putChunk(index) {
    return new Promise((resolve, reject) => {
      const start = index * this.chunkSize;
      const blob = this.file.slice(start, Math.min(start + this.chunkSize, this.file.size));
      const xhr = new XMLHttpRequest();
      this.xhrs.set(index, xhr);

      xhr.open('PUT', `/api/uploads/${encodeURIComponent(this.id)}/chunks/${index}`);
      xhr.responseType = 'text';
      if (state.token) xhr.setRequestHeader('authorization', `Bearer ${state.token}`);

      xhr.upload.onprogress = (event) => {
        this.inflight.set(index, event.loaded);
        this.render();
      };
      xhr.onload = () => {
        this.xhrs.delete(index);
        this.inflight.delete(index);
        this.note = '';
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve();
          return;
        }
        let parsed = null;
        try { parsed = JSON.parse(xhr.responseText); } catch { /* non-JSON error body */ }
        reject(Object.assign(new Error(parsed?.message || `Chunk ${index} failed (${xhr.status})`), {
          status: xhr.status,
          code: parsed?.error,
        }));
      };
      xhr.onerror = () => {
        this.xhrs.delete(index);
        this.inflight.delete(index);
        reject(Object.assign(new Error('Network error'), { status: 0 }));
      };
      xhr.ontimeout = () => reject(Object.assign(new Error('Timed out'), { status: 408 }));
      xhr.onabort = () => {
        this.xhrs.delete(index);
        this.inflight.delete(index);
        reject(Object.assign(new Error('Aborted'), { name: 'AbortError' }));
      };
      xhr.send(blob);
    });
  }

  async finish() {
    this.note = 'finalising';
    this.render();
    try {
      const file = await api(`/api/uploads/${encodeURIComponent(this.id)}/complete`, { method: 'POST' });
      registry.clear(this.file);
      this.note = '';
      this.setState('done');
      this.render();
      toast(`${file.filename} uploaded`, 'ok');
      refreshFiles();
      refreshStats();
    } catch (err) {
      if (err.code === 'incomplete_upload' && Array.isArray(err.details?.missing)) {
        // A chunk the server never recorded: put it back and keep going.
        for (const index of err.details.missing) {
          this.done.delete(index);
          this.pending.push(index);
        }
        this.doneBytes = [...this.done].reduce((sum, i) => sum + this.sizeOf(i), 0);
        await this.run();
        return;
      }
      this.fail(err.message);
    }
  }

  /* ------------------------------------------------------------- controls */

  /**
   * Stops scheduling new chunks and lets the in-flight ones land, so the
   * progress bar holds at a chunk boundary instead of discarding partial bytes.
   * Pausing again while draining aborts immediately, for very slow links.
   */
  pause() {
    if (this.state === 'pausing') {
      this.abortInflight();
      this.setState('paused');
      this.note = '';
      this.render();
      return;
    }
    if (this.state !== 'uploading') return;
    this.setState('pausing');
    this.note = 'finishing current chunks…';
    this.render();
  }

  async resume() {
    if (this.state === 'pausing') { // cancel a pause that is still draining
      this.setState('uploading');
      this.note = '';
      this.render();
      return;
    }
    if (this.state !== 'paused' && this.state !== 'error') return;
    this.error = '';
    this.setState('uploading');
    this.queueRemaining();
    this.render();
    await this.run();
  }

  cancel() {
    const wasActive = ['uploading', 'pausing', 'paused'].includes(this.state);
    this.setState('canceled');
    this.abortInflight();
    if (this.id && wasActive) {
      fetch(`/api/uploads/${encodeURIComponent(this.id)}`, { method: 'DELETE', headers: authHeaders() })
        .catch(() => { /* best effort; the sweeper will clean it up */ });
    }
    registry.clear(this.file);
    this.node?.remove();
    state.queue = state.queue.filter((item) => item !== this);
    renderOverall();
  }

  abortInflight() {
    for (const xhr of this.xhrs.values()) xhr.abort();
    this.xhrs.clear();
    this.inflight.clear();
  }

  fail(message) {
    this.error = message;
    this.setState('error');
    this.abortInflight();
    this.render();
    toast(`${this.file.name}: ${message}`, 'error');
  }

  setState(next) {
    this.state = next;
    if (this.node) this.node.dataset.state = next;
  }

  /* --------------------------------------------------------------- render */

  buildNode() {
    const node = $('queue-item').content.firstElementChild.cloneNode(true);
    node.querySelector('[data-role="icon"]').textContent = iconFor(this.file.name, this.file.type);
    node.querySelector('[data-role="name"]').textContent = this.file.name;
    node.querySelector('[data-action="toggle"]').addEventListener('click', () => {
      if (this.state === 'uploading' || this.state === 'pausing') this.pause();
      else this.resume();
    });
    node.querySelector('[data-action="cancel"]').addEventListener('click', () => this.cancel());
    this.node = node;
    this.render();
  }

  sampleSpeed() {
    const now = performance.now();
    const elapsed = (now - this.lastSample.at) / 1000;
    if (elapsed < 0.35) return;
    const delta = this.sentBytes - this.lastSample.bytes;
    const instant = delta / elapsed;
    // Exponential smoothing keeps the readout steady on bursty connections.
    this.speed = this.speed ? this.speed * 0.7 + instant * 0.3 : instant;
    this.lastSample = { at: now, bytes: this.sentBytes };
  }

  render() {
    if (!this.node) return;
    const node = this.node;
    if (this.state === 'uploading' || this.state === 'pausing') this.sampleSpeed();

    const percent = this.state === 'done' ? 100 : this.percent;
    node.querySelector('[data-role="fill"]').style.width = `${percent}%`;
    node.querySelector('[data-role="pct"]').textContent = `${percent.toFixed(percent < 100 ? 1 : 0)}%`;

    const meta = node.querySelector('[data-role="meta"]');
    meta.textContent = `${formatBytes(this.file.size)} · ${this.chunkCount} chunk${this.chunkCount === 1 ? '' : 's'}`;
    if (this.resumed && !meta.nextElementSibling) {
      const badge = document.createElement('span');
      badge.className = 'badge';
      badge.textContent = 'resumed';
      meta.after(badge);
      this.resumed = false; // badge is rendered once
    }

    node.querySelector('[data-role="progress"]').textContent =
      `${formatBytes(this.sentBytes)} of ${formatBytes(this.file.size)} · ${this.done.size}/${this.chunkCount} chunks`;

    const status = node.querySelector('[data-role="status"]');
    if (this.state === 'done') {
      status.textContent = 'Complete';
    } else if (this.state === 'error') {
      status.textContent = this.error || 'Failed — click ▶ to retry';
    } else if (this.state === 'paused') {
      status.textContent = 'Paused';
    } else if (this.note) {
      status.textContent = this.note;
    } else if (this.speed > 0 && (this.state === 'uploading' || this.state === 'pausing')) {
      const remaining = (this.file.size - this.sentBytes) / this.speed;
      status.textContent = `${formatBytes(this.speed)}/s · ${formatDuration(remaining)} left`;
    } else {
      status.textContent = 'Starting…';
    }

    const toggleIcon = node.querySelector('[data-role="toggle-icon"]');
    const toggleBtn = node.querySelector('[data-action="toggle"]');
    const playing = this.state === 'uploading' || this.state === 'pausing';
    toggleIcon.innerHTML = playing
      ? '<path d="M9 6v12M15 6v12" fill="none" stroke-width="2" stroke-linecap="round"/>'
      : '<path d="M8 5l11 7-11 7z" fill="currentColor" stroke="none"/>';
    toggleBtn.title = this.state === 'pausing' ? 'Pause now' : (playing ? 'Pause' : 'Resume');

    renderOverall();
  }
}

/* ------------------------------------------------------------------ queue */

function renderOverall() {
  const active = state.queue.filter((item) => ['uploading', 'pausing', 'paused'].includes(item.state));
  const label = $('overall');
  if (!active.length) {
    label.textContent = '';
    return;
  }
  const total = active.reduce((sum, item) => sum + item.file.size, 0);
  const sent = active.reduce((sum, item) => sum + item.sentBytes, 0);
  const speed = active.reduce((sum, item) => sum + (item.state === 'uploading' ? item.speed : 0), 0);
  label.textContent = `${active.length} active · ${formatBytes(sent)} / ${formatBytes(total)}${speed ? ` · ${formatBytes(speed)}/s` : ''}`;
}

function addFiles(files) {
  const list = [...files];
  if (!list.length) return;
  $('queue-panel').hidden = false;
  for (const file of list) {
    const upload = new Upload(file);
    state.queue.push(upload);
    $('queue').prepend(upload.node);
    upload.start();
  }
}

/* ------------------------------------------------------------------ files */

function fileRow(file) {
  const li = document.createElement('li');
  li.className = 'item';

  const row = document.createElement('div');
  row.className = 'file-row';
  row.innerHTML = `
    <span class="item-icon">${iconFor(file.filename, file.contentType)}</span>
    <div class="item-id">
      <p class="item-name"></p>
      <p class="item-meta"></p>
    </div>
    <div class="file-actions">
      <a class="icon-btn" title="Download" download>
        <svg viewBox="0 0 24 24"><path d="M12 4v11m0 0-4-4m4 4 4-4M5 19h14" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </a>
      <button class="icon-btn" data-act="copy" title="Copy link" type="button">
        <svg viewBox="0 0 24 24"><path d="M9 9h10v10H9zM5 15V5h10" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </button>
      <button class="icon-btn danger" data-act="delete" title="Delete" type="button">
        <svg viewBox="0 0 24 24"><path d="M5 7h14M10 7V5h4v2m-7 0 1 12h8l1-12" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </button>
    </div>`;

  // textContent, never innerHTML, for anything that came from a filename.
  row.querySelector('.item-name').textContent = file.filename;
  row.querySelector('.item-meta').textContent =
    `${formatBytes(file.size)} · ${formatDate(file.completedAt)}`;

  const href = withToken(`/api/files/${encodeURIComponent(file.id)}/download`);
  row.querySelector('a').href = href;

  row.querySelector('[data-act="copy"]').addEventListener('click', async () => {
    const url = new URL(href, location.href).toString();
    try {
      await navigator.clipboard.writeText(url);
      toast('Link copied', 'ok');
    } catch {
      toast(url);
    }
  });

  row.querySelector('[data-act="delete"]').addEventListener('click', async () => {
    if (!confirm(`Delete ${file.filename}?`)) return;
    try {
      await api(`/api/files/${encodeURIComponent(file.id)}`, { method: 'DELETE' });
      li.remove();
      toast('Deleted', 'ok');
      refreshFiles();
      refreshStats();
    } catch (err) {
      toast(err.message, 'error');
    }
  });

  li.append(row);
  return li;
}

async function refreshFiles() {
  try {
    const { files } = await api('/api/files');
    const list = $('files');
    list.replaceChildren(...files.map(fileRow));
    $('files-empty').hidden = files.length > 0;
  } catch (err) {
    if (err.status !== 401) toast(err.message, 'error');
  }
}

async function refreshStats() {
  try {
    const stats = await api('/api/stats');
    $('stat-files').textContent = stats.files;
    $('stat-stored').textContent = formatBytes(stats.bytes);
    $('stat-free').textContent = Number.isFinite(stats.freeSpace) ? formatBytes(stats.freeSpace) : '∞';
  } catch { /* stats are decorative */ }
}

/* ------------------------------------------------------------------- boot */

function wireDropzone() {
  const zone = $('dropzone');
  const picker = $('picker');

  zone.addEventListener('click', () => picker.click());
  zone.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      picker.click();
    }
  });
  picker.addEventListener('change', () => {
    addFiles(picker.files);
    picker.value = '';
  });

  let depth = 0;
  window.addEventListener('dragenter', (event) => {
    event.preventDefault();
    depth += 1;
    zone.classList.add('dragging');
  });
  window.addEventListener('dragover', (event) => event.preventDefault());
  window.addEventListener('dragleave', () => {
    depth = Math.max(0, depth - 1);
    if (!depth) zone.classList.remove('dragging');
  });
  window.addEventListener('drop', (event) => {
    event.preventDefault();
    depth = 0;
    zone.classList.remove('dragging');
    if (event.dataTransfer?.files?.length) addFiles(event.dataTransfer.files);
  });

  window.addEventListener('paste', (event) => {
    const files = event.clipboardData?.files;
    if (files?.length) addFiles(files);
  });
}

function wireChrome() {
  $('refresh').addEventListener('click', () => {
    refreshFiles();
    refreshStats();
  });

  $('clear-done').addEventListener('click', () => {
    for (const item of [...state.queue]) {
      if (['done', 'error'].includes(item.state)) {
        item.node?.remove();
        state.queue = state.queue.filter((entry) => entry !== item);
      }
    }
    if (!state.queue.length) $('queue-panel').hidden = true;
    renderOverall();
  });

  window.addEventListener('beforeunload', (event) => {
    if (state.queue.some((item) => ['uploading', 'pausing'].includes(item.state))) {
      event.preventDefault();
      event.returnValue = '';
    }
  });
}

async function askForToken() {
  const dialog = $('token-dialog');
  const input = $('token-input');
  input.value = state.token;
  dialog.showModal();
  await new Promise((resolve) => dialog.addEventListener('close', resolve, { once: true }));
  state.token = input.value.trim();
  localStorage.setItem('easyup.token', state.token);
}

async function boot() {
  wireDropzone();
  wireChrome();

  try {
    state.config = await api('/api/config');
  } catch (err) {
    if (err.status === 401) {
      await askForToken();
      try {
        state.config = await api('/api/config');
      } catch {
        toast('That token was not accepted.', 'error');
        return;
      }
    } else {
      toast(`Cannot reach the server: ${err.message}`, 'error');
      return;
    }
  }

  const limit = formatBytes(state.config.maxFileSize, 0);
  $('limit').textContent = limit;
  $('limit-2').textContent = limit;
  $('fact-chunk').textContent = formatBytes(state.config.chunkSize, 0);
  $('fact-conc').textContent = state.config.concurrency;
  $('version').textContent = `v${state.config.version}`;

  await Promise.all([refreshFiles(), refreshStats()]);

  // Keeps speed and ETA moving even when no progress event has fired recently.
  setInterval(() => {
    for (const item of state.queue) {
      if (item.state === 'uploading' || item.state === 'pausing') item.render();
    }
  }, 700);
}

boot();
