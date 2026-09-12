import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

// Config is read from the environment at import time, so it must be set first.
const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'easyup-test-'));
process.env.DATA_DIR = dataDir;
process.env.CHUNK_SIZE = '65536'; // 64 KiB keeps the fixtures small but multi-chunk
process.env.MAX_FILE_SIZE = '10MiB';
process.env.SYNC_CHUNKS = 'false';

const { createServer } = await import('../src/server.js');
const store = await import('../src/storage.js');
const { config, chunkSizeFor } = await import('../src/config.js');

await store.init();
const server = createServer();
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;

test.after(async () => {
  // Idle keep-alive sockets would otherwise hold close() open for the full
  // keepAliveTimeout, stalling the run long after the assertions are done.
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  await fs.rm(dataDir, { recursive: true, force: true });
});

const CHUNK = 65536;

async function api(method, route, body) {
  const res = await fetch(base + route, {
    method,
    ...(body === undefined ? {} : {
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    }),
  });
  const text = await res.text();
  return { status: res.status, headers: res.headers, body: text ? JSON.parse(text) : null };
}

/** Uploads a buffer the way the browser client does, one chunk per request. */
async function uploadBuffer(buffer, { filename = 'fixture.bin', sha256, skip = [] } = {}) {
  const init = await api('POST', '/api/uploads', {
    filename,
    size: buffer.length,
    contentType: 'application/octet-stream',
    ...(sha256 ? { sha256 } : {}),
  });
  assert.equal(init.status, 201, JSON.stringify(init.body));
  const upload = init.body;

  for (let i = 0; i < upload.chunkCount; i += 1) {
    if (skip.includes(i)) continue;
    const start = i * upload.chunkSize;
    const slice = buffer.subarray(start, Math.min(start + upload.chunkSize, buffer.length));
    const res = await fetch(`${base}/api/uploads/${upload.id}/chunks/${i}`, {
      method: 'PUT',
      headers: { 'content-type': 'application/octet-stream', 'content-length': String(slice.length) },
      body: slice,
    });
    const text = await res.text();
    assert.equal(res.status, 200, `chunk ${i}: ${text}`);
  }
  return upload;
}

test('health and config expose the upload limits', async () => {
  const health = await api('GET', '/api/health');
  assert.equal(health.status, 200);
  assert.equal(health.body.ok, true);

  const cfg = await api('GET', '/api/config');
  assert.equal(cfg.status, 200);
  assert.equal(cfg.body.chunkSize, CHUNK);
  assert.equal(cfg.body.maxFileSize, 10 * 1024 * 1024);
  assert.ok(cfg.body.concurrency >= 1);
});

test('a multi-chunk upload round-trips byte for byte', async () => {
  // 2.5 chunks, so the final short chunk is exercised too.
  const data = crypto.randomBytes(CHUNK * 2 + 1234);
  const upload = await uploadBuffer(data, { filename: 'movie.bin' });
  assert.equal(upload.chunkCount, 3);

  const status = await api('GET', `/api/uploads/${upload.id}`);
  assert.equal(status.body.complete, true);
  assert.equal(status.body.receivedChunks, 3);
  assert.equal(status.body.receivedBytes, data.length);

  const done = await api('POST', `/api/uploads/${upload.id}/complete`);
  assert.equal(done.status, 201);
  assert.equal(done.body.size, data.length);

  const res = await fetch(`${base}/api/files/${upload.id}/download`);
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('accept-ranges'), 'bytes');
  assert.match(res.headers.get('content-disposition'), /attachment; filename="movie.bin"/);
  const downloaded = Buffer.from(await res.arrayBuffer());
  assert.equal(downloaded.length, data.length);
  assert.ok(downloaded.equals(data), 'downloaded bytes must match the original');
});

test('chunks may arrive out of order and be re-sent', async () => {
  const data = crypto.randomBytes(CHUNK * 3);
  const init = await api('POST', '/api/uploads', { filename: 'jumbled.bin', size: data.length });
  const upload = init.body;

  for (const i of [2, 0, 1, 1]) { // 1 is deliberately sent twice
    const slice = data.subarray(i * CHUNK, (i + 1) * CHUNK);
    const res = await fetch(`${base}/api/uploads/${upload.id}/chunks/${i}`, {
      method: 'PUT', body: slice,
    });
    assert.equal(res.status, 200);
  }

  await api('POST', `/api/uploads/${upload.id}/complete`);
  const res = await fetch(`${base}/api/files/${upload.id}/download`);
  assert.ok(Buffer.from(await res.arrayBuffer()).equals(data));
});

test('an interrupted upload resumes from its bitmap', async () => {
  const data = crypto.randomBytes(CHUNK * 4);
  const upload = await uploadBuffer(data, { filename: 'resume.bin', skip: [1, 3] });

  let status = await api('GET', `/api/uploads/${upload.id}`);
  assert.equal(status.body.complete, false);
  assert.deepEqual(status.body.missing, [1, 3]);
  assert.equal(status.body.receivedBytes, CHUNK * 2);

  // The bitmap tells a resuming client exactly which chunks to re-send.
  const bitmap = Buffer.from(status.body.bitmap, 'base64');
  assert.deepEqual([...bitmap], [1, 0, 1, 0]);

  const early = await api('POST', `/api/uploads/${upload.id}/complete`);
  assert.equal(early.status, 409);
  assert.equal(early.body.error, 'incomplete_upload');

  for (const i of [1, 3]) {
    const slice = data.subarray(i * CHUNK, (i + 1) * CHUNK);
    await fetch(`${base}/api/uploads/${upload.id}/chunks/${i}`, { method: 'PUT', body: slice });
  }

  status = await api('GET', `/api/uploads/${upload.id}`);
  assert.equal(status.body.complete, true);
  await api('POST', `/api/uploads/${upload.id}/complete`);
  const res = await fetch(`${base}/api/files/${upload.id}/download`);
  assert.ok(Buffer.from(await res.arrayBuffer()).equals(data));
});

test('range requests serve byte ranges (seekable downloads)', async () => {
  const data = crypto.randomBytes(CHUNK + 500);
  const upload = await uploadBuffer(data, { filename: 'ranged.bin' });
  await api('POST', `/api/uploads/${upload.id}/complete`);

  const mid = await fetch(`${base}/api/files/${upload.id}/download`, {
    headers: { range: 'bytes=100-199' },
  });
  assert.equal(mid.status, 206);
  assert.equal(mid.headers.get('content-range'), `bytes 100-199/${data.length}`);
  assert.equal(mid.headers.get('content-length'), '100');
  assert.ok(Buffer.from(await mid.arrayBuffer()).equals(data.subarray(100, 200)));

  const tail = await fetch(`${base}/api/files/${upload.id}/download`, {
    headers: { range: 'bytes=-50' },
  });
  assert.equal(tail.status, 206);
  assert.ok(Buffer.from(await tail.arrayBuffer()).equals(data.subarray(data.length - 50)));

  const open = await fetch(`${base}/api/files/${upload.id}/download`, {
    headers: { range: 'bytes=64000-' },
  });
  assert.equal(open.status, 206);
  assert.ok(Buffer.from(await open.arrayBuffer()).equals(data.subarray(64000)));

  const bad = await fetch(`${base}/api/files/${upload.id}/download`, {
    headers: { range: `bytes=${data.length + 10}-` },
  });
  assert.equal(bad.status, 416);
  assert.equal(bad.headers.get('content-range'), `bytes */${data.length}`);

  const head = await fetch(`${base}/api/files/${upload.id}/download`, { method: 'HEAD' });
  assert.equal(head.status, 200);
  assert.equal(head.headers.get('content-length'), String(data.length));
});

test('sha256 supplied at init is verified on completion', async () => {
  const data = crypto.randomBytes(CHUNK + 10);
  const digest = crypto.createHash('sha256').update(data).digest('hex');

  const good = await uploadBuffer(data, { filename: 'checked.bin', sha256: digest });
  const ok = await api('POST', `/api/uploads/${good.id}/complete`);
  assert.equal(ok.status, 201);
  assert.equal(ok.body.sha256, digest);

  const wrong = crypto.createHash('sha256').update('something else').digest('hex');
  const bad = await uploadBuffer(data, { filename: 'corrupt.bin', sha256: wrong });
  const rejected = await api('POST', `/api/uploads/${bad.id}/complete`);
  assert.equal(rejected.status, 422);
  assert.equal(rejected.body.error, 'checksum_mismatch');
  // A rejected upload is cleaned up rather than left half-stored.
  assert.equal((await api('GET', `/api/uploads/${bad.id}`)).status, 404);
});

test('wrong-sized and out-of-range chunks are rejected', async () => {
  const init = await api('POST', '/api/uploads', { filename: 'strict.bin', size: CHUNK * 2 });
  const upload = init.body;

  const short = await fetch(`${base}/api/uploads/${upload.id}/chunks/0`, {
    method: 'PUT', body: Buffer.alloc(CHUNK - 1),
  });
  assert.equal(short.status, 400);
  assert.equal((await short.json()).error, 'chunk_size_mismatch');

  const long = await fetch(`${base}/api/uploads/${upload.id}/chunks/0`, {
    method: 'PUT', body: Buffer.alloc(CHUNK + 1),
  });
  assert.equal(long.status, 400);

  const outOfRange = await fetch(`${base}/api/uploads/${upload.id}/chunks/9`, {
    method: 'PUT', body: Buffer.alloc(CHUNK),
  });
  assert.equal(outOfRange.status, 400);
  assert.equal((await outOfRange.json()).error, 'invalid_chunk_index');

  // A rejected chunk must not be recorded as received.
  const status = await api('GET', `/api/uploads/${upload.id}`);
  assert.equal(status.body.receivedChunks, 0);
});

test('a rejected chunk closes its connection instead of pinning it', async () => {
  // The response goes out before the body has finished arriving, so the socket
  // cannot be reused. Without this the connection would sit in the keep-alive
  // pool for the full keepAliveTimeout.
  const init = await api('POST', '/api/uploads', { filename: 'closes.bin', size: CHUNK * 2 });
  const res = await fetch(`${base}/api/uploads/${init.body.id}/chunks/0`, {
    method: 'PUT', body: Buffer.alloc(CHUNK + 1),
  });
  assert.equal(res.status, 400);
  assert.equal(res.headers.get('connection'), 'close');
  await res.text();
});

test('files over the size limit are refused up front', async () => {
  const res = await api('POST', '/api/uploads', { filename: 'huge.bin', size: 11 * 1024 * 1024 });
  assert.equal(res.status, 413);
  assert.equal(res.body.error, 'file_too_large');
  assert.equal(res.body.details.maxFileSize, 10 * 1024 * 1024);

  assert.equal((await api('POST', '/api/uploads', { filename: 'x', size: -1 })).status, 400);
  assert.equal((await api('POST', '/api/uploads', { filename: 'x', size: 'abc' })).status, 400);
});

test('a zero-byte file is a valid upload', async () => {
  const upload = await uploadBuffer(Buffer.alloc(0), { filename: 'empty.txt' });
  assert.equal(upload.chunkCount, 1);
  // The single chunk is zero bytes long, which uploadBuffer already sent.
  const done = await api('POST', `/api/uploads/${upload.id}/complete`);
  assert.equal(done.status, 201);
  assert.equal(done.body.size, 0);
  const res = await fetch(`${base}/api/files/${upload.id}/download`);
  assert.equal(res.status, 200);
  assert.equal((await res.arrayBuffer()).byteLength, 0);
});

test('listing, metadata and deletion', async () => {
  const before = (await api('GET', '/api/files')).body.files.length;
  const upload = await uploadBuffer(crypto.randomBytes(100), { filename: 'listed.bin' });
  await api('POST', `/api/uploads/${upload.id}/complete`);

  const list = await api('GET', '/api/files');
  assert.equal(list.body.files.length, before + 1);
  assert.equal(list.body.files[0].id, upload.id, 'newest file comes first');
  assert.ok(!('path' in list.body.files[0]), 'filesystem paths are never exposed');

  const stats = await api('GET', '/api/stats');
  assert.ok(stats.body.files >= 1);
  assert.ok(stats.body.bytes >= 100);

  assert.equal((await api('DELETE', `/api/files/${upload.id}`)).status, 200);
  assert.equal((await api('GET', `/api/files/${upload.id}`)).status, 404);
  assert.equal((await api('DELETE', `/api/files/${upload.id}`)).status, 404);
});

test('an upload can be aborted, freeing its reservation', async () => {
  const init = await api('POST', '/api/uploads', { filename: 'abandoned.bin', size: CHUNK });
  assert.equal((await api('DELETE', `/api/uploads/${init.body.id}`)).status, 200);
  assert.equal((await api('GET', `/api/uploads/${init.body.id}`)).status, 404);
});

test('ids are validated, so paths cannot escape the data directory', async () => {
  for (const id of ['..', '../../etc', 'short', 'a'.repeat(64), '%2e%2e%2f']) {
    const res = await fetch(`${base}/api/uploads/${encodeURIComponent(id)}`);
    assert.ok([400, 404].includes(res.status), `${id} -> ${res.status}`);
  }
  const traversal = await fetch(`${base}/../src/config.js`);
  assert.ok([400, 403, 404].includes(traversal.status));
});

test('filenames are sanitized but stay recognisable', () => {
  assert.equal(store.sanitizeFilename('../../etc/passwd'), 'passwd');
  assert.equal(store.sanitizeFilename('C:\\Users\\me\\report.pdf'), 'report.pdf');
  assert.equal(store.sanitizeFilename('a/b/c.txt'), 'c.txt');
  assert.equal(store.sanitizeFilename(''), 'file');
  assert.equal(store.sanitizeFilename('.hidden'), 'hidden');
  assert.equal(store.sanitizeFilename('films/ünïcode 映画.mkv'), 'ünïcode 映画.mkv');
  assert.ok(Buffer.byteLength(store.sanitizeFilename(`${'x'.repeat(400)}.mkv`)) <= 200);
  assert.match(store.sanitizeFilename(`${'x'.repeat(400)}.mkv`), /\.mkv$/);
});

test('chunk sizing keeps very large files within maxChunks', () => {
  // The sizing rule is what makes a 50 GiB file practical: chunk count stays
  // bounded, so bookkeeping and request counts do not explode.
  const fiftyGiB = 50 * 1024 ** 3;
  const chunk = chunkSizeFor(fiftyGiB);
  assert.ok(Math.ceil(fiftyGiB / chunk) <= config.maxChunks);

  for (const size of [1, 1024, 5 * 1024 ** 2, 1024 ** 3, fiftyGiB, 200 * 1024 ** 3]) {
    const cs = chunkSizeFor(size);
    assert.ok(cs >= config.chunkSize, `chunk size for ${size} must not shrink below the baseline`);
    assert.ok(Math.ceil(size / cs) <= config.maxChunks, `too many chunks for ${size}`);
  }
});

test('the static UI is served', async () => {
  const res = await fetch(`${base}/`);
  assert.equal(res.status, 200);
  assert.match(res.headers.get('content-type'), /text\/html/);
  const html = await res.text();
  assert.match(html, /easyup/i);
});

test('unknown API routes and methods produce structured errors', async () => {
  const missing = await api('GET', '/api/nope');
  assert.equal(missing.status, 404);
  assert.equal(missing.body.error, 'not_found');

  const badMethod = await api('DELETE', '/api/config');
  assert.equal(badMethod.status, 404);
});
