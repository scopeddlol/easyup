import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'easyup-auth-'));
process.env.DATA_DIR = dataDir;
process.env.UPLOAD_TOKEN = 'super-secret-token';
process.env.SYNC_CHUNKS = 'false';

const { createServer } = await import('../src/server.js');
const store = await import('../src/storage.js');

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

const TOKEN = 'super-secret-token';

test('health stays open so orchestrators can probe it', async () => {
  const res = await fetch(`${base}/api/health`);
  assert.equal(res.status, 200);
  assert.equal((await res.json()).ok, true);
});

test('API routes reject missing or wrong tokens', async () => {
  for (const headers of [
    {},
    { authorization: 'Bearer wrong-token' },
    { authorization: 'Bearer ' },
    { authorization: 'super-secret-token' }, // missing the Bearer scheme
    { authorization: 'Bearer super-secret-tokenX' }, // longer, must not match
    { authorization: 'Bearer super-secret-toke' }, // shorter, must not match
  ]) {
    const res = await fetch(`${base}/api/files`, { headers });
    assert.equal(res.status, 401, `expected 401 for ${JSON.stringify(headers)}`);
    assert.equal((await res.json()).error, 'unauthorized');
  }
});

test('a valid bearer token unlocks the API', async () => {
  const res = await fetch(`${base}/api/config`, {
    headers: { authorization: `Bearer ${TOKEN}` },
  });
  assert.equal(res.status, 200);
  const body = await res.json();
  assert.equal(body.authRequired, true);
  assert.ok(!('token' in body), 'the token itself is never echoed back');
});

test('uploading requires the token at every step', async () => {
  const unauthorized = await fetch(`${base}/api/uploads`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ filename: 'x.bin', size: 10 }),
  });
  assert.equal(unauthorized.status, 401);

  const auth = { authorization: `Bearer ${TOKEN}` };
  const init = await fetch(`${base}/api/uploads`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...auth },
    body: JSON.stringify({ filename: 'secret.bin', size: 10 }),
  });
  assert.equal(init.status, 201);
  const upload = await init.json();

  // A chunk without the token must not be written.
  const noToken = await fetch(`${base}/api/uploads/${upload.id}/chunks/0`, {
    method: 'PUT', body: Buffer.alloc(10),
  });
  assert.equal(noToken.status, 401);

  const withToken = await fetch(`${base}/api/uploads/${upload.id}/chunks/0`, {
    method: 'PUT', headers: auth, body: Buffer.alloc(10),
  });
  assert.equal(withToken.status, 200);

  const done = await fetch(`${base}/api/uploads/${upload.id}/complete`, {
    method: 'POST', headers: auth,
  });
  assert.equal(done.status, 201);

  // Downloads accept the token as a query parameter, since a browser link
  // cannot set an Authorization header.
  const viaQuery = await fetch(`${base}/api/files/${upload.id}/download?token=${TOKEN}`);
  assert.equal(viaQuery.status, 200);
  assert.equal((await viaQuery.arrayBuffer()).byteLength, 10);

  const viaBadQuery = await fetch(`${base}/api/files/${upload.id}/download?token=nope`);
  assert.equal(viaBadQuery.status, 401);
});

test('the UI itself is still served so it can prompt for a token', async () => {
  const res = await fetch(`${base}/`);
  assert.equal(res.status, 200);
});
