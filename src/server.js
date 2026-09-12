#!/usr/bin/env node
import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { pipeline } from 'node:stream/promises';
import { config } from './config.js';
import * as store from './storage.js';
import { HttpError } from './storage.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(here, '..', 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.png': 'image/png',
  '.woff2': 'font/woff2',
};

/* ------------------------------------------------------------------ helpers */

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'no-store',
  });
  res.end(payload);
}

function sendError(res, err) {
  const status = err instanceof HttpError ? err.status : 500;
  const code = err instanceof HttpError ? err.code : 'internal_error';
  if (status >= 500) console.error('[easyup]', err);
  sendJson(res, status, {
    error: code,
    message: status >= 500 ? 'Internal server error.' : err.message,
    ...(err.details ? { details: err.details } : {}),
  });
}

/**
 * Rejecting a chunk usually means answering before its body has finished
 * arriving. Such a socket cannot safely be reused, and left in the keep-alive
 * pool it would pin a connection for the full keepAliveTimeout, so close it.
 */
function closeAfterResponse(res) {
  res.setHeader('connection', 'close');
}

/** Reads a small JSON request body. Chunk bodies never come through here. */
async function readJsonBody(req, limit = 64 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw new HttpError(413, 'body_too_large', 'Request body is too large.');
    chunks.push(chunk);
  }
  if (!size) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new HttpError(400, 'invalid_json', 'Request body must be valid JSON.');
  }
}

function safeEqual(a, b) {
  const left = Buffer.from(String(a));
  const right = Buffer.from(String(b));
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

/**
 * When UPLOAD_TOKEN is set, every API route needs it. Downloads accept it as a
 * query parameter too, so a plain browser link still works.
 */
function authorize(req, url) {
  if (!config.token) return;
  const header = req.headers.authorization || '';
  const bearer = header.startsWith('Bearer ') ? header.slice(7) : '';
  const supplied = bearer || url.searchParams.get('token') || '';
  if (!supplied || !safeEqual(supplied, config.token)) {
    throw new HttpError(401, 'unauthorized', 'A valid upload token is required.');
  }
}

/** RFC 6266 / 5987 disposition that survives non-ASCII filenames. */
function contentDisposition(filename, inline) {
  const ascii = filename.replace(/[^\x20-\x7e]/g, '_').replace(/["\\]/g, '_');
  const encoded = encodeURIComponent(filename);
  return `${inline ? 'inline' : 'attachment'}; filename="${ascii}"; filename*=UTF-8''${encoded}`;
}

/** Parses a single-range `Range` header. Returns null when not applicable. */
function parseRange(header, size) {
  if (!header) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!match) return null;
  const [, rawStart, rawEnd] = match;
  if (rawStart === '' && rawEnd === '') return null;
  let start;
  let end;
  if (rawStart === '') {
    const suffix = Number(rawEnd);
    if (!suffix) return { invalid: true };
    start = Math.max(0, size - suffix);
    end = size - 1;
  } else {
    start = Number(rawStart);
    end = rawEnd === '' ? size - 1 : Math.min(Number(rawEnd), size - 1);
  }
  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end || start >= size) {
    return { invalid: true };
  }
  return { start, end };
}

/* ------------------------------------------------------------------- routes */

async function serveDownload(req, res, id, { inline }) {
  const file = await store.getFile(id);
  if (!file) throw new HttpError(404, 'file_not_found', 'No such file.');

  const stat = await fsp.stat(file.path).catch(() => null);
  if (!stat) throw new HttpError(404, 'file_not_found', 'No such file.');

  const headers = {
    'content-type': inline ? file.contentType : 'application/octet-stream',
    'content-disposition': contentDisposition(file.filename, inline),
    'accept-ranges': 'bytes',
    'cache-control': 'private, max-age=0, must-revalidate',
    etag: `"${file.id}-${stat.size}"`,
    'last-modified': stat.mtime.toUTCString(),
  };

  const range = parseRange(req.headers.range, stat.size);
  if (range?.invalid) {
    res.writeHead(416, { ...headers, 'content-range': `bytes */${stat.size}` });
    res.end();
    return;
  }

  const start = range ? range.start : 0;
  const end = range ? range.end : Math.max(0, stat.size - 1);
  const length = stat.size === 0 ? 0 : end - start + 1;

  if (range) headers['content-range'] = `bytes ${start}-${end}/${stat.size}`;
  headers['content-length'] = length;

  res.writeHead(range ? 206 : 200, headers);
  if (req.method === 'HEAD' || length === 0) {
    res.end();
    return;
  }

  // Streamed straight off disk, so memory use is constant regardless of size.
  const source = fs.createReadStream(file.path, { start, end, highWaterMark: 1024 * 1024 });
  try {
    await pipeline(source, res);
  } catch (err) {
    if (!['ERR_STREAM_PREMATURE_CLOSE', 'EPIPE', 'ECONNRESET'].includes(err.code)) throw err;
  }
}

async function serveStatic(req, res, pathname) {
  const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const target = path.join(publicDir, rel);
  // Reject anything that resolves outside of public/.
  if (!target.startsWith(publicDir + path.sep) && target !== publicDir) {
    throw new HttpError(403, 'forbidden', 'Forbidden.');
  }
  const stat = await fsp.stat(target).catch(() => null);
  if (!stat || !stat.isFile()) throw new HttpError(404, 'not_found', 'Not found.');

  res.writeHead(200, {
    'content-type': MIME[path.extname(target).toLowerCase()] || 'application/octet-stream',
    'content-length': stat.size,
    'cache-control': 'no-cache',
    'x-content-type-options': 'nosniff',
  });
  if (req.method === 'HEAD') {
    res.end();
    return;
  }
  await pipeline(fs.createReadStream(target), res).catch((err) => {
    if (!['ERR_STREAM_PREMATURE_CLOSE', 'EPIPE', 'ECONNRESET'].includes(err.code)) throw err;
  });
}

async function route(req, res, url) {
  const { pathname } = url;
  const method = req.method || 'GET';

  if (pathname === '/api/health') {
    sendJson(res, 200, { ok: true, version: config.version, uptime: Math.round(process.uptime()) });
    return;
  }

  if (pathname.startsWith('/api/')) authorize(req, url);

  if (pathname === '/api/config' && method === 'GET') {
    sendJson(res, 200, {
      maxFileSize: config.maxFileSize,
      chunkSize: config.chunkSize,
      maxChunks: config.maxChunks,
      concurrency: config.concurrency,
      authRequired: Boolean(config.token),
      version: config.version,
    });
    return;
  }

  if (pathname === '/api/stats' && method === 'GET') {
    sendJson(res, 200, await store.stats());
    return;
  }

  if (pathname === '/api/uploads' && method === 'POST') {
    const body = await readJsonBody(req);
    sendJson(res, 201, await store.createUpload({
      filename: body.filename,
      size: body.size,
      contentType: body.contentType,
      sha256: body.sha256,
    }));
    return;
  }

  if (pathname === '/api/uploads' && method === 'GET') {
    sendJson(res, 200, { uploads: await store.listUploads() });
    return;
  }

  const chunkMatch = /^\/api\/uploads\/([^/]+)\/chunks\/(\d+)$/.exec(pathname);
  if (chunkMatch) {
    if (method !== 'PUT' && method !== 'POST') {
      throw new HttpError(405, 'method_not_allowed', 'Use PUT to upload a chunk.');
    }
    const declared = req.headers['content-length'] == null
      ? null
      : Number(req.headers['content-length']);
    sendJson(res, 200, await store.writeChunk(
      decodeURIComponent(chunkMatch[1]),
      Number(chunkMatch[2]),
      req,
      declared,
    ));
    return;
  }

  const completeMatch = /^\/api\/uploads\/([^/]+)\/complete$/.exec(pathname);
  if (completeMatch && method === 'POST') {
    sendJson(res, 201, await store.completeUpload(decodeURIComponent(completeMatch[1])));
    return;
  }

  const uploadMatch = /^\/api\/uploads\/([^/]+)$/.exec(pathname);
  if (uploadMatch) {
    const id = decodeURIComponent(uploadMatch[1]);
    if (method === 'GET') {
      sendJson(res, 200, await store.uploadStatus(id));
      return;
    }
    if (method === 'DELETE') {
      const existed = await store.abortUpload(id);
      if (!existed) throw new HttpError(404, 'upload_not_found', 'No such upload.');
      sendJson(res, 200, { deleted: true, id });
      return;
    }
  }

  if (pathname === '/api/files' && method === 'GET') {
    sendJson(res, 200, { files: await store.listFiles() });
    return;
  }

  const downloadMatch = /^\/api\/files\/([^/]+)\/(download|view)$/.exec(pathname);
  if (downloadMatch && (method === 'GET' || method === 'HEAD')) {
    await serveDownload(req, res, decodeURIComponent(downloadMatch[1]), {
      inline: downloadMatch[2] === 'view',
    });
    return;
  }

  const fileMatch = /^\/api\/files\/([^/]+)$/.exec(pathname);
  if (fileMatch) {
    const id = decodeURIComponent(fileMatch[1]);
    if (method === 'GET') {
      const file = await store.getFile(id);
      if (!file) throw new HttpError(404, 'file_not_found', 'No such file.');
      const { path: _omit, ...record } = file;
      sendJson(res, 200, record);
      return;
    }
    if (method === 'DELETE') {
      const existed = await store.deleteFile(id);
      if (!existed) throw new HttpError(404, 'file_not_found', 'No such file.');
      sendJson(res, 200, { deleted: true, id });
      return;
    }
  }

  if (pathname.startsWith('/api/')) {
    throw new HttpError(404, 'not_found', `No API route for ${method} ${pathname}.`);
  }

  if (method === 'GET' || method === 'HEAD') {
    await serveStatic(req, res, pathname);
    return;
  }

  throw new HttpError(405, 'method_not_allowed', `${method} is not allowed here.`);
}

export function createServer() {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
    res.setHeader('x-content-type-options', 'nosniff');
    route(req, res, url)
      .catch((err) => {
        if (res.headersSent) {
          res.destroy();
          return;
        }
        // An unread body means we are answering mid-upload: say so in the
        // response rather than draining megabytes we have already rejected.
        if (!req.readableEnded) closeAfterResponse(res);
        sendError(res, err);
      });
  });

  // Large chunk bodies must not race a request timeout: disable the overall
  // request deadline and keep only a headers deadline.
  server.requestTimeout = 0;
  server.headersTimeout = 60_000;
  server.keepAliveTimeout = 75_000;
  server.timeout = 0;
  server.maxRequestsPerSocket = 0;
  return server;
}

async function main() {
  await store.init();
  const server = createServer();

  const sweepTimer = setInterval(() => {
    store.sweep().then((removed) => {
      if (removed) console.log(`[easyup] swept ${removed} stale upload(s)`);
    }).catch((err) => console.error('[easyup] sweep failed', err));
  }, 60 * 60 * 1000);
  sweepTimer.unref();

  await new Promise((resolve) => server.listen(config.port, config.host, resolve));
  const maxGiB = (config.maxFileSize / 1024 ** 3).toFixed(1);
  console.log(`[easyup] v${config.version} listening on http://${config.host}:${config.port}`);
  console.log(`[easyup] data dir: ${config.dataDir}`);
  console.log(`[easyup] max file size: ${maxGiB} GiB | chunk size: ${config.chunkSize / 1024 ** 2} MiB | auth: ${config.token ? 'on' : 'off'}`);

  const shutdown = (signal) => {
    console.log(`[easyup] ${signal} received, shutting down`);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 10_000).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

const invokedDirectly = process.argv[1]
  && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) {
  main().catch((err) => {
    console.error('[easyup] failed to start', err);
    process.exit(1);
  });
}
