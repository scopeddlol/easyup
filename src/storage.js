import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { config, chunkSizeFor } from './config.js';

/** An error carrying an HTTP status and a stable machine-readable code. */
export class HttpError extends Error {
  constructor(status, code, message, details) {
    super(message || code);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

const uploadsDir = () => path.join(config.dataDir, 'uploads');
const filesDir = () => path.join(config.dataDir, 'files');

const ID_RE = /^[A-Za-z0-9_-]{16,44}$/;

function newId() {
  return crypto.randomBytes(12).toString('base64url'); // 16 url-safe chars
}

/** Guards every id that reaches the filesystem, so no path can escape dataDir. */
function assertId(id) {
  if (typeof id !== 'string' || !ID_RE.test(id)) {
    throw new HttpError(400, 'invalid_id', 'Malformed id.');
  }
  return id;
}

const uploadDir = (id) => path.join(uploadsDir(), assertId(id));
const blobPath = (id) => path.join(uploadDir(id), 'blob');
const bitsPath = (id) => path.join(uploadDir(id), 'bits');
const uploadMetaPath = (id) => path.join(uploadDir(id), 'meta.json');
const fileDir = (id) => path.join(filesDir(), assertId(id));
const fileMetaPath = (id) => path.join(fileDir(id), 'meta.json');

export async function init() {
  await fsp.mkdir(uploadsDir(), { recursive: true });
  await fsp.mkdir(filesDir(), { recursive: true });
}

/** Writes JSON via a temp file + rename, so readers never see a half-written doc. */
async function writeJson(target, value) {
  const tmp = `${target}.${process.pid}.tmp`;
  await fsp.writeFile(tmp, JSON.stringify(value, null, 2));
  await fsp.rename(tmp, target);
}

async function readJson(target) {
  try {
    return JSON.parse(await fsp.readFile(target, 'utf8'));
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw err;
  }
}

const UNSAFE_FILENAME_CHARS = new RegExp('[\\u0000-\\u001f\\u007f<>:"/\\\\|?*]', 'g');

/**
 * Strips anything that could confuse a filesystem or a Content-Disposition
 * header while keeping the name recognisable (unicode is preserved).
 */
export function sanitizeFilename(name) {
  const base = path.basename(String(name ?? '').replace(/\\/g, '/'));
  let clean = base.replace(UNSAFE_FILENAME_CHARS, '_').replace(/^\.+/, '').trim();
  if (!clean) clean = 'file';
  // Keep the extension intact when truncating to a filesystem-safe length.
  if (Buffer.byteLength(clean) > 200) {
    const ext = path.extname(clean).slice(0, 32);
    const stem = clean.slice(0, clean.length - path.extname(clean).length);
    clean = Buffer.from(stem).subarray(0, 200 - Buffer.byteLength(ext)).toString() + ext;
  }
  return clean;
}

export function expectedChunkSize(upload, index) {
  const isLast = index === upload.chunkCount - 1;
  return isLast ? upload.size - index * upload.chunkSize : upload.chunkSize;
}

async function freeSpace(dir) {
  try {
    const stats = await fsp.statfs(dir);
    return stats.bsize * stats.bavail;
  } catch {
    return Number.POSITIVE_INFINITY; // statfs unsupported: don't block uploads
  }
}

/* ------------------------------------------------------------------ uploads */

/**
 * Reserves an upload: allocates a sparse file of the final size plus a bitmap
 * with one byte per chunk. Chunks are then written straight to their offset,
 * so there is never a second copy of the data and no assembly step at the end.
 */
export async function createUpload({ filename, size, contentType, sha256 }) {
  const fileSize = Number(size);
  if (!Number.isSafeInteger(fileSize) || fileSize < 0) {
    throw new HttpError(400, 'invalid_size', 'size must be a non-negative integer number of bytes.');
  }
  if (fileSize > config.maxFileSize) {
    throw new HttpError(413, 'file_too_large',
      `File is larger than the ${config.maxFileSize} byte limit.`,
      { maxFileSize: config.maxFileSize });
  }
  if (sha256 != null && !/^[a-f0-9]{64}$/i.test(String(sha256))) {
    throw new HttpError(400, 'invalid_sha256', 'sha256 must be 64 hex characters.');
  }

  const available = await freeSpace(config.dataDir);
  if (available < fileSize + config.diskHeadroom) {
    throw new HttpError(507, 'insufficient_storage',
      'Not enough free disk space for this upload.',
      { available, required: fileSize + config.diskHeadroom });
  }

  const id = newId();
  const chunkSize = chunkSizeFor(fileSize);
  const chunkCount = Math.max(1, Math.ceil(fileSize / chunkSize));
  const upload = {
    id,
    filename: sanitizeFilename(filename),
    originalFilename: String(filename ?? 'file').slice(0, 512),
    size: fileSize,
    contentType: typeof contentType === 'string' && contentType
      ? contentType.slice(0, 128)
      : 'application/octet-stream',
    sha256: sha256 ? String(sha256).toLowerCase() : null,
    chunkSize,
    chunkCount,
    createdAt: new Date().toISOString(),
  };

  await fsp.mkdir(uploadDir(id), { recursive: true });
  // Sparse allocation: instant even at 50 GiB, and it fails fast if the
  // filesystem cannot represent the size at all.
  const handle = await fsp.open(blobPath(id), 'w');
  try {
    await handle.truncate(fileSize);
  } finally {
    await handle.close();
  }
  await fsp.writeFile(bitsPath(id), Buffer.alloc(chunkCount));
  await writeJson(uploadMetaPath(id), upload);
  return upload;
}

export async function getUpload(id) {
  return readJson(uploadMetaPath(id));
}

async function readBits(id) {
  try {
    return await fsp.readFile(bitsPath(id));
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw err;
  }
}

/** Marks one chunk received by flipping a single byte in place. */
async function setBit(id, index) {
  const handle = await fsp.open(bitsPath(id), 'r+');
  try {
    await handle.write(Buffer.from([1]), 0, 1, index);
    if (config.syncChunks) await handle.sync();
  } finally {
    await handle.close();
  }
}

function summarize(upload, bits) {
  let received = 0;
  let receivedBytes = 0;
  const missing = [];
  for (let i = 0; i < upload.chunkCount; i += 1) {
    if (bits[i]) {
      received += 1;
      receivedBytes += expectedChunkSize(upload, i);
    } else if (missing.length < 100) {
      missing.push(i);
    }
  }
  return {
    ...upload,
    receivedChunks: received,
    receivedBytes,
    complete: received === upload.chunkCount,
    missing,
    /** Compact resume map: one byte per chunk, base64-encoded. */
    bitmap: bits.toString('base64'),
  };
}

export async function uploadStatus(id) {
  const upload = await getUpload(id);
  const bits = upload && await readBits(id);
  if (!upload || !bits) throw new HttpError(404, 'upload_not_found', 'No such upload.');
  return summarize(upload, bits);
}

/**
 * Streams one chunk to its byte offset inside the preallocated file.
 * Nothing is buffered in memory, and the chunk is only marked received once
 * the exact expected number of bytes has landed on disk - so an interrupted
 * chunk is simply re-sent later.
 */
export async function writeChunk(id, index, source, declaredLength) {
  const upload = await getUpload(id);
  if (!upload) throw new HttpError(404, 'upload_not_found', 'No such upload.');
  if (!Number.isInteger(index) || index < 0 || index >= upload.chunkCount) {
    throw new HttpError(400, 'invalid_chunk_index',
      `Chunk index must be between 0 and ${upload.chunkCount - 1}.`);
  }

  const expected = expectedChunkSize(upload, index);
  if (declaredLength != null && declaredLength !== expected) {
    throw new HttpError(400, 'chunk_size_mismatch',
      `Chunk ${index} must be exactly ${expected} bytes, got ${declaredLength}.`,
      { expected, received: declaredLength });
  }

  const offset = index * upload.chunkSize;
  let written = 0;
  const limiter = new Transform({
    transform(chunk, _enc, cb) {
      written += chunk.length;
      if (written > expected) {
        cb(new HttpError(413, 'chunk_too_large', `Chunk ${index} exceeds ${expected} bytes.`));
        return;
      }
      cb(null, chunk);
    },
  });

  const sink = fs.createWriteStream(blobPath(id), { flags: 'r+', start: offset });
  await pipeline(source, limiter, sink);
  if (written !== expected) {
    throw new HttpError(400, 'chunk_size_mismatch',
      `Chunk ${index} must be exactly ${expected} bytes, got ${written}.`,
      { expected, received: written });
  }

  await setBit(id, index);
  const bits = await readBits(id);
  const status = summarize(upload, bits);
  return {
    index,
    bytes: written,
    receivedChunks: status.receivedChunks,
    chunkCount: upload.chunkCount,
    complete: status.complete,
  };
}

async function sha256File(target) {
  const hash = crypto.createHash('sha256');
  await pipeline(fs.createReadStream(target, { highWaterMark: 4 * 1024 * 1024 }), hash);
  return hash.digest('hex');
}

/**
 * Turns a fully-received upload into a stored file. The blob is moved with a
 * rename inside the same filesystem, so completing a 50 GiB upload is O(1).
 */
export async function completeUpload(id) {
  const upload = await getUpload(id);
  const bits = upload && await readBits(id);
  if (!upload || !bits) throw new HttpError(404, 'upload_not_found', 'No such upload.');

  const status = summarize(upload, bits);
  if (!status.complete) {
    throw new HttpError(409, 'incomplete_upload',
      `${upload.chunkCount - status.receivedChunks} of ${upload.chunkCount} chunks are still missing.`,
      { missing: status.missing, receivedChunks: status.receivedChunks, chunkCount: upload.chunkCount });
  }

  const stat = await fsp.stat(blobPath(id));
  if (stat.size !== upload.size) {
    throw new HttpError(422, 'size_mismatch',
      `Stored file is ${stat.size} bytes, expected ${upload.size}.`);
  }

  if (upload.sha256) {
    const actual = await sha256File(blobPath(id));
    if (actual !== upload.sha256) {
      await abortUpload(id);
      throw new HttpError(422, 'checksum_mismatch',
        'Uploaded data does not match the sha256 supplied at init.',
        { expected: upload.sha256, actual });
    }
  }

  const record = {
    id: upload.id,
    filename: upload.filename,
    originalFilename: upload.originalFilename,
    size: upload.size,
    contentType: upload.contentType,
    sha256: upload.sha256,
    chunkSize: upload.chunkSize,
    chunkCount: upload.chunkCount,
    createdAt: upload.createdAt,
    completedAt: new Date().toISOString(),
  };

  await fsp.mkdir(fileDir(id), { recursive: true });
  await fsp.rename(blobPath(id), path.join(fileDir(id), upload.filename));
  await writeJson(fileMetaPath(id), record);
  await fsp.rm(uploadDir(id), { recursive: true, force: true });
  return record;
}

export async function abortUpload(id) {
  const existed = await getUpload(id);
  await fsp.rm(uploadDir(id), { recursive: true, force: true });
  return Boolean(existed);
}

export async function listUploads() {
  const entries = await fsp.readdir(uploadsDir(), { withFileTypes: true }).catch(() => []);
  const out = [];
  for (const entry of entries) {
    if (!entry.isDirectory() || !ID_RE.test(entry.name)) continue;
    const status = await uploadStatus(entry.name).catch(() => null);
    if (status) out.push(status);
  }
  return out.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
}

/* -------------------------------------------------------------------- files */

export async function getFile(id) {
  const record = await readJson(fileMetaPath(id));
  if (!record) return null;
  return { ...record, path: path.join(fileDir(id), record.filename) };
}

export async function listFiles() {
  const entries = await fsp.readdir(filesDir(), { withFileTypes: true }).catch(() => []);
  const out = [];
  for (const entry of entries) {
    if (!entry.isDirectory() || !ID_RE.test(entry.name)) continue;
    const record = await readJson(fileMetaPath(entry.name));
    if (record) out.push(record);
  }
  return out.sort((a, b) => String(b.completedAt).localeCompare(String(a.completedAt)));
}

export async function deleteFile(id) {
  const record = await readJson(fileMetaPath(id));
  if (!record) return false;
  await fsp.rm(fileDir(id), { recursive: true, force: true });
  return true;
}

export async function stats() {
  const files = await listFiles();
  return {
    files: files.length,
    bytes: files.reduce((sum, file) => sum + file.size, 0),
    freeSpace: await freeSpace(config.dataDir),
  };
}

/** Drops incomplete uploads older than the configured TTL. */
export async function sweep(now = Date.now()) {
  const entries = await fsp.readdir(uploadsDir(), { withFileTypes: true }).catch(() => []);
  let removed = 0;
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const dir = path.join(uploadsDir(), entry.name);
    const stat = await fsp.stat(dir).catch(() => null);
    if (!stat) continue;
    if (now - stat.mtimeMs > config.uploadTtlMs) {
      await fsp.rm(dir, { recursive: true, force: true });
      removed += 1;
    }
  }
  return removed;
}
