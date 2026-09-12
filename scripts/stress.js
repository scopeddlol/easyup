#!/usr/bin/env node
/**
 * End-to-end stress check against a running easyup server.
 *
 * Uploads a synthetic file of the requested size through the real HTTP API with
 * parallel chunk requests, then verifies the stored bytes by re-reading ranges.
 * Data is generated on the fly, so no source file is needed on disk.
 *
 *   node scripts/stress.js --size 3GiB --url http://localhost:8080 --concurrency 4
 */
import crypto from 'node:crypto';
import { Readable } from 'node:stream';
import { parseSize } from '../src/config.js';

function arg(name, fallback) {
  const index = process.argv.indexOf(`--${name}`);
  return index === -1 ? fallback : process.argv[index + 1];
}

const baseUrl = (arg('url', process.env.EASYUP_URL || 'http://127.0.0.1:8080')).replace(/\/$/, '');
const totalSize = parseSize(arg('size', '3GiB'), 3 * 1024 ** 3);
const concurrency = Number(arg('concurrency', '4'));
const token = arg('token', process.env.UPLOAD_TOKEN || '');

const authHeaders = token ? { authorization: `Bearer ${token}` } : {};
const fmt = (bytes) => `${(bytes / 1024 ** 3).toFixed(2)} GiB`;

/**
 * Deterministic pseudo-random byte at a given absolute offset, so any range of
 * the uploaded file can be re-derived and compared without storing a copy.
 */
function fill(buffer, offset) {
  for (let i = 0; i < buffer.length; i += 1) {
    buffer[i] = ((offset + i) * 2654435761) >>> 24 & 0xff;
  }
  return buffer;
}

/** Streams a chunk's bytes without ever holding the whole chunk in memory. */
function chunkStream(offset, length) {
  const block = 256 * 1024;
  let produced = 0;
  return new Readable({
    read() {
      if (produced >= length) {
        this.push(null);
        return;
      }
      const size = Math.min(block, length - produced);
      this.push(fill(Buffer.allocUnsafe(size), offset + produced));
      produced += size;
    },
  });
}

async function main() {
  console.log(`stressing ${baseUrl} with a ${fmt(totalSize)} upload, concurrency ${concurrency}`);

  const initRes = await fetch(`${baseUrl}/api/uploads`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...authHeaders },
    body: JSON.stringify({
      filename: `stress-${fmt(totalSize).replace(/[ .]/g, '')}.bin`,
      size: totalSize,
      contentType: 'application/octet-stream',
    }),
  });
  if (!initRes.ok) throw new Error(`init failed: ${initRes.status} ${await initRes.text()}`);
  const upload = await initRes.json();
  console.log(`upload ${upload.id}: ${upload.chunkCount} chunks of ${upload.chunkSize / 1024 ** 2} MiB`);

  const started = Date.now();
  let sent = 0;
  let next = 0;
  let lastLog = 0;

  async function worker() {
    for (;;) {
      const index = next;
      next += 1;
      if (index >= upload.chunkCount) return;
      const offset = index * upload.chunkSize;
      const length = Math.min(upload.chunkSize, totalSize - offset);

      const res = await fetch(`${baseUrl}/api/uploads/${upload.id}/chunks/${index}`, {
        method: 'PUT',
        headers: { 'content-length': String(length), ...authHeaders },
        body: chunkStream(offset, length),
        duplex: 'half',
      });
      if (!res.ok) throw new Error(`chunk ${index} failed: ${res.status} ${await res.text()}`);
      await res.json();

      sent += length;
      const now = Date.now();
      if (now - lastLog > 2000) {
        lastLog = now;
        const rate = sent / ((now - started) / 1000) / 1024 ** 2;
        const pct = ((sent / totalSize) * 100).toFixed(1);
        console.log(`  ${pct}%  ${fmt(sent)} / ${fmt(totalSize)}  ${rate.toFixed(0)} MiB/s`);
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, worker));
  const uploadSeconds = (Date.now() - started) / 1000;
  console.log(`uploaded in ${uploadSeconds.toFixed(1)}s (${(totalSize / uploadSeconds / 1024 ** 2).toFixed(0)} MiB/s)`);

  const completeRes = await fetch(`${baseUrl}/api/uploads/${upload.id}/complete`, {
    method: 'POST',
    headers: authHeaders,
  });
  if (!completeRes.ok) throw new Error(`complete failed: ${completeRes.status} ${await completeRes.text()}`);
  const file = await completeRes.json();
  console.log(`stored as ${file.filename} (${file.size} bytes)`);
  if (file.size !== totalSize) throw new Error(`size mismatch: ${file.size} != ${totalSize}`);

  // Verify scattered ranges, including the boundary between the first two
  // chunks and the far end of the file past the 4 GiB mark.
  const probes = [
    0,
    upload.chunkSize - 8,
    upload.chunkSize,
    Math.floor(totalSize / 2),
    3 * 1024 ** 3 < totalSize ? 3 * 1024 ** 3 : Math.max(0, totalSize - 4096),
    Math.max(0, totalSize - 1024),
  ];
  for (const start of probes) {
    const end = Math.min(start + 511, totalSize - 1);
    if (end < start) continue;
    const res = await fetch(`${baseUrl}/api/files/${file.id}/download`, {
      headers: { range: `bytes=${start}-${end}`, ...authHeaders },
    });
    if (res.status !== 206) throw new Error(`range ${start} returned ${res.status}`);
    const actual = Buffer.from(await res.arrayBuffer());
    const expected = fill(Buffer.allocUnsafe(end - start + 1), start);
    if (!actual.equals(expected)) throw new Error(`byte mismatch at offset ${start}`);
    console.log(`  verified bytes ${start}-${end}`);
  }

  // Full-file digest, streamed, to prove nothing landed at the wrong offset.
  const hashStart = Date.now();
  const full = await fetch(`${baseUrl}/api/files/${file.id}/download`, { headers: authHeaders });
  const hash = crypto.createHash('sha256');
  const expectedHash = crypto.createHash('sha256');
  let verified = 0;
  for await (const part of full.body) {
    hash.update(part);
    expectedHash.update(fill(Buffer.allocUnsafe(part.length), verified));
    verified += part.length;
  }
  if (verified !== totalSize) throw new Error(`downloaded ${verified} of ${totalSize} bytes`);
  const actualDigest = hash.digest('hex');
  const wantDigest = expectedHash.digest('hex');
  if (actualDigest !== wantDigest) throw new Error(`digest mismatch:\n  got  ${actualDigest}\n  want ${wantDigest}`);
  console.log(`full-file digest matches (${((Date.now() - hashStart) / 1000).toFixed(1)}s to verify ${fmt(verified)})`);

  await fetch(`${baseUrl}/api/files/${file.id}`, { method: 'DELETE', headers: authHeaders });
  console.log('cleaned up. PASS');
}

main().catch((err) => {
  console.error(`FAIL: ${err.message}`);
  process.exit(1);
});
