import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'easyup-large-'));
process.env.DATA_DIR = dataDir;
process.env.SYNC_CHUNKS = 'false';

const { expectedChunkSize } = await import('../src/storage.js');
const { config, chunkSizeFor } = await import('../src/config.js');

test.after(() => fs.rm(dataDir, { recursive: true, force: true }));

const GiB = 1024 ** 3;
const MiB = 1024 ** 2;

/** Builds the upload record the server would create for a file of this size. */
function planFor(size) {
  const chunkSize = chunkSizeFor(size);
  return { size, chunkSize, chunkCount: Math.max(1, Math.ceil(size / chunkSize)) };
}

test('the 50 GiB plan uses 8 MiB chunks and covers every byte exactly once', () => {
  const plan = planFor(50 * GiB);
  assert.equal(plan.chunkSize, 8 * MiB);
  assert.equal(plan.chunkCount, 6400);
  assert.ok(plan.chunkCount <= config.maxChunks);

  // Every chunk is full: 50 GiB divides evenly by 8 MiB.
  assert.equal(expectedChunkSize(plan, 0), 8 * MiB);
  assert.equal(expectedChunkSize(plan, plan.chunkCount - 1), 8 * MiB);

  let total = 0;
  for (let i = 0; i < plan.chunkCount; i += 1) total += expectedChunkSize(plan, i);
  assert.equal(total, plan.size, 'chunk sizes must sum to the file size');
});

test('offsets past 4 GiB stay exact (no 32-bit truncation)', () => {
  const plan = planFor(50 * GiB);
  const lastOffset = (plan.chunkCount - 1) * plan.chunkSize;
  assert.equal(lastOffset, 53678702592);
  assert.equal(lastOffset + expectedChunkSize(plan, plan.chunkCount - 1), 50 * GiB);
  // Well inside the exact-integer range, so offset arithmetic never rounds.
  assert.ok(50 * GiB < Number.MAX_SAFE_INTEGER);
  assert.ok(Number.isSafeInteger(lastOffset));
});

test('an odd 50 GiB-scale size gets a short final chunk that still fits', () => {
  const size = 50 * GiB - 12345; // not a multiple of the chunk size
  const plan = planFor(size);
  assert.equal(plan.chunkCount, 6400);
  const last = expectedChunkSize(plan, plan.chunkCount - 1);
  assert.equal(last, 8 * MiB - 12345);
  assert.ok(last > 0 && last <= plan.chunkSize);

  let total = 0;
  for (let i = 0; i < plan.chunkCount; i += 1) total += expectedChunkSize(plan, i);
  assert.equal(total, size);
});

test('chunk plans stay bounded across nine orders of magnitude', () => {
  const sizes = [0, 1, 1024, MiB, 100 * MiB, GiB, 10 * GiB, 50 * GiB, 500 * GiB, 5 * 1024 * GiB];
  for (const size of sizes) {
    const plan = planFor(size);
    assert.ok(plan.chunkCount >= 1, `${size} must have at least one chunk`);
    assert.ok(plan.chunkCount <= config.maxChunks, `${size} exceeded maxChunks`);
    let total = 0;
    for (let i = 0; i < plan.chunkCount; i += 1) {
      const chunk = expectedChunkSize(plan, i);
      assert.ok(chunk >= 0 && chunk <= plan.chunkSize, `bad chunk ${i} for size ${size}`);
      total += chunk;
    }
    assert.equal(total, size, `coverage mismatch for size ${size}`);
  }
});

test('preallocation of a multi-GiB file is sparse and instant', async () => {
  // The reservation strategy is what makes 50 GiB cheap: the blob is created at
  // full size up front but occupies almost no blocks until chunks arrive.
  const blob = path.join(dataDir, 'sparse.bin');
  const size = 20 * GiB;
  const started = Date.now();
  const handle = await fs.open(blob, 'w');
  try {
    await handle.truncate(size);
  } finally {
    await handle.close();
  }
  const elapsed = Date.now() - started;

  const stat = await fs.stat(blob);
  assert.equal(stat.size, size, 'apparent size is the full file size');
  assert.ok(stat.blocks * 512 < 16 * MiB,
    `expected a sparse file, but ${stat.blocks * 512} bytes are allocated`);
  assert.ok(elapsed < 5000, `preallocation took ${elapsed}ms`);
  await fs.rm(blob);
});
