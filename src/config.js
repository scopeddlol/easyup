import path from 'node:path';

const KiB = 1024;
const MiB = 1024 * KiB;
const GiB = 1024 * MiB;

const UNITS = {
  b: 1,
  kb: 1000, mb: 1000 ** 2, gb: 1000 ** 3, tb: 1000 ** 4,
  kib: KiB, mib: MiB, gib: GiB, tib: 1024 * GiB,
  k: KiB, m: MiB, g: GiB, t: 1024 * GiB,
};

/**
 * Parses a byte size that may carry a unit suffix: "50GiB", "50gb", "8388608".
 * Plain numbers are bytes. Returns `fallback` for empty / unparseable input.
 */
export function parseSize(value, fallback) {
  if (value == null || value === '') return fallback;
  const match = String(value).trim().toLowerCase().match(/^(\d+(?:\.\d+)?)\s*([a-z]*)$/);
  if (!match) return fallback;
  const unit = match[2] === '' ? 'b' : match[2];
  const multiplier = UNITS[unit];
  if (!multiplier) return fallback;
  return Math.floor(Number(match[1]) * multiplier);
}

function int(value, fallback) {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function bool(value, fallback) {
  if (value == null || value === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

const env = process.env;

export const config = {
  host: env.HOST || '0.0.0.0',
  port: int(env.PORT, 8080),

  /** Root of all persisted state. Mount this as a volume in Docker. */
  dataDir: path.resolve(env.DATA_DIR || path.join(process.cwd(), 'data')),

  /** Hard ceiling for a single file. Default 50 GiB. */
  maxFileSize: parseSize(env.MAX_FILE_SIZE, 50 * GiB),

  /** Baseline chunk size. Grows automatically so chunkCount stays <= maxChunks. */
  chunkSize: parseSize(env.CHUNK_SIZE, 8 * MiB),

  /** Cap on chunks per upload, to keep bookkeeping and request counts sane. */
  maxChunks: int(env.MAX_CHUNKS, 10000),

  /** Parallel chunk requests the browser should use. */
  concurrency: Math.max(1, Math.min(16, int(env.UPLOAD_CONCURRENCY, 4))),

  /** Incomplete uploads older than this are swept away. Default 24h. */
  uploadTtlMs: int(env.UPLOAD_TTL_HOURS, 24) * 60 * 60 * 1000,

  /**
   * Client-side watchdogs. A chunk that moves no bytes for sendStallMs, or that
   * has been fully sent but unanswered for responseStallMs, is abandoned and
   * re-sent. Raise them for a slow disk or a high-latency link; lower them to
   * fail over faster.
   */
  sendStallMs: int(env.SEND_STALL_SECONDS, 45) * 1000,
  responseStallMs: int(env.RESPONSE_STALL_SECONDS, 120) * 1000,

  /** Free space that must remain after a reservation, so the host never fills up. */
  diskHeadroom: parseSize(env.DISK_HEADROOM, 1 * GiB),

  /** When set, every /api route requires `Authorization: Bearer <token>`. */
  token: env.UPLOAD_TOKEN || '',

  /** fdatasync each chunk before marking it received. Safer, slightly slower. */
  syncChunks: bool(env.SYNC_CHUNKS, true),

  version: env.APP_VERSION || '1.0.0',
};

/**
 * Picks the chunk size for a given file: the configured baseline, rounded up
 * (in MiB steps) until the file fits within `maxChunks` chunks.
 * A 50 GiB file with the defaults lands on 8 MiB chunks -> 6400 chunks.
 */
export function chunkSizeFor(fileSize) {
  const base = config.chunkSize;
  if (fileSize <= base) return base;
  const needed = Math.ceil(fileSize / config.maxChunks);
  if (needed <= base) return base;
  return Math.ceil(needed / MiB) * MiB;
}

export { KiB, MiB, GiB };
