# easyup

A small, self-hosted file uploader for **very large files** — up to **50 GiB** by default.
Uploads are split into chunks, sent in parallel, and resume after a dropped
connection, a browser reload, or a server restart.

No runtime dependencies. No database. Just Node and a directory.

![easyup](https://img.shields.io/badge/node-22-informational) ![no dependencies](https://img.shields.io/badge/runtime%20deps-0-success)

---

## Run it

```sh
docker run -d --name easyup -p 8080:8080 -v easyup-data:/data \
  ghcr.io/scopeddlol/easyup:latest
```

Open <http://localhost:8080>.

Or with Compose:

```sh
docker compose up -d
```

Without Docker (Node 22+):

```sh
npm start           # http://localhost:8080, data in ./data
```

> The `:latest` tag is published from the default branch. Every branch and tag
> also gets its own image — see [Published images](#published-images).

---

## How the chunking works

Most uploaders write chunks to temporary files and concatenate them at the end.
That needs twice the disk space and a full copy pass — at 50 GiB, that copy is
slow and can fail on its own.

easyup does it differently:

1. **Reserve.** `POST /api/uploads` allocates a *sparse* file of the final size
   and a bitmap with one byte per chunk. Allocating 50 GiB this way is instant
   and consumes almost no blocks until data arrives.
2. **Write in place.** Each chunk is streamed straight to its byte offset
   (`index * chunkSize`) inside that file. Chunks are independent, so they can
   arrive in any order, in parallel, or twice.
3. **Record.** Once a chunk's exact expected byte count has landed, one byte is
   flipped in the bitmap. An interrupted chunk never gets recorded, so it is
   simply re-sent.
4. **Complete.** `POST .../complete` checks the bitmap, then `rename()`s the
   file into place — an O(1) operation, even at 50 GiB.

The result: **no second copy, no assembly step, and constant memory use**
regardless of file size. A 5 GiB upload holds the server at ~110 MB RSS, the
same as a 5 MB one.

Resume works because the server owns the truth about what it has. A client asks
`GET /api/uploads/:id` and gets back a base64 bitmap of received chunks; it
re-sends only what is missing.

### Chunk sizing

Chunk size is chosen per file so the chunk count stays bounded:

| File size | Chunk size | Chunks |
|-----------|-----------|--------|
| 5 MiB     | 8 MiB     | 1      |
| 1 GiB     | 8 MiB     | 128    |
| 50 GiB    | 8 MiB     | 6400   |
| 500 GiB   | 52 MiB    | 9847   |

The baseline is `CHUNK_SIZE`; it only grows if a file would otherwise need more
than `MAX_CHUNKS` chunks.

---

## What you get

- Drag, drop, click or paste to upload — several files at once
- Live per-file progress, transfer rate and ETA
- **Pause** lets in-flight chunks finish, so nothing already sent is wasted
- **Resume** after a reload: re-pick the same file and it continues where it stopped
- Automatic retry with exponential backoff on network blips
- Range-request downloads, so large media files are seekable and resumable
- Optional bearer-token protection
- Dark and light themes, works down to phone width

---

## Configuration

Every setting is an environment variable. Sizes accept `50GiB`, `50GB`, `8MiB`
or a plain byte count.

| Variable | Default | What it does |
|----------|---------|--------------|
| `PORT` | `8080` | Listen port |
| `HOST` | `0.0.0.0` | Listen address |
| `DATA_DIR` | `./data` (`/data` in Docker) | Where uploads and files live |
| `MAX_FILE_SIZE` | `50GiB` | Largest single file accepted |
| `CHUNK_SIZE` | `8MiB` | Baseline chunk size |
| `MAX_CHUNKS` | `10000` | Cap on chunks per file; raises chunk size if needed |
| `UPLOAD_CONCURRENCY` | `4` | Parallel chunk requests the browser uses |
| `UPLOAD_TTL_HOURS` | `24` | Incomplete uploads are swept after this long |
| `DISK_HEADROOM` | `1GiB` | Free space that must remain after a reservation |
| `UPLOAD_TOKEN` | *(unset)* | If set, every API call needs this bearer token |
| `SYNC_CHUNKS` | `true` | `fdatasync` each chunk before recording it |

Uploads are rejected up front with `507` if the disk cannot fit the file plus
`DISK_HEADROOM`, rather than failing halfway through.

### Storage

Point `DATA_DIR` at a real filesystem with room for your files. In Docker, use a
volume or bind mount — the container layer is a poor place for 50 GiB, and
`rename()` is only instant when the upload and its destination share a
filesystem (they always do inside `DATA_DIR`).

```
$DATA_DIR/
  uploads/<id>/{blob,bits,meta.json}   # in-flight, swept after UPLOAD_TTL_HOURS
  files/<id>/{<filename>,meta.json}    # completed
```

---

## API

All routes return JSON. Errors carry a stable `error` code and a human `message`.

| Method | Route | Purpose |
|--------|-------|---------|
| `GET` | `/api/health` | Liveness — never requires a token |
| `GET` | `/api/config` | Limits and chunk settings |
| `GET` | `/api/stats` | File count, bytes stored, free space |
| `POST` | `/api/uploads` | Reserve an upload |
| `GET` | `/api/uploads` | List in-flight uploads |
| `GET` | `/api/uploads/:id` | Status + resume bitmap |
| `PUT` | `/api/uploads/:id/chunks/:index` | Upload one chunk (raw body) |
| `POST` | `/api/uploads/:id/complete` | Finalise |
| `DELETE` | `/api/uploads/:id` | Abort and free the reservation |
| `GET` | `/api/files` | List stored files |
| `GET` | `/api/files/:id` | One file's metadata |
| `GET` | `/api/files/:id/download` | Download (supports `Range`) |
| `GET` | `/api/files/:id/view` | Same, served inline |
| `DELETE` | `/api/files/:id` | Delete |

### Uploading from the command line

```sh
BASE=http://localhost:8080
FILE=bigfile.iso
SIZE=$(stat -c%s "$FILE")

# 1. Reserve — the response tells you the chunk size and count.
ID=$(curl -sS -X POST "$BASE/api/uploads" -H 'content-type: application/json' \
      -d "{\"filename\":\"$(basename "$FILE")\",\"size\":$SIZE}" \
     | node -pe 'JSON.parse(require("fs").readFileSync(0)).id')

# 2. Send the chunks (any order, in parallel if you like).
split -b 8388608 "$FILE" /tmp/chunk-
i=0
for part in /tmp/chunk-*; do
  curl -sS -X PUT --data-binary "@$part" "$BASE/api/uploads/$ID/chunks/$i"
  i=$((i+1))
done

# 3. Finalise.
curl -sS -X POST "$BASE/api/uploads/$ID/complete"
```

Pass a `sha256` when reserving and the server verifies the finished file against
it, rejecting the upload with `422 checksum_mismatch` if it does not match.

### Resuming

```sh
curl -sS "$BASE/api/uploads/$ID"
# { "receivedChunks": 812, "chunkCount": 6400, "missing": [813, 814, ...],
#   "bitmap": "AQEBAQEBAQAA..." }
```

`bitmap` is base64 of one byte per chunk (`1` = received). Re-send only the
chunks you need.

---

## Auth

Set `UPLOAD_TOKEN` and every `/api` route (except `/api/health`) requires it:

```sh
curl -H "Authorization: Bearer $UPLOAD_TOKEN" "$BASE/api/files"
```

The web UI prompts for the token once and remembers it. Download links accept
`?token=…` as well, since a browser link cannot set a header.

easyup has no user accounts — the token is a single shared secret. Put it behind
a reverse proxy with TLS if it faces the internet.

---

## Published images

Images are built for `linux/amd64` and `linux/arm64` and pushed to GHCR by
[`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml).

| Trigger | Tag |
|---------|-----|
| Push to the default branch | `latest`, `main`, `sha-<short>` |
| Push to any other branch | `<branch-name>` (slashes become `-`) |
| Tag `v1.2.3` | `1.2.3`, `1.2`, `sha-<short>` |
| Pull request | built and tested, not pushed |

The workflow runs the test suite first, then builds, then starts the image and
pushes a real multi-chunk upload through it before the job is allowed to pass.

Packages are **private by default**. To make yours public: repository →
*Packages* → `easyup` → *Package settings* → *Change visibility*.

---

## Development

```sh
npm start                      # run
npm run dev                    # run with --watch
npm test                       # 26 tests, no dependencies

# End-to-end throughput and integrity check against a running server
node scripts/stress.js --size 3GiB --url http://localhost:8080
```

`scripts/stress.js` generates data on the fly, uploads it in parallel chunks,
then verifies scattered byte ranges and a full-file digest — a way to confirm
offset handling on your own hardware without keeping a huge fixture around.

### Layout

```
src/config.js     environment parsing, chunk sizing
src/storage.js    reservations, chunk writes, completion, retention
src/server.js     HTTP routing, range downloads, auth, static files
public/           the web UI (vanilla JS, no build step)
test/             API and large-file behaviour
scripts/stress.js multi-GiB end-to-end check
```

---

## Limits and caveats

- One shared token, no user accounts or per-file permissions.
- Storage is the local filesystem; there is no S3 backend.
- Browser resume relies on `localStorage` plus re-picking the same file — a
  browser cannot re-read a file on its own after a reload.
- Files are stored as uploaded; there is no virus scanning or transcoding.

## License

MIT
