// functions/btfstorage/server/[fileId].js
// ⚡ ULTRA-FAST Streaming Server — Byte-range aware, multi-chunk reassembly
// Supports: Range requests, inline stream, forced download, HLS (.m3u8)
// Zero-KV-read path: uses cached directUrls, refreshes only when expired

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods':  'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers':  'Content-Type, Range',
  'Access-Control-Expose-Headers': 'Content-Range, Content-Length, Accept-Ranges',
};

const CHUNK_SIZE = 45 * 1024 * 1024; // Must match upload.js

export async function onRequest({ request, env, params }) {
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });

  const rawId  = params.fileId || '';  // e.g. "id1abc123.mp4" or "id1abc123.m3u8"
  const isM3u8 = rawId.endsWith('.m3u8');
  const fileId = rawId.replace(/\.[^.]+$/, ''); // strip extension

  const kvList    = getKvList(env);
  const botTokens = getBotTokens(env);

  // ── Load master metadata ──────────────────────────────────────────────────
  let meta = null;
  for (const { kv } of kvList) {
    try {
      const raw = await kv.get(fileId);
      if (raw) { meta = JSON.parse(raw); break; }
    } catch (_) {}
  }
  if (!meta) return errRes('File not found', 404);

  // ── HLS M3U8 playlist ─────────────────────────────────────────────────────
  if (isM3u8) {
    return buildM3u8(meta, fileId, new URL(request.url).origin);
  }

  const totalSize    = meta.size;
  const isDownload   = new URL(request.url).searchParams.has('dl');
  const contentType  = meta.contentType || 'application/octet-stream';
  const disposition  = isDownload
    ? `attachment; filename="${encodeURIComponent(meta.filename)}"`
    : `inline; filename="${encodeURIComponent(meta.filename)}"`;

  // ── Parse Range header ────────────────────────────────────────────────────
  const rangeHeader = request.headers.get('Range');
  let rangeStart = 0;
  let rangeEnd   = totalSize - 1;

  if (rangeHeader) {
    const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
    if (match) {
      rangeStart = parseInt(match[1], 10);
      rangeEnd   = match[2] ? parseInt(match[2], 10) : totalSize - 1;
    }
  }

  rangeEnd = Math.min(rangeEnd, totalSize - 1);
  const responseLength = rangeEnd - rangeStart + 1;

  // ── HEAD request ──────────────────────────────────────────────────────────
  if (request.method === 'HEAD') {
    return new Response(null, {
      status: 200,
      headers: {
        ...CORS,
        'Content-Type':        contentType,
        'Content-Length':      String(totalSize),
        'Content-Disposition': disposition,
        'Accept-Ranges':       'bytes',
        'Cache-Control':       'public, max-age=3600',
      },
    });
  }

  // ── Build a unified ReadableStream across chunks ──────────────────────────
  const stream = buildRangeStream({
    meta,
    rangeStart,
    rangeEnd,
    kvList,
    botTokens,
    env,
  });

  const status  = rangeHeader ? 206 : 200;
  const headers = {
    ...CORS,
    'Content-Type':        contentType,
    'Content-Length':      String(responseLength),
    'Content-Disposition': disposition,
    'Accept-Ranges':       'bytes',
    'Cache-Control':       'public, max-age=3600',
  };

  if (rangeHeader) {
    headers['Content-Range'] = `bytes ${rangeStart}-${rangeEnd}/${totalSize}`;
  }

  return new Response(stream, { status, headers });
}

// ─── Build a ReadableStream that stitches chunk responses together ─────────────
function buildRangeStream({ meta, rangeStart, rangeEnd, kvList, botTokens, env }) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();

  (async () => {
    try {
      // Figure out which chunks we need
      const firstChunk = Math.floor(rangeStart / CHUNK_SIZE);
      const lastChunk  = Math.floor(rangeEnd   / CHUNK_SIZE);

      for (let ci = firstChunk; ci <= lastChunk; ci++) {
        const chunkMeta = await getChunkMeta(meta, ci, kvList);

        // Byte offsets within THIS chunk
        const chunkGlobalStart = ci * CHUNK_SIZE;
        const chunkGlobalEnd   = chunkGlobalStart + (chunkMeta.size - 1);

        const fetchStart = Math.max(rangeStart, chunkGlobalStart) - chunkGlobalStart;
        const fetchEnd   = Math.min(rangeEnd,   chunkGlobalEnd)   - chunkGlobalStart;

        // Get a fresh direct URL if expired
        const directUrl = await getFreshDirectUrl(chunkMeta, kvList, botTokens, env);

        // Fetch only the needed byte range from Telegram CDN
        const rangeReq = new Request(directUrl, {
          headers: { Range: `bytes=${fetchStart}-${fetchEnd}` },
          signal:  AbortSignal.timeout(60_000),
        });

        const res = await fetch(rangeReq);
        if (!res.ok && res.status !== 206) {
          throw new Error(`Telegram CDN returned ${res.status} for chunk ${ci}`);
        }

        // Pipe this chunk's response body to client
        const reader = res.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          await writer.write(value);
        }
      }
      await writer.close();
    } catch (err) {
      await writer.abort(err);
    }
  })();

  return readable;
}

// ─── Get chunk metadata (from master meta first, then KV fallback) ─────────────
async function getChunkMeta(masterMeta, index, kvList) {
  // Fast path: chunk info already in master metadata
  if (masterMeta.chunks?.[index]) return masterMeta.chunks[index];

  // Slow path: look up individual chunk key across all KVs
  const key = `${masterMeta.id || masterMeta.chunks?.[0]?.parentFileId}_chunk_${index}`;
  for (const { kv } of kvList) {
    try {
      const raw = await kv.get(key);
      if (raw) return JSON.parse(raw);
    } catch (_) {}
  }
  throw new Error(`Chunk ${index} metadata not found`);
}

// ─── Refresh expired Telegram direct URL ──────────────────────────────────────
async function getFreshDirectUrl(chunkMeta, kvList, botTokens, env) {
  // Still valid? Return immediately
  if (chunkMeta.directUrl && Date.now() < (chunkMeta.directUrlExpiry || 0)) {
    return chunkMeta.directUrl;
  }

  // Expired — refresh via getFile API
  // Try the original bot first, then others
  const tokens = [chunkMeta.botToken, ...botTokens].filter(Boolean);

  for (const token of tokens) {
    try {
      const res  = await fetch(
        `https://api.telegram.org/bot${token}/getFile?file_id=${encodeURIComponent(chunkMeta.telegramFileId)}`,
        { signal: AbortSignal.timeout(10_000) }
      );
      const data = await res.json();
      if (data.ok && data.result?.file_path) {
        const freshUrl    = `https://api.telegram.org/file/bot${token}/${data.result.file_path}`;
        const freshExpiry = Date.now() + 55 * 60 * 1000;

        // Background-update the chunk metadata in KV
        const updated = { ...chunkMeta, directUrl: freshUrl, directUrlExpiry: freshExpiry };
        updateChunkInKv(updated, kvList); // fire and forget

        return freshUrl;
      }
    } catch (_) {}
  }

  throw new Error(`Could not refresh URL for Telegram file ${chunkMeta.telegramFileId}`);
}

// ─── Background KV update (no await) ──────────────────────────────────────────
function updateChunkInKv(chunkMeta, kvList) {
  const key = `${chunkMeta.parentFileId}_chunk_${chunkMeta.index}`;
  for (const { kv } of kvList) {
    kv.put(key, JSON.stringify(chunkMeta)).catch(() => {});
  }
}

// ─── HLS M3U8 playlist builder ─────────────────────────────────────────────────
function buildM3u8(meta, fileId, origin) {
  const ext       = meta.extension || '.mp4';
  const targetDur = Math.ceil(CHUNK_SIZE / (1 * 1024 * 1024)); // rough seconds estimate

  let m3u8 = `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:${targetDur}\n#EXT-X-MEDIA-SEQUENCE:0\n`;

  for (let i = 0; i < meta.totalChunks; i++) {
    const chunkDur = i < meta.totalChunks - 1
      ? targetDur
      : Math.ceil((meta.size % CHUNK_SIZE || CHUNK_SIZE) / (1 * 1024 * 1024));
    m3u8 += `#EXTINF:${chunkDur},\n${origin}/btfstorage/server/${fileId}${ext}?chunk=${i}\n`;
  }

  m3u8 += '#EXT-X-ENDLIST\n';

  return new Response(m3u8, {
    headers: {
      ...CORS,
      'Content-Type':  'application/vnd.apple.mpegurl',
      'Cache-Control': 'no-cache',
    },
  });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function getKvList(env) {
  return [
    env.FILES_KV,  env.FILES_KV2,  env.FILES_KV3,  env.FILES_KV4,  env.FILES_KV5,
    env.FILES_KV6, env.FILES_KV7,  env.FILES_KV8,  env.FILES_KV9,  env.FILES_KV10,
  ]
    .map((kv, i) => kv ? { kv, name: `FILES_KV${i === 0 ? '' : i + 1}` } : null)
    .filter(Boolean);
}

function getBotTokens(env) {
  return [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
}

function errRes(msg, status = 500) {
  return new Response(JSON.stringify({ error: msg }), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}
