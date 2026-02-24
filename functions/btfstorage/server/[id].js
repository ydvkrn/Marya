// functions/btfstorage/server/[id].js
// ⚡ ULTRA-FAST Cloudflare Pages Function — BTF Storage Streaming
// Optimizations: CF Cache API, parallel fetching, zero-block KV writes, minimal overhead

// ─── MIME TYPE MAP ─────────────────────────────────────────────────────────────
const MIME = {
  // Video
  mp4:'video/mp4', mkv:'video/x-matroska', avi:'video/x-msvideo',
  mov:'video/quicktime', m4v:'video/mp4', wmv:'video/x-ms-wmv',
  flv:'video/x-flv', '3gp':'video/3gpp', webm:'video/webm', ogv:'video/ogg',
  // Audio
  mp3:'audio/mpeg', wav:'audio/wav', aac:'audio/mp4', m4a:'audio/mp4',
  ogg:'audio/ogg', flac:'audio/flac', wma:'audio/x-ms-wma',
  // Image
  jpg:'image/jpeg', jpeg:'image/jpeg', png:'image/png', gif:'image/gif',
  webp:'image/webp', svg:'image/svg+xml', bmp:'image/bmp', tiff:'image/tiff',
  // Docs
  pdf:'application/pdf', doc:'application/msword',
  docx:'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  txt:'text/plain', zip:'application/zip', rar:'application/x-rar-compressed',
  // Streaming
  m3u8:'application/x-mpegURL', ts:'video/mp2t', mpd:'application/dash+xml'
};

// ─── STATIC CORS HEADERS (reuse object, avoid re-creating every request) ──────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Range, Content-Type, Authorization',
  'Access-Control-Expose-Headers':'Content-Length, Content-Range, Accept-Ranges',
  'Access-Control-Max-Age':       '86400',
};

// ─── MAIN HANDLER ──────────────────────────────────────────────────────────────
export async function onRequest({ request, env, params, waitUntil }) {
  // ── CORS preflight — respond instantly
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: CORS });
  }

  const fileId = params.id;           // e.g. "MSM221-48U91C62-no.mp4"
  const url    = new URL(request.url);

  try {
    // ── Parse extension & detect special modes ─────────────────────────────
    let actualId = fileId, ext = '', isM3U8 = false, isTS = false, segIdx = -1;

    const dotAt = fileId.lastIndexOf('.');
    if (dotAt !== -1) {
      ext      = fileId.slice(dotAt + 1).toLowerCase();
      actualId = fileId.slice(0, dotAt);

      if (ext === 'm3u8') {
        isM3U8 = true;
      } else if (ext === 'ts') {
        // Check for segment pattern: baseId-N.ts
        const dashAt = actualId.lastIndexOf('-');
        if (dashAt !== -1) {
          const maybeN = actualId.slice(dashAt + 1);
          if (/^\d+$/.test(maybeN)) {
            segIdx   = parseInt(maybeN, 10);
            actualId = actualId.slice(0, dashAt);
            isTS     = true;
          }
        }
      }
    }

    // ── KV lookup (single read, JSON parse) ────────────────────────────────
    const raw = await env.FILES_KV.get(actualId);
    if (!raw) return err('File not found', 404);

    const meta = JSON.parse(raw);
    if (!meta.filename || !meta.size) return err('Invalid metadata', 400);

    // backward-compat
    meta.telegramFileId = meta.telegramFileId || meta.fileIdCode;

    if (!meta.telegramFileId && !meta.chunks?.length)
      return err('Missing file source', 400);

    const mime = meta.contentType || MIME[ext] || 'application/octet-stream';

    // ── Route ──────────────────────────────────────────────────────────────
    if (isM3U8)       return hlsPlaylist(request, meta, actualId);
    if (isTS)         return hlsSegment(env, meta, segIdx, waitUntil);
    if (!meta.chunks?.length) return singleFile(request, env, meta, mime, url);
    return chunkedFile(request, env, meta, mime, url, waitUntil);

  } catch (e) {
    return err(e.message, 500);
  }
}

// ─── HLS PLAYLIST ──────────────────────────────────────────────────────────────
function hlsPlaylist(request, meta, actualId) {
  if (!meta.chunks?.length) return err('HLS unsupported for single files', 400);

  const base = new URL(request.url).origin;
  const seg  = 6; // seconds per segment
  let pl = `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:${seg}\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n`;

  for (let i = 0; i < meta.chunks.length; i++) {
    pl += `#EXTINF:${seg.toFixed(1)},\n${base}/btfstorage/server/${actualId}-${i}.ts\n`;
  }
  pl += '#EXT-X-ENDLIST\n';

  return new Response(pl, {
    status: 200,
    headers: {
      ...CORS,
      'Content-Type' : 'application/x-mpegURL',
      'Cache-Control': 'no-cache',
    }
  });
}

// ─── HLS SEGMENT ───────────────────────────────────────────────────────────────
async function hlsSegment(env, meta, segIdx, waitUntil) {
  if (!meta.chunks || segIdx < 0 || segIdx >= meta.chunks.length)
    return err('Segment not found', 404);

  const data = await loadChunk(env, meta.chunks[segIdx], waitUntil);
  return new Response(data, {
    status: 200,
    headers: {
      ...CORS,
      'Content-Type'  : 'video/mp2t',
      'Content-Length': data.byteLength.toString(),
      'Cache-Control' : 'public, max-age=31536000, immutable',
      'Accept-Ranges' : 'bytes',
    }
  });
}

// ─── SINGLE FILE (small file — direct Telegram proxy) ─────────────────────────
async function singleFile(request, env, meta, mime, url) {
  const tokens = getBotTokens(env);
  if (!tokens.length) return err('No bot tokens', 503);

  const range   = request.headers.get('Range');
  const isDL    = url.searchParams.has('dl') || url.searchParams.has('download');

  for (const token of tokens) {
    try {
      // Try to get Telegram direct URL (cached in CF Cache if possible)
      const directUrl = await getTelegramUrl(token, meta.telegramFileId, request.url);
      if (!directUrl) continue;

      const fetchHeaders = range ? { Range: range } : {};
      const tgRes = await fetch(directUrl, { headers: fetchHeaders });
      if (!tgRes.ok) continue;

      const resHeaders = new Headers(CORS);
      resHeaders.set('Content-Type', mime);
      resHeaders.set('Accept-Ranges', 'bytes');
      resHeaders.set('Cache-Control', 'public, max-age=31536000');
      resHeaders.set('Content-Disposition', isDL ? `attachment; filename="${meta.filename}"` : 'inline');

      for (const h of ['content-length', 'content-range', 'accept-ranges']) {
        const v = tgRes.headers.get(h);
        if (v) resHeaders.set(h, v);
      }

      return new Response(tgRes.body, { status: tgRes.status, headers: resHeaders });
    } catch (_) { /* try next token */ }
  }

  return err('All bots failed', 503);
}

// ─── CHUNKED FILE ──────────────────────────────────────────────────────────────
async function chunkedFile(request, env, meta, mime, url, waitUntil) {
  const range  = request.headers.get('Range');
  const isDL   = url.searchParams.has('dl') || url.searchParams.has('download');

  if (range) return smartRange(env, meta, mime, range, isDL, waitUntil);
  if (isDL)  return fullDownload(env, meta, mime, waitUntil);
  return instantPlay(env, meta, mime, waitUntil);
}

// ── Instant Play: stream first chunk IMMEDIATELY, rest follows ─────────────────
async function instantPlay(env, meta, mime, waitUntil) {
  const { chunks, size: total, filename } = meta;

  // Pre-fetch first chunk to respond fast
  const firstChunk = await loadChunk(env, chunks[0], waitUntil);

  const stream = new ReadableStream({
    async start(ctrl) {
      ctrl.enqueue(new Uint8Array(firstChunk));

      for (let i = 1; i < chunks.length; i++) {
        try {
          const data = await loadChunk(env, chunks[i], waitUntil);
          ctrl.enqueue(new Uint8Array(data));
        } catch (e) {
          ctrl.error(e);
          return;
        }
      }
      ctrl.close();
    }
  });

  return new Response(stream, {
    status: 200,
    headers: {
      ...CORS,
      'Content-Type'       : mime,
      'Content-Length'     : total.toString(),
      'Accept-Ranges'      : 'bytes',
      'Cache-Control'      : 'public, max-age=31536000',
      'Content-Disposition': 'inline',
      'X-Streaming-Mode'   : 'instant-play',
    }
  });
}

// ── Smart Range: only load chunks that cover the requested range ───────────────
async function smartRange(env, meta, mime, rangeHeader, isDL, waitUntil) {
  const { size: total, chunks, chunkSize: cs = 20971520, filename } = meta;

  const m = rangeHeader.match(/bytes=(\d+)-(\d*)/);
  if (!m) return err('Invalid range', 416, { 'Content-Range': `bytes */${total}` });

  const start = parseInt(m[1], 10);
  let   end   = m[2] ? parseInt(m[2], 10) : total - 1;
  if (end >= total)  end = total - 1;
  if (start > end || start >= total)
    return err('Range not satisfiable', 416, { 'Content-Range': `bytes */${total}` });

  const reqLen     = end - start + 1;
  const startChunk = Math.floor(start / cs);
  const endChunk   = Math.floor(end   / cs);
  const needed     = chunks.slice(startChunk, endChunk + 1);

  // ⚡ Parallel pre-fetch all needed chunks at once
  const fetched = await Promise.all(
    needed.map(chunk => loadChunk(env, chunk, waitUntil))
  );

  let pos = startChunk * cs;
  const stream = new ReadableStream({
    start(ctrl) {
      for (let i = 0; i < fetched.length; i++) {
        const arr       = new Uint8Array(fetched[i]);
        const sliceFrom = Math.max(start - pos, 0);
        const sliceTo   = Math.min(arr.length, end - pos + 1);
        if (sliceFrom < sliceTo) ctrl.enqueue(arr.slice(sliceFrom, sliceTo));
        pos += cs;
        if (pos > end) break;
      }
      ctrl.close();
    }
  });

  return new Response(stream, {
    status: 206,
    headers: {
      ...CORS,
      'Content-Type'       : mime,
      'Content-Length'     : reqLen.toString(),
      'Content-Range'      : `bytes ${start}-${end}/${total}`,
      'Accept-Ranges'      : 'bytes',
      'Cache-Control'      : 'public, max-age=31536000',
      'Content-Disposition': isDL ? `attachment; filename="${filename}"` : 'inline',
    }
  });
}

// ── Full download ──────────────────────────────────────────────────────────────
function fullDownload(env, meta, mime, waitUntil) {
  const { chunks, size: total, filename } = meta;

  const stream = new ReadableStream({
    async start(ctrl) {
      for (const chunk of chunks) {
        try {
          ctrl.enqueue(new Uint8Array(await loadChunk(env, chunk, waitUntil)));
        } catch (e) { ctrl.error(e); return; }
      }
      ctrl.close();
    }
  });

  return new Response(stream, {
    status: 200,
    headers: {
      ...CORS,
      'Content-Type'       : mime,
      'Content-Length'     : total.toString(),
      'Content-Disposition': `attachment; filename="${filename}"`,
      'Accept-Ranges'      : 'bytes',
      'Cache-Control'      : 'public, max-age=31536000',
    }
  });
}

// ─── CORE: Load a single chunk (CF Cache → KV directUrl → refresh) ─────────────
async function loadChunk(env, chunkInfo, waitUntil) {
  const kv      = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const key     = chunkInfo.keyName || chunkInfo.chunkKey;
  const metaRaw = await kv.get(key);
  if (!metaRaw) throw new Error(`Chunk not found: ${key}`);

  const cm = JSON.parse(metaRaw);
  cm.telegramFileId = cm.telegramFileId || cm.fileIdCode;

  // ── 1. Try cached directUrl first (fastest path)
  if (cm.directUrl) {
    const cached = await cacheGet(cm.directUrl);
    if (cached) return cached.arrayBuffer();

    // Hit origin directly — still fast
    const r = await fetch(cm.directUrl);
    if (r.ok) {
      // Store in CF Cache in background, don't block response
      if (waitUntil) waitUntil(cachePut(cm.directUrl, r.clone()));
      return r.arrayBuffer();
    }
  }

  // ── 2. Refresh URL via Telegram API
  const tokens = getBotTokens(env);
  for (const token of tokens) {
    try {
      const freshUrl = await getTelegramUrl(token, cm.telegramFileId);
      if (!freshUrl) continue;

      const r = await fetch(freshUrl);
      if (!r.ok) continue;

      // Background: update KV + warm CF Cache
      if (waitUntil) {
        const body = r.clone();
        waitUntil(
          Promise.all([
            cachePut(freshUrl, body),
            kv.put(key, JSON.stringify({ ...cm, directUrl: freshUrl, lastRefreshed: Date.now() }))
          ])
        );
      }
      return r.arrayBuffer();
    } catch (_) { /* next token */ }
  }

  throw new Error(`All refresh attempts failed: ${key}`);
}

// ─── HELPERS ───────────────────────────────────────────────────────────────────

// Get Telegram direct URL — uses CF Cache to avoid repeated getFile calls
async function getTelegramUrl(token, fileId, cacheKeyHint) {
  const cacheKey = `https://tg-url-cache.internal/${token.slice(-6)}/${fileId}`;

  // Try CF Cache
  const cached = await cacheGet(cacheKey);
  if (cached) {
    const text = await cached.text();
    if (text) return text;
  }

  // Call Telegram API
  const res  = await fetch(`https://api.telegram.org/bot${token}/getFile?file_id=${encodeURIComponent(fileId)}`, {
    signal: AbortSignal.timeout(8000)
  });
  const data = await res.json();
  if (!data.ok || !data.result?.file_path) return null;

  const url = `https://api.telegram.org/file/bot${token}/${data.result.file_path}`;

  // Cache URL for 55 minutes (Telegram URLs expire ~60min)
  cachePut(cacheKey, new Response(url, {
    headers: { 'Cache-Control': 'public, max-age=3300' }
  })).catch(() => {});

  return url;
}

// Cloudflare Cache API helpers
async function cacheGet(url) {
  try {
    const cache = caches.default;
    return await cache.match(new Request(url));
  } catch (_) { return null; }
}

async function cachePut(url, response) {
  try {
    const cache = caches.default;
    await cache.put(new Request(url), response);
  } catch (_) {}
}

function getBotTokens(env) {
  return [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
}

function err(msg, status = 500, extraHeaders = {}) {
  return new Response(JSON.stringify({ error: msg, status }), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json', ...extraHeaders }
  });
}
