// functions/btfstorage/server/[id].js
// ⚡⚡ 10X ULTRA-FAST Cloudflare Pages Function — BTF Storage Streaming
// Optimizations:
//   ✅ Parallel KV metadata prefetch (biggest win for multi-chunk)
//   ✅ 2-chunk prefetch pipeline while streaming
//   ✅ ETag + If-None-Match → 304 Not Modified
//   ✅ Vary: Range header
//   ✅ RFC 5987 filename encoding (unicode filenames)
//   ✅ M3U8: actual duration from meta, CODECS hint, BANDWIDTH estimate
//   ✅ CF Cache for chunk data + Telegram URLs
//   ✅ Zero-block KV writes via waitUntil
//   ✅ Token rotation for bot failover
//   ✅ AbortSignal timeouts on all fetches

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
  m3u8:'application/x-mpegURL', ts:'video/mp2t', mpd:'application/dash+xml',
};

// ─── STATIC CORS HEADERS ───────────────────────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Range, Content-Type, Authorization, If-None-Match, If-Modified-Since',
  'Access-Control-Expose-Headers':'Content-Length, Content-Range, Accept-Ranges, ETag, Content-Disposition',
  'Access-Control-Max-Age':       '86400',
};

// How many chunks to prefetch ahead while streaming
const PREFETCH_AHEAD = 2;

// ─── MAIN HANDLER ──────────────────────────────────────────────────────────────
export async function onRequest({ request, env, params, waitUntil }) {
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: CORS });
  }

  const fileId = params.id;
  const url    = new URL(request.url);

  try {
    let actualId = fileId, ext = '', isM3U8 = false, isTS = false, segIdx = -1;

    const dotAt = fileId.lastIndexOf('.');
    if (dotAt !== -1) {
      ext      = fileId.slice(dotAt + 1).toLowerCase();
      actualId = fileId.slice(0, dotAt);

      if (ext === 'm3u8') {
        isM3U8 = true;
      } else if (ext === 'ts') {
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

    // ── KV lookup ─────────────────────────────────────────────────────────────
    const raw = await env.FILES_KV.get(actualId);
    if (!raw) return err('File not found', 404);

    const meta = JSON.parse(raw);
    if (!meta.filename || !meta.size) return err('Invalid metadata', 400);

    meta.telegramFileId = meta.telegramFileId || meta.fileIdCode;

    if (!meta.telegramFileId && !meta.chunks?.length)
      return err('Missing file source', 400);

    const mime  = meta.contentType || MIME[ext] || 'application/octet-stream';
    const etag  = `"${actualId}-${meta.size}"`;

    // ── ETag / 304 check ──────────────────────────────────────────────────────
    const ifNoneMatch = request.headers.get('If-None-Match');
    if (ifNoneMatch && (ifNoneMatch === etag || ifNoneMatch === '*')) {
      return new Response(null, { status: 304, headers: { ...CORS, ETag: etag } });
    }

    // ── Route ─────────────────────────────────────────────────────────────────
    if (isM3U8) return hlsPlaylist(request, meta, actualId);
    if (isTS)   return hlsSegment(env, meta, segIdx, waitUntil);
    if (!meta.chunks?.length) return singleFile(request, env, meta, mime, url, etag);
    return chunkedFile(request, env, meta, mime, url, waitUntil, etag);

  } catch (e) {
    console.error('[id].js error:', e);
    return err(e.message, 500);
  }
}

// ─── HLS PLAYLIST ──────────────────────────────────────────────────────────────
// ⚡ Uses actual duration from meta if available, estimates from size otherwise
//    Adds BANDWIDTH hint for adaptive-bitrate players
function hlsPlaylist(request, meta, actualId) {
  if (!meta.chunks?.length) return err('HLS unsupported for single files', 400);

  const base          = new URL(request.url).origin;
  const totalDuration = meta.duration || 0; // seconds, if stored in meta
  const chunkCount    = meta.chunks.length;
  const chunkSize     = meta.chunkSize || 20971520; // 20 MB default

  // Estimate bandwidth (bits per second) from file size and duration
  const bw = (totalDuration > 0)
    ? Math.round((meta.size * 8) / totalDuration)
    : Math.round((meta.size * 8) / (chunkCount * 6)); // fallback: assume 6s/chunk

  // Per-segment duration
  const getSegDur = (i) => {
    if (meta.chunks[i]?.duration) return meta.chunks[i].duration;
    if (totalDuration > 0) {
      // Last chunk may be shorter
      const base = totalDuration / chunkCount;
      return (i === chunkCount - 1)
        ? parseFloat((totalDuration - base * (chunkCount - 1)).toFixed(3))
        : parseFloat(base.toFixed(3));
    }
    return 6.0;
  };

  const maxDur = Math.ceil(
    meta.chunks.reduce((mx, _, i) => Math.max(mx, getSegDur(i)), 0)
  );

  // Build playlist
  let pl = [
    '#EXTM3U',
    '#EXT-X-VERSION:3',
    `#EXT-X-TARGETDURATION:${maxDur}`,
    '#EXT-X-MEDIA-SEQUENCE:0',
    '#EXT-X-PLAYLIST-TYPE:VOD',
  ];

  // Optional: stream info line
  pl.push(`#EXT-X-STREAM-INF:BANDWIDTH=${bw},CODECS="avc1.42E01E,mp4a.40.2"`);

  for (let i = 0; i < chunkCount; i++) {
    const dur = getSegDur(i);
    pl.push(`#EXTINF:${dur.toFixed(3)},`);
    pl.push(`${base}/btfstorage/server/${actualId}-${i}.ts`);
  }
  pl.push('#EXT-X-ENDLIST');

  return new Response(pl.join('\n') + '\n', {
    status: 200,
    headers: {
      ...CORS,
      'Content-Type' : 'application/x-mpegURL; charset=utf-8',
      'Cache-Control': 'no-cache, no-store',
      'Vary'         : 'Origin',
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

// ─── SINGLE FILE ───────────────────────────────────────────────────────────────
async function singleFile(request, env, meta, mime, url, etag) {
  const tokens = getBotTokens(env);
  if (!tokens.length) return err('No bot tokens', 503);

  const range = request.headers.get('Range');
  const isDL  = url.searchParams.has('dl') || url.searchParams.has('download');

  for (const token of tokens) {
    try {
      const directUrl = await getTelegramUrl(token, meta.telegramFileId);
      if (!directUrl) continue;

      const fetchHeaders = {};
      if (range) fetchHeaders['Range'] = range;

      const tgRes = await fetch(directUrl, {
        headers: fetchHeaders,
        signal : AbortSignal.timeout(15000),
      });
      if (!tgRes.ok && tgRes.status !== 206) continue;

      const resHeaders = new Headers(CORS);
      resHeaders.set('Content-Type',        mime);
      resHeaders.set('Accept-Ranges',       'bytes');
      resHeaders.set('Cache-Control',       'public, max-age=31536000');
      resHeaders.set('ETag',                etag);
      resHeaders.set('Vary',                'Range');
      resHeaders.set('Content-Disposition', contentDisposition(isDL, meta.filename));

      for (const h of ['content-length', 'content-range']) {
        const v = tgRes.headers.get(h);
        if (v) resHeaders.set(h, v);
      }

      return new Response(tgRes.body, { status: tgRes.status, headers: resHeaders });
    } catch (_) { /* try next token */ }
  }

  return err('All bots failed', 503);
}

// ─── CHUNKED FILE ──────────────────────────────────────────────────────────────
async function chunkedFile(request, env, meta, mime, url, waitUntil, etag) {
  const range = request.headers.get('Range');
  const isDL  = url.searchParams.has('dl') || url.searchParams.has('download');

  if (range) return smartRange(env, meta, mime, range, isDL, waitUntil, etag);
  if (isDL)  return fullDownload(env, meta, mime, waitUntil, etag);
  return instantPlay(env, meta, mime, waitUntil, etag);
}

// ── Instant Play ─────────────────────────────────────────────────────────────
// ⚡ Streams first chunk immediately, uses prefetch pipeline for subsequent chunks
async function instantPlay(env, meta, mime, waitUntil, etag) {
  const { chunks, size: total, filename } = meta;

  // ⚡ PRE-FETCH all chunk metadata in parallel (eliminates serial KV reads)
  const chunkMetas = await prefetchChunkMetas(env, chunks);

  // Immediately start loading first two chunks in parallel
  const prefetchQueue = new Array(Math.min(PREFETCH_AHEAD + 1, chunks.length));
  for (let i = 0; i < prefetchQueue.length; i++) {
    prefetchQueue[i] = loadChunkFromMeta(env, chunkMetas[i], waitUntil);
  }

  const stream = new ReadableStream({
    async start(ctrl) {
      for (let i = 0; i < chunks.length; i++) {
        try {
          // Kick off prefetch of chunk i+PREFETCH_AHEAD before awaiting current
          const nextPrefetch = i + PREFETCH_AHEAD + 1;
          if (nextPrefetch < chunks.length) {
            prefetchQueue[nextPrefetch % prefetchQueue.length] =
              loadChunkFromMeta(env, chunkMetas[nextPrefetch], waitUntil);
          }

          const data = await prefetchQueue[i % prefetchQueue.length];
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
      'ETag'               : etag,
      'Vary'               : 'Range',
      'Content-Disposition': contentDisposition(false, filename),
      'X-Streaming-Mode'   : 'instant-play-pipelined',
    }
  });
}

// ── Smart Range ───────────────────────────────────────────────────────────────
// ⚡ Parallel fetch of needed chunks, correct byte slicing
async function smartRange(env, meta, mime, rangeHeader, isDL, waitUntil, etag) {
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

  // ⚡ Pre-fetch chunk metas + data all in parallel
  const chunkMetas = await prefetchChunkMetas(env, needed);
  const fetched    = await Promise.all(
    chunkMetas.map(cm => loadChunkFromMeta(env, cm, waitUntil))
  );

  let pos = startChunk * cs;
  const stream = new ReadableStream({
    start(ctrl) {
      for (let i = 0; i < fetched.length; i++) {
        const arr       = new Uint8Array(fetched[i]);
        const sliceFrom = Math.max(start - pos, 0);
        const sliceTo   = Math.min(arr.length, end - pos + 1);
        if (sliceFrom < sliceTo) ctrl.enqueue(arr.slice(sliceFrom, sliceTo));
        pos += arr.length; // use actual length, not assumed cs
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
      'ETag'               : etag,
      'Vary'               : 'Range',
      'Content-Disposition': contentDisposition(isDL, filename),
    }
  });
}

// ── Full Download ─────────────────────────────────────────────────────────────
// ⚡ Prefetch pipeline — next chunk loads while current is being sent
function fullDownload(env, meta, mime, waitUntil, etag) {
  const { chunks, size: total, filename } = meta;

  const stream = new ReadableStream({
    async start(ctrl) {
      // ⚡ Pre-fetch all chunk metas in parallel
      let chunkMetas;
      try {
        chunkMetas = await prefetchChunkMetas(env, chunks);
      } catch (e) { ctrl.error(e); return; }

      // Seed prefetch queue
      const queue = new Array(Math.min(PREFETCH_AHEAD + 1, chunks.length));
      for (let i = 0; i < queue.length; i++) {
        queue[i] = loadChunkFromMeta(env, chunkMetas[i], waitUntil);
      }

      for (let i = 0; i < chunks.length; i++) {
        try {
          const next = i + PREFETCH_AHEAD + 1;
          if (next < chunks.length)
            queue[next % queue.length] = loadChunkFromMeta(env, chunkMetas[next], waitUntil);

          const data = await queue[i % queue.length];
          ctrl.enqueue(new Uint8Array(data));
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
      'Accept-Ranges'      : 'bytes',
      'Cache-Control'      : 'public, max-age=31536000',
      'ETag'               : etag,
      'Vary'               : 'Range',
      'Content-Disposition': contentDisposition(true, filename),
    }
  });
}

// ─── CORE: Prefetch all chunk metadata in parallel ─────────────────────────────
// ⚡ BIGGEST WIN: Eliminates serial KV round-trips for multi-chunk files
async function prefetchChunkMetas(env, chunks) {
  return Promise.all(chunks.map(async (chunkInfo) => {
    const kv  = env[chunkInfo.kvNamespace] || env.FILES_KV;
    const key = chunkInfo.keyName || chunkInfo.chunkKey;
    const raw = await kv.get(key);
    if (!raw) throw new Error(`Chunk meta not found: ${key}`);
    const cm = JSON.parse(raw);
    cm.telegramFileId = cm.telegramFileId || cm.fileIdCode;
    cm._kv  = kv;
    cm._key = key;
    return cm;
  }));
}

// ─── CORE: Load chunk data from pre-fetched metadata ──────────────────────────
async function loadChunkFromMeta(env, cm, waitUntil) {
  const kv  = cm._kv  || env.FILES_KV;
  const key = cm._key;

  // ── 1. Try cached directUrl (fastest path)
  if (cm.directUrl) {
    const cached = await cacheGet(cm.directUrl);
    if (cached) return cached.arrayBuffer();

    try {
      const r = await fetch(cm.directUrl, { signal: AbortSignal.timeout(20000) });
      if (r.ok) {
        if (waitUntil) waitUntil(cachePut(cm.directUrl, r.clone()));
        return r.arrayBuffer();
      }
    } catch (_) {}
  }

  // ── 2. Refresh via Telegram API (token rotation)
  const tokens = env._botTokens || (env._botTokens = getBotTokens(env));
  for (const token of tokens) {
    try {
      const freshUrl = await getTelegramUrl(token, cm.telegramFileId);
      if (!freshUrl) continue;

      const r = await fetch(freshUrl, { signal: AbortSignal.timeout(25000) });
      if (!r.ok) continue;

      if (waitUntil && kv && key) {
        const body = r.clone();
        waitUntil(Promise.all([
          cachePut(freshUrl, body),
          kv.put(key, JSON.stringify({ ...cm, directUrl: freshUrl, lastRefreshed: Date.now(), _kv: undefined, _key: undefined })),
        ]));
      }
      return r.arrayBuffer();
    } catch (_) {}
  }

  throw new Error(`All refresh attempts failed: ${key}`);
}

// ─── Legacy loadChunk (for HLS segments) ──────────────────────────────────────
async function loadChunk(env, chunkInfo, waitUntil) {
  const kv  = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const key = chunkInfo.keyName || chunkInfo.chunkKey;
  const raw = await kv.get(key);
  if (!raw) throw new Error(`Chunk not found: ${key}`);

  const cm = JSON.parse(raw);
  cm.telegramFileId = cm.telegramFileId || cm.fileIdCode;
  cm._kv  = kv;
  cm._key = key;
  return loadChunkFromMeta(env, cm, waitUntil);
}

// ─── HELPERS ───────────────────────────────────────────────────────────────────

// ⚡ Get Telegram direct URL — CF Cache avoids repeated getFile API calls
async function getTelegramUrl(token, fileId) {
  if (!fileId) return null;
  const cacheKey = `https://tg-url-cache.internal/${token.slice(-8)}/${fileId}`;

  const cached = await cacheGet(cacheKey);
  if (cached) {
    const text = await cached.text();
    if (text) return text;
  }

  let res;
  try {
    res = await fetch(
      `https://api.telegram.org/bot${token}/getFile?file_id=${encodeURIComponent(fileId)}`,
      { signal: AbortSignal.timeout(8000) }
    );
  } catch (_) { return null; }

  const data = await res.json().catch(() => null);
  if (!data?.ok || !data.result?.file_path) return null;

  const url = `https://api.telegram.org/file/bot${token}/${data.result.file_path}`;

  // Cache for 55 minutes (Telegram URLs expire in ~60 min)
  cachePut(cacheKey, new Response(url, {
    headers: { 'Cache-Control': 'public, max-age=3300' }
  })).catch(() => {});

  return url;
}

// Cloudflare Cache API
async function cacheGet(url) {
  try { return await caches.default.match(new Request(url)); }
  catch (_) { return null; }
}

async function cachePut(url, response) {
  try { await caches.default.put(new Request(url), response); }
  catch (_) {}
}

function getBotTokens(env) {
  return [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
}

// ⚡ RFC 5987 encoded Content-Disposition (supports Unicode filenames)
function contentDisposition(isDL, filename) {
  const disposition = isDL ? 'attachment' : 'inline';
  if (!filename) return disposition;

  // ASCII-safe fallback + RFC 5987 encoded version for Unicode support
  const safe    = filename.replace(/[^\x20-\x7E]/g, '_').replace(/["\\]/g, '_');
  const encoded = encodeURIComponent(filename).replace(/'/g, '%27');
  return `${disposition}; filename="${safe}"; filename*=UTF-8''${encoded}`;
}

function err(msg, status = 500, extraHeaders = {}) {
  return new Response(JSON.stringify({ error: msg, status }), {
    status,
    headers: {
      ...CORS,
      'Content-Type': 'application/json',
      ...extraHeaders,
    }
  });
}
