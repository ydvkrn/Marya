// pages/functions/btfstorage/file/[id].js

const MIME = {
  mp4: 'video/mp4', webm: 'video/webm', mkv: 'video/mp4', avi: 'video/mp4',
  mov: 'video/mp4', m4v: 'video/mp4', wmv: 'video/mp4', flv: 'video/mp4', '3gp': 'video/mp4',
  mp3: 'audio/mpeg', wav: 'audio/wav', aac: 'audio/mp4', m4a: 'audio/mp4', ogg: 'audio/ogg',
  jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', gif: 'image/gif', webp: 'image/webp',
  pdf: 'application/pdf', txt: 'text/plain', zip: 'application/zip'
};

const INITIAL_RANGE_BYTES = 4 * 1024 * 1024; // 4MB kick to force Range mode

const extMime = (name) => {
  const ext = (name?.split('.').pop() || '').toLowerCase();
  return MIME[ext] || 'application/octet-stream';
};

const parseRange = (h, size) => {
  if (!h) return null;
  const m = h.match(/bytes=(\d+)-(\d*)/);
  if (!m) return null;
  const start = parseInt(m[1], 10);
  const end = m[2] ? Math.min(size - 1, parseInt(m[2], 10)) : size - 1;
  if (Number.isNaN(start) || Number.isNaN(end) || start > end || start >= size) return null;
  return [{ start, end }];
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const idWithExt = params.id;
  const id = idWithExt.includes('.') ? idWithExt.slice(0, idWithExt.lastIndexOf('.')) : idWithExt;
  if (!id.startsWith('MSM')) return new Response('Not found', { status: 404 });

  const kv = env.FILES_KV;
  const metaStr = await kv.get(id);
  if (!metaStr) return new Response('Not found', { status: 404 });

  const meta = JSON.parse(metaStr);
  const mime = extMime(meta.filename || idWithExt);
  const url = new URL(request.url);
  const isDl = url.searchParams.get('dl') === '1';
  const rangeHdr = request.headers.get('Range');

  // Fast path: single Telegram file (no chunks) -> proxy pass-through with Range
  if (meta.telegramFileId && (!meta.chunks || meta.chunks.length <= 1)) {
    return proxyTelegram(request, env, meta.telegramFileId, mime);
  }

  // Chunked path
  const size = meta.size;
  const chunks = meta.chunks || [];
  const chunkSize = meta.chunkSize || Math.ceil(size / Math.max(1, chunks.length));

  // If no Range and not a forced download, serve small 206 to push the browser into Range mode
  if (!rangeHdr && !isDl && size > INITIAL_RANGE_BYTES) {
    const start = 0;
    const end = Math.min(size - 1, INITIAL_RANGE_BYTES - 1);
    return serveRangeFromChunks(env, request, meta, chunks, chunkSize, mime, start, end);
  }

  // If Range present (video/audio seek) handle exact window
  if (rangeHdr && !isDl) {
    const r = parseRange(rangeHdr, size);
    if (!r) return new Response('Range Not Satisfiable', { status: 416, headers: { 'Content-Range': `bytes */${size}`, 'Accept-Ranges': 'bytes' } });
    const { start, end } = r[0];
    return serveRangeFromChunks(env, request, meta, chunks, chunkSize, mime, start, end);
  }

  // For large downloads with many chunks, start with a 206 to force the client into ranged download
  if (isDl && chunks.length > 45) {
    const start = 0;
    const end = Math.min(size - 1, INITIAL_RANGE_BYTES - 1);
    return serveRangeFromChunks(env, request, meta, chunks, chunkSize, mime, start, end, /*dl*/true);
  }

  // Small downloads (<=45 chunks): stream sequentially, one chunk at a time
  return streamAllChunksSequentially(env, chunks, mime, size, meta.filename, isDl);
}

// Pass-through proxy for a single Telegram file_id (for perfect streaming)
async function proxyTelegram(request, env, fileId, mime) {
  const range = request.headers.get('Range') || undefined;
  const bot = env.BOT_TOKEN || env.BOT_TOKEN2 || env.BOT_TOKEN3 || env.BOT_TOKEN4;
  const gf = await fetch(`https://api.telegram.org/bot${bot}/getFile?file_id=${encodeURIComponent(fileId)}`, { signal: AbortSignal.timeout(15000) });
  const data = await gf.json();
  if (!data?.ok || !data.result?.file_path) return new Response('telegram getFile failed', { status: 502 });
  const direct = `https://api.telegram.org/file/bot${bot}/${data.result.file_path}`;
  const upstream = await fetch(direct, { headers: range ? { Range: range } : {} });
  const headers = new Headers(upstream.headers);
  headers.set('Content-Type', mime);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  return new Response(upstream.body, { status: upstream.status, headers });
}

// Range window from chunks: fetch only overlapping chunks, minimal in-memory stitching
async function serveRangeFromChunks(env, request, meta, chunks, chunkSize, mime, start, end, forceDownload = false) {
  const size = meta.size;
  const startIdx = Math.floor(start / chunkSize);
  const endIdx = Math.floor(end / chunkSize);
  const needed = chunks.slice(startIdx, endIdx + 1);

  const parts = [];
  for (let i = 0; i < needed.length; i++) {
    const info = needed[i];
    const ab = await fetchChunk(env, info); // fetch this chunk fully (usually 10–20MB)
    parts.push(new Uint8Array(ab));
  }

  const total = parts.reduce((a, p) => a + p.byteLength, 0);
  const combined = new Uint8Array(total);
  let off = 0; for (const p of parts) { combined.set(p, off); off += p.byteLength; }

  const offset = start - (startIdx * chunkSize);
  const slice = combined.subarray(offset, offset + (end - start + 1));

  const h = new Headers();
  h.set('Content-Type', mime);
  h.set('Content-Length', String(slice.byteLength));
  h.set('Content-Range', `bytes ${start}-${end}/${size}`);
  h.set('Accept-Ranges', 'bytes');
  h.set('Access-Control-Allow-Origin', '*');
  if (forceDownload) h.set('Content-Disposition', `attachment; filename="${meta.filename || 'file'}"`);
  return new Response(slice, { status: 206, headers: h });
}

// Small files: stream all chunks sequentially without buffering whole file
function streamAllChunksSequentially(env, chunks, mime, size, filename, isDl) {
  const rs = new ReadableStream({
    async start(controller) {
      try {
        for (let i = 0; i < chunks.length; i++) {
          const ab = await fetchChunk(env, chunks[i]);
          controller.enqueue(new Uint8Array(ab));
          await new Promise(r => setTimeout(r, 10)); // tiny yield to keep CPU low
        }
        controller.close();
      } catch (e) {
        controller.error(e);
      }
    }
  });
  const h = new Headers();
  h.set('Content-Type', mime);
  h.set('Content-Length', String(size));
  h.set('Accept-Ranges', 'bytes');
  h.set('Access-Control-Allow-Origin', '*');
  if (isDl) h.set('Content-Disposition', `attachment; filename="${filename || 'file'}"`);
  else h.set('Content-Disposition', 'inline');
  return new Response(rs, { status: 200, headers: h });
}

// Load a single Telegram-backed chunk, auto-refreshing on 403/404
async function fetchChunk(env, info) {
  const kv = env[info.kvNamespace] || env.FILES_KV;
  const metaStr = await kv.get(info.keyName);
  if (!metaStr) throw new Error(`chunk meta missing: ${info.keyName}`);
  const m = JSON.parse(metaStr);

  // try direct url
  let res = await fetch(m.directUrl, { signal: AbortSignal.timeout(60000) });
  if (res.ok) return res.arrayBuffer();

  // refresh via first available bot
  const bot = env.BOT_TOKEN || env.BOT_TOKEN2 || env.BOT_TOKEN3 || env.BOT_TOKEN4;
  const gf = await fetch(`https://api.telegram.org/bot${bot}/getFile?file_id=${encodeURIComponent(m.telegramFileId)}`, { signal: AbortSignal.timeout(15000) });
  const j = await gf.json();
  if (!j?.ok || !j.result?.file_path) throw new Error('telegram getFile failed');
  const fresh = `https://api.telegram.org/file/bot${bot}/${j.result.file_path}`;
  res = await fetch(fresh, { signal: AbortSignal.timeout(60000) });
  if (!res.ok) throw new Error(`chunk fetch failed: ${res.status}`);
  // best-effort KV update
  kv.put(info.keyName, JSON.stringify({ ...m, directUrl: fresh, lastRefreshed: Date.now() })).catch(() => {});
  return res.arrayBuffer();
}
