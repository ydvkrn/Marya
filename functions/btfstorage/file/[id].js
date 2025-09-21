// Minimal, battle-tested video/file streaming for Cloudflare Workers
// Works for small (2MB) and large files, with proper Range (206) handling

const MIME_TYPES = {
  mp4: 'video/mp4',
  webm: 'video/webm',
  mkv: 'video/mp4',
  mov: 'video/quicktime',
  avi: 'video/mp4',
  m4v: 'video/mp4',
  wmv: 'video/mp4',
  flv: 'video/mp4',
  '3gp': 'video/3gpp',
  mp3: 'audio/mpeg',
  wav: 'audio/wav',
  flac: 'audio/flac',
  aac: 'audio/aac',
  m4a: 'audio/mp4',
  ogg: 'audio/ogg',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  png: 'image/png',
  gif: 'image/gif',
  webp: 'image/webp',
  pdf: 'application/pdf',
  txt: 'text/plain',
  zip: 'application/zip'
};

function getMimeType(ext) {
  const e = (ext || '').toLowerCase().replace('.', '');
  return MIME_TYPES[e] || 'application/octet-stream';
}

function isVideo(m) { return m.startsWith('video/'); }
function isStreamable(m) {
  return m.startsWith('video/') || m.startsWith('audio/') || m.startsWith('image/') || m === 'application/pdf';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  try {
    const actualId = fileId.includes('.') ? fileId.slice(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.slice(fileId.lastIndexOf('.') + 1) : '';

    if (!actualId || !actualId.startsWith('MSM')) {
      return new Response('Not found', { status: 404 });
    }

    const kv = {
      FILES_KV: env.FILES_KV,
      FILES_KV2: env.FILES_KV2,
      FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4,
      FILES_KV5: env.FILES_KV5,
      FILES_KV6: env.FILES_KV6,
      FILES_KV7: env.FILES_KV7
    };

    const metaStr = await kv.FILES_KV.get(actualId);
    if (!metaStr) return new Response('File not found', { status: 404 });

    const meta = JSON.parse(metaStr);
    const { filename, size, chunks, chunkSize: savedChunkSize } = meta;
    const mimeType = getMimeType(extension);
    const url = new URL(request.url);
    const isDownload = url.searchParams.get('dl') === '1';

    // Support HEAD quickly (players/probes) [HEAD should not send body]
    if (request.method === 'HEAD') {
      const h = baseHeaders(mimeType, size, filename, isDownload, isVideo(mimeType));
      return new Response(null, { status: 200, headers: h }); // HEAD OK [web:51]
    }

    // CORS preflight (just in case) [OPTIONS OK]
    if (request.method === 'OPTIONS') {
      const h = new Headers();
      h.set('Access-Control-Allow-Origin', '*');
      h.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
      h.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
      h.set('Access-Control-Max-Age', '86400');
      return new Response(null, { status: 200, headers: h }); // OPTIONS OK [web:52]
    }

    const range = request.headers.get('Range');

    // If player asks for Range, must return 206 with Content-Range [web:56][web:87][web:92]
    if (range && !isDownload) {
      return await serveRange(range, kv, meta, mimeType);
    }

    // For small files (<=10MB), load-all then return 200 (fast start) [practical]
    if (size <= 10 * 1024 * 1024) {
      const buf = await loadAllChunks(kv, chunks, env);
      const h = baseHeaders(mimeType, buf.byteLength, filename, isDownload, isVideo(mimeType));
      return new Response(buf, { status: 200, headers: h }); // small full file OK
    }

    // For larger files, stream sequentially (simple and stable)
    const stream = new ReadableStream({
      async start(controller) {
        try {
          for (let i = 0; i < chunks.length; i++) {
            const chunkData = await fetchChunk(kv, chunks[i], env);
            controller.enqueue(new Uint8Array(chunkData));
            await delay(50);
          }
          controller.close();
        } catch (e) {
          controller.error(e);
        }
      }
    });

    const h = baseHeaders(mimeType, size, filename, isDownload, isVideo(mimeType));
    return new Response(stream, { status: 200, headers: h }); // full stream OK
  } catch (e) {
    return new Response('Server error', { status: 500 });
  }
}

function baseHeaders(mimeType, size, filename, isDownload, isVideoFile) {
  const h = new Headers();
  h.set('Content-Type', mimeType);
  h.set('Content-Length', String(size));
  h.set('Accept-Ranges', 'bytes'); // critical for seeking [web:56]
  h.set('Access-Control-Allow-Origin', '*');
  h.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges, Content-Range, Content-Type'); // helpful [web:56]
  h.set('Cache-Control', isVideoFile ? 'public, max-age=31536000' : 'public, max-age=86400');

  if (isDownload || !isStreamable(mimeType)) {
    h.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    h.set('Content-Disposition', 'inline');
    if (isVideoFile) {
      h.set('X-Content-Type-Options', 'nosniff'); // avoid sniff issues
      h.set('Cross-Origin-Resource-Policy', 'cross-origin');
    }
  }
  return h;
}

async function serveRange(range, kv, meta, mimeType) {
  const { size, chunks } = meta;
  const chunkSize = meta.chunkSize || Math.ceil(size / chunks.length);

  const m = range.match(/bytes=(\d+)-(\d*)/);
  if (!m) {
    const h = new Headers();
    h.set('Content-Range', `bytes */${size}`);
    h.set('Accept-Ranges', 'bytes');
    return new Response('Range Not Satisfiable', { status: 416, headers: h }); // spec compliant [web:56]
  }

  let start = parseInt(m[1], 10);
  let end = m[2] ? parseInt(m[2], 10) : size - 1;

  if (Number.isNaN(start) || start < 0 || start >= size) {
    const h = new Headers();
    h.set('Content-Range', `bytes */${size}`);
    h.set('Accept-Ranges', 'bytes');
    return new Response('Range Not Satisfiable', { status: 416, headers: h }); // spec [web:56]
  }
  if (Number.isNaN(end) || end >= size) end = size - 1;
  if (start > end) {
    const h = new Headers();
    h.set('Content-Range', `bytes */${size}`);
    h.set('Accept-Ranges', 'bytes');
    return new Response('Range Not Satisfiable', { status: 416, headers: h }); // spec [web:56]
  }

  const needStart = Math.floor(start / chunkSize);
  const needEnd = Math.floor(end / chunkSize);
  const needed = chunks.slice(needStart, needEnd + 1);

  // Load all needed chunks sequentially (simple & reliable)
  const parts = [];
  for (let i = 0; i < needed.length; i++) {
    const buf = await fetchChunk(kv, needed[i], null);
    parts.push(new Uint8Array(buf));
  }
  const combinedLen = parts.reduce((s, b) => s + b.byteLength, 0);
  const combined = new Uint8Array(combinedLen);
  let off = 0;
  for (const p of parts) { combined.set(p, off); off += p.byteLength; }

  // Slice exact byte range from combined buffer
  const offsetWithinFirst = start - (needStart * chunkSize);
  const exactLen = (end - start + 1);
  const slice = combined.slice(offsetWithinFirst, offsetWithinFirst + exactLen);

  const h = new Headers();
  h.set('Content-Type', mimeType);
  h.set('Content-Length', String(slice.byteLength));
  h.set('Content-Range', `bytes ${start}-${end}/${size}`); // critical [web:92]
  h.set('Accept-Ranges', 'bytes');
  h.set('Access-Control-Allow-Origin', '*');
  h.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges, Content-Range, Content-Type');

  return new Response(slice, { status: 206, headers: h }); // must be 206 [web:87]
}

async function loadAllChunks(kv, chunks, env) {
  const bufs = [];
  for (let i = 0; i < chunks.length; i++) {
    const buf = await fetchChunk(kv, chunks[i], env);
    bufs.push(new Uint8Array(buf));
  }
  const total = bufs.reduce((s, b) => s + b.byteLength, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const b of bufs) { out.set(b, off); off += b.byteLength; }
  return out;
}

async function fetchChunk(kvNamespaces, chunkInfo, env) {
  const ns = kvNamespaces[chunkInfo.kvNamespace];
  const key = chunkInfo.keyName;

  const metaStr = await ns.get(key);
  if (!metaStr) throw new Error(`Chunk missing: ${key}`);
  const cm = JSON.parse(metaStr);

  let res = await fetch(cm.directUrl);
  if (!res.ok && (res.status === 403 || res.status === 404 || res.status === 410)) {
    // Single refresh attempt against Telegram getFile
    const tokens = [env?.BOT_TOKEN, env?.BOT_TOKEN2, env?.BOT_TOKEN3, env?.BOT_TOKEN4].filter(Boolean);
    if (tokens.length) {
      const t = tokens[0];
      const gf = await fetch(`https://api.telegram.org/bot${t}/getFile?file_id=${encodeURIComponent(cm.telegramFileId)}`);
      if (gf.ok) {
        const data = await gf.json();
        if (data.ok && data.result?.file_path) {
          const fresh = `https://api.telegram.org/file/bot${t}/${data.result.file_path}`;
          res = await fetch(fresh);
          if (res.ok) {
            // async update KV
            ns.put(key, JSON.stringify({ ...cm, directUrl: fresh, lastRefreshed: Date.now() })).catch(() => {});
          }
        }
      }
    }
  }
  if (!res.ok) throw new Error(`Chunk fetch failed ${res.status}`);
  return await res.arrayBuffer();
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }
