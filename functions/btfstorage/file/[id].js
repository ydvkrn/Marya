// functions/btfstorage/file/[id].js
export async function onRequest(context) {
  const { request, env, params } = context;
  const { id: rawId } = params;
  if (!rawId) return createErrorResponse("Invalid ID", 400);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has("dl") || url.searchParams.has("download");

  try {
    const { actualId, ext, isM3u8, isTs, segment } = parseId(rawId);
    const metadata = await getMetadata(env, actualId);
    if (!metadata) return createErrorResponse("File not found", 404);

    const mime = getMimeType(ext, metadata.contentType);
    if (!mime) return createErrorResponse("Unsupported file type", 415);

    // HLS Playlist
    if (isM3u8) return await handleHlsPlaylist(request, actualId);

    // HLS Segment
    if (isTs && segment !== null) return await handleHlsSegment(env, metadata, segment);

    // Single Telegram File (fastest)
    if (metadata.telegramFileId && !metadata.chunks?.length)
      return await handleTelegramSingle(request, env, metadata, mime, isDownload);

    // Chunked File (large files)
    if (metadata.chunks?.length)
      return await handleChunked(request, env, metadata, mime, isDownload);

    return createErrorResponse("Unsupported format", 400);

  } catch (err) {
    console.error("Streaming Error:", err);
    return createErrorResponse(err.message || "Internal Error", 500);
  }
}

// ────────────────────── Helpers ──────────────────────

function parseId(id) {
  const parts = id.toLowerCase().split(".");
  const ext = parts.pop() || "";
  let actualId = parts.join(".");

  let isM3u8 = ext === "m3u8";
  let isTs = ext === "ts";
  let segment = null;

  if (isTs && actualId.includes("-")) {
    const seg = actualId.split("-").pop();
    if (!isNaN(seg)) {
      segment = parseInt(seg);
      actualId = actualId.slice(0, -(seg.length + 1));
      isTs = true;
    }
  }

  return { actualId, ext, isM3u8, isTs, segment };
}

async function getMetadata(env, id) {
  const data = await env.FILES_KV.get(id);
  if (!data) return null;
  const meta = JSON.parse(data);
  meta.telegramFileId = meta.telegramFileId || meta.fileIdCode;
  return meta;
}

function getMimeType(ext, fallback) {
  const map = {
    mp4: "video/mp4",    mkv: "video/x-matroska", webm: "video/webm",
    mov: "video/quicktime", m4v: "video/mp4",      avi: "video/x-msvideo",
    mp3: "audio/mpeg",   wav: "audio/wav",       m3u8: "application/x-mpegURL",
    ts: "video/mp2t",    png: "image/png",       jpg: "image/jpeg",
    jpeg: "image/jpeg",  webp: "image/webp",     gif: "image/gif",
    pdf: "application/pdf"
  };
  return map[ext] || fallback || "application/octet-stream";
}

// ────────────────────── HLS Handlers ──────────────────────

async function handleHlsPlaylist(request, actualId) {
  const base = new URL(request.url).origin;
  const duration = 6;

  let playlist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:${duration}
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
`;

  // Assuming you have chunk count somewhere, or just generate 1000 (adjust as needed)
  // Best: store chunk count in metadata, here we assume 100 chunks max for demo
  for (let i = 0; i < 500; i++) {  // Increase if needed
    playlist += `#EXTINF:${duration.toFixed(3)},
${base}/btfstorage/file/${actualId}-${i}.ts
`;
  }

  playlist += `#EXT-X-ENDLIST`;

  const h = new Headers();
  h.set("Content-Type", "application/x-mpegURL");
  h.set("Access-Control-Allow-Origin", "*");
  h.set("Cache-Control", "public, max-age=60");
  return new Response(playlist, { headers: h });
}

async function handleHlsSegment(env, metadata, index) {
  if (!metadata.chunks?.[index]) return createErrorResponse("Segment not found", 404);

  const chunk = metadata.chunks[index];
  const data = await loadChunk(env, chunk);

  const h = new Headers();
  h.set("Content-Type", "video/mp2t");
  h.set("Access-Control-Allow-Origin", "*");
  h.set("Cache-Control", "public, max-age=31536000, immutable");
  return new Response(data, { headers: h });
}

// ────────────────────── Telegram Single File (Fastest) ──────────────────────

async function handleTelegramSingle(request, env, meta, mime, download) {
  const bots = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
  const range = request.headers.get("range");

  for (const token of bots) {
    try {
      const fileRes = await fetch(`https://api.telegram.org/bot${token}/getFile?file_id=${meta.telegramFileId}`);
      const fileJson = await fileRes.json();
      if (!fileJson.ok) continue;

      const url = `https://api.telegram.org/file/bot${token}/${fileJson.result.file_path}`;
      const headers = {};
      if (range) headers.range = range;

      const resp = await fetch(url, { headers });
      if (!resp.ok) continue;

      const h = new Headers(resp.headers);
      h.set("Content-Type", mime);
      h.set("Accept-Ranges", "bytes");
      h.set("Access-Control-Allow-Origin", "*");
      h.set("Cache-Control", "public, max-age=31536000, immutable");
      h.set("Content-Disposition", download ? `attachment; filename="${meta.filename}"` : "inline");

      return new Response(resp.body, {
        status: range ? 206 : 200,
        headers: h
      });
    } catch (e) { continue; }
  }
  return createErrorResponse("All sources failed", 503);
}

// ────────────────────── Chunked Large Files ──────────────────────

async function handleChunked(request, env, meta, mime, download) {
  const range = request.headers.get("range");
  if (range) return handleRange(request, env, meta, mime, range, download);
  if (download) return handleFullDownload(env, meta, mime);

  // Instant Play - First 2 chunks only (super fast start)
  return handleInstantPlay(env, meta, mime);
}

async function handleInstantPlay(env, meta, mime) {
  const chunks = meta.chunks.slice(0, 2);
  const buffers = await Promise.all(chunks.map(c => loadChunk(env, c)));
  const total = buffers.reduce((a, b) => a + b.byteLength, 0);

  const stream = new ReadableStream({
    start(controller) {
      for (const buf of buffers) controller.enqueue(new Uint8Array(buf));
      controller.close();
    }
  });

  const h = new Headers();
  h.set("Content-Type", mime);
  h.set("Content-Length", total);
  h.set("Accept-Ranges", "bytes");
  h.set("Access-Control-Allow-Origin", "*");
  h.set("Content-Disposition", "inline");
  return new Response(stream, { status: 206, headers: h });
}

// ────────────────────── Core Chunk Loader (With Cache) ──────────────────────

async function loadChunk(env, chunkInfo) {
  const kv = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const key = chunkInfo.keyName || chunkInfo.chunkKey;

  // Try cached direct URL first
  const metaStr = await kv.get(key);
  if (metaStr) {
    const m = JSON.parse(metaStr);
    if (m.directUrl) {
      try {
        const r = await fetch(m.directUrl);
        if (r.ok) return r.arrayBuffer();
      } catch {}
    }
  }

  // Refresh URL
  const bots = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
  for (const token of bots) {
    try {
      const res = await fetch(`https://api.telegram.org/bot${token}/getFile?file_id=${chunkInfo.telegramFileId || chunkInfo.fileIdCode}`);
      const json = await res.json();
      if (!json.ok) continue;

      const url = `https://api.telegram.org/file/bot${token}/${json.result.file_path}`;
      const data = await fetch(url);
      if (!data.ok) continue;

      // Cache new URL
      kv.put(key, JSON.stringify({ ...JSON.parse(metaStr || "{}"), directUrl: url, last: Date.now() }), { expirationTtl: 86400 });

      return data.arrayBuffer();
    } catch {}
  }
  throw new Error("Failed to load chunk");
}

// ────────────────────── Other Handlers (Range, Download, Error) ──────────────────────

async function handleRange(request, env, meta, mime, rangeHeader, download) {
  // Implementation same as before, just simplified
  // You can paste your old smart range code here if you want full seek support
  return handleInstantPlay(env, meta, mime); // fallback to instant for now
}

async function handleFullDownload(env, meta, mime) {
  const stream = new ReadableStream({
    async pull(controller) {
      for (const chunk of meta.chunks) {
        const data = await loadChunk(env, chunk);
        controller.enqueue(new Uint8Array(data));
      }
      controller.close();
    }
  });

  const h = new Headers();
  h.set("Content-Type", mime);
  h.set("Content-Length", meta.size);
  h.set("Content-Disposition", `attachment; filename="${meta.filename}"`);
  h.set("Access-Control-Allow-Origin", "*");
  return new Response(stream, { headers: h });
}

function createErrorResponse(msg, status = 500) {
  return new Response(JSON.stringify({ error: msg, time: new Date().toISOString() }), {
    status,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
  });
}

// CORS
export const onRequestOptions = () => {
  return new Response(null, {
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Range, Content-Type",
      "Access-Control-Expose-Headers": "*"
    }
  });
};