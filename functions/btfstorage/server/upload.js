// functions/btfstorage/upload.js
// ⚡ 2GB Support — Receives pre-chunked pieces from client, stores to Telegram + KV metadata
// Client splits file into 45MB chunks and uploads each chunk separately

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-File-Id, X-Chunk-Index, X-Total-Chunks, X-File-Name, X-File-Size, X-File-Type',
};

export async function onRequest({ request, env, waitUntil }) {
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });

  // ── GET: Check upload session status ────────────────────────────────────────
  if (request.method === 'GET') {
    const url    = new URL(request.url);
    const fileId = url.searchParams.get('fileId');
    if (!fileId) return jsonRes({ error: 'Missing fileId' }, 400);

    const kvList = getKvList(env);
    const meta   = await findInKv(kvList, fileId);
    if (!meta) return jsonRes({ error: 'Not found' }, 404);
    return jsonRes(meta);
  }

  if (request.method !== 'POST') return jsonRes({ error: 'Method not allowed' }, 405);

  try {
    const kvList    = getKvList(env);
    const botTokens = getBotTokens(env);
    const CHANNEL_ID = env.CHANNEL_ID;

    if (!botTokens.length || !CHANNEL_ID) throw new Error('Missing bot credentials');
    if (!kvList.length) throw new Error('No KV namespaces available');

    // ── Read headers sent by client ────────────────────────────────────────────
    const fileId      = request.headers.get('X-File-Id');
    const chunkIndex  = parseInt(request.headers.get('X-Chunk-Index') || '0', 10);
    const totalChunks = parseInt(request.headers.get('X-Total-Chunks') || '1', 10);
    const fileName    = request.headers.get('X-File-Name') || 'file';
    const fileSize    = parseInt(request.headers.get('X-File-Size') || '0', 10);
    const fileType    = request.headers.get('X-File-Type') || 'application/octet-stream';

    if (!fileId) throw new Error('Missing X-File-Id header');

    // ── Get chunk blob from body ───────────────────────────────────────────────
    const chunkBuffer = await request.arrayBuffer();
    if (!chunkBuffer.byteLength) throw new Error('Empty chunk body');

    const chunkBlob = new File(
      [chunkBuffer],
      `${fileName}.part${chunkIndex}`,
      { type: fileType }
    );

    // ── Pick bot + KV for this chunk (round-robin) ────────────────────────────
    const botToken = botTokens[chunkIndex % botTokens.length];
    const kvEntry  = kvList[chunkIndex % kvList.length];

    // ── Upload this chunk to Telegram ─────────────────────────────────────────
    const chunkResult = await uploadChunkWithRetry(
      chunkBlob, fileId, chunkIndex, botToken, CHANNEL_ID, kvEntry
    );

    // ── If all chunks done, assemble master metadata ──────────────────────────
    if (chunkIndex === totalChunks - 1) {
      // Collect all chunk metadata from KV
      const allChunks = await collectAllChunks(fileId, totalChunks, kvList);

      const ext = fileName.includes('.') ? fileName.slice(fileName.lastIndexOf('.')) : '';
      const masterMeta = {
        filename:    fileName,
        size:        fileSize,
        contentType: fileType || guessMime(ext),
        extension:   ext,
        uploadedAt:  Date.now(),
        type:        'multi_kv_chunked',
        totalChunks,
        chunkSize:   45 * 1024 * 1024,
        complete:    true,
        chunks:      allChunks,
      };

      waitUntil(kvList[0].kv.put(fileId, JSON.stringify(masterMeta)));

      const base        = new URL(request.url).origin;
      const streamUrl   = `${base}/btfstorage/server/${fileId}${ext}`;
      const downloadUrl = `${base}/btfstorage/server/${fileId}${ext}?dl=1`;
      const m3u8Url     = totalChunks > 1 ? `${base}/btfstorage/server/${fileId}.m3u8` : null;

      return jsonRes({
        success:     true,
        complete:    true,
        id:          fileId,
        filename:    fileName,
        size:        fileSize,
        contentType: fileType,
        urls: {
          stream:   streamUrl,
          download: downloadUrl,
          hls:      m3u8Url,
        },
        meta: {
          totalChunks,
          kvDistribution:  allChunks.map(c => c.kvNamespace),
          botDistribution: allChunks.map(c => c.botUsed),
        }
      });
    }

    // ── Intermediate chunk: return progress ───────────────────────────────────
    return jsonRes({
      success:    true,
      complete:   false,
      chunkIndex,
      totalChunks,
      chunkKey:   chunkResult.chunkKey,
      kvNamespace: chunkResult.kvNamespace,
    });

  } catch (error) {
    return jsonRes({ success: false, error: error.message }, 500);
  }
}

// ─── Collect all chunk metadata from across KV namespaces ─────────────────────
async function collectAllChunks(fileId, totalChunks, kvList) {
  const jobs = Array.from({ length: totalChunks }, async (_, i) => {
    const key = `${fileId}_chunk_${i}`;
    // Search all KV namespaces for this chunk
    for (const { kv } of kvList) {
      try {
        const val = await kv.get(key);
        if (val) return JSON.parse(val);
      } catch (_) {}
    }
    throw new Error(`Chunk ${i} metadata not found in any KV`);
  });
  return Promise.all(jobs);
}

// ─── Upload with retry ────────────────────────────────────────────────────────
async function uploadChunkWithRetry(chunkFile, fileId, index, botToken, channelId, kvEntry, maxRetries = 3) {
  let lastError;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await uploadChunk(chunkFile, fileId, index, botToken, channelId, kvEntry);
    } catch (e) {
      lastError = e;
      if (attempt < maxRetries - 1) await sleep(1000 * Math.pow(2, attempt));
    }
  }
  throw new Error(`Chunk ${index} failed after ${maxRetries} attempts: ${lastError?.message}`);
}

// ─── Core chunk upload to Telegram ───────────────────────────────────────────
async function uploadChunk(chunkFile, fileId, index, botToken, channelId, kvEntry) {
  const form = new FormData();
  form.append('chat_id', channelId);
  form.append('document', chunkFile);

  const tgUpload = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body:   form,
    signal: AbortSignal.timeout(180_000), // 3 min for large chunks
  });

  if (!tgUpload.ok) throw new Error(`Telegram HTTP ${tgUpload.status} chunk ${index}`);

  const tgData = await tgUpload.json();
  if (!tgData.ok || !tgData.result?.document?.file_id) {
    throw new Error(`Telegram upload failed chunk ${index}: ${tgData.description || 'unknown'}`);
  }

  const telegramFileId = tgData.result.document.file_id;

  // Get direct URL immediately
  const fileInfo = await fetch(
    `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
    { signal: AbortSignal.timeout(15_000) }
  ).then(r => r.json());

  if (!fileInfo.ok || !fileInfo.result?.file_path) {
    throw new Error(`getFile failed chunk ${index}`);
  }

  const directUrl       = `https://api.telegram.org/file/bot${botToken}/${fileInfo.result.file_path}`;
  const directUrlExpiry = Date.now() + 55 * 60 * 1000;

  const chunkKey  = `${fileId}_chunk_${index}`;
  const chunkMeta = {
    telegramFileId,
    directUrl,
    directUrlExpiry,
    botToken, // stored for URL refresh
    size:         chunkFile.size,
    index,
    parentFileId: fileId,
    kvNamespace:  kvEntry.name,
    uploadedAt:   Date.now(),
    botUsed:      `bot_${botToken.slice(-4)}`,
  };

  await kvEntry.kv.put(chunkKey, JSON.stringify(chunkMeta));

  return { ...chunkMeta, chunkKey };
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

async function findInKv(kvList, key) {
  for (const { kv } of kvList) {
    try {
      const val = await kv.get(key);
      if (val) return JSON.parse(val);
    } catch (_) {}
  }
  return null;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

function jsonRes(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

function guessMime(ext) {
  const map = {
    mp4: 'video/mp4', mkv: 'video/x-matroska', webm: 'video/webm',
    mp3: 'audio/mpeg', m4a: 'audio/mp4', wav: 'audio/wav',
    jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', webp: 'image/webp', gif: 'image/gif',
    pdf: 'application/pdf', zip: 'application/zip', rar: 'application/x-rar-compressed',
  };
  return map[ext?.replace('.', '').toLowerCase()] || 'application/octet-stream';
}
