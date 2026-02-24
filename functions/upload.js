// functions/btfstorage/upload.js
// ⚡ ULTRA-FAST Multi-KV Upload — Cloudflare Pages Function
// Fixes: directUrl in master metadata, multi-bot round-robin, retry logic, dynamic limits

// ─── CORS (static, reused) ─────────────────────────────────────────────────────
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB per chunk (Telegram max per file)

// ─── MAIN HANDLER ──────────────────────────────────────────────────────────────
export async function onRequest({ request, env, waitUntil }) {

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: CORS });
  }

  if (request.method !== 'POST') {
    return jsonRes({ success: false, error: 'Method not allowed' }, 405);
  }

  try {
    // ── Collect available resources ──────────────────────────────────────────
    const kvList = [
      env.FILES_KV,  env.FILES_KV2, env.FILES_KV3,
      env.FILES_KV4, env.FILES_KV5, env.FILES_KV6, env.FILES_KV7
    ]
      .map((kv, i) => kv ? { kv, name: `FILES_KV${i === 0 ? '' : i + 1}` } : null)
      .filter(Boolean);

    const botTokens = [
      env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4
    ].filter(Boolean);

    const CHANNEL_ID = env.CHANNEL_ID;

    if (!botTokens.length || !CHANNEL_ID) throw new Error('Missing bot credentials');
    if (!kvList.length)                   throw new Error('No KV namespaces available');

    // ── Parse file ───────────────────────────────────────────────────────────
    const formData = await request.formData();
    const file     = formData.get('file');
    if (!file) throw new Error('No file provided');

    // ── Validate size ────────────────────────────────────────────────────────
    const MAX_SIZE = kvList.length * CHUNK_SIZE; // Dynamic: 7 KV × 20MB = 140MB
    if (file.size > MAX_SIZE) {
      throw new Error(
        `File too large: ${mb(file.size)}MB (max ${mb(MAX_SIZE)}MB with ${kvList.length} KV namespaces)`
      );
    }

    // ── Generate file ID ─────────────────────────────────────────────────────
    const fileId    = `id${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const ext       = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvList.length) {
      throw new Error(`File needs ${totalChunks} chunks but only ${kvList.length} KV namespaces available`);
    }

    // ── Upload all chunks in PARALLEL ────────────────────────────────────────
    const chunkJobs = Array.from({ length: totalChunks }, (_, i) => {
      const start  = i * CHUNK_SIZE;
      const end    = Math.min(start + CHUNK_SIZE, file.size);
      const blob   = file.slice(start, end);
      const chunkFile = new File([blob], `${file.name}.part${i}`, { type: file.type });

      // Round-robin: different bot + different KV per chunk
      const token = botTokens[i % botTokens.length];
      const kv    = kvList[i % kvList.length];

      return uploadChunkWithRetry(chunkFile, fileId, i, token, CHANNEL_ID, kv);
    });

    const chunkResults = await Promise.all(chunkJobs);

    // ─ 🔑 KEY FIX: directUrl included in master metadata ─────────────────────
    const masterMeta = {
      filename:    file.name,
      size:        file.size,
      contentType: file.type || guessMime(ext),
      extension:   ext,
      uploadedAt:  Date.now(),
      type:        'multi_kv_chunked',
      totalChunks,
      chunkSize:   CHUNK_SIZE,
      chunks: chunkResults.map((r, i) => ({
        index:          i,
        kvNamespace:    r.kvNamespace,
        telegramFileId: r.telegramFileId,
        directUrl:      r.directUrl,      // ✅ Enables zero-KV-read streaming
        directUrlExpiry: r.directUrlExpiry,
        size:           r.size,
        chunkKey:       r.chunkKey,
      }))
    };

    // Background: save master metadata (don't block response)
    // Using waitUntil so response goes out instantly
    waitUntil(kvList[0].kv.put(fileId, JSON.stringify(masterMeta)));

    // ── Build response URLs ───────────────────────────────────────────────────
    const base        = new URL(request.url).origin;
    const streamUrl   = `${base}/btfstorage/server/${fileId}${ext}`;
    const downloadUrl = `${base}/btfstorage/server/${fileId}${ext}?dl=1`;
    const m3u8Url     = totalChunks > 1 ? `${base}/btfstorage/server/${fileId}.m3u8` : null;

    return jsonRes({
      success:   true,
      id:        fileId,
      filename:  file.name,
      size:      file.size,
      sizeMb:    mb(file.size),
      contentType: file.type,
      urls: {
        stream:   streamUrl,
        download: downloadUrl,
        hls:      m3u8Url,
      },
      meta: {
        chunks:          totalChunks,
        chunkSizeMb:     mb(CHUNK_SIZE),
        kvDistribution:  chunkResults.map(r => r.kvNamespace),
        botDistribution: chunkResults.map(r => r.botUsed),
      }
    });

  } catch (error) {
    return jsonRes({ success: false, error: error.message }, 500);
  }
}

// ─── UPLOAD WITH RETRY ─────────────────────────────────────────────────────────
async function uploadChunkWithRetry(chunkFile, fileId, index, botToken, channelId, kvEntry, maxRetries = 3) {
  let lastError;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await uploadChunk(chunkFile, fileId, index, botToken, channelId, kvEntry);
    } catch (e) {
      lastError = e;
      if (attempt < maxRetries - 1) {
        // Exponential backoff: 1s, 2s, 4s
        await sleep(1000 * Math.pow(2, attempt));
      }
    }
  }

  throw new Error(`Chunk ${index} failed after ${maxRetries} attempts: ${lastError?.message}`);
}

// ─── CORE CHUNK UPLOAD ─────────────────────────────────────────────────────────
async function uploadChunk(chunkFile, fileId, index, botToken, channelId, kvEntry) {
  // 1️⃣ Upload to Telegram
  const form = new FormData();
  form.append('chat_id', channelId);
  form.append('document', chunkFile);

  const tgUpload = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body:   form,
    signal: AbortSignal.timeout(120_000) // 2 min timeout for large chunks
  });

  if (!tgUpload.ok) {
    throw new Error(`Telegram upload HTTP ${tgUpload.status} for chunk ${index}`);
  }

  const tgData = await tgUpload.json();
  if (!tgData.ok || !tgData.result?.document?.file_id) {
    throw new Error(`Telegram upload failed for chunk ${index}: ${tgData.description || 'unknown'}`);
  }

  const telegramFileId = tgData.result.document.file_id;

  // 2️⃣ Get direct URL immediately (fresh, valid for ~60min)
  const getFile = await fetch(
    `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
    { signal: AbortSignal.timeout(10_000) }
  );

  const fileData = await getFile.json();
  if (!fileData.ok || !fileData.result?.file_path) {
    throw new Error(`getFile failed for chunk ${index}`);
  }

  const directUrl      = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
  const directUrlExpiry = Date.now() + 55 * 60 * 1000; // 55 min from now

  // 3️⃣ Store chunk metadata in its assigned KV namespace
  const chunkKey      = `${fileId}_chunk_${index}`;
  const chunkMetadata = {
    telegramFileId,
    directUrl,
    directUrlExpiry,
    size:        chunkFile.size,
    index,
    parentFileId: fileId,
    kvNamespace:  kvEntry.name,
    uploadedAt:   Date.now(),
  };

  await kvEntry.kv.put(chunkKey, JSON.stringify(chunkMetadata));

  return {
    telegramFileId,
    directUrl,
    directUrlExpiry,
    size:        chunkFile.size,
    chunkKey,
    kvNamespace: kvEntry.name,
    botUsed:     `bot_${botToken.slice(-4)}`, // Last 4 chars for debug, not full token
  };
}

// ─── HELPERS ───────────────────────────────────────────────────────────────────
const mb    = (bytes) => Math.round(bytes / 1024 / 1024);
const sleep = (ms)    => new Promise(r => setTimeout(r, ms));

function jsonRes(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS }
  });
}

function guessMime(ext) {
  const map = {
    mp4: 'video/mp4', mkv: 'video/x-matroska', webm: 'video/webm',
    mp3: 'audio/mpeg', m4a: 'audio/mp4',
    jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', webp: 'image/webp',
    pdf: 'application/pdf', zip: 'application/zip',
  };
  return map[ext?.replace('.', '').toLowerCase()] || 'application/octet-stream';
}
