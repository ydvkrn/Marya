// functions/btfstorage/upload.js
// 🚀 Cloudflare Pages Functions - Advanced File Upload Handler
// URL: marya-hosting.pages.dev/btfstorage/upload

const MAX_CHUNK_SIZE = 20 * 1024 * 1024;   // 20MB per chunk (Telegram bot limit safe zone)
const MAX_SINGLE_SIZE = 50 * 1024 * 1024;  // 50MB single file limit

/**
 * Generate a unique file ID
 */
function generateFileId() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  const part1 = Array.from({ length: 6 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  const part2 = Array.from({ length: 8 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  const part3 = Array.from({ length: 2 }, () => chars[Math.floor(Math.random() * chars.length)]).join('').toLowerCase();
  return `MSM${part1}-${part2}-${part3}`;
}

/**
 * Upload a buffer to Telegram using sendDocument
 */
async function uploadToTelegram(botToken, chatId, buffer, filename, mimeType) {
  const formData = new FormData();
  const blob = new Blob([buffer], { type: mimeType });
  formData.append('document', blob, filename);
  formData.append('chat_id', chatId);
  formData.append('disable_notification', 'true');

  const response = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: formData,
    signal: AbortSignal.timeout(120000) // 2 min timeout
  });

  const data = await response.json();

  if (!data.ok) {
    throw new Error(`Telegram upload failed: ${data.error_code} - ${data.description}`);
  }

  const doc = data.result.document;
  return doc.file_id;
}

/**
 * Get Telegram direct URL for a file_id
 */
async function getTelegramDirectUrl(botToken, fileId) {
  const response = await fetch(
    `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`,
    { signal: AbortSignal.timeout(15000) }
  );
  const data = await response.json();

  if (!data.ok || !data.result?.file_path) {
    throw new Error(`getFile failed: ${data.description}`);
  }

  return `https://api.telegram.org/file/bot${botToken}/${data.result.file_path}`;
}

/**
 * Upload single file (<=50MB)
 */
async function uploadSingleFile(env, buffer, filename, mimeType, fileId) {
  const botToken = env.BOT_TOKEN;
  const chatId = env.TELEGRAM_CHAT_ID;

  if (!botToken || !chatId) {
    throw new Error('BOT_TOKEN or TELEGRAM_CHAT_ID not configured');
  }

  console.log(`🚀 Single upload: ${filename} (${Math.round(buffer.byteLength / 1024 / 1024)}MB)`);

  const telegramFileId = await uploadToTelegram(botToken, chatId, buffer, filename, mimeType);
  const directUrl = await getTelegramDirectUrl(botToken, telegramFileId);

  const metadata = {
    filename,
    size: buffer.byteLength,
    contentType: mimeType,
    telegramFileId,
    directUrl,
    uploadedAt: new Date().toISOString(),
    uploadMode: 'single'
  };

  await env.FILES_KV.put(fileId, JSON.stringify(metadata));

  console.log(`✅ Single file uploaded: ${fileId}`);
  return metadata;
}

/**
 * Upload chunked file (>50MB)
 * Splits into MAX_CHUNK_SIZE pieces and uploads each to Telegram
 */
async function uploadChunkedFile(env, buffer, filename, mimeType, fileId) {
  const botToken = env.BOT_TOKEN;
  const chatId = env.TELEGRAM_CHAT_ID;

  if (!botToken || !chatId) {
    throw new Error('BOT_TOKEN or TELEGRAM_CHAT_ID not configured');
  }

  const totalSize = buffer.byteLength;
  const totalChunks = Math.ceil(totalSize / MAX_CHUNK_SIZE);

  console.log(`🧩 Chunked upload: ${filename}
📦 Total size: ${Math.round(totalSize / 1024 / 1024)}MB
🔢 Total chunks: ${totalChunks}`);

  const chunks = [];

  for (let i = 0; i < totalChunks; i++) {
    const start = i * MAX_CHUNK_SIZE;
    const end = Math.min(start + MAX_CHUNK_SIZE, totalSize);
    const chunkBuffer = buffer.slice(start, end);

    const chunkFilename = `${fileId}-chunk-${i}.bin`;
    const chunkKey = `chunk:${fileId}:${i}`;

    console.log(`📤 Uploading chunk ${i + 1}/${totalChunks}: ${Math.round(chunkBuffer.byteLength / 1024 / 1024)}MB`);

    // Try each bot token
    const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
    let chunkTelegramFileId = null;
    let chunkDirectUrl = null;

    for (let bi = 0; bi < botTokens.length; bi++) {
      try {
        chunkTelegramFileId = await uploadToTelegram(
          botTokens[bi], chatId, chunkBuffer, chunkFilename, 'application/octet-stream'
        );
        chunkDirectUrl = await getTelegramDirectUrl(botTokens[bi], chunkTelegramFileId);
        console.log(`✅ Chunk ${i + 1} uploaded via bot ${bi + 1}`);
        break;
      } catch (err) {
        console.error(`❌ Bot ${bi + 1} failed for chunk ${i + 1}:`, err.message);
        if (bi === botTokens.length - 1) {
          throw new Error(`All bots failed for chunk ${i + 1}: ${err.message}`);
        }
      }
    }

    // Store chunk metadata in KV
    const chunkMeta = {
      telegramFileId: chunkTelegramFileId,
      directUrl: chunkDirectUrl,
      chunkIndex: i,
      size: chunkBuffer.byteLength,
      uploadedAt: new Date().toISOString()
    };

    await env.FILES_KV.put(chunkKey, JSON.stringify(chunkMeta));

    chunks.push({
      keyName: chunkKey,
      kvNamespace: 'FILES_KV',
      size: chunkBuffer.byteLength,
      index: i
    });

    console.log(`✅ Chunk ${i + 1}/${totalChunks} stored in KV: ${chunkKey}`);
  }

  const metadata = {
    filename,
    size: totalSize,
    contentType: mimeType,
    chunks,
    chunkSize: MAX_CHUNK_SIZE,
    uploadedAt: new Date().toISOString(),
    uploadMode: 'chunked'
  };

  await env.FILES_KV.put(fileId, JSON.stringify(metadata));

  console.log(`✅ Chunked file metadata stored: ${fileId} (${totalChunks} chunks)`);
  return metadata;
}

/**
 * Main request handler
 */
export async function onRequest(context) {
  const { request, env } = context;

  // CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Expose-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(
      JSON.stringify({ error: 'Method not allowed', status: 405 }),
      { status: 405, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
    );
  }

  try {
    // Validate required env vars
    if (!env.FILES_KV) {
      throw new Error('FILES_KV KV namespace not bound');
    }
    if (!env.BOT_TOKEN) {
      throw new Error('BOT_TOKEN not configured');
    }
    if (!env.TELEGRAM_CHAT_ID) {
      throw new Error('TELEGRAM_CHAT_ID not configured');
    }

    // Parse multipart form data
    let formData;
    try {
      formData = await request.formData();
    } catch (e) {
      return new Response(
        JSON.stringify({ error: 'Invalid form data. Use multipart/form-data', status: 400 }),
        { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
      );
    }

    const file = formData.get('file');

    if (!file || typeof file === 'string') {
      return new Response(
        JSON.stringify({ error: 'No file provided. Field name must be "file"', status: 400 }),
        { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
      );
    }

    const filename = file.name || 'unnamed_file';
    const mimeType = file.type || 'application/octet-stream';
    const buffer = await file.arrayBuffer();
    const fileSize = buffer.byteLength;

    if (fileSize === 0) {
      return new Response(
        JSON.stringify({ error: 'File is empty', status: 400 }),
        { status: 400, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
      );
    }

    // Cloudflare Pages Functions body limit check (100MB default)
    const MAX_UPLOAD = 100 * 1024 * 1024;
    if (fileSize > MAX_UPLOAD) {
      return new Response(
        JSON.stringify({ error: `File too large. Max ${Math.round(MAX_UPLOAD / 1024 / 1024)}MB allowed`, status: 413 }),
        { status: 413, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
      );
    }

    // Generate unique file ID
    const fileId = generateFileId();
    const ext = filename.includes('.') ? filename.split('.').pop().toLowerCase() : '';

    console.log(`📂 Upload started:
📁 File: ${filename}
📊 Size: ${Math.round(fileSize / 1024 / 1024)}MB
🏷️ MIME: ${mimeType}
🆔 Generated ID: ${fileId}`);

    let metadata;

    // Choose upload strategy based on file size
    if (fileSize <= MAX_SINGLE_SIZE) {
      metadata = await uploadSingleFile(env, buffer, filename, mimeType, fileId);
    } else {
      metadata = await uploadChunkedFile(env, buffer, filename, mimeType, fileId);
    }

    // Build response
    const baseUrl = new URL(request.url).origin;
    const fileUrl = `${baseUrl}/btfstorage/file/${fileId}.${ext}`;
    const downloadUrl = `${fileUrl}?dl=1`;
    const hlsUrl = metadata.chunks ? `${baseUrl}/btfstorage/file/${fileId}.m3u8` : null;

    const response = {
      success: true,
      fileId,
      filename,
      size: fileSize,
      sizeHuman: fileSize >= 1024 * 1024
        ? `${(fileSize / 1024 / 1024).toFixed(2)}MB`
        : `${(fileSize / 1024).toFixed(2)}KB`,
      mimeType,
      uploadMode: metadata.uploadMode,
      chunks: metadata.chunks?.length || 0,
      urls: {
        stream: fileUrl,
        download: downloadUrl,
        ...(hlsUrl ? { hls: hlsUrl } : {})
      },
      uploadedAt: metadata.uploadedAt
    };

    console.log(`🎉 Upload complete: ${fileId}
🔗 Stream URL: ${fileUrl}
📥 Download URL: ${downloadUrl}`);

    return new Response(JSON.stringify(response, null, 2), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('❌ Upload error:', error.message);
    console.error('📍 Stack:', error.stack);

    return new Response(
      JSON.stringify({
        error: error.message,
        status: 500,
        timestamp: new Date().toISOString()
      }),
      { status: 500, headers: { 'Content-Type': 'application/json', ...corsHeaders } }
    );
  }
}
