// /functions/upload.js - DIRECT SYNCHRONOUS UPLOAD
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  const uploadId = crypto.randomUUID().split('-')[0];

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey });
      }
    }

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing config');
    }

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || !(file instanceof File)) {
      throw new Error('No file');
    }

    if (file.size === 0) throw new Error('Empty file');

    const CHUNK_SIZE = 15 * 1024 * 1024;
    const MAX_FILE_SIZE = kvNamespaces.length * CHUNK_SIZE;

    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`Max ${formatBytes(MAX_FILE_SIZE)}`);
    }

    const timestamp = Date.now().toString(36);
    const random = crypto.randomUUID().split('-')[0];
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`Need ${totalChunks} KV, have ${kvNamespaces.length}`);
    }

    console.log(`[${uploadId}] Start: ${file.name} (${formatBytes(file.size)})`);

    const uploadStartTime = Date.now();
    const chunkResults = [];

    // Stream processing
    const reader = file.stream().getReader();
    let chunkIndex = 0;
    let buffer = new Uint8Array(0);
    let eof = false;

    while (!eof) {
      while (buffer.length < CHUNK_SIZE && !eof) {
        const { done, value } = await reader.read();
        if (done) {
          eof = true;
          break;
        }
        if (value) {
          const newBuffer = new Uint8Array(buffer.length + value.length);
          newBuffer.set(buffer);
          newBuffer.set(value, buffer.length);
          buffer = newBuffer;
        }
      }

      if (buffer.length > 0) {
        const chunkSize = Math.min(CHUNK_SIZE, buffer.length);
        const chunkData = buffer.slice(0, chunkSize);
        buffer = buffer.slice(chunkSize);

        const targetKV = kvNamespaces[chunkIndex % kvNamespaces.length];

        console.log(`[${uploadId}] Chunk ${chunkIndex + 1}/${totalChunks}`);

        const result = await uploadChunk(
          chunkData,
          file.name,
          file.type,
          fileId,
          chunkIndex,
          BOT_TOKEN,
          CHANNEL_ID,
          targetKV
        );

        chunkResults.push(result);
        chunkIndex++;

        await sleep(150);
      }
    }

    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);

    // Save metadata
    const metadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      totalChunks: chunkResults.length,
      chunks: chunkResults.map((r, i) => ({
        index: i,
        kvNamespace: r.kvNamespace,
        telegramFileId: r.telegramFileId,
        telegramMessageId: r.telegramMessageId,
        size: r.size,
        chunkKey: r.chunkKey
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(metadata));

    const baseUrl = new URL(request.url).origin;

    // DIRECT RESPONSE WITH URLS
    const result = {
      success: true,
      data: {
        id: fileId,
        filename: file.name,
        size: formatBytes(file.size),
        duration: `${uploadDuration}s`,
        urls: {
          view: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
          download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
          stream: `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`
        }
      }
    };

    console.log(`[${uploadId}] ✓ Done`);

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error(`[${uploadId}] Error:`, error.message);

    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

async function uploadChunk(
  chunkData,
  filename,
  mimetype,
  fileId,
  chunkIndex,
  botToken,
  channelId,
  kvNamespace
) {
  let lastError;

  // 3 retries
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const chunkBlob = new Blob([chunkData], { type: mimetype || 'application/octet-stream' });
      const chunkFile = new File([chunkBlob], `${fileId}_${chunkIndex}.part`, { type: mimetype });

      const form = new FormData();
      form.append('chat_id', channelId);
      form.append('document', chunkFile);
      form.append('caption', `${chunkIndex}`);

      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 180000); // 3 min

      const res = await fetch(
        `https://api.telegram.org/bot${botToken}/sendDocument`,
        { method: 'POST', body: form, signal: controller.signal }
      );

      clearTimeout(timeout);

      if (!res.ok) {
        throw new Error(`Telegram ${res.status}`);
      }

      const data = await res.json();

      if (!data.ok || !data.result?.document?.file_id) {
        throw new Error('Invalid response');
      }

      const fileId = data.result.document.file_id;
      const messageId = data.result.message_id;

      // Get file path
      const pathRes = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`
      );

      const pathData = await pathRes.json();

      if (!pathData.ok || !pathData.result?.file_path) {
        throw new Error('No file path');
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${pathData.result.file_path}`;

      // Save chunk metadata
      const chunkKey = `${fileId}_chunk_${chunkIndex}`;
      const chunkMeta = {
        telegramFileId: fileId,
        telegramMessageId: messageId,
        directUrl,
        size: chunkData.length,
        index: chunkIndex,
        kvNamespace: kvNamespace.name
      };

      await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMeta));

      return {
        telegramFileId: fileId,
        telegramMessageId: messageId,
        size: chunkData.length,
        kvNamespace: kvNamespace.name,
        chunkKey
      };

    } catch (err) {
      lastError = err;
      
      if (attempt < 3) {
        await sleep(1000 * attempt);
      }
    }
  }

  throw lastError;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}