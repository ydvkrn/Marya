// /functions/upload-from-url.js
export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== UPLOAD-FROM-URL.JS HIT ===');
  console.log('Method:', request.method);

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: { message: 'Method not allowed' }
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }

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
      throw new Error('Missing BOT_TOKEN, CHANNEL_ID or KV namespaces');
    }

    // Parse body
    const body = await request.json();
    const fileUrl = body.fileUrl || body.url || body.telegramUrl || body.file_url;
    const customFilename = body.filename || body.name || null;

    if (!fileUrl || typeof fileUrl !== 'string') {
      throw new Error('Valid fileUrl is required');
    }

    console.log('Fetching file from:', fileUrl);

    // Fetch file with better headers
    const fileResponse = await fetch(fileUrl, {
      headers: {
        'User-Agent': 'MaryaVault/2.0 (+https://github.com/ydvkrn/Marya)'
      },
      redirect: 'follow'
    });

    if (!fileResponse.ok) {
      throw new Error(`Failed to download: ${fileResponse.status} ${fileResponse.statusText}`);
    }

    const arrayBuffer = await fileResponse.arrayBuffer();
    if (arrayBuffer.byteLength === 0) {
      throw new Error('Downloaded file is empty');
    }

    const contentType = fileResponse.headers.get('content-type') || 'application/octet-stream';

    // FIXED: Safe & correct filename extraction
    let filename = customFilename;

    if (!filename) {
      const disposition = fileResponse.headers.get('content-disposition');
      if (disposition) {
        const match = disposition.match(/filename[*]?=(?:UTF-8'')?["']?([^"';]+)["']?/i);
        if (match?.[1]) {
          filename = decodeURIComponent(match[1].trim());
        }
      }

      // Final fallback: from URL
      if (!filename) {
        const urlPath = new URL(fileUrl).pathname;
        const decodedPath = decodeURIComponent(urlPath);
        filename = decodedPath.split('/').pop() || `file_${Date.now()}`;
        // Clean weird characters
        filename = filename.split('?')[0].split('#')[0];
      }
    }

    // Ensure filename has no invalid chars for Telegram
    filename = filename.replace(/[\x00-\x1F\x7F<>:"/\\|?*-\u001F]/g, '_');

    const MAX_FILE_SIZE = 500 * 1024 * 1024; // 500 MB
    if (arrayBuffer.byteLength > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${formatBytes(arrayBuffer.byteLength)} (max 500MB)`);
    }

    const file = new File([arrayBuffer], filename, { type: contentType });
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const fileId = `url${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    const CHUNK_SIZE = 20 * 1024 * 1024; // 20 MB chunks
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File too big: needs ${totalChunks} chunks but only ${kvNamespaces.length} KV namespaces available`);
    }

    const chunkPromises = [];
    const uploadStartTime = Date.now();

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const chunkFile = new File([chunk], `${filename}.part${i}`, { type: contentType });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      chunkPromises.push(
        uploadChunkToKV(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV)
      );
    }

    const chunkResults = await Promise.all(chunkPromises);
    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);

    // Save master metadata in first KV
    const masterMetadata = {
      filename,
      size: file.size,
      contentType,
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_chunked_url',
      version: '2.1',
      sourceUrl: fileUrl,
      totalChunks,
      chunks: chunkResults.map((r, i) => ({
        index: i,
        kvNamespace: r.kvNamespace,
        telegramFileId: r.telegramFileId,
        telegramMessageId: r.telegramMessageId,
        size: r.size,
        chunkKey: r.chunkKey,
        uploadedAt: r.uploadedAt
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;

    const result = {
      success: true,
      message: 'File successfully imported from URL',
      data: {
        id: fileId,
        filename,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType,
        extension,
        uploadDuration: `${uploadDuration}s`,
        urls: {
          view: customUrl,
          download: downloadUrl
        },
        storage: {
          strategy: 'multi_kv_chunked_url',
          totalChunks,
          kvDistribution: chunkResults.map(r => r.kvNamespace)
        },
        uploadedAt: new Date().toISOString()
      }
    };

    console.log('URL UPLOAD SUCCESS:', fileId, filename, formatBytes(file.size));

    return new Response(JSON.stringify(result, null, 2), {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });

  } catch (error) {
    console.error('URL UPLOAD FAILED:', error.message);

    return new Response(JSON.stringify({
      success: false,
      error: {
        message: error.message || 'Unknown error occurred',
        type: error.name || 'UploadError',
        timestamp: new Date().toISOString()
      }
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }
}

// Upload single chunk to Telegram + save metadata in KV
async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const form = new FormData();
  form.append('chat_id', channelId);
  form.append('document', chunkFile);
  form.append('caption', `MaryaVault Chunk | ${fileId} | Part ${chunkIndex}`);

  const res = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: form
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Telegram upload failed (${res.status}): ${text}`);
  }

  const data = await res.json();
  if (!data.ok || !data.result?.document?.file_id) {
    throw new Error('Telegram failed to return file_id');
  }

  const telegramFileId = data.result.document.file_id;
  const telegramMessageId = data.result.message_id;

  // Get direct link
  const fileInfo = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`);
  const fileData = await fileInfo.json();

  if (!fileData.ok || !fileData.result?.file_path) {
    throw new Error('Failed to get Telegram file path');
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;

  const metadata = {
    telegramFileId,
    telegramMessageId,
    directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    uploadedAt: Date.now(),
    lastRefreshed: Date.now(),
    refreshCount: 0,
    version: '2.1'
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(metadata));

  return {
    telegramFileId,
    telegramMessageId,
    size: chunkFile.size,
    directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey,
    uploadedAt: Date.now()
  };
}

// Helper function
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}