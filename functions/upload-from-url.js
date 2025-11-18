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
      throw new Error('Missing credentials or KV namespaces');
    }

    // Parse JSON body
    const body = await request.json();
    const fileUrl = body.fileUrl || body.url || body.telegramUrl;
    const customFilename = body.filename || null;

    if (!fileUrl) {
      throw new Error('No URL provided');
    }

    console.log('Fetching URL:', fileUrl);

    // Fetch file
    const fileResponse = await fetch(fileUrl, {
      headers: { 'User-Agent': 'MaryaVault/2.0' }
    });

    if (!fileResponse.ok) {
      throw new Error(`Failed to fetch: ${fileResponse.status}`);
    }

    const arrayBuffer = await fileResponse.arrayBuffer();

    if (arrayBuffer.byteLength === 0) {
      throw new Error('Downloaded file is empty');
    }

    const contentType = fileResponse.headers.get('content-type') || 'application/octet-stream';

    let filename = customFilename;
    if (!filename) {
      const disposition = fileResponse.headers.get('content-disposition');
      if (disposition && disposition.includes('filename=')) {
        const matches = disposition.match(/filename[^;=
]*=((['"]).*?\u0002|[^;
]*)/);
        if (matches && matches[1]) {
          filename = matches[1].replace(/['"]/g, '');
        }
      }

      if (!filename) {
        filename = new URL(fileUrl).pathname.split('/').pop() || `file_${Date.now()}`;
      }
    }

    const MAX_FILE_SIZE = 500 * 1024 * 1024;
    if (arrayBuffer.byteLength > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(arrayBuffer.byteLength / 1024 / 1024)}MB`);
    }

    const file = new File([arrayBuffer], filename, { type: contentType });
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `url${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    const CHUNK_SIZE = 20 * 1024 * 1024;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File requires ${totalChunks} chunks`);
    }

    const chunkPromises = [];
    const uploadStartTime = Date.now();

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const chunkFile = new File([chunk], `${filename}.part${i}`, { type: contentType });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      chunkPromises.push(uploadChunkToKV(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV));
    }

    const chunkResults = await Promise.all(chunkPromises);
    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);

    const masterMetadata = {
      filename,
      size: file.size,
      contentType,
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_chunked_url',
      version: '2.0',
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
      message: 'URL import completed',
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
      },
      timestamp: new Date().toISOString()
    };

    console.log('✅ URL UPLOAD COMPLETED:', fileId);

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });

  } catch (error) {
    console.error('❌ URL UPLOAD ERROR:', error);

    return new Response(JSON.stringify({
      success: false,
      error: {
        message: error.message,
        type: error.name || 'UrlUploadError',
        timestamp: new Date().toISOString()
      }
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }
}

async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

  if (!telegramResponse.ok) {
    throw new Error(`Telegram upload failed (${telegramResponse.status})`);
  }

  const telegramData = await telegramResponse.json();
  if (!telegramData.ok || !telegramData.result?.document?.file_id) {
    throw new Error('Invalid Telegram response');
  }

  const telegramFileId = telegramData.result.document.file_id;
  const telegramMessageId = telegramData.result.message_id;

  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
  const getFileData = await getFileResponse.json();

  if (!getFileData.ok || !getFileData.result?.file_path) {
    throw new Error('Failed to get file path');
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;

  const chunkMetadata = {
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
    version: '2.0'
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

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

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}