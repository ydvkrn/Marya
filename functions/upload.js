// /functions/upload.js
export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== UPLOAD.JS HIT ===');
  console.log('Method:', request.method);
  console.log('URL:', request.url);

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept'
  };

  // Handle OPTIONS
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: corsHeaders
    });
  }

  // Only POST allowed
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

    // Get all 25 KV namespaces
    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey });
      }
    }

    console.log(`Available KV namespaces: ${kvNamespaces.length}`);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing BOT_TOKEN or CHANNEL_ID');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces bound');
    }

    // Parse form data
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log('File received:', file.name, file.size);

    // Validate size (500MB max)
    const MAX_FILE_SIZE = 500 * 1024 * 1024;
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(file.size / 1024 / 1024)}MB (max 500MB)`);
    }

    if (file.size === 0) {
      throw new Error('File is empty');
    }

    // Generate unique file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    console.log('Generated file ID:', fileId);

    // Chunking (20MB per chunk)
    const CHUNK_SIZE = 20 * 1024 * 1024;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File requires ${totalChunks} chunks, but only ${kvNamespaces.length} KV namespaces available`);
    }

    console.log(`Splitting into ${totalChunks} chunks`);

    const uploadStartTime = Date.now();
    const chunkPromises = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      const chunkFile = new File([chunk], `${file.name}.part${i}`, { type: file.type });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      const chunkPromise = uploadChunkToKV(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV);
      chunkPromises.push(chunkPromise);
    }

    const chunkResults = await Promise.all(chunkPromises);
    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);

    console.log(`All ${totalChunks} chunks uploaded in ${uploadDuration}s`);

    // Store master metadata
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension: extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_chunked',
      version: '2.0',
      totalChunks: totalChunks,
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        telegramFileId: result.telegramFileId,
        telegramMessageId: result.telegramMessageId,
        size: result.size,
        chunkKey: result.chunkKey,
        uploadedAt: result.uploadedAt
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    // Generate URLs
    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    const result = {
      success: true,
      message: 'File uploaded successfully',
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType: file.type || 'application/octet-stream',
        extension: extension,
        uploadDuration: `${uploadDuration}s`,
        uploadSpeed: `${(file.size / 1024 / parseFloat(uploadDuration)).toFixed(2)} KB/s`,
        urls: {
          view: customUrl,
          download: downloadUrl,
          stream: streamUrl
        },
        storage: {
          strategy: 'multi_kv_chunked',
          totalChunks: totalChunks,
          kvDistribution: chunkResults.map(r => r.kvNamespace),
          redundancy: 'distributed'
        },
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    console.log('✅ UPLOAD COMPLETED:', fileId);

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'X-File-Id': fileId,
        ...corsHeaders
      }
    });

  } catch (error) {
    console.error('❌ UPLOAD ERROR:', error);

    return new Response(JSON.stringify({
      success: false,
      error: {
        message: error.message,
        type: error.name || 'UploadError',
        timestamp: new Date().toISOString()
      }
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }
}

// Upload chunk helper
async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const uploadStartTime = Date.now();
  console.log(`📤 Uploading chunk ${chunkIndex} (${formatBytes(chunkFile.size)})`);

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
    telegramFileId: telegramFileId,
    telegramMessageId: telegramMessageId,
    directUrl: directUrl,
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

  const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);
  console.log(`✅ Chunk ${chunkIndex} stored (${uploadDuration}s)`);

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
