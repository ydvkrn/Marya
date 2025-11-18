// /functions/upload.js - ASYNC UPLOAD WITH PROGRESS
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  const url = new URL(request.url);

  // GET request - Check upload status
  if (request.method === 'GET' && url.pathname.includes('/upload/status/')) {
    const uploadId = url.pathname.split('/').pop();
    
    try {
      const status = await env.FILES_KV.get(`upload_status_${uploadId}`);
      
      if (!status) {
        return new Response(JSON.stringify({
          success: false,
          error: 'Upload not found'
        }), {
          status: 404,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      return new Response(status, {
        status: 200,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        success: false,
        error: error.message
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }
  }

  // POST request - Start upload
  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed'
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey, index: i });
      }
    }

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing BOT_TOKEN, CHANNEL_ID, or KV');
    }

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || !(file instanceof File)) {
      throw new Error('No valid file provided');
    }

    if (file.size === 0) {
      throw new Error('File is empty');
    }

    const CHUNK_SIZE = 15 * 1024 * 1024;
    const MAX_FILE_SIZE = kvNamespaces.length * CHUNK_SIZE;

    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${formatBytes(file.size)}. Max: ${formatBytes(MAX_FILE_SIZE)}`);
    }

    const timestamp = Date.now().toString(36);
    const random = crypto.randomUUID().split('-')[0];
    const uploadId = `${timestamp}${random}`;
    const fileId = `id${uploadId}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`Need ${totalChunks} KV, have ${kvNamespaces.length}`);
    }

    // Initialize upload status
    const initialStatus = {
      uploadId,
      fileId,
      filename: file.name,
      size: file.size,
      totalChunks,
      uploadedChunks: 0,
      status: 'processing',
      progress: 0,
      startedAt: Date.now(),
      error: null
    };

    await kvNamespaces[0].kv.put(
      `upload_status_${uploadId}`,
      JSON.stringify(initialStatus),
      { expirationTtl: 3600 } // 1 hour
    );

    // Start async upload (background processing)
    context.waitUntil(
      processUpload(
        file,
        fileId,
        uploadId,
        extension,
        totalChunks,
        CHUNK_SIZE,
        BOT_TOKEN,
        CHANNEL_ID,
        kvNamespaces,
        request.url
      )
    );

    // Return immediately with upload ID
    return new Response(JSON.stringify({
      success: true,
      message: 'Upload started',
      data: {
        uploadId,
        fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        totalChunks,
        statusUrl: `${new URL(request.url).origin}/upload/status/${uploadId}`,
        estimatedTime: `${Math.ceil(file.size / (1024 * 1024))} seconds`
      },
      timestamp: new Date().toISOString()
    }), {
      status: 202, // Accepted
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Upload init error:', error.message);

    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Background upload processing
async function processUpload(
  file,
  fileId,
  uploadId,
  extension,
  totalChunks,
  CHUNK_SIZE,
  botToken,
  channelId,
  kvNamespaces,
  baseUrl
) {
  const uploadedChunks = [];
  const uploadStartTime = Date.now();

  try {
    console.log(`[${uploadId}] Start processing: ${file.name}`);

    const reader = file.stream().getReader();
    let chunkIndex = 0;
    let buffer = new Uint8Array(0);
    let eof = false;

    while (!eof) {
      // Read chunk
      while (buffer.length < CHUNK_SIZE && !eof) {
        const { done, value } = await reader.read();
        
        if (done) {
          eof = true;
          break;
        }

        if (value && value.length > 0) {
          const newBuffer = new Uint8Array(buffer.length + value.length);
          newBuffer.set(buffer, 0);
          newBuffer.set(value, buffer.length);
          buffer = newBuffer;
        }
      }

      if (buffer.length > 0) {
        const chunkSize = Math.min(CHUNK_SIZE, buffer.length);
        const chunkData = buffer.slice(0, chunkSize);
        buffer = buffer.slice(chunkSize);

        const targetKV = kvNamespaces[chunkIndex % kvNamespaces.length];

        console.log(`[${uploadId}] Uploading chunk ${chunkIndex + 1}/${totalChunks}`);

        try {
          const result = await uploadChunkWithRetry(
            chunkData,
            file.name,
            file.type,
            fileId,
            chunkIndex,
            botToken,
            channelId,
            targetKV,
            uploadId,
            5
          );

          uploadedChunks.push(result);

          // Update progress
          const progress = Math.round(((chunkIndex + 1) / totalChunks) * 100);
          await updateUploadStatus(kvNamespaces[0].kv, uploadId, {
            uploadedChunks: chunkIndex + 1,
            status: 'uploading',
            progress
          });

          // Adaptive delay
          await sleep(200);

        } catch (chunkError) {
          console.error(`[${uploadId}] Chunk ${chunkIndex} failed:`, chunkError.message);
          
          await updateUploadStatus(kvNamespaces[0].kv, uploadId, {
            status: 'failed',
            error: `Chunk ${chunkIndex + 1} failed: ${chunkError.message}`
          });

          // Cleanup
          await cleanupChunks(uploadedChunks, botToken, channelId);
          return;
        }

        chunkIndex++;
      }
    }

    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);
    const uploadSpeed = (file.size / 1024 / 1024) / parseFloat(uploadDuration);

    console.log(`[${uploadId}] Completed in ${uploadDuration}s`);

    // Save metadata
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      uploadSpeed: parseFloat(uploadSpeed.toFixed(2)),
      uploadId,
      type: 'streaming_multi_kv',
      version: '5.1',
      totalChunks: uploadedChunks.length,
      chunks: uploadedChunks.map((r, i) => ({
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

    const origin = new URL(baseUrl).origin;

    // Update final status
    await updateUploadStatus(kvNamespaces[0].kv, uploadId, {
      status: 'completed',
      progress: 100,
      uploadDuration: `${uploadDuration}s`,
      uploadSpeed: `${uploadSpeed.toFixed(2)} MB/s`,
      urls: {
        view: `${origin}/btfstorage/file/${fileId}${extension}`,
        download: `${origin}/btfstorage/file/${fileId}${extension}?dl=1`,
        stream: `${origin}/btfstorage/file/${fileId}${extension}?stream=1`
      }
    });

  } catch (error) {
    console.error(`[${uploadId}] Process error:`, error.message);

    await updateUploadStatus(kvNamespaces[0].kv, uploadId, {
      status: 'failed',
      error: error.message
    });

    if (uploadedChunks.length > 0) {
      await cleanupChunks(uploadedChunks, botToken, channelId);
    }
  }
}

async function updateUploadStatus(kv, uploadId, updates) {
  try {
    const statusKey = `upload_status_${uploadId}`;
    const currentStatus = await kv.get(statusKey);
    
    if (currentStatus) {
      const status = JSON.parse(currentStatus);
      const updatedStatus = { ...status, ...updates, updatedAt: Date.now() };
      await kv.put(statusKey, JSON.stringify(updatedStatus), { expirationTtl: 3600 });
    }
  } catch (error) {
    console.error('Failed to update status:', error.message);
  }
}

async function uploadChunkWithRetry(
  chunkData,
  filename,
  mimetype,
  fileId,
  chunkIndex,
  botToken,
  channelId,
  kvNamespace,
  uploadId,
  maxRetries = 5
) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await uploadChunkToTelegram(
        chunkData,
        filename,
        mimetype,
        fileId,
        chunkIndex,
        botToken,
        channelId,
        kvNamespace,
        uploadId
      );
    } catch (error) {
      lastError = error;
      
      if (attempt < maxRetries) {
        const baseDelay = Math.min(1000 * Math.pow(2, attempt - 1), 8000);
        const jitter = Math.random() * 1000;
        await sleep(baseDelay + jitter);
      }
    }
  }

  throw lastError;
}

async function uploadChunkToTelegram(
  chunkData,
  filename,
  mimetype,
  fileId,
  chunkIndex,
  botToken,
  channelId,
  kvNamespace,
  uploadId
) {
  const chunkBlob = new Blob([chunkData], { type: mimetype || 'application/octet-stream' });
  const chunkFile = new File([chunkBlob], `${fileId}_${chunkIndex}.part`, { type: mimetype });

  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `📦 ${chunkIndex} • ${fileId}`);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 180000); // 3 min timeout

  try {
    const telegramResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/sendDocument`,
      {
        method: 'POST',
        body: telegramForm,
        signal: controller.signal
      }
    );

    clearTimeout(timeout);

    if (!telegramResponse.ok) {
      const errorText = await telegramResponse.text();
      throw new Error(`Telegram error ${telegramResponse.status}: ${errorText.slice(0, 200)}`);
    }

    const telegramData = await telegramResponse.json();
    
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    const telegramFileId = telegramData.result.document.file_id;
    const telegramMessageId = telegramData.result.message_id;

    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`
    );

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
      size: chunkData.length,
      index: chunkIndex,
      parentFileId: fileId,
      kvNamespace: kvNamespace.name,
      uploadedAt: Date.now(),
      uploadId,
      version: '5.1'
    };

    await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

    return {
      telegramFileId,
      telegramMessageId,
      size: chunkData.length,
      kvNamespace: kvNamespace.name,
      chunkKey,
      uploadedAt: Date.now()
    };

  } catch (error) {
    clearTimeout(timeout);
    
    if (error.name === 'AbortError') {
      throw new Error('Upload timeout (3 min)');
    }
    
    throw error;
  }
}

async function cleanupChunks(chunks, botToken, channelId) {
  if (!chunks || chunks.length === 0) return;

  const deletePromises = chunks.map(async (chunk) => {
    try {
      await fetch(
        `https://api.telegram.org/bot${botToken}/deleteMessage`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: channelId,
            message_id: chunk.telegramMessageId
          })
        }
      );
    } catch (err) {
      console.warn(`Delete failed: ${chunk.telegramMessageId}`);
    }
  });

  await Promise.allSettled(deletePromises);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}