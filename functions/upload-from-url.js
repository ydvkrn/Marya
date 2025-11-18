// /functions/upload-from-url.js - STREAMING URL UPLOAD (No Memory Limit)
export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== STREAMING URL UPLOAD START ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
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

    // Get all KV namespaces
    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey });
      }
    }

    console.log(`Available KV: ${kvNamespaces.length}`);

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing credentials or KV');
    }

    // Parse JSON body
    let body;
    try {
      body = await request.json();
    } catch {
      throw new Error('Invalid JSON body');
    }

    const fileUrl = body.fileUrl || body.url || body.telegramUrl;
    const customFilename = body.filename || null;

    if (!fileUrl) {
      throw new Error('No URL provided');
    }

    console.log(`Fetching: ${fileUrl}`);

    // Fetch URL with timeout
    const fetchController = new AbortController();
    const fetchTimeout = setTimeout(() => fetchController.abort(), 180000); // 3 min

    let fileResponse;
    try {
      fileResponse = await fetch(fileUrl, {
        method: 'GET',
        headers: {
          'User-Agent': 'MaryaVault/4.0',
          'Accept': '*/*'
        },
        signal: fetchController.signal
      });
      clearTimeout(fetchTimeout);
    } catch (fetchError) {
      clearTimeout(fetchTimeout);
      throw new Error(`Fetch failed: ${fetchError.message}`);
    }

    if (!fileResponse.ok) {
      throw new Error(`HTTP ${fileResponse.status}: ${fileResponse.statusText}`);
    }

    // Get file info
    const contentType = fileResponse.headers.get('content-type') || 'application/octet-stream';
    const contentLength = parseInt(fileResponse.headers.get('content-length') || '0');

    console.log(`Type: ${contentType}, Size: ${contentLength || 'unknown'}`);

    // Determine filename
    let filename = customFilename;
    if (!filename) {
      const disposition = fileResponse.headers.get('content-disposition');
      if (disposition && disposition.includes('filename=')) {
        const matches = disposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
        if (matches && matches[1]) {
          filename = matches[1].replace(/['"]/g, '');
        }
      }

      if (!filename) {
        try {
          const urlPath = new URL(fileUrl).pathname;
          filename = urlPath.split('/').pop() || `file_${Date.now()}`;
        } catch {
          filename = `file_${Date.now()}`;
        }

        if (!filename.includes('.')) {
          const extMap = {
            'image/jpeg': '.jpg',
            'image/png': '.png',
            'video/mp4': '.mp4',
            'audio/mpeg': '.mp3',
            'application/pdf': '.pdf'
          };
          filename += extMap[contentType] || '.bin';
        }
      }
    }

    console.log(`Filename: ${filename}`);

    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `url${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    const CHUNK_SIZE = 18 * 1024 * 1024; // 18MB
    const MAX_FILE_SIZE = kvNamespaces.length * CHUNK_SIZE;

    // Check size if known
    if (contentLength > 0 && contentLength > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${formatBytes(contentLength)}. Max: ${formatBytes(MAX_FILE_SIZE)}`);
    }

    const uploadStartTime = Date.now();
    const chunkResults = [];

    // Get response stream (NOT loading full file!)
    const responseStream = fileResponse.body;
    const reader = responseStream.getReader();

    let chunkIndex = 0;
    let buffer = new Uint8Array(0);
    let totalBytesRead = 0;

    console.log('Starting streaming upload...');

    // Stream and upload chunks
    while (true) {
      const { done, value } = await reader.read();

      if (value) {
        totalBytesRead += value.length;
        console.log(`Read ${formatBytes(totalBytesRead)} so far...`);

        // Append to buffer
        const newBuffer = new Uint8Array(buffer.length + value.length);
        newBuffer.set(buffer);
        newBuffer.set(value, buffer.length);
        buffer = newBuffer;
      }

      // Upload chunk when buffer >= CHUNK_SIZE or done
      while (buffer.length >= CHUNK_SIZE || (done && buffer.length > 0)) {
        const chunkSize = Math.min(CHUNK_SIZE, buffer.length);
        const chunkData = buffer.slice(0, chunkSize);
        buffer = buffer.slice(chunkSize);

        // Check KV limit
        if (chunkIndex >= kvNamespaces.length) {
          throw new Error(`File too large: exceeded ${kvNamespaces.length} KV limit`);
        }

        const chunkBlob = new Blob([chunkData], { type: contentType });
        const chunkFile = new File([chunkBlob], `${filename}.part${chunkIndex}`, { type: contentType });

        const targetKV = kvNamespaces[chunkIndex % kvNamespaces.length];

        console.log(`Uploading chunk ${chunkIndex + 1} (${formatBytes(chunkFile.size)}) to ${targetKV.name}`);

        const result = await uploadChunkWithRetry(
          chunkFile,
          fileId,
          chunkIndex,
          BOT_TOKEN,
          CHANNEL_ID,
          targetKV,
          3
        );

        chunkResults.push(result);
        chunkIndex++;

        // Small delay
        await sleep(200);

        // Break if no more data
        if (done && buffer.length === 0) break;
      }

      if (done && buffer.length === 0) break;
    }

    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);
    console.log(`Completed in ${uploadDuration}s, Total size: ${formatBytes(totalBytesRead)}`);

    // Store metadata
    const masterMetadata = {
      filename,
      size: totalBytesRead,
      contentType,
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_url_streaming',
      version: '4.0',
      sourceUrl: fileUrl,
      totalChunks: chunkResults.length,
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

    const result = {
      success: true,
      message: 'URL import completed successfully',
      data: {
        id: fileId,
        filename,
        size: totalBytesRead,
        sizeFormatted: formatBytes(totalBytesRead),
        contentType,
        extension,
        uploadDuration: `${uploadDuration}s`,
        urls: {
          view: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
          download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
          stream: `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`
        },
        storage: {
          strategy: 'multi_kv_url_streaming',
          totalChunks: chunkResults.length,
          kvDistribution: chunkResults.map(r => r.kvNamespace)
        },
        source: {
          url: fileUrl,
          type: fileUrl.includes('api.telegram.org') ? 'telegram' : 'external'
        },
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });

  } catch (error) {
    console.error('❌ URL UPLOAD ERROR:', error.message);

    return new Response(JSON.stringify({
      success: false,
      error: { message: error.message, timestamp: new Date().toISOString() }
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }
}

async function uploadChunkWithRetry(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await uploadChunkToTelegram(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace);
    } catch (error) {
      console.error(`Chunk ${chunkIndex} attempt ${attempt} failed: ${error.message}`);
      if (attempt < maxRetries) {
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        console.log(`Retrying after ${delay}ms...`);
        await sleep(delay);
      } else {
        throw error;
      }
    }
  }
}

async function uploadChunkToTelegram(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 90000); // 90s

  try {
    const telegramResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/sendDocument`,
      { method: 'POST', body: telegramForm, signal: controller.signal }
    );

    clearTimeout(timeout);

    if (!telegramResponse.ok) {
      throw new Error(`Telegram error ${telegramResponse.status}`);
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
      size: chunkFile.size,
      index: chunkIndex,
      parentFileId: fileId,
      kvNamespace: kvNamespace.name,
      uploadedAt: Date.now(),
      lastRefreshed: Date.now(),
      refreshCount: 0,
      version: '4.0'
    };

    await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

    console.log(`✅ Chunk ${chunkIndex} stored in ${kvNamespace.name}`);

    return {
      telegramFileId,
      telegramMessageId,
      size: chunkFile.size,
      directUrl,
      kvNamespace: kvNamespace.name,
      chunkKey,
      uploadedAt: Date.now()
    };

  } catch (error) {
    clearTimeout(timeout);
    throw error;
  }
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
