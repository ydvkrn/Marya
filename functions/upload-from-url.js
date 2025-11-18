// /functions/upload-from-url.js - Memory Optimized Sequential URL Upload
export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MEMORY-OPTIMIZED URL UPLOAD START ===');

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

    // Get all available KV namespaces (up to 25)
    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey });
      }
    }

    console.log(`Available KV: ${kvNamespaces.length}`);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing BOT_TOKEN or CHANNEL_ID');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces bound');
    }

    // Parse JSON body
    let body;
    try {
      body = await request.json();
    } catch (parseError) {
      throw new Error('Invalid JSON body');
    }

    const fileUrl = body.fileUrl || body.url || body.telegramUrl;
    const customFilename = body.filename || null;

    if (!fileUrl) {
      throw new Error('No URL provided (use fileUrl, url, or telegramUrl)');
    }

    console.log(`Fetching: ${fileUrl}`);

    // Fetch file with timeout
    const fetchController = new AbortController();
    const fetchTimeout = setTimeout(() => fetchController.abort(), 120000); // 2 min timeout

    let fileResponse;
    try {
      fileResponse = await fetch(fileUrl, {
        method: 'GET',
        headers: {
          'User-Agent': 'MaryaVault/3.0',
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
      throw new Error(`Fetch error: ${fileResponse.status} ${fileResponse.statusText}`);
    }

    // Get file info
    const contentType = fileResponse.headers.get('content-type') || 'application/octet-stream';
    const contentLength = parseInt(fileResponse.headers.get('content-length') || '0');

    console.log(`Content-Type: ${contentType}, Size: ${contentLength}`);

    // Convert to ArrayBuffer (memory efficient)
    console.log('Reading response body...');
    const arrayBuffer = await fileResponse.arrayBuffer();
    
    if (arrayBuffer.byteLength === 0) {
      throw new Error('Downloaded file is empty (0 bytes)');
    }

    console.log(`File loaded: ${arrayBuffer.byteLength} bytes`);

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

        // Add extension based on content type if missing
        if (!filename.includes('.')) {
          const extMap = {
            'image/jpeg': '.jpg',
            'image/png': '.png',
            'image/gif': '.gif',
            'video/mp4': '.mp4',
            'audio/mpeg': '.mp3',
            'application/pdf': '.pdf'
          };
          filename += extMap[contentType] || '.bin';
        }
      }
    }

    console.log(`Filename: ${filename}`);

    // Size validation
    const CHUNK_SIZE = 18 * 1024 * 1024; // 18MB chunks
    const MAX_FILE_SIZE = kvNamespaces.length * CHUNK_SIZE;

    if (arrayBuffer.byteLength > MAX_FILE_SIZE) {
      throw new Error(
        `File too large: ${formatBytes(arrayBuffer.byteLength)}. Max: ${formatBytes(MAX_FILE_SIZE)} ` +
        `(${kvNamespaces.length} KV × 18MB)`
      );
    }

    // Generate file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `url${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    console.log(`File ID: ${fileId}`);

    // Calculate chunks
    const totalChunks = Math.ceil(arrayBuffer.byteLength / CHUNK_SIZE);
    console.log(`Total chunks: ${totalChunks}`);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`Need ${totalChunks} KV, only ${kvNamespaces.length} available`);
    }

    const uploadStartTime = Date.now();
    const chunkResults = [];

    // Sequential upload (one by one)
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, arrayBuffer.byteLength);

      // Extract chunk from buffer
      const chunkBuffer = arrayBuffer.slice(start, end);
      const chunkBlob = new Blob([chunkBuffer], { type: contentType });
      const chunkFile = new File([chunkBlob], `${filename}.part${i}`, { type: contentType });

      const targetKV = kvNamespaces[i % kvNamespaces.length];

      console.log(`Uploading chunk ${i + 1}/${totalChunks} to ${targetKV.name}`);

      // Upload with retry
      const result = await uploadChunkWithRetry(
        chunkFile,
        fileId,
        i,
        BOT_TOKEN,
        CHANNEL_ID,
        targetKV,
        3 // max retries
      );

      chunkResults.push(result);

      // Small delay to avoid rate limits
      if (i < totalChunks - 1) {
        await sleep(200);
      }
    }

    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);
    console.log(`Upload completed in ${uploadDuration}s`);

    // Store master metadata
    const masterMetadata = {
      filename,
      size: arrayBuffer.byteLength,
      contentType,
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_url_sequential',
      version: '3.0',
      sourceUrl: fileUrl,
      totalChunks: totalChunks,
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

    // Generate URLs
    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    const result = {
      success: true,
      message: 'URL import completed successfully',
      data: {
        id: fileId,
        filename,
        size: arrayBuffer.byteLength,
        sizeFormatted: formatBytes(arrayBuffer.byteLength),
        contentType,
        extension,
        uploadDuration: `${uploadDuration}s`,
        uploadSpeed: `${(arrayBuffer.byteLength / 1024 / parseFloat(uploadDuration)).toFixed(2)} KB/s`,
        urls: {
          view: viewUrl,
          download: downloadUrl,
          stream: streamUrl
        },
        storage: {
          strategy: 'multi_kv_url_sequential',
          totalChunks: totalChunks,
          kvDistribution: chunkResults.map(r => r.kvNamespace),
          redundancy: 'distributed'
        },
        source: {
          url: fileUrl,
          type: fileUrl.includes('api.telegram.org') ? 'telegram' : 'external'
        },
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    console.log('✅ URL UPLOAD SUCCESS');

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'X-File-Id': fileId,
        ...corsHeaders
      }
    });

  } catch (error) {
    console.error('❌ URL UPLOAD ERROR:', error.message);

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

// Upload chunk with retry logic
async function uploadChunkWithRetry(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, maxRetries = 3) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Chunk ${chunkIndex}: Attempt ${attempt}/${maxRetries}`);
      return await uploadChunkToTelegram(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace);
    } catch (error) {
      console.error(`Chunk ${chunkIndex} attempt ${attempt} failed:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        console.log(`Retrying after ${delay}ms...`);
        await sleep(delay);
      }
    }
  }

  throw new Error(`Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${lastError.message}`);
}

// Upload chunk to Telegram
async function uploadChunkToTelegram(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const uploadStart = Date.now();

  // Create form data for Telegram
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

  // Upload to Telegram with timeout
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 60000); // 60s timeout

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
      throw new Error(`Telegram error ${telegramResponse.status}: ${errorText.slice(0, 100)}`);
    }

    const telegramData = await telegramResponse.json();

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    const telegramFileId = telegramData.result.document.file_id;
    const telegramMessageId = telegramData.result.message_id;

    console.log(`Chunk ${chunkIndex} uploaded: ${telegramFileId}`);

    // Get file path
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`
    );

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Failed to get file path');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store in KV
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
      version: '3.0'
    };

    await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

    const duration = ((Date.now() - uploadStart) / 1000).toFixed(2);
    console.log(`✅ Chunk ${chunkIndex} stored in ${kvNamespace.name} (${duration}s)`);

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

// Helper: Sleep
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper: Format bytes
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
