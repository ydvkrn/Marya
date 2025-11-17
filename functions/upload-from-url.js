export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT URL UPLOAD START ===');
  console.log('Request method:', request.method);
  console.log('Request URL:', request.url);

  // ✅ Enhanced CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
    'Access-Control-Max-Age': '86400'
  };

  // ✅ Handle preflight OPTIONS
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: corsHeaders
    });
  }

  // ✅ Only allow POST
  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: { message: 'Method not allowed. Use POST method.' }
    }), {
      status: 405,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        ...corsHeaders
      }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    console.log('Environment check:', {
      hasBotToken: !!BOT_TOKEN,
      hasChannelId: !!CHANNEL_ID
    });

    // ✅ All KV namespaces
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    console.log(`Available KV namespaces: ${kvNamespaces.length}`);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing bot credentials. Configure BOT_TOKEN and CHANNEL_ID.');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces available. Bind at least FILES_KV.');
    }

    // ✅ Parse JSON body with multiple field support
    let body;
    try {
      const rawBody = await request.text();
      console.log('Raw body length:', rawBody.length);
      body = JSON.parse(rawBody);
      console.log('Parsed body keys:', Object.keys(body));
    } catch (parseError) {
      console.error('JSON parse error:', parseError.message);
      throw new Error('Invalid JSON body. Please send valid JSON.');
    }

    // ✅ Support multiple URL field names
    const fileUrl = body.telegramUrl || body.url || body.fileUrl;
    const customFilename = body.filename || null;

    if (!fileUrl) {
      console.error('No URL in body:', body);
      throw new Error('No URL provided. Include "url", "telegramUrl", or "fileUrl" in request body.');
    }

    console.log('Processing URL:', fileUrl);
    console.log('Custom filename:', customFilename);

    // ✅ Validate URL format
    let validUrl;
    try {
      validUrl = new URL(fileUrl);
      console.log('URL validated:', {
        protocol: validUrl.protocol,
        hostname: validUrl.hostname,
        pathname: validUrl.pathname
      });
    } catch (urlError) {
      throw new Error('Invalid URL format. Provide a valid HTTP/HTTPS URL.');
    }

    // ✅ Special handling for Telegram URLs
    let actualFileUrl = fileUrl;
    let isTelegramUrl = fileUrl.includes('api.telegram.org');

    if (isTelegramUrl) {
      console.log('Telegram URL detected');
      
      // Check if it's a file_id instead of full URL
      if (!fileUrl.includes('http')) {
        // It's a file_id, need to get URL first
        console.log('Converting file_id to URL...');
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(fileUrl)}`
        );
        
        if (!getFileResponse.ok) {
          throw new Error('Failed to get Telegram file info');
        }
        
        const getFileData = await getFileResponse.json();
        if (!getFileData.ok || !getFileData.result?.file_path) {
          throw new Error('Invalid Telegram file_id');
        }
        
        actualFileUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
        console.log('Converted to URL:', actualFileUrl);
      }
    }

    // ✅ Fetch file from URL with proper headers and timeout
    console.log('Fetching file from URL...');
    
    const fetchController = new AbortController();
    const fetchTimeout = setTimeout(() => fetchController.abort(), 60000); // 60s timeout

    let fileResponse;
    try {
      fileResponse = await fetch(actualFileUrl, {
        method: 'GET',
        headers: {
          'User-Agent': 'MaryaVault/2.0 (Cloudflare Worker)',
          'Accept': '*/*'
        },
        signal: fetchController.signal
      });
      clearTimeout(fetchTimeout);
    } catch (fetchError) {
      clearTimeout(fetchTimeout);
      console.error('Fetch error:', fetchError.message);
      throw new Error(`Failed to fetch file: ${fetchError.message}`);
    }

    if (!fileResponse.ok) {
      console.error('Fetch failed:', {
        status: fileResponse.status,
        statusText: fileResponse.statusText
      });
      throw new Error(`Failed to fetch file: ${fileResponse.status} ${fileResponse.statusText}`);
    }

    // ✅ Get content info
    const contentLength = parseInt(fileResponse.headers.get('content-length') || '0');
    const contentType = fileResponse.headers.get('content-type') || 'application/octet-stream';
    
    console.log('Response headers:', {
      contentLength,
      contentType,
      status: fileResponse.status
    });

    // ✅ Convert to ArrayBuffer first (more reliable than blob)
    console.log('Reading response body...');
    const arrayBuffer = await fileResponse.arrayBuffer();
    console.log('ArrayBuffer size:', arrayBuffer.byteLength);

    if (arrayBuffer.byteLength === 0) {
      throw new Error('Downloaded file is empty (0 bytes)');
    }

    // ✅ Determine filename
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
        const urlPath = validUrl.pathname;
        filename = urlPath.split('/').pop() || `file_${Date.now()}`;
        
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

    console.log('Final filename:', filename);

    // ✅ Size validation
    const MAX_FILE_SIZE = 175 * 1024 * 1024; // 175MB
    if (arrayBuffer.byteLength > MAX_FILE_SIZE) {
      throw new Error(
        `File too large: ${Math.round(arrayBuffer.byteLength / 1024 / 1024)}MB (max 175MB)`
      );
    }

    // ✅ Create File object from ArrayBuffer
    const file = new File([arrayBuffer], filename, { type: contentType });
    console.log('File created:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // Generate unique file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `url${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    console.log('Generated file ID:', fileId);

    // ✅ Chunking strategy
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB per chunk
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(
        `File requires ${totalChunks} chunks, but only ${kvNamespaces.length} KV namespaces available`
      );
    }

    console.log(`Splitting into ${totalChunks} chunks`);

    // ✅ Upload chunks with retry logic
    const chunkPromises = [];
    const uploadStartTime = Date.now();

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      const chunkFile = new File([chunk], `${filename}.part${i}`, { type: contentType });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      console.log(`Queuing chunk ${i + 1}/${totalChunks} for ${targetKV.name}`);

      const chunkPromise = uploadChunkToKVWithRetry(
        chunkFile,
        fileId,
        i,
        BOT_TOKEN,
        CHANNEL_ID,
        targetKV,
        3 // max retries
      );
      
      chunkPromises.push(chunkPromise);
    }

    const chunkResults = await Promise.all(chunkPromises);
    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);

    console.log(`All ${totalChunks} chunks uploaded in ${uploadDuration}s`);

    // ✅ Store master metadata
    const masterMetadata = {
      filename: filename,
      size: file.size,
      contentType: contentType,
      extension: extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_chunked_url',
      version: '2.0',
      sourceUrl: fileUrl,
      isTelegramSource: isTelegramUrl,
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
    console.log('Master metadata stored');

    // ✅ Generate URLs
    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    // ✅ Enhanced response matching upload.js structure
    const result = {
      success: true,
      message: 'URL import completed successfully',
      data: {
        id: fileId,
        filename: filename,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType: contentType,
        extension: extension,
        uploadDuration: `${uploadDuration}s`,
        uploadSpeed: `${(file.size / 1024 / parseFloat(uploadDuration)).toFixed(2)} KB/s`,
        urls: {
          view: customUrl,
          download: downloadUrl,
          stream: streamUrl
        },
        storage: {
          strategy: 'multi_kv_chunked_url',
          totalChunks: totalChunks,
          kvDistribution: chunkResults.map(r => r.kvNamespace),
          redundancy: 'distributed'
        },
        source: {
          url: fileUrl,
          type: isTelegramUrl ? 'telegram' : 'external'
        },
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    console.log('✅ URL UPLOAD COMPLETED:', {
      fileId,
      filename,
      size: file.size,
      chunks: totalChunks,
      duration: uploadDuration
    });

    return new Response(JSON.stringify(result, null, 2), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'X-File-Id': fileId,
        'X-Upload-Duration': uploadDuration,
        'X-Total-Chunks': totalChunks.toString(),
        ...corsHeaders
      }
    });

  } catch (error) {
    console.error('❌ URL UPLOAD ERROR:', error);
    console.error('Error stack:', error.stack);

    const errorResponse = {
      success: false,
      error: {
        message: error.message,
        type: error.name || 'UrlUploadError',
        timestamp: new Date().toISOString()
      },
      debug: {
        requestUrl: request.url,
        requestMethod: request.method
      }
    };

    return new Response(JSON.stringify(errorResponse, null, 2), {
      status: 500,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        ...corsHeaders
      }
    });
  }
}

// ✅ Upload chunk with retry logic
async function uploadChunkToKVWithRetry(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, maxRetries = 3) {
  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Chunk ${chunkIndex}: Upload attempt ${attempt}/${maxRetries}`);
      return await uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace);
    } catch (error) {
      console.error(`Chunk ${chunkIndex}: Attempt ${attempt} failed:`, error.message);
      lastError = error;
      
      if (attempt < maxRetries) {
        const delayMs = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        console.log(`Retrying chunk ${chunkIndex} after ${delayMs}ms...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
  
  throw new Error(`Failed to upload chunk ${chunkIndex} after ${maxRetries} attempts: ${lastError.message}`);
}

// ✅ Upload chunk to KV
async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const uploadStartTime = Date.now();
  console.log(`📤 Uploading chunk ${chunkIndex} (${formatBytes(chunkFile.size)}) to Telegram...`);

  // Upload to Telegram
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

  const telegramResponse = await fetch(
    `https://api.telegram.org/bot${botToken}/sendDocument`,
    {
      method: 'POST',
      body: telegramForm
    }
  );

  if (!telegramResponse.ok) {
    const errorText = await telegramResponse.text();
    throw new Error(`Telegram upload failed (${telegramResponse.status}): ${errorText}`);
  }

  const telegramData = await telegramResponse.json();

  if (!telegramData.ok) {
    throw new Error(`Telegram API error: ${telegramData.description || 'Unknown error'}`);
  }

  if (!telegramData.result?.document?.file_id) {
    throw new Error('Invalid Telegram response: missing file_id');
  }

  const telegramFileId = telegramData.result.document.file_id;
  const telegramMessageId = telegramData.result.message_id;

  console.log(`✅ Chunk ${chunkIndex} uploaded (file_id: ${telegramFileId})`);

  // Get file URL
  const getFileResponse = await fetch(
    `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`
  );

  if (!getFileResponse.ok) {
    throw new Error(`GetFile API failed (${getFileResponse.status})`);
  }

  const getFileData = await getFileResponse.json();

  if (!getFileData.ok || !getFileData.result?.file_path) {
    throw new Error('Failed to get file path from Telegram');
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  // Store chunk metadata
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
  console.log(`✅ Chunk ${chunkIndex} stored in ${kvNamespace.name} (${uploadDuration}s)`);

  return {
    telegramFileId: telegramFileId,
    telegramMessageId: telegramMessageId,
    size: chunkFile.size,
    directUrl: directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey: chunkKey,
    uploadedAt: Date.now()
  };
}

// ✅ Helper: Format bytes
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
