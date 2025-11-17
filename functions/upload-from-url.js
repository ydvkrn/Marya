export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT URL UPLOAD START ===');
  console.log('Request method:', request.method);
  console.log('Request URL:', request.url);
  console.log('Timestamp:', new Date().toISOString());

  // ✅ Enhanced CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, X-Requested-With',
    'Access-Control-Max-Age': '86400',
    'Access-Control-Expose-Headers': 'X-File-Id, X-Upload-Duration, X-Total-Chunks'
  };

  // ✅ Handle preflight OPTIONS request
  if (request.method === 'OPTIONS') {
    console.log('✅ Handling OPTIONS preflight request');
    return new Response(null, {
      status: 204,
      headers: corsHeaders
    });
  }

  // ✅ Only allow POST method
  if (request.method !== 'POST') {
    console.error(`❌ Invalid method: ${request.method}`);
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed. Use POST method.',
      allowedMethods: ['POST', 'OPTIONS'],
      timestamp: new Date().toISOString()
    }), {
      status: 405,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Allow': 'POST, OPTIONS',
        ...corsHeaders
      }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    console.log('Environment check:', {
      hasBotToken: !!BOT_TOKEN,
      hasChannelId: !!CHANNEL_ID,
      timestamp: Date.now()
    });

    // ✅ All 25 KV namespaces array
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' },
      { kv: env.FILES_KV8, name: 'FILES_KV8' },
      { kv: env.FILES_KV9, name: 'FILES_KV9' },
      { kv: env.FILES_KV10, name: 'FILES_KV10' },
      { kv: env.FILES_KV11, name: 'FILES_KV11' },
      { kv: env.FILES_KV12, name: 'FILES_KV12' },
      { kv: env.FILES_KV13, name: 'FILES_KV13' },
      { kv: env.FILES_KV14, name: 'FILES_KV14' },
      { kv: env.FILES_KV15, name: 'FILES_KV15' },
      { kv: env.FILES_KV16, name: 'FILES_KV16' },
      { kv: env.FILES_KV17, name: 'FILES_KV17' },
      { kv: env.FILES_KV18, name: 'FILES_KV18' },
      { kv: env.FILES_KV19, name: 'FILES_KV19' },
      { kv: env.FILES_KV20, name: 'FILES_KV20' },
      { kv: env.FILES_KV21, name: 'FILES_KV21' },
      { kv: env.FILES_KV22, name: 'FILES_KV22' },
      { kv: env.FILES_KV23, name: 'FILES_KV23' },
      { kv: env.FILES_KV24, name: 'FILES_KV24' },
      { kv: env.FILES_KV25, name: 'FILES_KV25' }
    ].filter(item => item.kv);

    console.log(`✅ Available KV namespaces: ${kvNamespaces.length}/25`);

    // ✅ Validation checks
    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing bot credentials. Please configure BOT_TOKEN and CHANNEL_ID in environment variables.');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces available. Please bind at least FILES_KV in your Worker settings.');
    }

    // ✅ Parse JSON body with robust error handling
    let body;
    try {
      const rawBody = await request.text();
      console.log('Raw request body length:', rawBody.length);
      
      if (!rawBody || rawBody.trim().length === 0) {
        throw new Error('Request body is empty');
      }
      
      body = JSON.parse(rawBody);
      console.log('✅ JSON body parsed successfully');
    } catch (parseError) {
      console.error('❌ JSON parsing error:', parseError.message);
      throw new Error(`Invalid JSON body: ${parseError.message}. Please send valid JSON with "fileUrl" or "url" field.`);
    }

    // ✅ Extract and validate URL
    const fileUrl = body.fileUrl || body.url;
    const customFilename = body.filename || null;

    if (!fileUrl) {
      console.error('❌ No URL provided in body:', body);
      throw new Error('No URL provided. Please include "fileUrl" or "url" field in request body.');
    }

    // ✅ URL validation
    let parsedUrl;
    try {
      parsedUrl = new URL(fileUrl);
      
      // Check for valid protocols
      if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
        throw new Error(`Invalid protocol: ${parsedUrl.protocol}. Only HTTP and HTTPS are supported.`);
      }
      
      console.log('✅ URL validated:', {
        protocol: parsedUrl.protocol,
        hostname: parsedUrl.hostname,
        pathname: parsedUrl.pathname
      });
      
    } catch (urlError) {
      console.error('❌ Invalid URL:', urlError.message);
      throw new Error(`Invalid URL format: ${urlError.message}`);
    }

    console.log('📥 Fetching file from URL:', fileUrl);

    // ✅ Fetch file from URL with timeout and retry logic
    const fetchStartTime = Date.now();
    let fileResponse;
    let fetchAttempt = 0;
    const maxFetchAttempts = 3;

    while (fetchAttempt < maxFetchAttempts) {
      try {
        fetchAttempt++;
        console.log(`Fetch attempt ${fetchAttempt}/${maxFetchAttempts}...`);

        fileResponse = await fetch(fileUrl, {
          method: 'GET',
          headers: {
            'User-Agent': 'MaryaVault/2.0 (File Storage Bot)',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br'
          },
          redirect: 'follow'
        });

        if (fileResponse.ok) {
          console.log('✅ File fetched successfully');
          break;
        } else {
          console.warn(`Fetch attempt ${fetchAttempt} failed with status: ${fileResponse.status}`);
          
          if (fetchAttempt < maxFetchAttempts) {
            const delayMs = Math.min(1000 * Math.pow(2, fetchAttempt - 1), 3000);
            console.log(`Retrying after ${delayMs}ms...`);
            await new Promise(resolve => setTimeout(resolve, delayMs));
          }
        }
        
      } catch (fetchError) {
        console.error(`Fetch attempt ${fetchAttempt} error:`, fetchError.message);
        
        if (fetchAttempt >= maxFetchAttempts) {
          throw new Error(`Failed to fetch file after ${maxFetchAttempts} attempts: ${fetchError.message}`);
        }
      }
    }

    if (!fileResponse || !fileResponse.ok) {
      throw new Error(`Failed to fetch file: ${fileResponse?.status || 'Unknown'} ${fileResponse?.statusText || 'Error'}`);
    }

    const fetchDuration = ((Date.now() - fetchStartTime) / 1000).toFixed(2);
    console.log(`File fetched in ${fetchDuration}s`);

    // ✅ Get file metadata from headers
    const contentLength = parseInt(fileResponse.headers.get('content-length') || '0');
    const contentType = fileResponse.headers.get('content-type') || 'application/octet-stream';
    const contentDisposition = fileResponse.headers.get('content-disposition');

    console.log('📊 Response headers:', {
      status: fileResponse.status,
      contentLength: contentLength,
      contentType: contentType,
      disposition: contentDisposition
    });

    // ✅ Determine filename
    let filename = customFilename;
    if (!filename) {
      if (contentDisposition && contentDisposition.includes('filename=')) {
        const matches = contentDisposition.match(/filename[^;=
]*=((['"]).*?\u0002|[^;
]*)/);
        if (matches && matches[1]) {
          filename = matches[1].replace(/['"]/g, '');
        }
      }
      
      if (!filename) {
        const urlPath = parsedUrl.pathname;
        filename = urlPath.split('/').filter(Boolean).pop() || `file_${Date.now()}`;
        
        // Decode URL-encoded filename
        try {
          filename = decodeURIComponent(filename);
        } catch (e) {
          console.warn('Could not decode filename:', e.message);
        }
      }
    }

    // ✅ Sanitize filename
    filename = filename
      .replace(/[<>:"|?*]/g, '_')
      .replace(/s+/g, '_')
      .substring(0, 255);

    console.log('📄 File info:', {
      filename: filename,
      size: contentLength,
      type: contentType
    });

    // ✅ Convert response to blob
    const fileBlob = await fileResponse.blob();
    const actualSize = fileBlob.size;
    
    console.log('Blob created:', {
      size: actualSize,
      contentLengthHeader: contentLength,
      match: actualSize === contentLength
    });

    // ✅ Validate file size
    if (actualSize === 0) {
      throw new Error('Downloaded file is empty (0 bytes)');
    }

    // ✅ Enhanced size validation - 500MB max with 25 KV namespaces
    const MAX_FILE_SIZE = 500 * 1024 * 1024; // 500MB
    if (actualSize > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${formatBytes(actualSize)}. Maximum allowed: 500MB`);
    }

    // ✅ Create File object
    const file = new File([fileBlob], filename, { type: contentType });

    // ✅ Generate unique file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `url${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    console.log('Generated file ID:', fileId);

    // ✅ Smart chunking strategy
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB per chunk
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File requires ${totalChunks} chunks (${formatBytes(file.size)}), but only ${kvNamespaces.length} KV namespaces available. Please add more KV namespaces.`);
    }

    console.log(`File will be split into ${totalChunks} chunks of ${formatBytes(CHUNK_SIZE)} each`);

    // ✅ Upload progress tracking
    const uploadStartTime = Date.now();

    // ✅ Upload chunks to different KV namespaces with retry logic
    const chunkPromises = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      const chunkFile = new File([chunk], `${filename}.part${i}`, { type: contentType });
      const targetKV = kvNamespaces[i % kvNamespaces.length]; // Round-robin distribution

      console.log(`Preparing chunk ${i + 1}/${totalChunks} (${formatBytes(chunk.size)}) for ${targetKV.name}`);

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
    const uploadEndTime = Date.now();
    const uploadDuration = ((uploadEndTime - uploadStartTime) / 1000).toFixed(2);
    const totalDuration = ((uploadEndTime - fetchStartTime) / 1000).toFixed(2);

    console.log(`✅ All ${totalChunks} chunks uploaded successfully in ${uploadDuration}s`);

    // ✅ Store master metadata in primary KV with enhanced info
    const masterMetadata = {
      filename: filename,
      size: file.size,
      contentType: contentType,
      extension: extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      fetchDuration: parseFloat(fetchDuration),
      totalDuration: parseFloat(totalDuration),
      type: 'multi_kv_chunked_url',
      version: '2.0',
      sourceUrl: fileUrl,
      sourceHost: parsedUrl.hostname,
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
    console.log('✅ Master metadata stored in', kvNamespaces[0].name);

    // ✅ Generate response URLs
    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    // ✅ Enhanced response with all details
    const result = {
      success: true,
      message: 'File uploaded successfully from URL',
      data: {
        id: fileId,
        filename: filename,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType: contentType,
        extension: extension,
        timing: {
          fetchDuration: `${fetchDuration}s`,
          uploadDuration: `${uploadDuration}s`,
          totalDuration: `${totalDuration}s`,
          uploadSpeed: `${(file.size / 1024 / parseFloat(uploadDuration)).toFixed(2)} KB/s`
        },
        urls: {
          view: customUrl,
          download: downloadUrl,
          stream: streamUrl
        },
        storage: {
          strategy: 'multi_kv_chunked_url',
          totalChunks: totalChunks,
          chunkSize: formatBytes(CHUNK_SIZE),
          kvDistribution: chunkResults.map(r => r.kvNamespace),
          redundancy: 'distributed'
        },
        source: {
          url: fileUrl,
          host: parsedUrl.hostname,
          fetchedAt: new Date(fetchStartTime).toISOString()
        },
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    console.log('✅✅✅ URL UPLOAD COMPLETED:', {
      fileId,
      filename,
      size: formatBytes(file.size),
      chunks: totalChunks,
      totalDuration: `${totalDuration}s`
    });

    // ✅ Return proper JSON response with correct headers
    return new Response(JSON.stringify(result, null, 2), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'X-File-Id': fileId,
        'X-Upload-Duration': uploadDuration,
        'X-Total-Chunks': totalChunks.toString(),
        'X-File-Size': file.size.toString(),
        ...corsHeaders
      }
    });

  } catch (error) {
    console.error('❌❌❌ URL UPLOAD ERROR:', error);
    console.error('Error name:', error.name);
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);

    // ✅ Enhanced error response with debugging info
    const errorResponse = {
      success: false,
      error: {
        message: error.message,
        type: error.name || 'UrlUploadError',
        timestamp: new Date().toISOString()
      },
      debug: {
        requestUrl: request.url,
        requestMethod: request.method,
        userAgent: request.headers.get('user-agent') || 'Unknown'
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

// ✅ Enhanced upload chunk function with retry logic
async function uploadChunkToKVWithRetry(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, maxRetries = 3) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`📤 Chunk ${chunkIndex}: Upload attempt ${attempt}/${maxRetries} to ${kvNamespace.name}`);

      return await uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace);

    } catch (error) {
      console.error(`❌ Chunk ${chunkIndex}: Attempt ${attempt} failed:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        // Exponential backoff with jitter
        const baseDelay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        const jitter = Math.random() * 500;
        const delayMs = baseDelay + jitter;
        
        console.log(`⏳ Retrying chunk ${chunkIndex} after ${delayMs.toFixed(0)}ms...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }

  throw new Error(`Failed to upload chunk ${chunkIndex} to ${kvNamespace.name} after ${maxRetries} attempts: ${lastError.message}`);
}

// ✅ Upload chunk to specific KV namespace
async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const uploadStartTime = Date.now();
  console.log(`📤 Uploading chunk ${chunkIndex} (${formatBytes(chunkFile.size)}) to Telegram...`);

  // ✅ Upload to Telegram with proper error handling
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `📦 Chunk ${chunkIndex} | File: ${fileId} | Size: ${formatBytes(chunkFile.size)}`);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

  if (!telegramResponse.ok) {
    const errorText = await telegramResponse.text();
    console.error(`❌ Telegram upload failed for chunk ${chunkIndex}:`, errorText);
    throw new Error(`Telegram upload failed (${telegramResponse.status}): ${errorText.substring(0, 200)}`);
  }

  const telegramData = await telegramResponse.json();

  if (!telegramData.ok) {
    const errorDesc = telegramData.description || 'Unknown error';
    console.error(`❌ Telegram API error for chunk ${chunkIndex}:`, errorDesc);
    throw new Error(`Telegram API error: ${errorDesc}`);
  }

  if (!telegramData.result?.document?.file_id) {
    console.error('❌ Invalid Telegram response:', telegramData);
    throw new Error('Invalid Telegram response: missing file_id');
  }

  const telegramFileId = telegramData.result.document.file_id;
  const telegramMessageId = telegramData.result.message_id;

  console.log(`✅ Chunk ${chunkIndex} uploaded to Telegram (file_id: ${telegramFileId}, msg_id: ${telegramMessageId})`);

  // ✅ Get file URL with error handling
  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);

  if (!getFileResponse.ok) {
    const errorText = await getFileResponse.text();
    console.error(`❌ GetFile API failed for chunk ${chunkIndex}:`, errorText);
    throw new Error(`GetFile API failed (${getFileResponse.status})`);
  }

  const getFileData = await getFileResponse.json();

  if (!getFileData.ok || !getFileData.result?.file_path) {
    console.error('❌ No file_path in GetFile response:', getFileData);
    throw new Error('Failed to get file path from Telegram');
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  // ✅ Store chunk metadata with enhanced info
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

// ✅ Helper function to format bytes
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}