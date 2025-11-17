export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT UPLOAD START ===');
  console.log('Request method:', request.method);
  console.log('Request URL:', request.url);
  console.log('Timestamp:', new Date().toISOString());

  // ✅ CRITICAL: Enhanced CORS headers for all responses
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET, PUT, DELETE',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, X-Requested-With',
    'Access-Control-Max-Age': '86400',
    'Access-Control-Expose-Headers': 'X-File-Id, X-Upload-Duration, X-Total-Chunks'
  };

  // ✅ CRITICAL: Handle preflight OPTIONS request FIRST
  if (request.method === 'OPTIONS') {
    console.log('✅ Handling OPTIONS preflight request');
    return new Response(null, {
      status: 204,
      headers: corsHeaders
    });
  }

  // ✅ CRITICAL: Only allow POST method for uploads
  if (request.method !== 'POST') {
    console.error(`❌ Invalid method: ${request.method}`);
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed. Use POST method for file uploads.',
      allowedMethods: ['POST', 'OPTIONS'],
      receivedMethod: request.method,
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

    // ✅ Parse multipart form data
    let formData;
    try {
      formData = await request.formData();
      console.log('✅ Form data parsed successfully');
    } catch (parseError) {
      console.error('❌ Form data parse error:', parseError);
      throw new Error('Invalid form data. Please ensure you are sending multipart/form-data.');
    }

    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided. Please include a file in the form data with key "file".');
    }

    console.log('📄 File received:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // ✅ Enhanced size validation - 500MB max with 25 KV namespaces
    const MAX_FILE_SIZE = 500 * 1024 * 1024; // 500MB
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(file.size / 1024 / 1024)}MB. Maximum allowed: 500MB`);
    }

    if (file.size === 0) {
      throw new Error('File is empty. Please select a valid file.');
    }

    // ✅ Generate unique file ID with better entropy
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    console.log('Generated file ID:', fileId);

    // ✅ Smart chunking strategy
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB per chunk
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File requires ${totalChunks} chunks, but only ${kvNamespaces.length} KV namespaces available. Please add more KV namespaces.`);
    }

    console.log(`File will be split into ${totalChunks} chunks`);

    // ✅ Upload progress tracking
    const uploadStartTime = Date.now();

    // ✅ Upload chunks to different KV namespaces with retry logic
    const chunkPromises = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      const chunkFile = new File([chunk], `${file.name}.part${i}`, { type: file.type });
      const targetKV = kvNamespaces[i % kvNamespaces.length]; // Round-robin distribution

      console.log(`Preparing chunk ${i + 1}/${totalChunks} for ${targetKV.name}`);

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

    console.log(`✅ All ${totalChunks} chunks uploaded successfully in ${uploadDuration}s`);

    // ✅ Store master metadata in primary KV with enhanced info
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
    console.log('✅ Master metadata stored in', kvNamespaces[0].name);

    // ✅ Generate response URLs
    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    // ✅ Enhanced response with all details
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

    console.log('✅✅✅ UPLOAD COMPLETED:', {
      fileId,
      filename: file.name,
      size: file.size,
      chunks: totalChunks,
      duration: uploadDuration
    });

    // ✅ CRITICAL: Return proper JSON response with CORS headers
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
    console.error('❌❌❌ UPLOAD ERROR:', error);
    console.error('Error stack:', error.stack);

    // ✅ Enhanced error response with CORS
    const errorResponse = {
      success: false,
      error: {
        message: error.message,
        type: error.name || 'UploadError',
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
        // Exponential backoff
        const delayMs = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        console.log(`⏳ Retrying chunk ${chunkIndex} after ${delayMs}ms...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }

  throw new Error(`Failed to upload chunk ${chunkIndex} after ${maxRetries} attempts: ${lastError.message}`);
}

// ✅ Upload chunk to specific KV namespace
async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const uploadStartTime = Date.now();
  console.log(`📤 Uploading chunk ${chunkIndex} (${formatBytes(chunkFile.size)}) to Telegram...`);

  // ✅ Upload to Telegram with proper error handling
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

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

  console.log(`✅ Chunk ${chunkIndex} uploaded to Telegram (file_id: ${telegramFileId})`);

  // ✅ Get file URL with error handling
  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);

  if (!getFileResponse.ok) {
    throw new Error(`GetFile API failed (${getFileResponse.status})`);
  }

  const getFileData = await getFileResponse.json();

  if (!getFileData.ok || !getFileData.result?.file_path) {
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
