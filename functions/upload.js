export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT UPLOAD START ===');
  console.log('Method:', request.method);
  console.log('URL:', request.url);
  console.log('Content-Type:', request.headers.get('content-type'));

  // ✅ CRITICAL: Comprehensive CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': '*',
    'Access-Control-Max-Age': '86400',
    'Access-Control-Expose-Headers': '*'
  };

  // ✅ Handle OPTIONS preflight
  if (request.method === 'OPTIONS') {
    console.log('✅ OPTIONS request handled');
    return new Response(null, {
      status: 204,
      headers: corsHeaders
    });
  }

  // ✅ Allow only POST
  if (request.method !== 'POST') {
    console.error('❌ Invalid method:', request.method);
    return new Response(JSON.stringify({
      success: false,
      error: `Method ${request.method} not allowed. Use POST.`
    }), {
      status: 405,
      headers: {
        'Content-Type': 'application/json',
        'Allow': 'POST, OPTIONS',
        ...corsHeaders
      }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    console.log('Env check:', { 
      hasToken: !!BOT_TOKEN, 
      hasChannel: !!CHANNEL_ID 
    });

    // ✅ All 25 KV namespaces
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

    console.log(`Available KV: ${kvNamespaces.length}/25`);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing BOT_TOKEN or CHANNEL_ID');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces bound');
    }

    // ✅ CRITICAL: Parse form data with error handling
    let formData;
    try {
      const contentType = request.headers.get('content-type') || '';
      console.log('Parsing form data, content-type:', contentType);
      
      if (!contentType.includes('multipart/form-data')) {
        throw new Error('Content-Type must be multipart/form-data');
      }
      
      formData = await request.formData();
      console.log('✅ Form data parsed');
    } catch (parseError) {
      console.error('❌ Parse error:', parseError);
      throw new Error(`Invalid form data: ${parseError.message}`);
    }

    const file = formData.get('file');

    if (!file || !file.size) {
      throw new Error('No file provided or file is empty');
    }

    console.log('File:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // ✅ Size validation
    const MAX_SIZE = 500 * 1024 * 1024; // 500MB
    if (file.size > MAX_SIZE) {
      throw new Error(`File too large: ${(file.size / 1024 / 1024).toFixed(2)}MB. Max: 500MB`);
    }

    // ✅ Generate file ID
    const fileId = `id${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    console.log('File ID:', fileId);

    // ✅ Chunking
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`Need ${totalChunks} chunks but only ${kvNamespaces.length} KV available`);
    }

    console.log(`Splitting into ${totalChunks} chunks`);

    const uploadStart = Date.now();
    const chunkPromises = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const chunkFile = new File([chunk], `${file.name}.part${i}`, { type: file.type });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      chunkPromises.push(
        uploadChunk(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV)
      );
    }

    const chunkResults = await Promise.all(chunkPromises);
    const uploadDuration = ((Date.now() - uploadStart) / 1000).toFixed(2);

    console.log(`✅ ${totalChunks} chunks uploaded in ${uploadDuration}s`);

    // ✅ Store metadata
    const metadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension: extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_chunked',
      version: '2.0',
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

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(metadata));
    console.log('✅ Metadata stored');

    // ✅ Generate URLs
    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    // ✅ CRITICAL: Return proper response
    const response = {
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
          view: viewUrl,
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

    console.log('✅✅✅ UPLOAD SUCCESS');

    return new Response(JSON.stringify(response, null, 2), {
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
    console.error('❌ ERROR:', error.message);
    console.error('Stack:', error.stack);

    return new Response(JSON.stringify({
      success: false,
      error: {
        message: error.message,
        type: error.name || 'UploadError',
        timestamp: new Date().toISOString()
      },
      debug: {
        url: request.url,
        method: request.method,
        contentType: request.headers.get('content-type')
      }
    }, null, 2), {
      status: 500,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        ...corsHeaders
      }
    });
  }
}

// ✅ Upload chunk with retry
async function uploadChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const maxRetries = 3;
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Chunk ${chunkIndex}: Attempt ${attempt}/${maxRetries}`);

      const uploadStart = Date.now();

      // ✅ Upload to Telegram
      const telegramForm = new FormData();
      telegramForm.append('chat_id', channelId);
      telegramForm.append('document', chunkFile);
      telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

      const telegramRes = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
        method: 'POST',
        body: telegramForm
      });

      if (!telegramRes.ok) {
        const errorText = await telegramRes.text();
        throw new Error(`Telegram error ${telegramRes.status}: ${errorText.substring(0, 100)}`);
      }

      const telegramData = await telegramRes.json();

      if (!telegramData.ok || !telegramData.result?.document?.file_id) {
        throw new Error('Invalid Telegram response');
      }

      const telegramFileId = telegramData.result.document.file_id;
      const telegramMessageId = telegramData.result.message_id;

      console.log(`✅ Chunk ${chunkIndex} uploaded to Telegram`);

      // ✅ Get file URL
      const getFileRes = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);

      if (!getFileRes.ok) {
        throw new Error(`GetFile error ${getFileRes.status}`);
      }

      const getFileData = await getFileRes.json();

      if (!getFileData.ok || !getFileData.result?.file_path) {
        throw new Error('No file_path in response');
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

      // ✅ Store in KV
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

      const duration = ((Date.now() - uploadStart) / 1000).toFixed(2);
      console.log(`✅ Chunk ${chunkIndex} stored in ${kvNamespace.name} (${duration}s)`);

      return {
        telegramFileId: telegramFileId,
        telegramMessageId: telegramMessageId,
        size: chunkFile.size,
        directUrl: directUrl,
        kvNamespace: kvNamespace.name,
        chunkKey: chunkKey,
        uploadedAt: Date.now()
      };

    } catch (error) {
      console.error(`❌ Chunk ${chunkIndex} attempt ${attempt} failed:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        const delay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        console.log(`Retrying after ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  throw new Error(`Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${lastError.message}`);
}

// ✅ Format bytes helper
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
