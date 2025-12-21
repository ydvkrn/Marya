// ✅ COMPLETE UPLOAD HANDLER - BACKWARD COMPATIBLE WITH YOUR [id].js
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-File-Size',
    'Access-Control-Max-Age': '86400'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Use POST' }), {
      status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // ✅ ALL 25 KV NAMESPACES (same as your original)
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

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing config: BOT_TOKEN, CHANNEL_ID, or KV namespaces');
    }

    const formData = await request.formData();
    const file = formData.get('file');
    
    if (!file) throw new Error('No file provided');

    // ✅ 1GB LIMIT
    const MAX_FILE_SIZE = 1024 * 1024 * 1024; // 1GB
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`Max 1GB. File size: ${(file.size/1024/1024).toFixed(1)}MB`);
    }

    if (file.size === 0) {
      throw new Error('File is empty');
    }

    // ✅ UNIQUE FILE ID (same format as your old uploads)
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    console.log(`📤 1GB UPLOAD START: ${file.name} (${formatBytes(file.size)})`);

    // ✅ 35MB CHUNKS FOR 1GB SAFETY (Cloudflare 128MB memory)
    const CHUNK_SIZE = 35 * 1024 * 1024; // 35MB
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    console.log(`📦 Total chunks needed: ${totalChunks} (Available KV: ${kvNamespaces.length})`);

    if (totalChunks > kvNamespaces.length * 2) {
      throw new Error(`File needs ${totalChunks} chunks, only ${kvNamespaces.length} KV available`);
    }

    // ✅ UPLOAD CHUNKS PARALLEL (with retry)
    const chunkPromises = [];
    console.log('🔄 Preparing chunks...');

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      
      // ✅ STREAMING SLICE - NO MEMORY OVERLOAD
      const chunkBlob = file.slice(start, end);
      const chunkFile = new File([chunkBlob], `${file.name}.part${i}`, { type: file.type });
      
      const targetKV = kvNamespaces[i % kvNamespaces.length];
      
      chunkPromises.push(
        uploadChunkToKVWithRetry(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV, 5)
      );
    }

    const chunkResults = await Promise.all(chunkPromises);
    console.log(`✅ ALL ${totalChunks} CHUNKS UPLOADED!`);

    // ✅ MASTER METADATA (BACKWARD COMPATIBLE WITH YOUR [id].js)
    const masterMetadata = {
      // ✅ OLD FORMAT FIELDS (for your [id].js compatibility)
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension: extension,
      uploadedAt: Date.now(),
      
      // ✅ NEW FIELDS (for 1GB multi-KV)
      type: 'multi_kv_chunked',
      version: '3.0',
      totalChunks: totalChunks,
      chunkSize: CHUNK_SIZE,
      
      // ✅ CHUNKS ARRAY (exactly what your [id].js expects)
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        telegramFileId: result.telegramFileId,
        telegramMessageId: result.telegramMessageId,
        size: result.size,
        chunkKey: result.chunkKey,  // ✅ Your [id].js uses this
        uploadedAt: result.uploadedAt
      }))
    };

    // ✅ STORE IN PRIMARY KV (same as your original)
    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));
    console.log(`💾 Master metadata saved: ${kvNamespaces[0].name}`);

    // ✅ GENERATE URLS (same format as frontend expects)
    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    const result = {
      success: true,
      message: '1GB File uploaded successfully!',
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType: file.type || 'application/octet-stream',
        extension: extension,
        totalChunks: totalChunks,
        urls: {
          view: customUrl,
          download: downloadUrl,
          stream: streamUrl
        },
        storage: {
          strategy: 'multi_kv_chunked_v3',
          totalChunks: totalChunks,
          kvDistribution: chunkResults.map(r => r.kvNamespace),
          maxSize: '1GB'
        },
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    return new Response(JSON.stringify(result, null, 2), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'X-File-Id': fileId,
        'X-Total-Chunks': totalChunks.toString(),
        ...corsHeaders
      }
    });

  } catch (error) {
    console.error('❌ UPLOAD FAILED:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// ✅ CHUNK UPLOADER WITH 5 RETRIES (production ready)
async function uploadChunkToKVWithRetry(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, maxRetries = 5) {
  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`📤 Chunk ${chunkIndex + 1}/${Math.ceil(chunkFile.size)} - Attempt ${attempt}/${maxRetries}`);
      return await uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace);
    } catch (error) {
      console.error(`❌ Chunk ${chunkIndex + 1} attempt ${attempt} failed:`, error.message);
      lastError = error;
      
      if (attempt < maxRetries) {
        const delay = Math.min(2000 * Math.pow(2, attempt - 1), 10000); // Max 10s
        await new Promise(r => setTimeout(r, delay));
      }
    }
  }
  
  throw new Error(`Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${lastError.message}`);
}

// ✅ SINGLE CHUNK UPLOADER (Telegram + KV storage)
async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const startTime = Date.now();
  
  // ✅ UPLOAD TO TELEGRAM
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `1GB-Chunk-${chunkIndex}-${fileId}`);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

  if (!telegramResponse.ok) {
    const errorText = await telegramResponse.text();
    throw new Error(`Telegram failed (${telegramResponse.status}): ${errorText}`);
  }

  const telegramData = await telegramResponse.json();
  if (!telegramData.ok || !telegramData.result?.document?.file_id) {
    throw new Error(`Telegram API error: ${telegramData.description || 'Unknown'}`);
  }

  const telegramFileId = telegramData.result.document.file_id;
  const telegramMessageId = telegramData.result.message_id;

  // ✅ REFRESH FILE URL
  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
  const getFileData = await getFileResponse.json();
  
  if (!getFileData.ok || !getFileData.result?.file_path) {
    throw new Error('Failed to get Telegram file path');
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  // ✅ STORE CHUNK METADATA (EXACTLY what your [id].js expects)
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
    chunkKey: chunkKey  // ✅ CRITICAL for your [id].js
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`✅ Chunk ${chunkIndex + 1} OK: ${formatBytes(chunkFile.size)} in ${duration}s`);

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
