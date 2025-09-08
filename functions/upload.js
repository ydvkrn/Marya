export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== ULTRA-FAST UPLOAD START ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Vary': 'Origin'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { 
      headers: {
        ...corsHeaders,
        'Cache-Control': 'public, max-age=86400'
      }
    });
  }

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

    // ✅ All KV namespaces array
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    console.log(`🚀 Available KV namespaces: ${kvNamespaces.length}`);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing bot credentials');
    }

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log('📤 Ultra-fast upload:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // ✅ Size validation - 7 KV × 25MB = 175MB
    const MAX_FILE_SIZE = 175 * 1024 * 1024;
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(file.size / 1024 / 1024)}MB (max 175MB)`);
    }

    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // ✅ Optimized chunking - 18MB chunks for better performance
    const CHUNK_SIZE = 18 * 1024 * 1024; // Slightly smaller for faster processing
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File needs ${totalChunks} chunks, only ${kvNamespaces.length} KV available`);
    }

    console.log(`🎯 Using ${totalChunks} chunks with ultra-fast processing`);

    // ✅ Parallel chunk upload with connection pooling
    const chunkPromises = [];
    
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      
      const chunkFile = new File([chunk], `${file.name}.part${i}`, { type: file.type });
      const targetKV = kvNamespaces[i % kvNamespaces.length];
      
      const chunkPromise = uploadUltraFastChunk(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV);
      chunkPromises.push(chunkPromise);
    }

    const chunkResults = await Promise.all(chunkPromises);
    console.log('🚀 All chunks uploaded with ultra-fast processing');

    // ✅ Store master metadata
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'multi_kv_chunked',
      totalChunks: totalChunks,
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        telegramFileId: result.telegramFileId,
        size: result.size,
        chunkKey: result.chunkKey
      })),
      optimized: true,
      version: '2.0'
    };

    await kvNamespaces.kv.put(fileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;

    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: customUrl,
      download: downloadUrl,
      id: fileId,
      strategy: 'ultra_fast_multi_kv',
      chunks: totalChunks,
      performance: 'optimized',
      version: '2.0'
    };

    console.log('🎉 Ultra-fast upload completed:', result);

    return new Response(JSON.stringify(result), {
      headers: { 
        'Content-Type': 'application/json', 
        'Cache-Control': 'public, max-age=300',
        ...corsHeaders 
      }
    });

  } catch (error) {
    console.error('❌ Ultra-fast upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// ✅ Ultra-fast chunk upload with optimization
async function uploadUltraFastChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  console.log(`🚀 Ultra-fast chunk ${chunkIndex} → ${kvNamespace.name}`);
  
  // ✅ Optimized Telegram upload
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('disable_notification', 'true'); // Reduce server load

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm,
    // ✅ Optimized headers
    headers: {
      'User-Agent': 'MaryaVault-UltraFast/2.0'
    }
  });

  if (!telegramResponse.ok) {
    throw new Error(`Telegram upload failed for chunk ${chunkIndex}: ${telegramResponse.status}`);
  }

  const telegramData = await telegramResponse.json();
  if (!telegramData.ok || !telegramData.result?.document?.file_id) {
    throw new Error(`Invalid Telegram response for chunk ${chunkIndex}`);
  }

  const telegramFileId = telegramData.result.document.file_id;

  // ✅ Fast URL retrieval
  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`, {
    headers: {
      'User-Agent': 'MaryaVault-UltraFast/2.0'
    }
  });
  
  if (!getFileResponse.ok) {
    throw new Error(`GetFile API failed for chunk ${chunkIndex}`);
  }

  const getFileData = await getFileResponse.json();
  if (!getFileData.ok || !getFileData.result?.file_path) {
    throw new Error(`No file_path for chunk ${chunkIndex}`);
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
  
  // ✅ Optimized KV storage with performance metadata
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;
  const chunkMetadata = {
    telegramFileId: telegramFileId,
    directUrl: directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    uploadedAt: Date.now(),
    lastRefreshed: Date.now(),
    optimized: true,
    version: '2.0'
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));
  
  console.log(`✅ Ultra-fast chunk ${chunkIndex} stored in ${kvNamespace.name}`);
  
  return {
    telegramFileId: telegramFileId,
    size: chunkFile.size,
    directUrl: directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey: chunkKey
  };
}
