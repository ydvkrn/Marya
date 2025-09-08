export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT OPTIMIZED UPLOAD ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed'
    }), { status: 405, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing bot credentials');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces available');
    }

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    // ✅ Increased size limit to 175MB
    const MAX_FILE_SIZE = 175 * 1024 * 1024;
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(file.size / 1024 / 1024)}MB (max 175MB)`);
    }

    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB chunks
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File requires ${totalChunks} chunks, but only ${kvNamespaces.length} KV namespaces available`);
    }

    console.log(`📦 Processing ${totalChunks} chunks`);

    // ✅ Parallel upload with optimized chunking
    const chunkPromises = [];
    
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      
      const chunkFile = new File([chunk], `${file.name}.part${i}`, { type: file.type });
      const targetKV = kvNamespaces[i % kvNamespaces.length];
      
      const chunkPromise = uploadOptimizedChunk(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV);
      chunkPromises.push(chunkPromise);
    }

    const chunkResults = await Promise.all(chunkPromises);
    console.log('✅ All chunks uploaded with optimization');

    // ✅ Store optimized master metadata
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'multi_kv_chunked',
      totalChunks: totalChunks,
      optimized: true, // Mark as optimized
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        telegramFileId: result.telegramFileId,
        size: result.size,
        chunkKey: result.chunkKey,
        uploadedAt: Date.now()
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

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
      strategy: 'optimized_multi_kv',
      chunks: totalChunks,
      optimized: true
    };

    console.log('🚀 Optimized upload completed:', result);
    
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('❌ Upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), { status: 500, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }
}

// ✅ Optimized chunk upload
async function uploadOptimizedChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  console.log(`⚡ Optimized upload chunk ${chunkIndex} to ${kvNamespace.name}`);
  
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

  if (!telegramResponse.ok) {
    throw new Error(`Telegram upload failed for chunk ${chunkIndex}: ${telegramResponse.status}`);
  }

  const telegramData = await telegramResponse.json();
  if (!telegramData.ok || !telegramData.result?.document?.file_id) {
    throw new Error(`Invalid Telegram response for chunk ${chunkIndex}`);
  }

  const telegramFileId = telegramData.result.document.file_id;

  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
  
  if (!getFileResponse.ok) {
    throw new Error(`GetFile failed for chunk ${chunkIndex}`);
  }

  const getFileData = await getFileResponse.json();
  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
  
  // ✅ Store with optimization metadata
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;
  const optimizedMetadata = {
    telegramFileId: telegramFileId,
    directUrl: directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    uploadedAt: Date.now(),
    lastRefreshed: Date.now(),
    optimized: true
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(optimizedMetadata));
  
  console.log(`✅ Optimized chunk ${chunkIndex} stored in ${kvNamespace.name}`);
  
  return {
    telegramFileId: telegramFileId,
    size: chunkFile.size,
    directUrl: directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey: chunkKey
  };
}
