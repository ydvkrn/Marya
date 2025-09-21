export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT MULTI-KV CHUNK KEYS UPLOAD START ===');

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
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    // Multiple bot tokens for faster parallel upload
    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    const CHANNEL_ID = env.CHANNEL_ID;

    if (botTokens.length === 0 || !CHANNEL_ID) {
      throw new Error('Missing bot credentials');
    }

    console.log(`Available bot tokens: ${botTokens.length}`);

    // All KV namespaces
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces available');
    }

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log('File received:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // Calculate max file size: 7 KV × 40 keys × 20MB = 5.6GB theoretical limit
    const MAX_KEYS_PER_KV = 40;
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB per chunk/key
    const MAX_FILE_SIZE = kvNamespaces.length * MAX_KEYS_PER_KV * CHUNK_SIZE;

    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(file.size / 1024 / 1024)}MB (max ${Math.round(MAX_FILE_SIZE / 1024 / 1024)}MB)`);
    }

    // Generate unique file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // Calculate chunks (each chunk = 1 key in KV)
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    console.log(`File will be split into ${totalChunks} chunks (keys) across ${kvNamespaces.length} KV namespaces`);

    // Upload chunks in parallel using multiple bots
    const chunkPromises = [];
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      
      // Determine which KV namespace to use (distribute evenly)
      const kvIndex = Math.floor(i / MAX_KEYS_PER_KV);
      const keyIndex = i % MAX_KEYS_PER_KV;
      
      if (kvIndex >= kvNamespaces.length) {
        throw new Error(`Not enough KV namespaces for ${totalChunks} chunks`);
      }

      const targetKV = kvNamespaces[kvIndex];
      const botToken = botTokens[i % botTokens.length]; // Round-robin bot selection
      
      const chunkPromise = uploadChunkAsKey(
        chunk,
        fileId,
        i,
        kvIndex,
        keyIndex,
        botToken,
        CHANNEL_ID,
        targetKV,
        file.name
      );
      
      chunkPromises.push(chunkPromise);
    }

    // Wait for all chunks to upload
    console.log('Starting parallel chunk upload...');
    const chunkResults = await Promise.all(chunkPromises);
    console.log(`All ${totalChunks} chunks uploaded successfully as KV keys`);

    // Store master metadata in primary KV
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'multi_kv_chunked_keys',
      totalChunks: totalChunks,
      maxKeysPerKV: MAX_KEYS_PER_KV,
      chunkSize: CHUNK_SIZE,
      kvDistribution: {},
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size,
        kvIndex: result.kvIndex,
        keyIndex: result.keyIndex
      }))
    };

    // Count chunks per KV for statistics
    chunkResults.forEach(result => {
      const kvName = result.kvNamespace;
      masterMetadata.kvDistribution[kvName] = (masterMetadata.kvDistribution[kvName] || 0) + 1;
    });

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
      strategy: 'multi_kv_chunked_keys',
      chunks: totalChunks,
      kvDistribution: masterMetadata.kvDistribution,
      maxFileSize: `${Math.round(MAX_FILE_SIZE / 1024 / 1024)}MB`,
      botsUsed: botTokens.length
    };

    console.log('Multi-KV chunked keys upload completed:', result);
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Upload single chunk as KV key
async function uploadChunkAsKey(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  console.log(`Uploading chunk ${chunkIndex} as key to ${kvNamespace.name}[${keyIndex}]...`);

  try {
    // Create chunk file
    const chunkFile = new File([chunk], `${originalFilename}.chunk${chunkIndex}`, { type: 'application/octet-stream' });

    // Upload to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      timeout: 60000 // 60 second timeout
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error(`Invalid Telegram response: ${JSON.stringify(telegramData)}`);
    }

    const telegramFileId = telegramData.result.document.file_id;

    // Get file URL
    const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);

    if (!getFileResponse.ok) {
      throw new Error(`GetFile API failed: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error(`No file_path in response: ${JSON.stringify(getFileData)}`);
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store chunk metadata as KV key
    const keyName = `${fileId}_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
    const chunkMetadata = {
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      size: chunk.size,
      chunkIndex: chunkIndex,
      kvIndex: kvIndex,
      keyIndex: keyIndex,
      parentFileId: fileId,
      kvNamespace: kvNamespace.name,
      uploadedAt: Date.now(),
      lastRefreshed: Date.now(),
      botTokenUsed: botToken.slice(-10) // Last 10 chars for identification
    };

    await kvNamespace.kv.put(keyName, JSON.stringify(chunkMetadata));
    console.log(`✅ Chunk ${chunkIndex} stored as key: ${keyName}`);

    return {
      telegramFileId: telegramFileId,
      size: chunk.size,
      directUrl: directUrl,
      kvNamespace: kvNamespace.name,
      keyName: keyName,
      kvIndex: kvIndex,
      keyIndex: keyIndex
    };

  } catch (error) {
    console.error(`❌ Failed to upload chunk ${chunkIndex}:`, error);
    
    // Retry once with exponential backoff
    await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * 3000));
    
    try {
      console.log(`🔄 Retrying chunk ${chunkIndex}...`);
      return await uploadChunkAsKey(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename);
    } catch (retryError) {
      throw new Error(`Chunk ${chunkIndex} failed after retry: ${retryError.message}`);
    }
  }
}
