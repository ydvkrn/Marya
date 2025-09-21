export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT MICRO-CHUNK UPLOAD START ===');

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
    // Multiple bot tokens for ultra-fast parallel upload
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

    // Generate custom file ID (MSM format)
    const customFileId = generateCustomId();
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // Smart chunking strategy (like YouTube/Instagram)
    const KEYS_PER_KV = 40;
    const totalKeys = kvNamespaces.length * KEYS_PER_KV; // 7 * 40 = 280 keys max

    // Calculate micro-chunk size dynamically
    let chunkSize;
    if (file.size <= 5 * 1024 * 1024) {
      // Small files: divide into 40 keys in first KV
      chunkSize = Math.ceil(file.size / KEYS_PER_KV);
    } else {
      // Large files: use all KVs, micro-chunks for fast fetch
      chunkSize = Math.ceil(file.size / totalKeys);
      if (chunkSize > 20 * 1024 * 1024) {
        chunkSize = 20 * 1024 * 1024; // Max 20MB per chunk
      }
      if (chunkSize < 100 * 1024) {
        chunkSize = 100 * 1024; // Min 100KB per chunk for efficiency
      }
    }

    const totalChunks = Math.ceil(file.size / chunkSize);
    console.log(`File will be split into ${totalChunks} micro-chunks (${Math.round(chunkSize/1024)}KB each)`);

    if (totalChunks > totalKeys) {
      throw new Error(`File too large: needs ${totalChunks} chunks but only ${totalKeys} keys available`);
    }

    // Upload micro-chunks in parallel batches
    const batchSize = 20; // Process 20 chunks at a time
    const allChunkResults = [];

    for (let batchStart = 0; batchStart < totalChunks; batchStart += batchSize) {
      const batchEnd = Math.min(batchStart + batchSize, totalChunks);
      const batchPromises = [];

      for (let i = batchStart; i < batchEnd; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, file.size);
        const chunk = file.slice(start, end);
        
        // Determine KV and key position
        const kvIndex = Math.floor(i / KEYS_PER_KV);
        const keyIndex = i % KEYS_PER_KV;
        
        if (kvIndex >= kvNamespaces.length) {
          throw new Error(`Not enough KV namespaces`);
        }

        const targetKV = kvNamespaces[kvIndex];
        const botToken = botTokens[i % botTokens.length];
        
        const chunkPromise = uploadMicroChunk(
          chunk,
          customFileId,
          i,
          kvIndex,
          keyIndex,
          botToken,
          CHANNEL_ID,
          targetKV,
          file.name
        );
        
        batchPromises.push(chunkPromise);
      }

      console.log(`Uploading batch ${Math.floor(batchStart/batchSize) + 1}/${Math.ceil(totalChunks/batchSize)} (${batchPromises.length} chunks)`);
      const batchResults = await Promise.all(batchPromises);
      allChunkResults.push(...batchResults);

      // Small delay between batches to avoid rate limits
      if (batchEnd < totalChunks) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }

    console.log(`All ${totalChunks} micro-chunks uploaded successfully`);

    // Store master metadata
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'micro_chunked_keys',
      totalChunks: totalChunks,
      chunkSize: chunkSize,
      keysPerKV: KEYS_PER_KV,
      kvDistribution: {},
      chunks: allChunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size,
        kvIndex: result.kvIndex,
        keyIndex: result.keyIndex
      }))
    };

    // Count chunks per KV
    allChunkResults.forEach(result => {
      const kvName = result.kvNamespace;
      masterMetadata.kvDistribution[kvName] = (masterMetadata.kvDistribution[kvName] || 0) + 1;
    });

    await kvNamespaces[0].kv.put(customFileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${customFileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${customFileId}${extension}?dl=1`;

    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: customUrl,
      download: downloadUrl,
      id: customFileId,
      strategy: 'micro_chunked_keys',
      chunks: totalChunks,
      chunkSize: `${Math.round(chunkSize/1024)}KB`,
      kvDistribution: masterMetadata.kvDistribution,
      botsUsed: botTokens.length
    };

    console.log('Micro-chunk upload completed:', result);
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

// Generate custom ID format: MSM000-999X99X00-99
function generateCustomId() {
  const timestamp = Date.now();
  const random1 = Math.floor(Math.random() * 1000).toString().padStart(3, '0'); // 000-999
  const random2 = Math.floor(Math.random() * 100).toString().padStart(2, '0');  // 00-99
  const random3 = Math.floor(Math.random() * 100).toString().padStart(2, '0');  // 00-99
  const random4 = Math.floor(Math.random() * 100).toString().padStart(2, '0');  // 00-99
  const randomChar1 = String.fromCharCode(65 + Math.floor(Math.random() * 26)); // A-Z
  const randomChar2 = String.fromCharCode(65 + Math.floor(Math.random() * 26)); // A-Z
  
  return `MSM${random1}-${random2}${randomChar1}${random3}${randomChar2}${random4}-${timestamp.toString(36).slice(-2)}`;
}

// Upload single micro-chunk
async function uploadMicroChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  console.log(`Uploading micro-chunk ${chunkIndex} (${Math.round(chunk.size/1024)}KB) to ${kvNamespace.name}[${keyIndex}]...`);

  try {
    // Create micro-chunk file
    const chunkFile = new File([chunk], `${originalFilename}.micro${chunkIndex}`, { type: 'application/octet-stream' });

    // Upload to Telegram with timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout

    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    if (!telegramResponse.ok) {
      throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error(`Invalid Telegram response`);
    }

    const telegramFileId = telegramData.result.document.file_id;

    // Get file URL
    const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);

    if (!getFileResponse.ok) {
      throw new Error(`GetFile API failed: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error(`No file_path in response`);
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store micro-chunk metadata as KV key
    const keyName = `${fileId}_micro_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
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
      lastRefreshed: Date.now()
    };

    await kvNamespace.kv.put(keyName, JSON.stringify(chunkMetadata));
    console.log(`✅ Micro-chunk ${chunkIndex} stored as key: ${keyName}`);

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
    console.error(`❌ Failed to upload micro-chunk ${chunkIndex}:`, error);
    
    // Retry with exponential backoff
    const retryDelay = 1000 + Math.random() * 2000;
    await new Promise(resolve => setTimeout(resolve, retryDelay));
    
    try {
      console.log(`🔄 Retrying micro-chunk ${chunkIndex}...`);
      return await uploadMicroChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename);
    } catch (retryError) {
      throw new Error(`Micro-chunk ${chunkIndex} failed after retry: ${retryError.message}`);
    }
  }
}
