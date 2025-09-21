export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== LIGHTNING FAST UPLOAD START ===');

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
    // Multiple bot tokens for parallel speed
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

    // KV namespaces
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

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

    // Generate custom MSM ID
    const customFileId = generateMSMId();
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // SMART CHUNKING STRATEGY (Super Fast)
    let chunkSize, strategy;

    if (file.size <= 1 * 1024 * 1024) {
      // Small files (< 1MB): Direct upload, no chunking
      strategy = 'direct';
      chunkSize = file.size;
    } else if (file.size <= 20 * 1024 * 1024) {
      // Medium files (1-20MB): 1MB chunks for speed
      strategy = 'fast_chunks';
      chunkSize = 1 * 1024 * 1024; // 1MB chunks
    } else {
      // Large files (> 20MB): 5MB chunks for efficiency
      strategy = 'large_chunks';
      chunkSize = 5 * 1024 * 1024; // 5MB chunks
    }

    console.log(`Using strategy: ${strategy}, chunk size: ${Math.round(chunkSize/1024/1024)}MB`);

    if (strategy === 'direct') {
      // Direct upload for small files
      return await handleDirectUpload(file, customFileId, extension, botTokens[0], CHANNEL_ID, kvNamespaces[0], request, corsHeaders);
    } else {
      // Chunked upload for larger files
      return await handleChunkedUpload(file, customFileId, extension, strategy, chunkSize, botTokens, CHANNEL_ID, kvNamespaces, request, corsHeaders);
    }

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

// Direct upload for small files (< 1MB) - Super Fast
async function handleDirectUpload(file, fileId, extension, botToken, channelId, kvNamespace, request, corsHeaders) {
  console.log('Using direct upload strategy');

  try {
    // Upload directly to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', file);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: AbortSignal.timeout(15000) // 15 second timeout
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    const telegramFileId = telegramData.result.document.file_id;

    // Get direct URL
    const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`);
    const getFileData = await getFileResponse.json();
    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store metadata
    const metadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'direct_upload',
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      strategy: 'direct'
    };

    await kvNamespace.kv.put(fileId, JSON.stringify(metadata));

    const baseUrl = new URL(request.url).origin;
    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      url: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
      id: fileId,
      strategy: 'direct'
    };

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    throw new Error(`Direct upload failed: ${error.message}`);
  }
}

// Chunked upload for larger files
async function handleChunkedUpload(file, fileId, extension, strategy, chunkSize, botTokens, channelId, kvNamespaces, request, corsHeaders) {
  console.log(`Using chunked upload strategy: ${strategy}`);

  const totalChunks = Math.ceil(file.size / chunkSize);
  const maxChunks = 280; // 7 KV × 40 keys

  if (totalChunks > maxChunks) {
    throw new Error(`File too large: ${totalChunks} chunks needed, max ${maxChunks} supported`);
  }

  console.log(`File will be split into ${totalChunks} chunks`);

  // Upload chunks in SMALL BATCHES to avoid timeout
  const BATCH_SIZE = 5; // Process only 5 chunks at a time
  const allChunkResults = [];

  for (let batchStart = 0; batchStart < totalChunks; batchStart += BATCH_SIZE) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE, totalChunks);
    const batchPromises = [];

    for (let i = batchStart; i < batchEnd; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, file.size);
      const chunk = file.slice(start, end);
      
      const kvIndex = Math.floor(i / 40); // 40 keys per KV
      const keyIndex = i % 40;
      const targetKV = kvNamespaces[kvIndex];
      const botToken = botTokens[i % botTokens.length];
      
      const chunkPromise = uploadChunkFast(
        chunk, fileId, i, kvIndex, keyIndex, botToken, channelId, targetKV, file.name
      );
      
      batchPromises.push(chunkPromise);
    }

    console.log(`Uploading batch ${Math.floor(batchStart/BATCH_SIZE) + 1}/${Math.ceil(totalChunks/BATCH_SIZE)}`);
    
    try {
      const batchResults = await Promise.all(batchPromises);
      allChunkResults.push(...batchResults);
    } catch (batchError) {
      console.error(`Batch ${Math.floor(batchStart/BATCH_SIZE) + 1} failed:`, batchError);
      throw new Error(`Upload failed at batch ${Math.floor(batchStart/BATCH_SIZE) + 1}: ${batchError.message}`);
    }

    // Small delay between batches to avoid rate limits
    if (batchEnd < totalChunks) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }

  // Store master metadata
  const masterMetadata = {
    filename: file.name,
    size: file.size,
    contentType: file.type,
    extension: extension,
    uploadedAt: Date.now(),
    type: 'chunked_upload',
    totalChunks: totalChunks,
    chunkSize: chunkSize,
    strategy: strategy,
    chunks: allChunkResults.map((result, index) => ({
      index: index,
      kvNamespace: result.kvNamespace,
      keyName: result.keyName,
      telegramFileId: result.telegramFileId,
      size: result.size
    }))
  };

  await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

  const baseUrl = new URL(request.url).origin;
  const result = {
    success: true,
    filename: file.name,
    size: file.size,
    url: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
    download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
    id: fileId,
    strategy: strategy,
    chunks: totalChunks
  };

  return new Response(JSON.stringify(result), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// Fast chunk upload with timeout protection
async function uploadChunkFast(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  try {
    const chunkFile = new File([chunk], `${originalFilename}.part${chunkIndex}`, { 
      type: 'application/octet-stream' 
    });

    // Upload to Telegram with strict timeout
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: AbortSignal.timeout(10000) // 10 second timeout per chunk
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram failed: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    const telegramFileId = telegramData.result.document.file_id;

    // Get file URL quickly
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`,
      { signal: AbortSignal.timeout(5000) }
    );
    
    const getFileData = await getFileResponse.json();
    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store in KV quickly
    const keyName = `${fileId}_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
    const chunkMetadata = {
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      size: chunk.size,
      chunkIndex: chunkIndex,
      uploadedAt: Date.now()
    };

    await kvNamespace.kv.put(keyName, JSON.stringify(chunkMetadata));

    return {
      telegramFileId: telegramFileId,
      size: chunk.size,
      directUrl: directUrl,
      kvNamespace: kvNamespace.name,
      keyName: keyName
    };

  } catch (error) {
    console.error(`Chunk ${chunkIndex} failed:`, error);
    throw new Error(`Chunk ${chunkIndex} failed: ${error.message}`);
  }
}

// Generate MSM format ID
function generateMSMId() {
  const timestamp = Date.now();
  const r1 = Math.floor(Math.random() * 1000).toString().padStart(3, '0');
  const r2 = Math.floor(Math.random() * 100).toString().padStart(2, '0');
  const r3 = Math.floor(Math.random() * 100).toString().padStart(2, '0');
  const r4 = Math.floor(Math.random() * 100).toString().padStart(2, '0');
  const c1 = String.fromCharCode(65 + Math.floor(Math.random() * 26));
  const c2 = String.fromCharCode(65 + Math.floor(Math.random() * 26));
  
  return `MSM${r1}-${r2}${c1}${r3}${c2}${r4}-${timestamp.toString(36).slice(-2)}`;
}
