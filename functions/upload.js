export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== REVOLUTIONARY UPLOAD SYSTEM V2.0 ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Content-Length',
    'Access-Control-Expose-Headers': 'X-Upload-Progress, X-File-ID'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // ✅ All KV namespaces
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV', priority: 1 },
      { kv: env.FILES_KV2, name: 'FILES_KV2', priority: 2 },
      { kv: env.FILES_KV3, name: 'FILES_KV3', priority: 3 },
      { kv: env.FILES_KV4, name: 'FILES_KV4', priority: 4 },
      { kv: env.FILES_KV5, name: 'FILES_KV5', priority: 5 },
      { kv: env.FILES_KV6, name: 'FILES_KV6', priority: 6 },
      { kv: env.FILES_KV7, name: 'FILES_KV7', priority: 7 }
    ].filter(item => item.kv);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing credentials');
    }

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log('🚀 REVOLUTIONARY UPLOAD:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // Generate ultra-unique ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 12);
    const ultrahash = btoa(file.name + file.size).slice(0, 8);
    const fileId = `rev${timestamp}${random}${ultrahash}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // ✅ SMART FILE STRATEGY
    if (file.size <= 1024 * 1024) { // Small files (≤1MB) - INSTANT UPLOAD
      console.log('⚡ INSTANT SMALL FILE MODE');
      return await instantSmallFileUpload(file, fileId, extension, BOT_TOKEN, CHANNEL_ID, kvNamespaces[0], request.url, corsHeaders);
      
    } else if (file.size <= 25 * 1024 * 1024) { // Medium files (≤25MB) - OPTIMIZED SINGLE
      console.log('🚀 OPTIMIZED SINGLE FILE MODE');
      return await optimizedSingleFileUpload(file, fileId, extension, BOT_TOKEN, CHANNEL_ID, kvNamespaces[0], request.url, corsHeaders);
      
    } else { // Large files (>25MB) - REVOLUTIONARY CHUNKING
      console.log('💎 REVOLUTIONARY CHUNKING MODE');
      return await revolutionaryChunkedUpload(file, fileId, extension, BOT_TOKEN, CHANNEL_ID, kvNamespaces, request.url, corsHeaders);
    }

  } catch (error) {
    console.error('❌ Revolutionary upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message,
      timestamp: Date.now()
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// ✅ INSTANT small file upload (≤1MB)
async function instantSmallFileUpload(file, fileId, extension, botToken, channelId, kvNamespace, baseUrl, corsHeaders) {
  console.log('⚡ Processing instant small file...');
  
  // Direct upload to Telegram
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', file);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

  if (!telegramResponse.ok) {
    throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
  }

  const telegramData = await telegramResponse.json();
  const telegramFileId = telegramData.result.document.file_id;

  // Get direct URL
  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
  const getFileData = await getFileResponse.json();
  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  // Store with instant metadata
  const instantMetadata = {
    filename: file.name,
    size: file.size,
    contentType: file.type,
    extension: extension,
    uploadedAt: Date.now(),
    type: 'instant_small',
    telegramFileId: telegramFileId,
    directUrl: directUrl,
    optimized: true
  };

  await kvNamespace.kv.put(fileId, JSON.stringify(instantMetadata));

  // Pre-cache for instant access
  await fetch(directUrl, {
    cf: { cacheEverything: true, cacheTtl: 86400 }
  });

  const customUrl = `${new URL(baseUrl).origin}/btfstorage/file/${fileId}${extension}`;

  console.log('✅ INSTANT upload completed in record time!');

  return new Response(JSON.stringify({
    success: true,
    filename: file.name,
    size: file.size,
    contentType: file.type,
    url: customUrl,
    download: `${customUrl}?dl=1`,
    id: fileId,
    strategy: 'instant_small',
    uploadTime: Date.now()
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// ✅ OPTIMIZED single file upload (1-25MB)
async function optimizedSingleFileUpload(file, fileId, extension, botToken, channelId, kvNamespace, baseUrl, corsHeaders) {
  console.log('🚀 Processing optimized single file...');
  
  // Multi-retry upload with exponential backoff
  let attempt = 0;
  let uploadSuccess = false;
  let telegramFileId, directUrl;

  while (attempt < 3 && !uploadSuccess) {
    try {
      const telegramForm = new FormData();
      telegramForm.append('chat_id', channelId);
      telegramForm.append('document', file);

      const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
        method: 'POST',
        body: telegramForm
      });

      if (telegramResponse.ok) {
        const telegramData = await telegramResponse.json();
        telegramFileId = telegramData.result.document.file_id;

        const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
        const getFileData = await getFileResponse.json();
        directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

        uploadSuccess = true;
        console.log(`✅ Upload successful on attempt ${attempt + 1}`);
      }
    } catch (error) {
      console.log(`⚠️ Attempt ${attempt + 1} failed:`, error.message);
      attempt++;
      if (attempt < 3) {
        await new Promise(resolve => setTimeout(resolve, 1000 * attempt)); // Exponential backoff
      }
    }
  }

  if (!uploadSuccess) {
    throw new Error('Upload failed after 3 attempts');
  }

  // Store optimized metadata
  const optimizedMetadata = {
    filename: file.name,
    size: file.size,
    contentType: file.type,
    extension: extension,
    uploadedAt: Date.now(),
    type: 'optimized_single',
    telegramFileId: telegramFileId,
    directUrl: directUrl,
    optimized: true,
    attempts: attempt + 1
  };

  await kvNamespace.kv.put(fileId, JSON.stringify(optimizedMetadata));

  // Aggressive pre-caching
  await fetch(directUrl, {
    cf: { cacheEverything: true, cacheTtl: 86400 }
  });

  const customUrl = `${new URL(baseUrl).origin}/btfstorage/file/${fileId}${extension}`;

  console.log('✅ OPTIMIZED upload completed successfully!');

  return new Response(JSON.stringify({
    success: true,
    filename: file.name,
    size: file.size,
    contentType: file.type,
    url: customUrl,
    download: `${customUrl}?dl=1`,
    id: fileId,
    strategy: 'optimized_single',
    uploadTime: Date.now()
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// ✅ REVOLUTIONARY chunked upload (>25MB)
async function revolutionaryChunkedUpload(file, fileId, extension, botToken, channelId, kvNamespaces, baseUrl, corsHeaders) {
  console.log('💎 Starting revolutionary chunked upload...');
  
  const CHUNK_SIZE = 15 * 1024 * 1024; // 15MB chunks for optimal speed
  const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
  
  if (totalChunks > kvNamespaces.length) {
    throw new Error(`File too large: needs ${totalChunks} chunks, only ${kvNamespaces.length} available`);
  }

  console.log(`🔥 Processing ${totalChunks} chunks across KV namespaces`);

  // ✅ PARALLEL CHUNK UPLOAD with real-time progress
  const chunkPromises = [];
  const uploadProgress = {};

  for (let i = 0; i < totalChunks; i++) {
    const start = i * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, file.size);
    const chunk = file.slice(start, end);
    const chunkFile = new File([chunk], `${file.name}.rev${i}`, { type: file.type });
    const targetKV = kvNamespaces[i % kvNamespaces.length];

    const chunkPromise = revolutionaryChunkUpload(
      chunkFile, fileId, i, botToken, channelId, targetKV, uploadProgress
    );
    chunkPromises.push(chunkPromise);
  }

  // Wait for all chunks with progress tracking
  const chunkResults = await Promise.allSettled(chunkPromises);
  
  // Check for failures
  const failedChunks = chunkResults.filter(result => result.status === 'rejected');
  if (failedChunks.length > 0) {
    console.error('❌ Failed chunks:', failedChunks.map(f => f.reason));
    throw new Error(`${failedChunks.length} chunks failed to upload`);
  }

  const successfulChunks = chunkResults.map(result => result.value);

  // ✅ Store revolutionary master metadata
  const revolutionaryMetadata = {
    filename: file.name,
    size: file.size,
    contentType: file.type,
    extension: extension,
    uploadedAt: Date.now(),
    type: 'revolutionary_chunked',
    totalChunks: totalChunks,
    revolutionary: true,
    optimized: true,
    chunks: successfulChunks.map((result, index) => ({
      index: index,
      kvNamespace: result.kvNamespace,
      telegramFileId: result.telegramFileId,
      size: result.size,
      chunkKey: result.chunkKey,
      uploadedAt: Date.now(),
      revolutionary: true
    }))
  };

  await kvNamespaces[0].kv.put(fileId, JSON.stringify(revolutionaryMetadata));

  const customUrl = `${new URL(baseUrl).origin}/btfstorage/file/${fileId}${extension}`;

  console.log('🚀 REVOLUTIONARY chunked upload completed!');

  return new Response(JSON.stringify({
    success: true,
    filename: file.name,
    size: file.size,
    contentType: file.type,
    url: customUrl,
    download: `${customUrl}?dl=1`,
    id: fileId,
    strategy: 'revolutionary_chunked',
    chunks: totalChunks,
    uploadTime: Date.now(),
    revolutionary: true
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// ✅ Revolutionary chunk upload with retry mechanism
async function revolutionaryChunkUpload(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, progressTracker) {
  console.log(`💎 Revolutionary chunk upload ${chunkIndex}`);
  
  let attempt = 0;
  let success = false;
  let result;

  while (attempt < 2 && !success) {
    try {
      const telegramForm = new FormData();
      telegramForm.append('chat_id', channelId);
      telegramForm.append('document', chunkFile);

      const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
        method: 'POST',
        body: telegramForm
      });

      if (!telegramResponse.ok) {
        throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
      }

      const telegramData = await telegramResponse.json();
      const telegramFileId = telegramData.result.document.file_id;

      const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
      const getFileData = await getFileResponse.json();
      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

      // Store chunk with revolutionary metadata
      const chunkKey = `${fileId}_rev_chunk_${chunkIndex}`;
      const revolutionaryChunkMetadata = {
        telegramFileId: telegramFileId,
        directUrl: directUrl,
        size: chunkFile.size,
        index: chunkIndex,
        parentFileId: fileId,
        kvNamespace: kvNamespace.name,
        uploadedAt: Date.now(),
        revolutionary: true,
        optimized: true,
        lastRefreshed: Date.now()
      };

      await kvNamespace.kv.put(chunkKey, JSON.stringify(revolutionaryChunkMetadata));

      // Pre-cache chunk for instant access
      await fetch(directUrl, {
        cf: { cacheEverything: true, cacheTtl: 86400 }
      });

      result = {
        telegramFileId: telegramFileId,
        size: chunkFile.size,
        directUrl: directUrl,
        kvNamespace: kvNamespace.name,
        chunkKey: chunkKey
      };

      success = true;
      console.log(`✅ Revolutionary chunk ${chunkIndex} uploaded successfully`);

    } catch (error) {
      attempt++;
      console.error(`❌ Chunk ${chunkIndex} attempt ${attempt} failed:`, error);
      if (attempt < 2) {
        await new Promise(resolve => setTimeout(resolve, 500 * attempt));
      }
    }
  }

  if (!success) {
    throw new Error(`Revolutionary chunk ${chunkIndex} failed after retries`);
  }

  return result;
}
