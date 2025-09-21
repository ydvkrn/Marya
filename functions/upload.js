export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== FIXED UPLOAD SYSTEM START ===');

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
    // Bot tokens
    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2 || env.BOT_TOKEN,
      env.BOT_TOKEN3 || env.BOT_TOKEN,
      env.BOT_TOKEN4 || env.BOT_TOKEN
    ].filter(token => token);

    const CHANNEL_ID = env.CHANNEL_ID;

    if (!botTokens[0] || !CHANNEL_ID) {
      throw new Error('Missing BOT_TOKEN or CHANNEL_ID');
    }

    console.log(`Available bot tokens: ${botTokens.length}`);

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

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces configured');
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

    // Generate MSM ID
    const fileId = generateMSMId();
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // SMART CHUNKING (Fixed for Cloudflare Workers limits)
    const WORKER_LIMIT = 95 * 1024 * 1024; // 95MB safe limit for Workers
    const TELEGRAM_LIMIT = 50 * 1024 * 1024; // 50MB Telegram limit
    
    let chunkSize;
    let strategy;

    if (file.size <= WORKER_LIMIT) {
      // Small-medium files: Process in one go but split for Telegram
      if (file.size <= 1 * 1024 * 1024) {
        // Very small files: direct upload
        strategy = 'direct';
        chunkSize = file.size;
      } else {
        // Medium files: 10MB chunks
        strategy = 'medium_chunks';
        chunkSize = 10 * 1024 * 1024;
      }
    } else {
      // Large files: Small chunks to avoid Worker timeout
      strategy = 'small_chunks';
      chunkSize = 2 * 1024 * 1024; // 2MB chunks for speed
    }

    const totalChunks = Math.ceil(file.size / chunkSize);
    const maxChunks = 280; // 7 KV × 40 keys

    if (totalChunks > maxChunks) {
      throw new Error(`File too large: needs ${totalChunks} chunks, max ${maxChunks} supported`);
    }

    console.log(`Strategy: ${strategy}, chunk size: ${Math.round(chunkSize/1024/1024)}MB, total chunks: ${totalChunks}`);

    if (strategy === 'direct') {
      // Direct upload for very small files
      return await handleDirectUpload(file, fileId, extension, botTokens[0], CHANNEL_ID, kvNamespaces[0], request, corsHeaders);
    }

    // Chunked upload with timeout protection
    const uploadResults = [];
    const BATCH_SIZE = 3; // Only 3 chunks at a time to avoid timeouts

    for (let batchStart = 0; batchStart < totalChunks; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE, totalChunks);
      const batchPromises = [];

      for (let i = batchStart; i < batchEnd; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, file.size);
        const chunk = file.slice(start, end);
        
        const kvIndex = Math.floor(i / 40);
        const keyIndex = i % 40;
        const targetKV = kvNamespaces[kvIndex];
        const botToken = botTokens[i % botTokens.length];
        
        batchPromises.push(
          uploadSingleChunk(chunk, fileId, i, kvIndex, keyIndex, botToken, CHANNEL_ID, targetKV, file.name)
        );
      }

      console.log(`Processing batch ${Math.floor(batchStart/BATCH_SIZE) + 1}/${Math.ceil(totalChunks/BATCH_SIZE)}`);
      
      try {
        const batchResults = await Promise.all(batchPromises);
        uploadResults.push(...batchResults);
        
        // Progress update (small delay to avoid rate limits)
        if (batchEnd < totalChunks) {
          await new Promise(resolve => setTimeout(resolve, 200));
        }
      } catch (batchError) {
        console.error(`Batch failed:`, batchError);
        throw new Error(`Upload failed: ${batchError.message}`);
      }
    }

    // Store metadata
    const metadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'chunked_upload',
      totalChunks: totalChunks,
      chunkSize: chunkSize,
      strategy: strategy,
      chunks: uploadResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(metadata));

    const baseUrl = new URL(request.url).origin;
    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
      id: fileId,
      strategy: strategy,
      chunks: totalChunks
    };

    console.log('Upload completed successfully:', result.id);
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Upload error:', error);
    
    // Always return valid JSON
    const errorResponse = {
      success: false,
      error: error.message || 'Unknown error occurred'
    };
    
    return new Response(JSON.stringify(errorResponse), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Direct upload for very small files
async function handleDirectUpload(file, fileId, extension, botToken, channelId, kvNamespace, request, corsHeaders) {
  console.log('Direct upload for small file');

  try {
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', file);

    const response = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: AbortSignal.timeout(20000)
    });

    if (!response.ok) {
      throw new Error(`Telegram API error: ${response.status}`);
    }

    const data = await response.json();
    if (!data.ok || !data.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    const telegramFileId = data.result.document.file_id;

    // Get URL
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
      contentType: file.type,
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

// Upload single chunk with proper error handling
async function uploadSingleChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  console.log(`Uploading chunk ${chunkIndex} (${Math.round(chunk.size/1024)}KB)`);

  try {
    const chunkFile = new File([chunk], `${originalFilename}.chunk${chunkIndex}`, { 
      type: 'application/octet-stream' 
    });

    // Upload to Telegram with timeout
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: AbortSignal.timeout(30000) // 30 second timeout
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    const telegramFileId = telegramData.result.document.file_id;

    // Get file URL
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`,
      { signal: AbortSignal.timeout(10000) }
    );
    
    if (!getFileResponse.ok) {
      throw new Error(`GetFile failed: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('No file path in response');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store in KV
    const keyName = `${fileId}_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
    const chunkMetadata = {
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      size: chunk.size,
      chunkIndex: chunkIndex,
      uploadedAt: Date.now(),
      lastRefreshed: Date.now()
    };

    await kvNamespace.kv.put(keyName, JSON.stringify(chunkMetadata));

    console.log(`✅ Chunk ${chunkIndex} uploaded successfully`);

    return {
      telegramFileId: telegramFileId,
      size: chunk.size,
      directUrl: directUrl,
      kvNamespace: kvNamespace.name,
      keyName: keyName
    };

  } catch (error) {
    console.error(`❌ Chunk ${chunkIndex} failed:`, error);
    
    // Single retry with delay
    console.log(`🔄 Retrying chunk ${chunkIndex}...`);
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    try {
      return await uploadSingleChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename);
    } catch (retryError) {
      throw new Error(`Chunk ${chunkIndex} failed: ${retryError.message}`);
    }
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
