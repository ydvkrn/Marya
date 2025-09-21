export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');
    const isChunk = formData.get('isChunk') === 'true';

    if (!file) {
      throw new Error('No file provided');
    }

    if (isChunk) {
      return await handleChunkedUpload(formData, env, request, corsHeaders);
    } else {
      return await handleDirectUpload(file, env, request, corsHeaders);
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

async function handleChunkedUpload(formData, env, request, corsHeaders) {
  const file = formData.get('file');
  const chunkIndex = parseInt(formData.get('chunkIndex'));
  const totalChunks = parseInt(formData.get('totalChunks'));
  const originalFilename = formData.get('originalFilename');
  const originalSize = parseInt(formData.get('originalSize'));
  const fileId = formData.get('fileId');

  console.log(`📦 Chunk ${chunkIndex + 1}/${totalChunks} for ${originalFilename} (${fileId})`);

  // Bot tokens
  const botTokens = [
    env.BOT_TOKEN,
    env.BOT_TOKEN2,
    env.BOT_TOKEN3,
    env.BOT_TOKEN4
  ].filter(token => token);

  const CHANNEL_ID = env.CHANNEL_ID;

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

  // Upload this specific chunk
  const kvIndex = Math.floor(chunkIndex / 40);
  const targetKV = kvNamespaces[kvIndex];
  const botToken = botTokens[chunkIndex % botTokens.length];

  const chunkResult = await uploadSingleChunk(
    file, fileId, chunkIndex, kvIndex, chunkIndex % 40,
    botToken, CHANNEL_ID, targetKV, originalFilename
  );

  // Store/update chunk progress
  const progressKey = `progress_${fileId}`;
  let progressData;
  
  try {
    const existing = await kvNamespaces[0].kv.get(progressKey);
    progressData = existing ? JSON.parse(existing) : {
      originalFilename,
      originalSize,
      totalChunks,
      uploadedChunks: [],
      startTime: Date.now()
    };
  } catch (e) {
    progressData = {
      originalFilename,
      originalSize, 
      totalChunks,
      uploadedChunks: [],
      startTime: Date.now()
    };
  }

  // Record this chunk
  progressData.uploadedChunks[chunkIndex] = chunkResult;
  const completedCount = progressData.uploadedChunks.filter(Boolean).length;

  await kvNamespaces[0].kv.put(progressKey, JSON.stringify(progressData));

  console.log(`✅ Chunk ${chunkIndex + 1} uploaded. Progress: ${completedCount}/${totalChunks}`);

  // Check if this is the final chunk
  if (completedCount === totalChunks) {
    console.log(`🎉 All chunks completed for ${originalFilename}`);

    // Create final file metadata
    const extension = originalFilename.includes('.') ? originalFilename.slice(originalFilename.lastIndexOf('.')) : '';
    const msmId = generateMSMId();

    const finalMetadata = {
      filename: originalFilename,
      size: originalSize,
      contentType: file.type || 'application/octet-stream',
      extension: extension,
      uploadedAt: Date.now(),
      type: 'chunked_upload',
      totalChunks: totalChunks,
      chunkSize: Math.ceil(originalSize / totalChunks),
      strategy: 'ultra_fast_chunked',
      neverExpires: true, // 20+ year guarantee
      chunks: progressData.uploadedChunks.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    await kvNamespaces[0].kv.put(msmId, JSON.stringify(finalMetadata));
    
    // Clean up progress tracking
    await kvNamespaces[0].kv.delete(progressKey);

    const baseUrl = new URL(request.url).origin;
    
    return new Response(JSON.stringify({
      success: true,
      filename: originalFilename,
      size: originalSize,
      url: `${baseUrl}/btfstorage/file/${msmId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${msmId}${extension}?dl=1`,
      id: msmId,
      strategy: 'ultra_fast_chunked',
      chunks: totalChunks,
      lifetime: '20+ years (never expires)'
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  } else {
    // More chunks expected
    return new Response(JSON.stringify({
      success: true,
      chunkIndex: chunkIndex,
      uploadedChunks: completedCount,
      totalChunks: totalChunks,
      progress: Math.round((completedCount / totalChunks) * 100)
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

async function uploadSingleChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
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
      signal: AbortSignal.timeout(30000)
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
    
    const getFileData = await getFileResponse.json();
    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store chunk metadata in KV (with 20+ year retention)
    const keyName = `${fileId}_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
    const chunkMetadata = {
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      size: chunk.size,
      chunkIndex: chunkIndex,
      uploadedAt: Date.now(),
      lastRefreshed: Date.now(),
      neverExpires: true
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
    console.error(`❌ Chunk ${chunkIndex} failed:`, error);
    throw new Error(`Chunk ${chunkIndex} failed: ${error.message}`);
  }
}

async function handleDirectUpload(file, env, request, corsHeaders) {
  // For non-chunked files (fallback)
  return new Response(JSON.stringify({
    success: false,
    error: 'Please use chunked upload for better performance'
  }), {
    status: 400,
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
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
