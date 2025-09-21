export async function onRequest(context) {
  const { request, env } = context;

  console.log('🚀 UPLOAD REQUEST:', request.method, request.url);

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  };

  // Handle preflight OPTIONS request
  if (request.method === 'OPTIONS') {
    console.log('✅ OPTIONS request handled');
    return new Response(null, { 
      status: 200,
      headers: corsHeaders 
    });
  }

  // Only allow POST requests
  if (request.method !== 'POST') {
    console.log('❌ Method not allowed:', request.method);
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
    console.log('📝 Processing POST request...');

    const formData = await request.formData();
    const file = formData.get('file');
    const isChunk = formData.get('isChunk') === 'true';

    if (!file) {
      throw new Error('No file provided in request');
    }

    console.log('📦 File received:', file.name, `${Math.round(file.size/1024)}KB`);

    if (isChunk) {
      console.log('🔄 Processing as chunked upload...');
      return await handleChunkedUpload(formData, env, request, corsHeaders);
    } else {
      console.log('🔄 Processing as direct upload...');
      return await handleDirectUpload(file, env, request, corsHeaders);
    }

  } catch (error) {
    console.error('💥 Upload error:', error);
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

// Handle chunked upload
async function handleChunkedUpload(formData, env, request, corsHeaders) {
  const file = formData.get('file');
  const chunkIndex = parseInt(formData.get('chunkIndex'));
  const totalChunks = parseInt(formData.get('totalChunks'));
  const originalFilename = formData.get('originalFilename');
  const originalSize = parseInt(formData.get('originalSize'));
  const fileId = formData.get('fileId');

  console.log(`📦 Chunk ${chunkIndex + 1}/${totalChunks} for "${originalFilename}" (ID: ${fileId})`);

  // Environment validation
  const botTokens = [
    env.BOT_TOKEN,
    env.BOT_TOKEN2,
    env.BOT_TOKEN3,
    env.BOT_TOKEN4
  ].filter(token => token);

  const CHANNEL_ID = env.CHANNEL_ID;

  if (botTokens.length === 0) {
    throw new Error('No BOT_TOKEN configured in environment variables');
  }

  if (!CHANNEL_ID) {
    throw new Error('CHANNEL_ID not configured in environment variables');
  }

  console.log(`🤖 Using ${botTokens.length} bot tokens, Channel: ${CHANNEL_ID}`);

  // KV namespaces validation
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
    throw new Error('No KV namespaces configured. Please bind FILES_KV in environment.');
  }

  console.log(`💾 Using ${kvNamespaces.length} KV namespaces`);

  // Upload this specific chunk
  const kvIndex = Math.floor(chunkIndex / 40);
  const targetKV = kvNamespaces[kvIndex];
  const botToken = botTokens[chunkIndex % botTokens.length];

  if (!targetKV) {
    throw new Error(`KV namespace ${kvIndex} not available for chunk ${chunkIndex}`);
  }

  console.log(`⬆️ Uploading chunk ${chunkIndex} to ${targetKV.name} using bot ${botToken.slice(-10)}`);

  const chunkResult = await uploadSingleChunk(
    file, fileId, chunkIndex, kvIndex, chunkIndex % 40,
    botToken, CHANNEL_ID, targetKV, originalFilename
  );

  // Progress tracking
  const progressKey = `progress_${fileId}`;
  let progressData;
  
  try {
    const existing = await kvNamespaces[0].kv.get(progressKey);
    progressData = existing ? JSON.parse(existing) : {
      originalFilename,
      originalSize,
      totalChunks,
      uploadedChunks: new Array(totalChunks).fill(null),
      startTime: Date.now()
    };
  } catch (e) {
    console.log('📝 Creating new progress tracker...');
    progressData = {
      originalFilename,
      originalSize, 
      totalChunks,
      uploadedChunks: new Array(totalChunks).fill(null),
      startTime: Date.now()
    };
  }

  // Record this chunk
  progressData.uploadedChunks[chunkIndex] = chunkResult;
  const completedCount = progressData.uploadedChunks.filter(chunk => chunk !== null).length;

  await kvNamespaces[0].kv.put(progressKey, JSON.stringify(progressData));

  console.log(`✅ Chunk ${chunkIndex + 1} completed. Progress: ${completedCount}/${totalChunks}`);

  // Check if ALL chunks are done
  if (completedCount === totalChunks) {
    console.log(`🎉 ALL CHUNKS COMPLETED! Creating final file...`);

    // Generate MSM ID
    const extension = originalFilename.includes('.') ? originalFilename.slice(originalFilename.lastIndexOf('.')) : '';
    const msmId = generateMSMId();

    // Create final metadata
    const finalMetadata = {
      filename: originalFilename,
      size: originalSize,
      contentType: file.type || getMimeType(extension),
      extension: extension,
      uploadedAt: Date.now(),
      type: 'chunked_upload',
      totalChunks: totalChunks,
      chunkSize: Math.ceil(originalSize / totalChunks),
      strategy: 'red_theme_fixed',
      neverExpires: true,
      chunks: progressData.uploadedChunks.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    // Store final file
    await kvNamespaces[0].kv.put(msmId, JSON.stringify(finalMetadata));
    
    // Cleanup progress
    await kvNamespaces[0].kv.delete(progressKey);

    const baseUrl = new URL(request.url).origin;
    
    console.log(`🎯 FINAL URL CREATED: ${msmId}${extension}`);
    
    // Return SUCCESS with final URL
    return new Response(JSON.stringify({
      success: true,
      filename: originalFilename,
      size: originalSize,
      contentType: finalMetadata.contentType,
      url: `${baseUrl}/btfstorage/file/${msmId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${msmId}${extension}?dl=1`,
      id: msmId,
      strategy: 'red_theme_fixed',
      chunks: totalChunks,
      lifetime: 'Permanent (Never Expires)',
      message: '🎉 Upload completed successfully!'
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  } else {
    // Still more chunks to go
    return new Response(JSON.stringify({
      success: true,
      chunkIndex: chunkIndex,
      uploadedChunks: completedCount,
      totalChunks: totalChunks,
      progress: Math.round((completedCount / totalChunks) * 100),
      message: `Chunk ${chunkIndex + 1}/${totalChunks} uploaded successfully`
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Upload single chunk to Telegram + KV
async function uploadSingleChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  try {
    console.log(`⬆️ Uploading chunk ${chunkIndex} (${Math.round(chunk.size/1024)}KB) to ${kvNamespace.name}...`);
    
    // Create chunk file
    const chunkFile = new File([chunk], `${originalFilename}.chunk${chunkIndex}`, { 
      type: 'application/octet-stream' 
    });

    // Upload to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);

    console.log(`📤 Sending to Telegram API...`);
    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: AbortSignal.timeout(60000) // 60 second timeout
    });

    if (!telegramResponse.ok) {
      const errorText = await telegramResponse.text();
      throw new Error(`Telegram API error ${telegramResponse.status}: ${errorText}`);
    }

    const telegramData = await telegramResponse.json();
    
    if (!telegramData.ok) {
      throw new Error(`Telegram API failed: ${telegramData.description || 'Unknown error'}`);
    }

    if (!telegramData.result?.document?.file_id) {
      throw new Error('No file_id returned from Telegram');
    }

    const telegramFileId = telegramData.result.document.file_id;
    console.log(`📤 Telegram upload successful: ${telegramFileId}`);

    // Get file URL
    console.log(`🔗 Getting file URL...`);
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`,
      { signal: AbortSignal.timeout(30000) }
    );
    
    if (!getFileResponse.ok) {
      throw new Error(`GetFile API error ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();
    
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('No file_path in GetFile response');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
    console.log(`🔗 Direct URL obtained`);

    // Store in KV
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

    console.log(`💾 Storing in KV: ${keyName}`);
    await kvNamespace.kv.put(keyName, JSON.stringify(chunkMetadata));

    console.log(`✅ Chunk ${chunkIndex} uploaded successfully to ${kvNamespace.name}`);

    return {
      telegramFileId: telegramFileId,
      size: chunk.size,
      directUrl: directUrl,
      kvNamespace: kvNamespace.name,
      keyName: keyName
    };

  } catch (error) {
    console.error(`💥 Chunk ${chunkIndex} upload failed:`, error);
    
    // Single retry with delay
    console.log(`🔄 Retrying chunk ${chunkIndex} in 3 seconds...`);
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    try {
      console.log(`🔄 Retry attempt for chunk ${chunkIndex}...`);
      return await uploadSingleChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename);
    } catch (retryError) {
      throw new Error(`Chunk ${chunkIndex} failed after retry: ${retryError.message}`);
    }
  }
}

// Handle direct upload (fallback)
async function handleDirectUpload(file, env, request, corsHeaders) {
  console.log('⚠️ Direct upload requested - redirecting to chunked upload');
  
  return new Response(JSON.stringify({
    success: false,
    error: 'Please use chunked upload for better performance. Refresh the page and try again.'
  }), {
    status: 400,
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// Get MIME type from extension
function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  const mimeMap = {
    'mkv': 'video/x-matroska',
    'mp4': 'video/mp4',
    'avi': 'video/x-msvideo',
    'mov': 'video/quicktime',
    'webm': 'video/webm',
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'png': 'image/png',
    'gif': 'image/gif',
    'mp3': 'audio/mpeg',
    'wav': 'audio/wav',
    'pdf': 'application/pdf',
    'zip': 'application/zip'
  };
  return mimeMap[ext] || 'application/octet-stream';
}

// Generate MSM ID
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
