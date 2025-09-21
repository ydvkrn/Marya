export async function onRequest(context) {
  const { request, env } = context;

  console.log('🚀 ULTIMATE UPLOAD REQUEST:', request.method, request.url);

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 200, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: `Method ${request.method} not allowed. Use POST.`
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', 'Allow': 'POST, OPTIONS', ...corsHeaders }
    });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');
    const isChunk = formData.get('isChunk') === 'true';

    if (!file) {
      throw new Error('No file provided in request');
    }

    console.log('📦 File received:', file.name, `${Math.round(file.size/1024/1024)}MB`);

    if (isChunk) {
      return await handleChunkedUpload(formData, env, request, corsHeaders);
    } else {
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

// Handle chunked upload with smart retry
async function handleChunkedUpload(formData, env, request, corsHeaders) {
  const file = formData.get('file');
  const chunkIndex = parseInt(formData.get('chunkIndex'));
  const totalChunks = parseInt(formData.get('totalChunks'));
  const originalFilename = formData.get('originalFilename');
  const originalSize = parseInt(formData.get('originalSize'));
  const fileId = formData.get('fileId');

  console.log(`📦 Processing chunk ${chunkIndex + 1}/${totalChunks} for "${originalFilename}" (${Math.round(file.size/1024/1024)}MB)`);

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
    throw new Error('No KV namespaces configured. Please bind FILES_KV in environment.');
  }

  // Smart bot token rotation to avoid rate limits
  const kvIndex = Math.floor(chunkIndex / 40);
  const targetKV = kvNamespaces[kvIndex];
  const botToken = botTokens[chunkIndex % botTokens.length];

  if (!targetKV) {
    throw new Error(`KV namespace ${kvIndex} not available for chunk ${chunkIndex}`);
  }

  console.log(`⬆️ Uploading chunk ${chunkIndex} to ${targetKV.name} using bot ending ...${botToken.slice(-4)}`);

  // Upload chunk with enhanced retry logic
  const chunkResult = await uploadSingleChunkWithRetry(
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
    progressData = {
      originalFilename,
      originalSize, 
      totalChunks,
      uploadedChunks: new Array(totalChunks).fill(null),
      startTime: Date.now()
    };
  }

  progressData.uploadedChunks[chunkIndex] = chunkResult;
  const completedCount = progressData.uploadedChunks.filter(chunk => chunk !== null).length;

  await kvNamespaces[0].kv.put(progressKey, JSON.stringify(progressData));

  console.log(`✅ Chunk ${chunkIndex + 1} completed. Progress: ${completedCount}/${totalChunks}`);

  // Check if ALL chunks are done
  if (completedCount === totalChunks) {
    console.log(`🎉 ALL CHUNKS COMPLETED! Creating final streamable file...`);

    const extension = originalFilename.includes('.') ? originalFilename.slice(originalFilename.lastIndexOf('.')) : '';
    const msmId = generateMSMId();

    // Enhanced metadata for streaming
    const finalMetadata = {
      filename: originalFilename,
      size: originalSize,
      contentType: getEnhancedMimeType(extension, originalFilename),
      extension: extension,
      uploadedAt: Date.now(),
      type: 'chunked_upload',
      totalChunks: totalChunks,
      chunkSize: Math.ceil(originalSize / totalChunks),
      strategy: 'ultimate_streaming',
      neverExpires: true,
      streamable: true,
      chunks: progressData.uploadedChunks.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    await kvNamespaces[0].kv.put(msmId, JSON.stringify(finalMetadata));
    await kvNamespaces[0].kv.delete(progressKey);

    const baseUrl = new URL(request.url).origin;
    
    console.log(`🎯 FINAL STREAMABLE URL CREATED: ${msmId}${extension}`);
    
    return new Response(JSON.stringify({
      success: true,
      filename: originalFilename,
      size: originalSize,
      contentType: finalMetadata.contentType,
      url: `${baseUrl}/btfstorage/file/${msmId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${msmId}${extension}?dl=1`,
      stream: `${baseUrl}/btfstorage/file/${msmId}${extension}?stream=1`,
      id: msmId,
      strategy: 'ultimate_streaming',
      chunks: totalChunks,
      streamable: true,
      lifetime: 'Permanent (Never Expires)',
      message: '🎉 Upload completed! File ready for streaming!'
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  } else {
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

// Enhanced chunk upload with retry and rate limit handling
async function uploadSingleChunkWithRetry(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  const maxRetries = 3;
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      console.log(`⬆️ Attempt ${attempt + 1}/${maxRetries} for chunk ${chunkIndex} (${Math.round(chunk.size/1024)}KB)`);
      
      const chunkFile = new File([chunk], `${originalFilename}.chunk${chunkIndex}`, { 
        type: 'application/octet-stream' 
      });

      // Add delay to avoid rate limits
      if (attempt > 0) {
        const delay = Math.min(2000 * Math.pow(2, attempt), 10000);
        console.log(`⏰ Rate limit delay: ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }

      // Upload to Telegram with proper error handling
      const telegramForm = new FormData();
      telegramForm.append('chat_id', channelId);
      telegramForm.append('document', chunkFile);

      console.log(`📤 Sending to Telegram API...`);
      const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
        method: 'POST',
        body: telegramForm,
        signal: AbortSignal.timeout(90000) // 90 second timeout
      });

      if (!telegramResponse.ok) {
        const errorText = await telegramResponse.text();
        
        // Handle specific Telegram errors
        if (telegramResponse.status === 429) {
          const retryAfter = telegramResponse.headers.get('Retry-After');
          const delay = retryAfter ? parseInt(retryAfter) * 1000 : 30000;
          console.log(`🚦 Rate limited, retrying after ${delay}ms`);
          await new Promise(resolve => setTimeout(resolve, delay));
          attempt++;
          continue;
        }
        
        if (telegramResponse.status === 400) {
          // Bad request - might be file too large for single upload
          if (chunk.size > 50 * 1024 * 1024) {
            throw new Error(`Chunk ${chunkIndex} too large (${Math.round(chunk.size/1024/1024)}MB). Try smaller chunks.`);
          }
        }
        
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

      // Get file URL quickly
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

      // Store in KV with streaming metadata
      const keyName = `${fileId}_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
      const chunkMetadata = {
        telegramFileId: telegramFileId,
        directUrl: directUrl,
        size: chunk.size,
        chunkIndex: chunkIndex,
        uploadedAt: Date.now(),
        lastRefreshed: Date.now(),
        neverExpires: true,
        streamable: true
      };

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
      console.error(`💥 Chunk ${chunkIndex} attempt ${attempt + 1} failed:`, error);
      
      attempt++;
      if (attempt >= maxRetries) {
        throw new Error(`Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${error.message}`);
      }
      
      // Exponential backoff
      const backoffDelay = Math.min(1000 * Math.pow(2, attempt), 15000);
      console.log(`🔄 Retrying chunk ${chunkIndex} in ${backoffDelay}ms...`);
      await new Promise(resolve => setTimeout(resolve, backoffDelay));
    }
  }
}

// Enhanced MIME type detection for streaming
function getEnhancedMimeType(extension, filename) {
  const ext = extension.toLowerCase().replace('.', '');
  
  const streamingMimeMap = {
    'mkv': 'video/x-matroska',
    'mp4': 'video/mp4',
    'avi': 'video/x-msvideo',
    'mov': 'video/quicktime',
    'webm': 'video/webm',
    'flv': 'video/x-flv',
    '3gp': 'video/3gpp',
    'wmv': 'video/x-ms-wmv',
    'mpg': 'video/mpeg',
    'mpeg': 'video/mpeg',
    'm4v': 'video/x-m4v',
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'png': 'image/png',
    'gif': 'image/gif',
    'webp': 'image/webp',
    'bmp': 'image/bmp',
    'tiff': 'image/tiff',
    'mp3': 'audio/mpeg',
    'wav': 'audio/wav',
    'flac': 'audio/flac',
    'aac': 'audio/aac',
    'm4a': 'audio/mp4',
    'ogg': 'audio/ogg',
    'pdf': 'application/pdf',
    'zip': 'application/zip',
    'rar': 'application/vnd.rar',
    '7z': 'application/x-7z-compressed'
  };
  
  return streamingMimeMap[ext] || 'application/octet-stream';
}

// Handle direct upload (redirect to chunked)
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
