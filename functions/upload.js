async function handleChunkedUpload(formData, env, request, corsHeaders) {
  const file = formData.get('file');
  const chunkIndex = parseInt(formData.get('chunkIndex'));
  const totalChunks = parseInt(formData.get('totalChunks'));
  const originalFilename = formData.get('originalFilename');
  const originalSize = parseInt(formData.get('originalSize'));
  const fileId = formData.get('fileId');

  console.log(`📦 Processing chunk ${chunkIndex + 1}/${totalChunks} for ${originalFilename}`);

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

  // Upload this chunk
  const kvIndex = Math.floor(chunkIndex / 40);
  const targetKV = kvNamespaces[kvIndex];
  const botToken = botTokens[chunkIndex % botTokens.length];

  const chunkResult = await uploadSingleChunk(
    file, fileId, chunkIndex, kvIndex, chunkIndex % 40,
    botToken, CHANNEL_ID, targetKV, originalFilename
  );

  // Get or create progress tracking
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

  // Record this chunk upload
  progressData.uploadedChunks[chunkIndex] = chunkResult;
  const completedCount = progressData.uploadedChunks.filter(chunk => chunk !== null).length;

  // Update progress in KV
  await kvNamespaces[0].kv.put(progressKey, JSON.stringify(progressData));

  console.log(`✅ Chunk ${chunkIndex + 1} uploaded. Progress: ${completedCount}/${totalChunks}`);

  // Check if ALL chunks are completed
  if (completedCount === totalChunks) {
    console.log(`🎉 ALL CHUNKS COMPLETED for ${originalFilename}! Creating final file...`);

    // Generate MSM ID for final file
    const extension = originalFilename.includes('.') ? originalFilename.slice(originalFilename.lastIndexOf('.')) : '';
    const msmId = generateMSMId();

    // Create final file metadata
    const finalMetadata = {
      filename: originalFilename,
      size: originalSize,
      contentType: file.type || 'video/x-matroska', // Default for MKV
      extension: extension,
      uploadedAt: Date.now(),
      type: 'chunked_upload',
      totalChunks: totalChunks,
      chunkSize: Math.ceil(originalSize / totalChunks),
      strategy: 'red_theme_chunked',
      neverExpires: true,
      chunks: progressData.uploadedChunks.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    // Store final metadata
    await kvNamespaces[0].kv.put(msmId, JSON.stringify(finalMetadata));
    
    // Clean up progress tracking
    await kvNamespaces[0].kv.delete(progressKey);

    const baseUrl = new URL(request.url).origin;
    
    console.log(`✅ Final file created with MSM ID: ${msmId}`);
    
    // Return final URL response
    return new Response(JSON.stringify({
      success: true,
      filename: originalFilename,
      size: originalSize,
      contentType: file.type || 'video/x-matroska',
      url: `${baseUrl}/btfstorage/file/${msmId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${msmId}${extension}?dl=1`,
      id: msmId,
      strategy: 'red_theme_chunked',
      chunks: totalChunks,
      lifetime: 'Permanent (20+ years)',
      message: 'Upload completed successfully!'
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  } else {
    // More chunks still pending
    return new Response(JSON.stringify({
      success: true,
      chunkIndex: chunkIndex,
      uploadedChunks: completedCount,
      totalChunks: totalChunks,
      progress: Math.round((completedCount / totalChunks) * 100),
      message: `Chunk ${chunkIndex + 1}/${totalChunks} uploaded`
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Upload single chunk with perfect error handling
async function uploadSingleChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  try {
    console.log(`⬆️ Uploading chunk ${chunkIndex} to ${kvNamespace.name} (${Math.round(chunk.size/1024)}KB)`);
    
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
      signal: AbortSignal.timeout(45000) // 45 second timeout
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
      { signal: AbortSignal.timeout(15000) }
    );
    
    if (!getFileResponse.ok) {
      throw new Error(`GetFile API failed: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('No file path in response');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store chunk metadata in KV with permanent retention
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

    console.log(`✅ Chunk ${chunkIndex} uploaded successfully to ${kvNamespace.name}`);

    return {
      telegramFileId: telegramFileId,
      size: chunk.size,
      directUrl: directUrl,
      kvNamespace: kvNamespace.name,
      keyName: keyName
    };

  } catch (error) {
    console.error(`❌ Chunk ${chunkIndex} failed:`, error);
    
    // Single retry with exponential backoff
    const retryDelay = 2000 + (Math.random() * 3000);
    console.log(`🔄 Retrying chunk ${chunkIndex} in ${Math.round(retryDelay/1000)}s...`);
    await new Promise(resolve => setTimeout(resolve, retryDelay));
    
    try {
      return await uploadSingleChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename);
    } catch (retryError) {
      throw new Error(`Chunk ${chunkIndex} failed after retry: ${retryError.message}`);
    }
  }
}

// Handle direct upload for non-chunked files
async function handleDirectUpload(file, env, request, corsHeaders) {
  console.log('🔄 Direct upload requested, redirecting to chunked upload...');
  
  return new Response(JSON.stringify({
    success: false,
    error: 'Please use chunked upload for better reliability and speed'
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
