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

    // Handle chunked upload
    if (isChunk) {
      return await handleChunkedUpload(formData, env, corsHeaders);
    } else {
      // Handle regular upload (existing logic)
      return await handleRegularUpload(file, env, request, corsHeaders);
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

// Handle chunked upload from client
async function handleChunkedUpload(formData, env, corsHeaders) {
  const file = formData.get('file');
  const chunkIndex = parseInt(formData.get('chunkIndex'));
  const totalChunks = parseInt(formData.get('totalChunks'));
  const originalFilename = formData.get('originalFilename');
  const originalSize = parseInt(formData.get('originalSize'));

  console.log(`Received chunk ${chunkIndex + 1}/${totalChunks} for ${originalFilename}`);

  // Bot tokens
  const botTokens = [
    env.BOT_TOKEN,
    env.BOT_TOKEN2 || env.BOT_TOKEN,
    env.BOT_TOKEN3 || env.BOT_TOKEN,
    env.BOT_TOKEN4 || env.BOT_TOKEN
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

  // Generate file ID on first chunk or retrieve from KV
  let fileId;
  if (chunkIndex === 0) {
    fileId = generateMSMId();
    
    // Store temporary metadata
    const tempMetadata = {
      originalFilename: originalFilename,
      originalSize: originalSize,
      totalChunks: totalChunks,
      uploadedChunks: 0,
      chunks: [],
      uploadStarted: Date.now()
    };
    
    await kvNamespaces[0].kv.put(`temp_${fileId}`, JSON.stringify(tempMetadata));
  } else {
    // This is a subsequent chunk - need to get fileId from somewhere
    // For simplicity, we'll generate it deterministically from filename and size
    fileId = generateDeterministicId(originalFilename, originalSize);
  }

  // Upload this chunk to Telegram
  const kvIndex = Math.floor(chunkIndex / 40);
  const targetKV = kvNamespaces[kvIndex];
  const botToken = botTokens[chunkIndex % botTokens.length];

  const chunkResult = await uploadSingleChunk(
    file, fileId, chunkIndex, kvIndex, chunkIndex % 40, 
    botToken, CHANNEL_ID, targetKV, originalFilename
  );

  // Update metadata
  const tempMetadataString = await kvNamespaces[0].kv.get(`temp_${fileId}`);
  let tempMetadata;
  
  if (tempMetadataString) {
    tempMetadata = JSON.parse(tempMetadataString);
  } else {
    // Create if doesn't exist
    tempMetadata = {
      originalFilename: originalFilename,
      originalSize: originalSize,
      totalChunks: totalChunks,
      uploadedChunks: 0,
      chunks: [],
      uploadStarted: Date.now()
    };
  }

  tempMetadata.uploadedChunks++;
  tempMetadata.chunks[chunkIndex] = chunkResult;

  await kvNamespaces[0].kv.put(`temp_${fileId}`, JSON.stringify(tempMetadata));

  // Check if this is the last chunk
  if (tempMetadata.uploadedChunks === totalChunks) {
    // All chunks uploaded - finalize
    console.log(`All chunks uploaded for ${originalFilename}`);

    const extension = originalFilename.includes('.') ? originalFilename.slice(originalFilename.lastIndexOf('.')) : '';

    // Create final metadata
    const finalMetadata = {
      filename: originalFilename,
      size: originalSize,
      contentType: file.type || 'application/octet-stream',
      extension: extension,
      uploadedAt: Date.now(),
      type: 'chunked_upload',
      totalChunks: totalChunks,
      chunkSize: Math.ceil(originalSize / totalChunks),
      strategy: 'client_chunked',
      chunks: tempMetadata.chunks.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(finalMetadata));
    
    // Clean up temp metadata
    await kvNamespaces[0].kv.delete(`temp_${fileId}`);

    const baseUrl = 'https://marya-hosting.pages.dev'; // Replace with your actual domain
    
    return new Response(JSON.stringify({
      success: true,
      filename: originalFilename,
      size: originalSize,
      url: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
      id: fileId,
      strategy: 'client_chunked',
      chunks: totalChunks
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  } else {
    // More chunks to come
    return new Response(JSON.stringify({
      success: true,
      chunkIndex: chunkIndex,
      uploadedChunks: tempMetadata.uploadedChunks,
      totalChunks: totalChunks
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Regular upload handler (your existing code)
async function handleRegularUpload(file, env, request, corsHeaders) {
  // Your existing upload logic here
  // ... (same as before)
  
  return new Response(JSON.stringify({
    success: false,
    error: 'Regular upload not implemented - use chunked upload'
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// Upload single chunk
async function uploadSingleChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  try {
    const chunkFile = new File([chunk], `${originalFilename}.chunk${chunkIndex}`, { 
      type: 'application/octet-stream' 
    });

    // Upload to Telegram
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

    console.log(`✅ Chunk ${chunkIndex} uploaded to ${kvNamespace.name}`);

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

// Generate deterministic ID for multi-chunk uploads
function generateDeterministicId(filename, size) {
  const hash = filename + size.toString();
  let result = 'MSM';
  for (let i = 0; i < hash.length; i++) {
    result += hash.charCodeAt(i).toString(36);
  }
  return result.slice(0, 20);
}
