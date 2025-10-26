export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== UPLOAD FROM TELEGRAM URL START ===');

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
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // ✅ All KV namespaces array
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    console.log(`Available KV namespaces: ${kvNamespaces.length}`);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing bot credentials');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces available');
    }

    // ✅ Parse request body (expecting JSON with Telegram file URL)
    const body = await request.json();
    const { telegramUrl, filename } = body;

    if (!telegramUrl) {
      throw new Error('No Telegram URL provided');
    }

    console.log('Processing Telegram URL:', {
      url: telegramUrl,
      filename: filename || 'unknown'
    });

    // ✅ Extract file_id from Telegram URL if it's a getFile URL
    let fileId, directUrl, fileSize, originalFilename;

    // Check if it's a direct Telegram file URL
    if (telegramUrl.includes('api.telegram.org/file/bot')) {
      // Direct file URL
      directUrl = telegramUrl;
      
      // Fetch file metadata
      const headResponse = await fetch(directUrl, { method: 'HEAD' });
      fileSize = parseInt(headResponse.headers.get('content-length') || '0');
      originalFilename = filename || extractFilenameFromUrl(telegramUrl);
      
    } else if (telegramUrl.includes('t.me/') || telegramUrl.includes('telegram.me/')) {
      // Public Telegram link - need to extract file_id
      throw new Error('Public Telegram links not supported. Please use bot getFile URL or file_id');
      
    } else {
      // Assume it's a file_id
      fileId = telegramUrl;
      
      // Get file info from Telegram
      const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(fileId)}`);
      
      if (!getFileResponse.ok) {
        throw new Error('Failed to get file info from Telegram');
      }
      
      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) {
        throw new Error('Invalid file_id or file not found');
      }
      
      directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
      fileSize = getFileData.result.file_size || 0;
      originalFilename = filename || getFileData.result.file_path.split('/').pop();
    }

    console.log('File info:', {
      url: directUrl,
      size: fileSize,
      filename: originalFilename
    });

    // ✅ Size validation - 7 KV namespaces × 25MB = 175MB max
    const MAX_FILE_SIZE = 175 * 1024 * 1024;
    if (fileSize > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(fileSize / 1024 / 1024)}MB (max 175MB)`);
    }

    // Generate unique file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const newFileId = `url${timestamp}${random}`;
    const extension = originalFilename.includes('.') ? originalFilename.slice(originalFilename.lastIndexOf('.')) : '';

    // ✅ Download file from Telegram
    console.log('Downloading file from Telegram...');
    const fileResponse = await fetch(directUrl);
    
    if (!fileResponse.ok) {
      throw new Error(`Failed to download file: ${fileResponse.status}`);
    }

    const fileBlob = await fileResponse.blob();
    const fileBuffer = await fileBlob.arrayBuffer();
    
    console.log(`File downloaded: ${fileBuffer.byteLength} bytes`);

    // ✅ Chunking strategy
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB per chunk
    const totalChunks = Math.ceil(fileBuffer.byteLength / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File requires ${totalChunks} chunks, but only ${kvNamespaces.length} KV namespaces available`);
    }

    console.log(`Using ${totalChunks} chunks across KV namespaces`);

    // ✅ Re-upload chunks to Telegram with better organization
    const chunkPromises = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, fileBuffer.byteLength);
      const chunkBuffer = fileBuffer.slice(start, end);
      
      const chunkBlob = new Blob([chunkBuffer]);
      const chunkFilename = `${originalFilename}.part${i}`;
      const targetKV = kvNamespaces[i % kvNamespaces.length]; // Round-robin distribution

      const chunkPromise = uploadChunkFromBuffer(
        chunkBlob, 
        chunkFilename, 
        newFileId, 
        i, 
        BOT_TOKEN, 
        CHANNEL_ID, 
        targetKV
      );
      
      chunkPromises.push(chunkPromise);
    }

    const chunkResults = await Promise.all(chunkPromises);
    console.log('All chunks re-uploaded to Telegram successfully');

    // ✅ Store master metadata in primary KV
    const masterMetadata = {
      filename: originalFilename,
      size: fileBuffer.byteLength,
      contentType: fileBlob.type || 'application/octet-stream',
      extension: extension,
      uploadedAt: Date.now(),
      type: 'url_import_multi_kv',
      sourceUrl: telegramUrl,
      totalChunks: totalChunks,
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        telegramFileId: result.telegramFileId,
        size: result.size,
        chunkKey: result.chunkKey
      }))
    };

    await kvNamespaces[0].kv.put(newFileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${newFileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${newFileId}${extension}?dl=1`;

    const result = {
      success: true,
      filename: originalFilename,
      size: fileBuffer.byteLength,
      contentType: fileBlob.type,
      url: customUrl,
      download: downloadUrl,
      id: newFileId,
      strategy: 'url_import_multi_kv',
      chunks: totalChunks,
      kvDistribution: chunkResults.map(r => r.kvNamespace),
      message: 'File imported from Telegram URL and stored across multiple KV namespaces'
    };

    console.log('URL import completed:', result);
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('URL upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// ✅ Upload chunk from buffer to Telegram and store in KV
async function uploadChunkFromBuffer(chunkBlob, chunkFilename, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  console.log(`Uploading chunk ${chunkIndex} (${chunkBlob.size} bytes) to ${kvNamespace.name}...`);

  // Create File object from Blob
  const chunkFile = new File([chunkBlob], chunkFilename, { type: 'application/octet-stream' });

  // Upload to Telegram
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

  if (!telegramResponse.ok) {
    const errorText = await telegramResponse.text();
    throw new Error(`Telegram upload failed for chunk ${chunkIndex}: ${telegramResponse.status} - ${errorText}`);
  }

  const telegramData = await telegramResponse.json();
  if (!telegramData.ok || !telegramData.result?.document?.file_id) {
    throw new Error(`Invalid Telegram response for chunk ${chunkIndex}`);
  }

  const telegramFileId = telegramData.result.document.file_id;

  // Get file URL
  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);

  if (!getFileResponse.ok) {
    throw new Error(`GetFile API failed for chunk ${chunkIndex}`);
  }

  const getFileData = await getFileResponse.json();
  if (!getFileData.ok || !getFileData.result?.file_path) {
    throw new Error(`No file_path for chunk ${chunkIndex}`);
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  // ✅ Store chunk metadata with auto-refresh support
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;
  const chunkMetadata = {
    telegramFileId: telegramFileId,
    directUrl: directUrl,
    size: chunkBlob.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    uploadedAt: Date.now(),
    lastRefreshed: Date.now(),
    refreshCount: 0
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

  console.log(`✅ Chunk ${chunkIndex} stored in ${kvNamespace.name}`);

  return {
    telegramFileId: telegramFileId,
    size: chunkBlob.size,
    directUrl: directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey: chunkKey
  };
}

// ✅ Helper function to extract filename from URL
function extractFilenameFromUrl(url) {
  try {
    const urlPath = new URL(url).pathname;
    const parts = urlPath.split('/');
    return parts[parts.length - 1] || 'downloaded_file';
  } catch {
    return 'downloaded_file';
  }
}