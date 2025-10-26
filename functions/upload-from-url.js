export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT URL UPLOAD START ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Accept'
  };

  if (request.method === 'OPTIONS') {
    console.log('Handling OPTIONS request');
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    console.error(`Invalid method: ${request.method}`);
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

    // ✅ Parse JSON body with robust error handling
    let body;
    try {
      const rawBody = await request.text();
      console.log('Raw request body:', rawBody);
      body = JSON.parse(rawBody);
    } catch (parseError) {
      console.error('JSON parsing error:', parseError.message);
      throw new Error('Invalid JSON body');
    }

    const fileUrl = body.fileUrl || body.url; // Accept both 'fileUrl' and 'url'
    const customFilename = body.filename || null;

    if (!fileUrl) {
      console.error('No URL provided in body:', body);
      throw new Error('No URL provided');
    }

    console.log('URL received:', fileUrl);

    // ✅ Fetch file from URL
    console.log('Fetching file from URL...');
    const fileResponse = await fetch(fileUrl, {
      headers: { 'User-Agent': 'MaryaVault/1.0' }
    });

    if (!fileResponse.ok) {
      console.error(`Fetch failed: ${fileResponse.status} ${fileResponse.statusText}`);
      throw new Error(`Failed to fetch file: ${fileResponse.status} ${fileResponse.statusText}`);
    }

    // Get file info
    const contentLength = parseInt(fileResponse.headers.get('content-length') || '0');
    const contentType = fileResponse.headers.get('content-type') || 'application/octet-stream';

    // ✅ Check for empty file
    if (contentLength === 0) {
      console.error('Fetched file is empty');
      throw new Error('Fetched file is empty');
    }

    // Extract filename from URL or Content-Disposition header
    let filename = customFilename;
    if (!filename) {
      const disposition = fileResponse.headers.get('content-disposition');
      if (disposition && disposition.includes('filename=')) {
        filename = disposition.split('filename=')[1].replace(/['"]/g, '');
      } else {
        const urlPath = new URL(fileUrl).pathname;
        filename = urlPath.split('/').pop() || `file_${Date.now()}`;
      }
    }

    console.log('File info:', {
      filename: filename,
      size: contentLength,
      type: contentType
    });

    // ✅ Size validation - 7 KV namespaces × 25MB = 175MB max
    const MAX_FILE_SIZE = 175 * 1024 * 1024;
    if (contentLength > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(contentLength / 1024 / 1024)}MB (max 175MB)`);
    }

    // Convert response to blob/file
    const fileBlob = await fileResponse.blob();
    const file = new File([fileBlob], filename, { type: contentType });

    // ✅ Verify blob size
    if (file.size === 0) {
      console.error('Blob is empty after fetch');
      throw new Error('Downloaded file is empty');
    }

    // Generate unique file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `url${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    // ✅ Chunking strategy
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB per chunk
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File requires ${totalChunks} chunks, but only ${kvNamespaces.length} KV namespaces available`);
    }

    console.log(`Using ${totalChunks} chunks across KV namespaces`);

    // ✅ Upload chunks to different KV namespaces
    const chunkPromises = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      const chunkFile = new File([chunk], `${filename}.part${i}`, { type: contentType });
      const targetKV = kvNamespaces[i % kvNamespaces.length]; // Round-robin distribution

      const chunkPromise = uploadChunkToKV(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV);
      chunkPromises.push(chunkPromise);
    }

    const chunkResults = await Promise.all(chunkPromises);
    console.log('All chunks uploaded successfully from URL:', chunkResults);

    // ✅ Store master metadata in primary KV
    const masterMetadata = {
      filename: filename,
      size: file.size,
      contentType: contentType,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'multi_kv_chunked_url',
      sourceUrl: fileUrl,
      totalChunks: totalChunks,
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        telegramFileId: result.telegramFileId,
        size: result.size,
        chunkKey: result.chunkKey,
        directUrl: result.directUrl
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));
    console.log(`Stored master metadata for fileId: ${fileId}`);

    // ✅ Generate URLs
    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;

    const result = {
      success: true,
      data: {
        filename: filename,
        size: file.size,
        contentType: contentType,
        url: customUrl,
        download: downloadUrl,
        id: fileId,
        strategy: 'multi_kv_chunked_url',
        chunks: totalChunks,
        kvDistribution: chunkResults.map(r => r.kvNamespace),
        sourceUrl: fileUrl
      }
    };

    console.log('URL upload completed:', result);
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('URL upload error:', error.message);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// ✅ Upload chunk to specific KV namespace
async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  console.log(`Uploading chunk ${chunkIndex} to ${kvNamespace.name}...`);

  // Upload to Telegram
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

  if (!telegramResponse.ok) {
    console.error(`Telegram upload failed for chunk ${chunkIndex}: ${telegramResponse.status}`);
    throw new Error(`Telegram upload failed for chunk ${chunkIndex}: ${telegramResponse.status}`);
  }

  const telegramData = await telegramResponse.json();
  if (!telegramData.ok || !telegramData.result?.document?.file_id) {
    console.error('Invalid Telegram response:', telegramData);
    throw new Error(`Invalid Telegram response for chunk ${chunkIndex}`);
  }

  const telegramFileId = telegramData.result.document.file_id;
  console.log(`Chunk ${chunkIndex} uploaded to Telegram, file_id: ${telegramFileId}`);

  // Get file URL
  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);

  if (!getFileResponse.ok) {
    console.error(`GetFile API failed for chunk ${chunkIndex}: ${getFileResponse.status}`);
    throw new Error(`GetFile API failed for chunk ${chunkIndex}`);
  }

  const getFileData = await getFileResponse.json();
  if (!getFileData.ok || !getFileData.result?.file_path) {
    console.error('No file_path in GetFile response:', getFileData);
    throw new Error(`No file_path for chunk ${chunkIndex}`);
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
  console.log(`Chunk ${chunkIndex} direct URL: ${directUrl}`);

  // ✅ Store chunk with auto-refresh metadata
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;
  const chunkMetadata = {
    telegramFileId: telegramFileId,
    directUrl: directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    uploadedAt: Date.now(),
    lastRefreshed: Date.now()
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));
  console.log(`Chunk ${chunkIndex} stored in ${kvNamespace.name} with key ${chunkKey}`);

  return {
    telegramFileId: telegramFileId,
    size: chunkFile.size,
    directUrl: directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey: chunkKey
  };
}