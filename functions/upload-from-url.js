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
    const { url } = await request.json();
    
    if (!url || !url.startsWith('http')) {
      throw new Error('Invalid URL provided');
    }

    console.log('📥 Importing from URL:', url);

    // Download file from URL with proper headers
    const response = await fetch(url, {
      headers: {
        'User-Agent': 'Marya-Vault/1.0 (+https://marya-vault.pages.dev)'
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to download: HTTP ${response.status}`);
    }

    // Extract filename from URL or Content-Disposition
    let filename = url.split('/').pop().split('?')[0] || 'imported_file';
    const contentDisposition = response.headers.get('Content-Disposition');
    if (contentDisposition) {
      const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
      if (filenameMatch) {
        filename = filenameMatch[1].replace(/['"]/g, '');
      }
    }

    // Ensure filename has extension
    if (!filename.includes('.')) {
      const contentType = response.headers.get('Content-Type') || 'application/octet-stream';
      const extension = getExtensionFromMime(contentType);
      filename += extension;
    }

    const contentType = response.headers.get('Content-Type') || 'application/octet-stream';
    
    // Convert to File object
    const fileBuffer = await response.arrayBuffer();
    const file = new File([fileBuffer], filename, { type: contentType });

    console.log(`📦 File imported: ${filename} (${Math.round(file.size/1024/1024)}MB)`);

    // Generate MSM ID
    const msmId = generateMSMId();
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

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

    // Chunk and upload the imported file
    const CHUNK_SIZE = 8 * 1024 * 1024; // 8MB chunks
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    const chunkResults = [];

    console.log(`🚀 Starting chunked import of ${totalChunks} chunks...`);

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      const kvIndex = Math.floor(i / 40);
      const targetKV = kvNamespaces[kvIndex];
      const botToken = botTokens[i % botTokens.length];

      console.log(`⬆️ Importing chunk ${i + 1}/${totalChunks}...`);

      const chunkResult = await uploadImportedChunk(
        chunk, msmId, i, kvIndex, i % 40,
        botToken, CHANNEL_ID, targetKV, filename
      );

      chunkResults.push(chunkResult);
    }

    // Store final metadata
    const finalMetadata = {
      filename: filename,
      size: file.size,
      contentType: contentType,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'url_import',
      originalUrl: url,
      totalChunks: totalChunks,
      strategy: 'imported_chunked',
      neverExpires: true,
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    await kvNamespaces[0].kv.put(msmId, JSON.stringify(finalMetadata));

    const baseUrl = new URL(request.url).origin;

    console.log(`✅ URL import completed: ${filename}`);

    return new Response(JSON.stringify({
      success: true,
      filename: filename,
      size: file.size,
      contentType: contentType,
      url: `${baseUrl}/btfstorage/file/${msmId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${msmId}${extension}?dl=1`,
      id: msmId,
      strategy: 'imported_from_url',
      originalUrl: url,
      chunks: totalChunks,
      lifetime: 'Permanent (20+ years)'
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('URL import error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Upload imported chunk
async function uploadImportedChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  try {
    const chunkFile = new File([chunk], `${originalFilename}.imported.chunk${chunkIndex}`, { 
      type: 'application/octet-stream' 
    });

    // Upload to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: AbortSignal.timeout(45000)
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    const telegramFileId = telegramData.result.document.file_id;

    // Get file URL
    const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`);
    const getFileData = await getFileResponse.json();
    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store in KV
    const keyName = `${fileId}_imported_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
    const chunkMetadata = {
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      size: chunk.size,
      chunkIndex: chunkIndex,
      uploadedAt: Date.now(),
      neverExpires: true,
      importedFromUrl: true
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
    throw new Error(`Imported chunk ${chunkIndex} failed: ${error.message}`);
  }
}

// Get file extension from MIME type
function getExtensionFromMime(mimeType) {
  const mimeMap = {
    'video/mp4': '.mp4',
    'video/x-matroska': '.mkv',
    'video/webm': '.webm',
    'video/quicktime': '.mov',
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'audio/mpeg': '.mp3',
    'audio/wav': '.wav',
    'application/pdf': '.pdf',
    'application/zip': '.zip'
  };
  return mimeMap[mimeType] || '.bin';
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
