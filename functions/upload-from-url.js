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

    // Download file from URL
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to download: HTTP ${response.status}`);
    }

    // Get filename from URL or Content-Disposition
    let filename = url.split('/').pop().split('?')[0] || 'download';
    const contentDisposition = response.headers.get('Content-Disposition');
    if (contentDisposition) {
      const filenameMatch = contentDisposition.match(/filename[^;=
]*=((['"]).*?\u0002|[^;
]*)/);
      if (filenameMatch) {
        filename = filenameMatch[1].replace(/['"]/g, '');
      }
    }

    const contentType = response.headers.get('Content-Type') || 'application/octet-stream';
    const contentLength = response.headers.get('Content-Length');
    
    // Convert to File object
    const fileBuffer = await response.arrayBuffer();
    const file = new File([fileBuffer], filename, { type: contentType });

    console.log(`📦 File imported: ${filename} (${Math.round(file.size/1024/1024)}MB)`);

    // If it's a Telegram file, copy it to our channel
    if (url.includes('api.telegram.org/file/bot')) {
      console.log('🔄 Telegram file detected - copying to our channel');
    }

    // Upload using our chunked system
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

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      const kvIndex = Math.floor(i / 40);
      const targetKV = kvNamespaces[kvIndex];
      const botToken = botTokens[i % botTokens.length];

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
      lifetime: '20+ years (never expires)'
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
      signal: AbortSignal.timeout(30000)
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