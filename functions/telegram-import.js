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
    const { telegramUrl } = await request.json();
    
    if (!telegramUrl) {
      throw new Error('Telegram URL not provided');
    }

    console.log('📱 Processing Telegram URL:', telegramUrl);

    // Handle different Telegram URL formats
    let fileId, fileName, fileSize;

    // Try to extract file info from various Telegram URL patterns
    if (telegramUrl.includes('api.telegram.org/file/bot')) {
      // Direct file URL - extract from path
      const pathMatch = telegramUrl.match(/\/file\/bot\d+\/(.+)/);
      if (pathMatch) {
        fileName = decodeURIComponent(pathMatch[1].split('/').pop());
      }
    } else if (telegramUrl.includes('t.me/')) {
      // t.me share URL - cannot directly access, need bot API
      return new Response(JSON.stringify({
        success: false,
        error: 'Cannot import from t.me links directly. Please:\n1. Right-click file in Telegram\n2. Copy direct download link\n3. Use that URL instead',
        suggestion: 'Use direct Telegram file URLs or upload the file directly'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Download file from Telegram
    const response = await fetch(telegramUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; MaryaVault/1.0)'
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to download from Telegram: HTTP ${response.status}`);
    }

    // Get file info
    const contentLength = response.headers.get('Content-Length');
    const contentDisposition = response.headers.get('Content-Disposition');
    
    if (contentDisposition) {
      const nameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
      if (nameMatch) {
        fileName = nameMatch[1].replace(/['"]/g, '');
      }
    }

    if (!fileName) {
      fileName = 'telegram_file_' + Date.now();
    }

    fileSize = contentLength ? parseInt(contentLength) : 0;

    console.log(`📦 Telegram file: ${fileName} (${Math.round(fileSize/1024/1024)}MB)`);

    // Convert to File object
    const fileBuffer = await response.arrayBuffer();
    const file = new File([fileBuffer], fileName, { 
      type: 'application/octet-stream' 
    });

    // Now upload to our system using the same chunking logic
    const msmId = generateMSMId();
    const extension = fileName.includes('.') ? fileName.slice(fileName.lastIndexOf('.')) : '';

    // Bot tokens for re-uploading to our channel
    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    const CHANNEL_ID = env.CHANNEL_ID;

    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    // Smart chunking based on file size
    const chunkSize = file.size > 100 * 1024 * 1024 ? 20 * 1024 * 1024 : 10 * 1024 * 1024;
    const totalChunks = Math.ceil(file.size / chunkSize);
    const chunkResults = [];

    console.log(`🚀 Re-uploading to our channel: ${totalChunks} chunks`);

    for (let i = 0; i < totalChunks; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, file.size);
      const chunk = file.slice(start, end);

      const kvIndex = Math.floor(i / 40);
      const targetKV = kvNamespaces[kvIndex];
      const botToken = botTokens[i % botTokens.length];

      console.log(`⬆️ Uploading chunk ${i + 1}/${totalChunks} to our channel...`);

      const chunkResult = await uploadTelegramChunk(
        chunk, msmId, i, kvIndex, i % 40,
        botToken, CHANNEL_ID, targetKV, fileName
      );

      chunkResults.push(chunkResult);
    }

    // Store final metadata
    const finalMetadata = {
      filename: fileName,
      size: file.size,
      contentType: getMimeType(extension),
      extension: extension,
      uploadedAt: Date.now(),
      type: 'telegram_import',
      originalUrl: telegramUrl,
      totalChunks: totalChunks,
      chunkSize: chunkSize,
      strategy: 'telegram_imported',
      neverExpires: true,
      streamable: true,
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

    console.log(`✅ Telegram import completed: ${fileName}`);

    return new Response(JSON.stringify({
      success: true,
      filename: fileName,
      size: file.size,
      contentType: getMimeType(extension),
      url: `${baseUrl}/btfstorage/file/${msmId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${msmId}${extension}?dl=1`,
      id: msmId,
      strategy: 'telegram_imported',
      originalUrl: telegramUrl,
      chunks: totalChunks,
      lifetime: 'Permanent (Never Expires)',
      message: '✅ File imported from Telegram and uploaded to your channel!'
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Telegram import error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

async function uploadTelegramChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  try {
    const chunkFile = new File([chunk], `${originalFilename}.telegram.chunk${chunkIndex}`, { 
      type: 'application/octet-stream' 
    });

    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: AbortSignal.timeout(90000)
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error(`Telegram API error: ${telegramData.description || 'Unknown error'}`);
    }

    const telegramFileId = telegramData.result.document.file_id;

    const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`);
    const getFileData = await getFileResponse.json();
    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    const keyName = `${fileId}_telegram_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
    const chunkMetadata = {
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      size: chunk.size,
      chunkIndex: chunkIndex,
      uploadedAt: Date.now(),
      neverExpires: true,
      telegramImport: true
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
    throw new Error(`Telegram chunk ${chunkIndex} failed: ${error.message}`);
  }
}

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  const mimeMap = {
    'mkv': 'video/mp4', 'mp4': 'video/mp4', 'avi': 'video/mp4',
    'mov': 'video/mp4', 'webm': 'video/webm',
    'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/mpeg',
    'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
    'gif': 'image/gif', 'webp': 'image/webp',
    'pdf': 'application/pdf', 'zip': 'application/zip'
  };
  return mimeMap[ext] || 'application/octet-stream';
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
