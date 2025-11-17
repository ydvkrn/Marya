// ✅ CRITICAL: Export individual functions for each HTTP method
export async function onRequestPost(context) {
  return handleUpload(context);
}

export async function onRequestOptions(context) {
  return new Response(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': '*',
      'Access-Control-Max-Age': '86400'
    }
  });
}

// ✅ Main upload handler
async function handleUpload(context) {
  const { request, env } = context;

  console.log('=== UPLOAD START ===');
  console.log('Method:', request.method);
  console.log('URL:', request.url);

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': '*',
    'Access-Control-Expose-Headers': '*'
  };

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing BOT_TOKEN or CHANNEL_ID');
    }

    // ✅ All 25 KV namespaces
    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' },
      { kv: env.FILES_KV8, name: 'FILES_KV8' },
      { kv: env.FILES_KV9, name: 'FILES_KV9' },
      { kv: env.FILES_KV10, name: 'FILES_KV10' },
      { kv: env.FILES_KV11, name: 'FILES_KV11' },
      { kv: env.FILES_KV12, name: 'FILES_KV12' },
      { kv: env.FILES_KV13, name: 'FILES_KV13' },
      { kv: env.FILES_KV14, name: 'FILES_KV14' },
      { kv: env.FILES_KV15, name: 'FILES_KV15' },
      { kv: env.FILES_KV16, name: 'FILES_KV16' },
      { kv: env.FILES_KV17, name: 'FILES_KV17' },
      { kv: env.FILES_KV18, name: 'FILES_KV18' },
      { kv: env.FILES_KV19, name: 'FILES_KV19' },
      { kv: env.FILES_KV20, name: 'FILES_KV20' },
      { kv: env.FILES_KV21, name: 'FILES_KV21' },
      { kv: env.FILES_KV22, name: 'FILES_KV22' },
      { kv: env.FILES_KV23, name: 'FILES_KV23' },
      { kv: env.FILES_KV24, name: 'FILES_KV24' },
      { kv: env.FILES_KV25, name: 'FILES_KV25' }
    ].filter(item => item.kv);

    console.log(`Available KV: ${kvNamespaces.length}/25`);

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces bound');
    }

    // ✅ Parse form data
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || !file.size) {
      throw new Error('No file provided');
    }

    console.log('File:', file.name, formatBytes(file.size));

    // ✅ Size validation
    const MAX_SIZE = 500 * 1024 * 1024;
    if (file.size > MAX_SIZE) {
      throw new Error(`File too large: ${formatBytes(file.size)}. Max: 500MB`);
    }

    // ✅ Generate file ID
    const fileId = `id${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // ✅ Chunking
    const CHUNK_SIZE = 20 * 1024 * 1024;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`Need ${totalChunks} chunks but only ${kvNamespaces.length} KV available`);
    }

    console.log(`Splitting into ${totalChunks} chunks`);

    const uploadStart = Date.now();
    const chunkPromises = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const chunkFile = new File([chunk], `${file.name}.part${i}`, { type: file.type });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      chunkPromises.push(
        uploadChunk(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV)
      );
    }

    const chunkResults = await Promise.all(chunkPromises);
    const uploadDuration = ((Date.now() - uploadStart) / 1000).toFixed(2);

    console.log(`✅ ${totalChunks} chunks uploaded in ${uploadDuration}s`);

    // ✅ Store metadata
    const metadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension: extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_chunked',
      version: '2.0',
      totalChunks: totalChunks,
      chunks: chunkResults
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(metadata));

    // ✅ Generate URLs
    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    const response = {
      success: true,
      message: 'File uploaded successfully',
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType: file.type || 'application/octet-stream',
        extension: extension,
        uploadDuration: `${uploadDuration}s`,
        urls: {
          view: viewUrl,
          download: downloadUrl,
          stream: streamUrl
        },
        storage: {
          strategy: 'multi_kv_chunked',
          totalChunks: totalChunks,
          kvDistribution: chunkResults.map(r => r.kvNamespace)
        },
        uploadedAt: new Date().toISOString()
      }
    };

    console.log('✅ SUCCESS');

    return new Response(JSON.stringify(response, null, 2), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        ...corsHeaders
      }
    });

  } catch (error) {
    console.error('❌ ERROR:', error.message);

    return new Response(JSON.stringify({
      success: false,
      error: {
        message: error.message,
        type: error.name || 'UploadError',
        timestamp: new Date().toISOString()
      }
    }, null, 2), {
      status: 500,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        ...corsHeaders
      }
    });
  }
}

async function uploadChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const maxRetries = 3;
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const telegramForm = new FormData();
      telegramForm.append('chat_id', channelId);
      telegramForm.append('document', chunkFile);
      telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

      const telegramRes = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
        method: 'POST',
        body: telegramForm
      });

      if (!telegramRes.ok) {
        throw new Error(`Telegram error ${telegramRes.status}`);
      }

      const telegramData = await telegramRes.json();

      if (!telegramData.ok || !telegramData.result?.document?.file_id) {
        throw new Error('Invalid Telegram response');
      }

      const telegramFileId = telegramData.result.document.file_id;
      const telegramMessageId = telegramData.result.message_id;

      const getFileRes = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
      const getFileData = await getFileRes.json();

      if (!getFileData.ok || !getFileData.result?.file_path) {
        throw new Error('No file_path');
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

      const chunkKey = `${fileId}_chunk_${chunkIndex}`;
      const chunkMetadata = {
        telegramFileId: telegramFileId,
        telegramMessageId: telegramMessageId,
        directUrl: directUrl,
        size: chunkFile.size,
        index: chunkIndex,
        parentFileId: fileId,
        kvNamespace: kvNamespace.name,
        uploadedAt: Date.now(),
        version: '2.0'
      };

      await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

      console.log(`✅ Chunk ${chunkIndex} → ${kvNamespace.name}`);

      return {
        telegramFileId,
        telegramMessageId,
        size: chunkFile.size,
        directUrl,
        kvNamespace: kvNamespace.name,
        chunkKey,
        uploadedAt: Date.now()
      };

    } catch (error) {
      console.error(`❌ Chunk ${chunkIndex} attempt ${attempt}:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
      }
    }
  }

  throw new Error(`Chunk ${chunkIndex} failed: ${lastError.message}`);
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
