// ✅ MULTI-VIP 1GB UPLOAD - CLOUDflare PAGES FUNCTIONS READY
export async function onRequestPost(context) {
  const { request, env } = context;

  console.log('🚀 MARYA VAULT 1GB MULTI-VIP UPLOAD START');
  
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400'
  };

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // ✅ ALL 25 KV NAMESPACES
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

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      return Response.json({ success: false, error: 'Missing config' }, { 
        status: 500, headers: corsHeaders 
      });
    }

    const formData = await request.formData();
    const file = formData.get('file');
    
    if (!file) {
      return Response.json({ success: false, error: 'No file provided' }, { 
        status: 400, headers: corsHeaders 
      });
    }

    // ✅ 1GB LIMIT
    const MAX_FILE_SIZE = 1024 * 1024 * 1024;
    if (file.size > MAX_FILE_SIZE) {
      return Response.json({ 
        success: false, 
        error: `Max 1GB. File: ${(file.size/1024/1024).toFixed(1)}MB` 
      }, { status: 413, headers: corsHeaders });
    }

    const fileId = `id${Date.now().toString(36)}${Math.random().toString(36).slice(2,8)}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';
    const CHUNK_SIZE = 35 * 1024 * 1024; // 35MB
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length * 2) {
      return Response.json({ 
        success: false, 
        error: `Need ${totalChunks} chunks, have ${kvNamespaces.length} KV` 
      }, { status: 413, headers: corsHeaders });
    }

    // ✅ PARALLEL CHUNK UPLOAD
    const chunkPromises = [];
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunkBlob = file.slice(start, end);
      const chunkFile = new File([chunkBlob], `${file.name}.part${i}`, { type: file.type });
      const targetKV = kvNamespaces[i % kvNamespaces.length];
      
      chunkPromises.push(uploadChunk(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV));
    }

    const chunkResults = await Promise.all(chunkPromises);

    // ✅ BACKWARD COMPATIBLE METADATA (works with your [id].js)
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension: extension,
      uploadedAt: Date.now(),
      type: 'multi_kv_chunked',
      version: '3.0',
      totalChunks: totalChunks,
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        telegramFileId: result.telegramFileId,
        telegramMessageId: result.telegramMessageId,
        size: result.size,
        chunkKey: result.chunkKey,
        uploadedAt: result.uploadedAt
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const urls = {
      view: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`
    };

    return Response.json({
      success: true,
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        totalChunks,
        urls
      }
    }, { 
      status: 200, 
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders 
      } 
    });

  } catch (error) {
    console.error('❌ UPLOAD ERROR:', error);
    return Response.json({ 
      success: false, 
      error: error.message 
    }, { 
      status: 500, 
      headers: corsHeaders 
    });
  }
}

// ✅ CHUNK UPLOAD FUNCTION
async function uploadChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const form = new FormData();
  form.append('chat_id', channelId);
  form.append('document', chunkFile);
  form.append('caption', `Chunk-${chunkIndex}-${fileId}`);

  const res = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: form
  });

  if (!res.ok) throw new Error(`Telegram ${res.status}`);

  const data = await res.json();
  if (!data.ok) throw new Error(data.description);

  const telegramFileId = data.result.document.file_id;
  const getFileRes = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`);
  const getFileData = await getFileRes.json();
  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  const chunkKey = `${fileId}_chunk_${chunkIndex}`;
  const chunkMeta = {
    telegramFileId,
    telegramMessageId: data.result.message_id,
    directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    chunkKey,
    uploadedAt: Date.now()
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMeta));

  return {
    telegramFileId,
    telegramMessageId: data.result.message_id,
    size: chunkFile.size,
    kvNamespace: kvNamespace.name,
    chunkKey,
    uploadedAt: Date.now()
  };
}

function formatBytes(bytes) {
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
