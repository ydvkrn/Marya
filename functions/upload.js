export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-File-Size',
    'Access-Control-Max-Age': '86400'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Use POST' }), {
      status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

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
      throw new Error('Missing config');
    }

    const formData = await request.formData();
    const file = formData.get('file');
    
    if (!file) throw new Error('No file');

    // ✅ 1GB LIMIT
    const MAX_FILE_SIZE = 1024 * 1024 * 1024;
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`Max 1GB. File: ${(file.size/1024/1024).toFixed(1)}MB`);
    }

    const fileId = `id${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const CHUNK_SIZE = 35 * 1024 * 1024; // 35MB chunks
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length * 2) {
      throw new Error(`Need ${totalChunks} chunks, have ${kvNamespaces.length} KV`);
    }

    const chunkResults = await uploadFileWithStreaming(file, fileId, CHUNK_SIZE, kvNamespaces, BOT_TOKEN, CHANNEL_ID);
    
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension: file.name.slice(file.name.lastIndexOf('.')),
      uploadedAt: Date.now(),
      totalChunks,
      chunks: chunkResults,
      type: '1gb_multi_kv',
      version: '3.0'
    };
    
    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${masterMetadata.extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${masterMetadata.extension}?dl=1`;

    return new Response(JSON.stringify({
      success: true,
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        totalChunks,
        urls: { view: customUrl, download: downloadUrl },
        storage: { strategy: 'multi_kv_chunked', totalChunks }
      }
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

async function uploadFileWithStreaming(file, fileId, chunkSize, kvNamespaces, botToken, channelId) {
  const chunks = [];
  let chunkIndex = 0;
  
  const reader = file.stream().getReader();
  let buffer = [];
  let bufferSize = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      if (bufferSize > 0) {
        chunks.push(await processSingleChunk(new Blob(buffer), fileId, chunkIndex++, botToken, channelId, kvNamespaces));
      }
      break;
    }

    buffer.push(value);
    bufferSize += value.length;

    while (bufferSize >= chunkSize) {
      const chunkBlob = new Blob(buffer.slice(0, Math.ceil(chunkSize / value.length)));
      buffer = buffer.slice(-1); // Keep remainder
      bufferSize = buffer.reduce((sum, b) => sum + b.length, 0);
      
      chunks.push(await processSingleChunk(chunkBlob, fileId, chunkIndex++, botToken, channelId, kvNamespaces));
    }
  }

  return chunks;
}

async function processSingleChunk(chunkBlob, fileId, chunkIndex, botToken, channelId, kvNamespaces) {
  const chunkFile = new File([chunkBlob], `${fileId}_chunk_${chunkIndex}`);
  const kv = kvNamespaces[chunkIndex % kvNamespaces.length];

  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      const form = new FormData();
      form.append('chat_id', channelId);
      form.append('document', chunkFile);
      form.append('caption', `1GB-Chunk-${chunkIndex}-${fileId}`);

      const telegramRes = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
        method: 'POST', body: form
      });

      if (!telegramRes.ok) {
        const error = await telegramRes.text();
        throw new Error(`Telegram ${telegramRes.status}: ${error}`);
      }

      const data = await telegramRes.json();
      if (!data.ok || !data.result?.document?.file_id) {
        throw new Error('Invalid Telegram response');
      }

      const chunkKey = `${fileId}_chunk_${chunkIndex}`;
      const chunkMeta = {
        telegramFileId: data.result.document.file_id,
        messageId: data.result.message_id,
        size: chunkFile.size,
        index: chunkIndex,
        kvNamespace: kv.name,
        uploadedAt: Date.now()
      };

      await kv.kv.put(chunkKey, JSON.stringify(chunkMeta));
      return chunkMeta;
    } catch (error) {
      if (attempt === 5) throw error;
      await new Promise(r => setTimeout(r, 1000 * Math.pow(2, attempt - 1)));
    }
  }
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
