export async function onRequest(context) {
  const { request, env } = context;

  // ✅ CORS headers (same)
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
    return new Response(JSON.stringify({
      success: false, error: 'Use POST method'
    }), { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;
    
    // ✅ All 25 KV (same array - unchanged)
    const kvNamespaces = [/* your 25 KV array */].filter(item => item.kv);

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing config');
    }

    const formData = await request.formData();
    const file = formData.get('file');
    
    if (!file) throw new Error('No file provided');

    // ✅ 1GB LIMIT
    const MAX_FILE_SIZE = 1024 * 1024 * 1024;
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`Max 1GB. File: ${(file.size/1024/1024).toFixed(1)}MB`);
    }

    // ✅ Generate ID (same)
    const fileId = `id${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    
    // ✅ CRITICAL: 35MB chunks for 1GB = ~29 chunks
    const CHUNK_SIZE = 35 * 1024 * 1024;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    
    if (totalChunks > kvNamespaces.length * 2) { // Allow 2x rotation
      throw new Error(`Need ${totalChunks} chunks, have ${kvNamespaces.length} KV`);
    }

    // ✅ STREAMING UPLOAD - No memory explosion!
    const chunkResults = await uploadFileWithStreaming(file, fileId, CHUNK_SIZE, kvNamespaces, BOT_TOKEN, CHANNEL_ID);
    
    // ✅ Store metadata (same)
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      totalChunks,
      chunks: chunkResults,
      type: '1gb_multi_kv',
      version: '3.0'
    };
    
    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    return new Response(JSON.stringify({
      success: true,
      data: { id: fileId, filename: file.name, size: file.size, chunks: totalChunks }
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// ✅ NEW: Streaming chunk uploader (KEY FOR 1GB)
async function uploadFileWithStreaming(file, fileId, chunkSize, kvNamespaces, botToken, channelId) {
  const chunks = [];
  let offset = 0;
  let chunkIndex = 0;

  // ✅ ReadableStream processing - NO FULL FILE IN MEMORY
  const reader = file.stream().getReader();
  
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    // ✅ Build chunk incrementally
    const chunkData = new Uint8Array(value);
    const chunk = new Blob([chunkData], { type: file.type });
    
    if (chunk.size > chunkSize) {
      // Split large reads
      for (let i = 0; i < chunk.size; i += chunkSize) {
        const chunkSlice = chunk.slice(i, i + chunkSize);
        chunks.push(await processSingleChunk(chunkSlice, fileId, chunkIndex++, botToken, channelId, kvNamespaces));
      }
    } else {
      chunks.push(await processSingleChunk(chunk, fileId, chunkIndex++, botToken, channelId, kvNamespaces));
    }
  }

  return chunks;
}

// ✅ Enhanced single chunk processor with 5 retries
async function processSingleChunk(chunkBlob, fileId, chunkIndex, botToken, channelId, kvNamespaces) {
  const chunkFile = new File([chunkBlob], `${fileId}_chunk_${chunkIndex}`);
  const kv = kvNamespaces[chunkIndex % kvNamespaces.length];

  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      // ✅ Telegram upload (same logic but with better error handling)
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

      // ✅ Store metadata
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
      
      // ✅ Exponential backoff: 1s, 2s, 4s, 8s, 16s
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 16000);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}
