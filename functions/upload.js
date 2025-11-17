export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT PRO v2.5 | 500MB MAX | 25 KV ===');

  // CORS Headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Use POST method only' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // Auto-detect all 25 KV namespaces (jo bind hoga wo hi chalega)
    const kvNamespaces = [
      { kv: env.FILES_KV1,  name: 'FILES_KV1'  }, { kv: env.FILES_KV2,  name: 'FILES_KV2'  },
      { kv: env.FILES_KV3,  name: 'FILES_KV3'  }, { kv: env.FILES_KV4,  name: 'FILES_KV4'  },
      { kv: env.FILES_KV5,  name: 'FILES_KV5'  }, { kv: env.FILES_KV6,  name: 'FILES_KV6'  },
      { kv: env.FILES_KV7,  name: 'FILES_KV7'  }, { kv: env.FILES_KV8,  name: 'FILES_KV8'  },
      { kv: env.FILES_KV9,  name: 'FILES_KV9'  }, { kv: env.FILES_KV10, name: 'FILES_KV10' },
      { kv: env.FILES_KV11, name: 'FILES_KV11' }, { kv: env.FILES_KV12, name: 'FILES_KV12' },
      { kv: env.FILES_KV13, name: 'FILES_KV13' }, { kv: env.FILES_KV14, name: 'FILES_KV14' },
      { kv: env.FILES_KV15, name: 'FILES_KV15' }, { kv: env.FILES_KV16, name: 'FILES_KV16' },
      { kv: env.FILES_KV17, name: 'FILES_KV17' }, { kv: env.FILES_KV18, name: 'FILES_KV18' },
      { kv: env.FILES_KV19, name: 'FILES_KV19' }, { kv: env.FILES_KV20, name: 'FILES_KV20' },
      { kv: env.FILES_KV21, name: 'FILES_KV21' }, { kv: env.FILES_KV22, name: 'FILES_KV22' },
      { kv: env.FILES_KV23, name: 'FILES_KV23' }, { kv: env.FILES_KV24, name: 'FILES_KV24' },
      { kv: env.FILES_KV25, name: 'FILES_KV25' }
    ].filter(item => item.kv);

    console.log(`Detected ${kvNamespaces.length}/25 KV namespaces`);

    if (!BOT_TOKEN || !CHANNEL_ID) throw new Error('BOT_TOKEN or CHANNEL_ID missing in env');
    if (kvNamespaces.length === 0) throw new Error('No KV namespace bound');

    // MAX LIMITS (500 MB safe + future-proof)
    const MAX_FILE_SIZE = 500 * 1024 * 1024;    // 500 MB
    const CHUNK_SIZE = 25 * 1024 * 1024;        // 25 MB (KV max safe)

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) throw new Error('No file found. Use key: file');
    if (file.size === 0) throw new Error('Empty file not allowed');
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${(file.size/1024/1024).toFixed(1)} MB\nMax allowed: 500 MB`);
    }

    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    if (totalChunks > kvNamespaces.length) {
      throw new Error(`Need ${totalChunks} chunks but only ${kvNamespaces.length} KV available\nAdd more KV or reduce file size`);
    }

    console.log(`File: ${file.name} | Size: ${formatBytes(file.size)} | Chunks: ${totalChunks}`);

    // Unique ID
    const fileId = `id${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const ext = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    const uploadStart = Date.now();

    // Upload all chunks
    const chunkResults = await Promise.all(
      Array.from({ length: totalChunks }, (_, i) => {
        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        const chunk = file.slice(start, end);
        const chunkFile = new File([chunk], `${file.name}.part${i}`, { type: file.type });
        const targetKV = kvNamespaces[i % kvNamespaces.length];

        return uploadChunkWithRetry(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV, 3);
      })
    );

    const uploadTime = ((Date.now() - uploadStart) / 1000).toFixed(1);

    // Save master metadata in first KV
    await kvNamespaces[0].kv.put(fileId, JSON.stringify({
      filename: file.name,
      size: file.size,
      type: file.type,
      extension: ext,
      uploadedAt: Date.now(),
      totalChunks,
      chunks: chunkResults.map((r, i) => ({
        index: i,
        kv: r.kvNamespace,
        telegramFileId: r.telegramFileId,
        messageId: r.telegramMessageId,
        size: r.size
      }))
    }));

    const base = new URL(request.url).origin;
    const url = `${base}/btfstorage/file/${fileId}${ext}`;

    return new Response(JSON.stringify({
      success: true,
      message: "Upload successful! 500MB system active",
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        chunks: totalChunks,
        uploadTime: `${uploadTime}s`,
        speed: `${(file.size / 1024 / parseFloat(uploadTime)).toFixed(2)} KB/s`,
        urls: {
          view: url,
          download: `${url}?dl=1`,
          stream: `${url}?stream=1`
        },
        max_limit: "500 MB",
        kv_used: kvNamespaces.length
      }
    }, null, 2), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (err) {
    console.error('Upload failed:', err);
    return new Response(JSON.stringify({
      success: false,
      error: err.message || 'Unknown error',
      tip: "Max 500 MB | Use 25 KV namespaces"
    }, null, 2), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// With retry
async function uploadChunkWithRetry(chunkFile, fileId, idx, token, chatId, kvObj, retries = 3) {
  for (let i = 1; i <= retries; i++) {
    try {
      return await uploadChunk(chunkFile, fileId, idx, token, chatId, kvObj);
    } catch (e) {
      if (i === retries) throw e;
      await new Promise(r => setTimeout(r, 1000 * i));
    }
  }
}

async function uploadChunk(chunkFile, fileId, idx, token, chatId, kvObj) {
  const form = new FormData();
  form.append('chat_id', chatId);
  form.append('document', chunkFile);
  form.append('caption', `Chunk ${idx} | ${fileId}`);

  const res = await fetch(`https://api.telegram.org/bot${token}/sendDocument`, { method: 'POST', body: form });
  const data = await res.json();

  if (!data.ok) throw new Error(data.description || 'Telegram upload failed');

  const telegramFileId = data.result.document.file_id;
  const messageId = data.result.message_id;

  const fileRes = await fetch(`https://api.telegram.org/bot${token}/getFile?file_id=${telegramFileId}`);
  const fileData = await fileRes.json();
  if (!fileData.ok) throw new Error('Failed to get file path');

  const directUrl = `https://api.telegram.org/file/bot${token}/${fileData.result.file_path}`;

  const key = `${fileId}_chunk_${idx}`;
  await kvObj.kv.put(key, JSON.stringify({
    telegramFileId,
    messageId,
    directUrl,
    size: chunkFile.size,
    uploadedAt: Date.now()
  }));

  return {
    telegramFileId,
    telegramMessageId: messageId,
    size: chunkFile.size,
    kvNamespace: kvObj.name,
    directUrl
  };
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}