export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT ULTRA 625MB UPLOADER START ===');
  console.log('Method:', request.method, '| URL:', request.url);

  // CORS Headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Use POST method only' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders },
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // 25 KV Namespaces (tu ne bana diye hain)
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
      { kv: env.FILES_KV25, name: 'FILES_KV25' },
    ].filter(item => item.kv); // Auto skip if any missing

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing BOT_TOKEN, CHANNEL_ID or KV bindings');
    }

    const formData = await request.formData();
    const file = formData.get('file');
    if (!file) throw new Error('No file found! Use key: file');

    console.log('File:', file.name, '| Size:', formatBytes(file.size));

    // MAX LIMIT AB 625 MB (25 × 25 MB)
    const MAX_FILE_SIZE = 625 * 1024 * 1024; // 625 MB
    const CHUNK_SIZE = 25 * 1024 * 1024;      // 25 MB (Cloudflare KV max safe)

    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too big: ${formatBytes(file.size)}\nMaximum allowed: 625 MB (free tier)`);
    }
    if (file.size === 0) throw new Error('Empty file not allowed');

    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    if (totalChunks > kvNamespaces.length) {
      throw new Error(`Need ${totalChunks} chunks but only ${kvNamespaces.length} KV available.\nAdd more KV or reduce file size.`);
    }

    // Unique ID
    const fileId = `id${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    const uploadStart = Date.now();
    const chunkPromises = [];

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const chunkFile = new File([chunk], `${file.name}.part${i}`, { type: file.type });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      chunkPromises.push(
        uploadChunkWithRetry(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV, 3)
      );
    }

    const results = await Promise.all(chunkPromises);
    const uploadTime = ((Date.now() - uploadStart) / 1000).toFixed(1);

    // Master metadata
    const metadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadTime),
      totalChunks,
      chunks: results.map((r, i) => ({
        index: i,
        kv: r.kvNamespace,
        telegramFileId: r.telegramFileId,
        messageId: r.telegramMessageId,
        size: r.size,
        key: r.chunkKey
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(metadata));

    const base = new URL(request.url).origin;
    const url = `${base}/btfstorage/file/${fileId}${extension}`;

    return new Response(JSON.stringify({
      success: true,
      message: 'Upload successful!',
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        uploadedIn: `${uploadTime}s`,
        speed: `${(file.size / 1024 / parseFloat(uploadTime)).toFixed(2)} KB/s`,
        maxLimit: "625 MB",
        chunksUsed: totalChunks,
        urls: {
          view: url,
          download: `${url}?dl=1`,
          stream: `${url}?stream=1`
        }
      }
    }, null, 2), {
      status: 200,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (err) {
    return new Response(JSON.stringify({
      success: false,
      error: err.message || 'Upload failed',
      timestamp: new Date().toISOString()
    }, null, 2), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Retry upload function
async function uploadChunkWithRetry(chunkFile, fileId, idx, token, chatId, kvObj, retries = 3) {
  for (let i = 1; i <= retries; i++) {
    try {
      return await uploadChunk(chunkFile, fileId, idx, token, chatId, kvObj);
    } catch (e) {
      if (i === retries) throw e;
      await new Promise(r => setTimeout(r, 1000 * i * 2));
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
  if (!fileData.ok) throw new Error('getFile failed');

  const directUrl = `https://api.telegram.org/file/bot${token}/${fileData.result.file_path}`;

  const key = `${fileId}_chunk_${idx}`;
  const meta = {
    telegramFileId, messageId, directUrl, size: chunkFile.size,
    index: idx, parentFileId: fileId, kvNamespace: kvObj.name,
    uploadedAt: Date.now()
  };

  await kvObj.kv.put(key, JSON.stringify(meta));

  return { ...meta, telegramFileId, telegramMessageId: messageId, kvNamespace: kvObj.name, chunkKey: key };
}

// Helper
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}