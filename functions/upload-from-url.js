// /functions/upload-from-url.js
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, X-Requested-With',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (!['GET', 'POST'].includes(request.method)) {
    return new Response(
      JSON.stringify({ success: false, error: { message: 'Method not allowed. Use GET or POST.' }}),
      { status: 405, headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }}
    );
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey });
      }
    }

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing BOT_TOKEN, CHANNEL_ID or KV namespaces');
    }

    let fileUrl, customFilename;

    if (request.method === 'GET') {
      const url = new URL(request.url);
      fileUrl = url.searchParams.get('fileUrl') || url.searchParams.get('url') || url.searchParams.get('file');
      customFilename = url.searchParams.get('filename') || url.searchParams.get('name') || null;
    } else if (request.method === 'POST') {
      const body = await request.json();
      fileUrl = body.fileUrl || body.url || body.telegramUrl || body.file_url || body.file;
      customFilename = body.filename || body.name || null;
    }

    if (!fileUrl || typeof fileUrl !== 'string') {
      return new Response(
        JSON.stringify({ success: false, error: { message: 'Missing or invalid fileUrl parameter' }}),
        { status: 400, headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }}
      );
    }

    let parsedUrl;
    try {
      parsedUrl = new URL(fileUrl);
      if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
        throw new Error('Only HTTP/HTTPS URLs are supported');
      }
    } catch (err) {
      return new Response(
        JSON.stringify({ success: false, error: { message: 'Invalid URL format. Use a valid HTTP/HTTPS URL.' }}),
        { status: 400, headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }}
      );
    }

    const fileResponse = await fetch(fileUrl, {
      headers: { 'User-Agent': 'MaryaVault/2.1', 'Accept': '*/*' },
      redirect: 'follow',
      cf: { cacheTtl: 0, cacheEverything: false }
    });

    if (!fileResponse.ok) {
      throw new Error(`Failed to download file: ${fileResponse.status} ${fileResponse.statusText}`);
    }

    const arrayBuffer = await fileResponse.arrayBuffer();
    if (arrayBuffer.byteLength === 0) {
      throw new Error('Downloaded file is empty (0 bytes)');
    }

    const contentType = fileResponse.headers.get('content-type') || 'application/octet-stream';

    let filename = customFilename;
    if (!filename) {
      const disposition = fileResponse.headers.get('content-disposition');
      if (disposition) {
        const match = disposition.match(/filename[*]?=(?:UTF-8'')?["']?([^"';]+)["']?/i);
        if (match?.[1]) {
          filename = decodeURIComponent(match[1].trim());
        }
      }

      if (!filename) {
        const urlPath = parsedUrl.pathname;
        const decodedPath = decodeURIComponent(urlPath);
        filename = decodedPath.split('/').pop() || `file_${Date.now()}`;
        filename = filename.split('?')[0].split('#')[0];
      }
    }

    // ✅ FIXED LINE - No more regex error
    filename = filename.replace(/[<>:"/\\|?*]/g, '_').replace(/[-\u001F]/g, '').trim();
    if (filename.length === 0) {
      filename = `file_${Date.now()}`;
    }

    const MAX_FILE_SIZE = 500 * 1024 * 1024;
    if (arrayBuffer.byteLength > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${formatBytes(arrayBuffer.byteLength)} (max 500MB)`);
    }

    const file = new File([arrayBuffer], filename, { type: contentType });

    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 10);
    const fileId = `url_${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    const CHUNK_SIZE = 20 * 1024 * 1024;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`File requires ${totalChunks} chunks but only ${kvNamespaces.length} KV namespaces available`);
    }

    const chunkPromises = [];
    const uploadStartTime = Date.now();

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const chunkFile = new File([chunk], `${filename}.part${i}`, { type: contentType });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      chunkPromises.push(uploadChunkToKV(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV));
    }

    const chunkResults = await Promise.all(chunkPromises);
    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);

    const masterMetadata = {
      filename,
      size: file.size,
      contentType,
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_chunked_url',
      version: '2.1',
      sourceUrl: fileUrl,
      sourceMethod: request.method,
      totalChunks,
      chunks: chunkResults.map((r, i) => ({
        index: i,
        kvNamespace: r.kvNamespace,
        telegramFileId: r.telegramFileId,
        telegramMessageId: r.telegramMessageId,
        size: r.size,
        chunkKey: r.chunkKey,
        uploadedAt: r.uploadedAt,
      })),
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;

    const result = {
      success: true,
      message: 'File successfully imported from URL',
      data: {
        id: fileId,
        filename,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType,
        extension,
        uploadDuration: `${uploadDuration}s`,
        urls: { view: customUrl, download: downloadUrl, api: `${baseUrl}/api/file/${fileId}` },
        storage: { strategy: 'multi_kv_chunked_url', totalChunks, kvDistribution: chunkResults.map((r) => r.kvNamespace) },
        meta: { sourceUrl: fileUrl, sourceMethod: request.method, uploadedAt: new Date().toISOString() },
      },
    };

    return new Response(JSON.stringify(result, null, 2), {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders },
    });
  } catch (error) {
    return new Response(
      JSON.stringify({ success: false, error: { message: error.message || 'Unknown error occurred', type: error.name || 'UploadError', timestamp: new Date().toISOString() }}),
      { status: 500, headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }}
    );
  }
}

async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const form = new FormData();
  form.append('chat_id', channelId);
  form.append('document', chunkFile);
  form.append('caption', `MaryaVault Chunk | ${fileId} | Part ${chunkIndex}`);

  const res = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, { method: 'POST', body: form });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Telegram upload failed for chunk ${chunkIndex} (${res.status}): ${text}`);
  }

  const data = await res.json();
  if (!data.ok || !data.result?.document?.file_id) {
    throw new Error(`Telegram failed to return file_id for chunk ${chunkIndex}`);
  }

  const telegramFileId = data.result.document.file_id;
  const telegramMessageId = data.result.message_id;

  const fileInfo = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`);
  const fileData = await fileInfo.json();

  if (!fileData.ok || !fileData.result?.file_path) {
    throw new Error(`Failed to get Telegram file path for chunk ${chunkIndex}`);
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;

  const metadata = {
    telegramFileId,
    telegramMessageId,
    directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    uploadedAt: Date.now(),
    lastRefreshed: Date.now(),
    refreshCount: 0,
    version: '2.1',
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(metadata));

  return {
    telegramFileId,
    telegramMessageId,
    size: chunkFile.size,
    directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey,
    uploadedAt: Date.now(),
  };
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}