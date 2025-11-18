// /functions/upload.js - STREAMING UPLOAD (Cloudflare Compatible)
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: { message: 'Method not allowed', code: 'METHOD_NOT_ALLOWED' }
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }

  const uploadId = crypto.randomUUID().split('-')[0];
  let uploadedChunks = [];

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // Load KV namespaces
    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey, index: i });
      }
    }

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new AppError('Missing BOT_TOKEN, CHANNEL_ID, or KV', 'CONFIG_ERROR');
    }

    // Parse form data (Cloudflare handles this properly)
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || !(file instanceof File)) {
      throw new AppError('No valid file provided', 'INVALID_FILE');
    }

    // Validate file
    const validation = validateFile(file.name, file.size, file.type);
    if (!validation.valid) {
      throw new AppError(validation.error, 'VALIDATION_FAILED');
    }

    const CHUNK_SIZE = 15 * 1024 * 1024; // 15MB
    const MAX_FILE_SIZE = kvNamespaces.length * CHUNK_SIZE;

    if (file.size > MAX_FILE_SIZE) {
      throw new AppError(
        `File too large: ${formatBytes(file.size)}. Max: ${formatBytes(MAX_FILE_SIZE)}`,
        'FILE_TOO_LARGE'
      );
    }

    if (file.size === 0) {
      throw new AppError('File is empty', 'EMPTY_FILE');
    }

    const timestamp = Date.now().toString(36);
    const random = crypto.randomUUID().split('-')[0];
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new AppError(
        `Need ${totalChunks} KV, have ${kvNamespaces.length}`,
        'INSUFFICIENT_KV'
      );
    }

    console.log(`[${uploadId}] Start: ${file.name} (${formatBytes(file.size)}, ${totalChunks} chunks)`);

    const uploadStartTime = Date.now();
    const chunkResults = [];

    // STREAMING: Read file in chunks without loading full file
    const reader = file.stream().getReader();
    let chunkIndex = 0;
    let buffer = new Uint8Array(0);
    let eof = false;

    while (!eof) {
      // Read until we have enough data for a chunk
      while (buffer.length < CHUNK_SIZE && !eof) {
        const { done, value } = await reader.read();
        
        if (done) {
          eof = true;
          break;
        }

        if (value && value.length > 0) {
          // Efficient concatenation
          const newBuffer = new Uint8Array(buffer.length + value.length);
          newBuffer.set(buffer, 0);
          newBuffer.set(value, buffer.length);
          buffer = newBuffer;
        }
      }

      // Process chunk if we have data
      if (buffer.length > 0) {
        const chunkSize = Math.min(CHUNK_SIZE, buffer.length);
        const chunkData = buffer.slice(0, chunkSize);
        buffer = buffer.slice(chunkSize);

        const targetKV = kvNamespaces[chunkIndex % kvNamespaces.length];

        console.log(`[${uploadId}] Chunk ${chunkIndex + 1}/${totalChunks} -> ${targetKV.name}`);

        try {
          const result = await uploadChunkWithRetry(
            chunkData,
            file.name,
            file.type,
            fileId,
            chunkIndex,
            BOT_TOKEN,
            CHANNEL_ID,
            targetKV,
            uploadId,
            5
          );

          chunkResults.push(result);
          uploadedChunks.push(result);

          // Adaptive delay
          const successRate = chunkResults.length / (chunkIndex + 1);
          const delay = successRate < 0.9 ? 400 : 150;
          await sleep(delay);

        } catch (chunkError) {
          console.error(`[${uploadId}] Chunk ${chunkIndex} failed:`, chunkError.message);
          await cleanupChunks(uploadedChunks, BOT_TOKEN, CHANNEL_ID);
          throw new AppError(
            `Chunk ${chunkIndex + 1} failed: ${chunkError.message}`,
            'CHUNK_UPLOAD_FAILED'
          );
        }

        chunkIndex++;
      }
    }

    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);
    const uploadSpeed = (file.size / 1024 / 1024) / parseFloat(uploadDuration);

    console.log(`[${uploadId}] ✓ Done in ${uploadDuration}s (${uploadSpeed.toFixed(2)} MB/s)`);

    // Store metadata with checksum
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      uploadSpeed: parseFloat(uploadSpeed.toFixed(2)),
      uploadId,
      type: 'streaming_multi_kv',
      version: '5.0',
      totalChunks: chunkResults.length,
      checksum: await generateChecksum(chunkResults),
      chunks: chunkResults.map((r, i) => ({
        index: i,
        kvNamespace: r.kvNamespace,
        kvIndex: r.kvIndex,
        telegramFileId: r.telegramFileId,
        telegramMessageId: r.telegramMessageId,
        size: r.size,
        chunkKey: r.chunkKey,
        uploadedAt: r.uploadedAt
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    // Backup metadata
    if (kvNamespaces.length > 1) {
      await kvNamespaces[kvNamespaces.length - 1].kv.put(
        `backup_${fileId}`,
        JSON.stringify(masterMetadata)
      );
    }

    const baseUrl = new URL(request.url).origin;

    const result = {
      success: true,
      message: 'File uploaded successfully',
      data: {
        id: fileId,
        uploadId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType: file.type || 'application/octet-stream',
        extension,
        uploadDuration: `${uploadDuration}s`,
        uploadSpeed: `${uploadSpeed.toFixed(2)} MB/s`,
        urls: {
          view: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
          download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
          stream: `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`,
          info: `${baseUrl}/btfstorage/info/${fileId}`
        },
        storage: {
          strategy: 'streaming_multi_kv',
          totalChunks: chunkResults.length,
          chunkSize: CHUNK_SIZE,
          kvDistribution: chunkResults.reduce((acc, r) => {
            acc[r.kvNamespace] = (acc[r.kvNamespace] || 0) + 1;
            return acc;
          }, {}),
          redundancy: kvNamespaces.length > 1 ? 'enabled' : 'disabled'
        },
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });

  } catch (error) {
    console.error(`❌ [${uploadId}] ERROR:`, error.message);

    if (uploadedChunks.length > 0) {
      await cleanupChunks(uploadedChunks, env.BOT_TOKEN, env.CHANNEL_ID).catch(err =>
        console.error(`[${uploadId}] Cleanup failed:`, err.message)
      );
    }

    const statusCode = error instanceof AppError ? 400 : 500;

    return new Response(JSON.stringify({
      success: false,
      error: {
        message: error.message,
        code: error.code || 'INTERNAL_ERROR',
        uploadId,
        timestamp: new Date().toISOString()
      }
    }), {
      status: statusCode,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }
}

class AppError extends Error {
  constructor(message, code) {
    super(message);
    this.code = code;
    this.name = 'AppError';
  }
}

function validateFile(filename, filesize, mimetype) {
  const allowedExtensions = [
    '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.bmp', '.ico',
    '.mp4', '.webm', '.mkv', '.avi', '.mov', '.flv', '.wmv',
    '.mp3', '.wav', '.ogg', '.flac', '.aac', '.m4a',
    '.pdf', '.doc', '.docx', '.txt', '.zip', '.rar', '.7z', '.tar', '.gz',
    '.json', '.xml', '.csv', '.js', '.py', '.java', '.cpp', '.html', '.css',
    '.apk', '.exe', '.dmg', '.iso'
  ];

  const ext = filename.slice(filename.lastIndexOf('.')).toLowerCase();
  
  if (!allowedExtensions.includes(ext)) {
    return { valid: false, error: `File type ${ext} not allowed` };
  }

  const dangerousPatterns = ['../', '.\\', '<script', '<?php'];
  if (dangerousPatterns.some(p => filename.toLowerCase().includes(p))) {
    return { valid: false, error: 'Invalid filename' };
  }

  if (filename.length > 255) {
    return { valid: false, error: 'Filename too long (max 255 chars)' };
  }

  return { valid: true };
}

async function uploadChunkWithRetry(
  chunkData,
  filename,
  mimetype,
  fileId,
  chunkIndex,
  botToken,
  channelId,
  kvNamespace,
  uploadId,
  maxRetries = 5
) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await uploadChunkToTelegram(
        chunkData,
        filename,
        mimetype,
        fileId,
        chunkIndex,
        botToken,
        channelId,
        kvNamespace,
        uploadId
      );
    } catch (error) {
      lastError = error;
      
      if (attempt < maxRetries) {
        const baseDelay = Math.min(1000 * Math.pow(2, attempt - 1), 8000);
        const jitter = Math.random() * 1000;
        const delay = baseDelay + jitter;
        
        console.warn(`[${uploadId}] Retry ${attempt}/${maxRetries} in ${(delay/1000).toFixed(1)}s`);
        await sleep(delay);
      }
    }
  }

  throw new AppError(
    `Chunk ${chunkIndex} failed after ${maxRetries} attempts`,
    'CHUNK_RETRY_EXHAUSTED'
  );
}

async function uploadChunkToTelegram(
  chunkData,
  filename,
  mimetype,
  fileId,
  chunkIndex,
  botToken,
  channelId,
  kvNamespace,
  uploadId
) {
  const chunkBlob = new Blob([chunkData], { type: mimetype || 'application/octet-stream' });
  const chunkFilename = `${fileId}_${chunkIndex}.part`;
  const chunkFile = new File([chunkBlob], chunkFilename, { type: mimetype });

  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `📦 ${chunkIndex}
🆔 ${fileId}
📄 ${filename}`);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120000);

  try {
    const telegramResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/sendDocument`,
      {
        method: 'POST',
        body: telegramForm,
        signal: controller.signal
      }
    );

    clearTimeout(timeout);

    if (!telegramResponse.ok) {
      const errorText = await telegramResponse.text();
      throw new AppError(
        `Telegram error ${telegramResponse.status}: ${errorText.slice(0, 100)}`,
        'TELEGRAM_ERROR'
      );
    }

    const telegramData = await telegramResponse.json();
    
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new AppError('Invalid Telegram response', 'TELEGRAM_INVALID');
    }

    const telegramFileId = telegramData.result.document.file_id;
    const telegramMessageId = telegramData.result.message_id;

    // Get file path
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`
    );

    if (!getFileResponse.ok) {
      throw new AppError('Failed to get file path', 'TELEGRAM_GETFILE_FAILED');
    }

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new AppError('Invalid getFile response', 'TELEGRAM_GETFILE_INVALID');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store chunk metadata
    const chunkKey = `${fileId}_chunk_${chunkIndex}`;
    const chunkMetadata = {
      telegramFileId,
      telegramMessageId,
      directUrl,
      urlExpiresAt: Date.now() + (50 * 60 * 1000), // 50 min
      size: chunkData.length,
      index: chunkIndex,
      parentFileId: fileId,
      kvNamespace: kvNamespace.name,
      kvIndex: kvNamespace.index,
      uploadedAt: Date.now(),
      uploadId,
      version: '5.0'
    };

    await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

    return {
      telegramFileId,
      telegramMessageId,
      size: chunkData.length,
      directUrl,
      kvNamespace: kvNamespace.name,
      kvIndex: kvNamespace.index,
      chunkKey,
      uploadedAt: Date.now()
    };

  } catch (error) {
    clearTimeout(timeout);
    
    if (error.name === 'AbortError') {
      throw new AppError('Upload timeout', 'TIMEOUT');
    }
    
    throw error;
  }
}

async function cleanupChunks(chunks, botToken, channelId) {
  if (!chunks || chunks.length === 0) return;

  const deletePromises = chunks.map(async (chunk) => {
    try {
      await fetch(
        `https://api.telegram.org/bot${botToken}/deleteMessage`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: channelId,
            message_id: chunk.telegramMessageId
          })
        }
      );
    } catch (err) {
      console.warn(`Delete msg ${chunk.telegramMessageId} failed`);
    }
  });

  await Promise.allSettled(deletePromises);
}

async function generateChecksum(chunkResults) {
  const data = chunkResults.map(r => `${r.telegramFileId}:${r.size}`).join('|');
  const buffer = new TextEncoder().encode(data);
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('').slice(0, 16);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}