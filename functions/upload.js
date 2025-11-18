// /functions/upload.js - TRUE STREAMING UPLOAD (Memory Optimized)
export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Upload-ID'
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

  let uploadedChunks = []; // Track for cleanup
  const uploadId = crypto.randomUUID();

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // Load KV namespaces with health check
    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey, index: i });
      }
    }

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new AppError('Missing BOT_TOKEN, CHANNEL_ID, or KV namespaces', 'CONFIG_ERROR');
    }

    // TRUE STREAMING: Parse multipart boundary manually without loading full file
    const contentType = request.headers.get('content-type') || '';
    const boundaryMatch = contentType.match(/boundary=(.+?)(?:;|$)/i);
    
    if (!boundaryMatch) {
      throw new AppError('Missing multipart boundary', 'INVALID_REQUEST');
    }

    const boundary = boundaryMatch[1];
    const reader = request.body.getReader();
    
    // Extract metadata from multipart headers (filename, size, type)
    const { filename, filesize, mimetype } = await extractFileMetadata(reader, boundary);

    if (!filename) {
      throw new AppError('No filename provided', 'INVALID_FILE');
    }

    // Validate file
    const validation = validateFile(filename, filesize, mimetype);
    if (!validation.valid) {
      throw new AppError(validation.error, 'VALIDATION_FAILED');
    }

    const CHUNK_SIZE = 15 * 1024 * 1024; // 15MB (safer for Telegram 50MB limit)
    const MAX_FILE_SIZE = kvNamespaces.length * CHUNK_SIZE;

    if (filesize > MAX_FILE_SIZE) {
      throw new AppError(
        `File too large: ${formatBytes(filesize)}. Max: ${formatBytes(MAX_FILE_SIZE)}`,
        'FILE_TOO_LARGE'
      );
    }

    if (filesize === 0) {
      throw new AppError('File is empty', 'EMPTY_FILE');
    }

    // Generate file ID with collision prevention
    const timestamp = Date.now().toString(36);
    const random = crypto.randomUUID().split('-')[0];
    const fileId = `id${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    const totalChunks = Math.ceil(filesize / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length) {
      throw new AppError(
        `Need ${totalChunks} KV namespaces, only ${kvNamespaces.length} available`,
        'INSUFFICIENT_KV'
      );
    }

    console.log(`[${uploadId}] Starting: ${filename} (${formatBytes(filesize)}, ${totalChunks} chunks)`);

    const uploadStartTime = Date.now();
    const chunkResults = [];

    // TRUE STREAMING: Process chunks without buffering entire file
    let chunkIndex = 0;
    let bytesProcessed = 0;
    let buffer = new Uint8Array(0);
    let eof = false;

    while (!eof && chunkIndex < totalChunks) {
      // Read data efficiently until we have enough for a chunk
      while (buffer.length < CHUNK_SIZE && !eof) {
        const { done, value } = await reader.read();
        
        if (done) {
          eof = true;
          break;
        }

        if (value && value.length > 0) {
          // Efficient buffer append using typed array views
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

        // Select KV using round-robin for even distribution
        const targetKV = kvNamespaces[chunkIndex % kvNamespaces.length];

        console.log(`[${uploadId}] Uploading chunk ${chunkIndex + 1}/${totalChunks} (${formatBytes(chunkData.length)})`);

        try {
          const result = await uploadChunkWithRetry(
            chunkData,
            filename,
            mimetype,
            fileId,
            chunkIndex,
            BOT_TOKEN,
            CHANNEL_ID,
            targetKV,
            uploadId,
            5 // Increased retries
          );

          chunkResults.push(result);
          uploadedChunks.push(result); // Track for cleanup
          bytesProcessed += chunkData.length;

          // Adaptive delay based on chunk size and success rate
          const successRate = chunkResults.length / (chunkIndex + 1);
          const delay = successRate < 0.8 ? 500 : 150; // Longer delay if failures
          await sleep(delay);

        } catch (chunkError) {
          console.error(`[${uploadId}] Chunk ${chunkIndex} failed:`, chunkError.message);
          
          // Cleanup uploaded chunks on failure
          await cleanupChunks(uploadedChunks, BOT_TOKEN, CHANNEL_ID);
          
          throw new AppError(
            `Chunk ${chunkIndex + 1} upload failed: ${chunkError.message}`,
            'CHUNK_UPLOAD_FAILED'
          );
        }

        chunkIndex++;
      }
    }

    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);
    const uploadSpeed = (filesize / 1024 / 1024) / parseFloat(uploadDuration);

    console.log(`[${uploadId}] Completed in ${uploadDuration}s (${uploadSpeed.toFixed(2)} MB/s)`);

    // Store master metadata with checksum
    const masterMetadata = {
      filename,
      size: filesize,
      contentType: mimetype,
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
        uploadedAt: r.uploadedAt,
        lastVerified: r.uploadedAt
      }))
    };

    // Store in primary KV with backup
    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));
    
    // Backup metadata in last KV for redundancy
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
        filename,
        size: filesize,
        sizeFormatted: formatBytes(filesize),
        contentType: mimetype,
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

    // Cleanup on error
    if (uploadedChunks.length > 0) {
      console.log(`[${uploadId}] Cleaning up ${uploadedChunks.length} uploaded chunks...`);
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

// Custom error class for better error handling
class AppError extends Error {
  constructor(message, code) {
    super(message);
    this.code = code;
    this.name = 'AppError';
  }
}

// Extract metadata without loading full file
async function extractFileMetadata(reader, boundary) {
  let buffer = new Uint8Array(0);
  let headersParsed = false;
  let filename = '';
  let filesize = 0;
  let mimetype = 'application/octet-stream';

  // Read only headers (first ~2KB usually enough)
  while (!headersParsed && buffer.length < 2048) {
    const { done, value } = await reader.read();
    if (done) break;

    const newBuffer = new Uint8Array(buffer.length + value.length);
    newBuffer.set(buffer, 0);
    newBuffer.set(value, buffer.length);
    buffer = newBuffer;

    const text = new TextDecoder().decode(buffer);
    
    // Parse Content-Disposition header
    const filenameMatch = text.match(/filename="([^"]+)"/);
    if (filenameMatch) filename = filenameMatch[1];

    // Parse Content-Type
    const mimeMatch = text.match(/Content-Type:s*([^
]+)/i);
    if (mimeMatch) mimetype = mimeMatch[1].trim();

    // Check if headers complete (double CRLF)
    if (text.includes('

')) {
      headersParsed = true;
    }
  }

  // Note: filesize from Content-Length if available
  return { filename, filesize: 0, mimetype }; // filesize calculated during stream
}

// Validate file before upload
function validateFile(filename, filesize, mimetype) {
  const allowedExtensions = [
    '.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg',
    '.mp4', '.webm', '.mkv', '.avi', '.mov',
    '.mp3', '.wav', '.ogg', '.flac',
    '.pdf', '.doc', '.docx', '.txt', '.zip', '.rar', '.7z',
    '.json', '.xml', '.csv', '.js', '.py', '.java'
  ];

  const ext = filename.slice(filename.lastIndexOf('.')).toLowerCase();
  
  if (!allowedExtensions.includes(ext)) {
    return { valid: false, error: `File type ${ext} not allowed` };
  }

  const dangerousPatterns = ['..', '/', '\\', '<', '>', '|', ':'];
  if (dangerousPatterns.some(p => filename.includes(p))) {
    return { valid: false, error: 'Invalid filename characters' };
  }

  return { valid: true };
}

// Upload single chunk with exponential backoff
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
      console.warn(`[${uploadId}] Chunk ${chunkIndex} attempt ${attempt}/${maxRetries} failed: ${error.message}`);

      if (attempt < maxRetries) {
        // Exponential backoff with jitter
        const baseDelay = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
        const jitter = Math.random() * 1000;
        const delay = baseDelay + jitter;
        
        console.log(`[${uploadId}] Retrying chunk ${chunkIndex} in ${(delay/1000).toFixed(1)}s...`);
        await sleep(delay);
      }
    }
  }

  throw new AppError(
    `Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${lastError.message}`,
    'CHUNK_RETRY_EXHAUSTED'
  );
}

// Upload chunk to Telegram
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
  const chunkBlob = new Blob([chunkData], { type: mimetype });
  const chunkFilename = `${fileId}_${chunkIndex}_${filename}.part`;
  const chunkFile = new File([chunkBlob], chunkFilename, { type: mimetype });

  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `📦 Chunk ${chunkIndex}
🆔 ${fileId}
📄 ${filename}`);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120000); // 2 min

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
        `Telegram API error ${telegramResponse.status}: ${errorText}`,
        'TELEGRAM_API_ERROR'
      );
    }

    const telegramData = await telegramResponse.json();
    
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new AppError('Invalid Telegram response structure', 'TELEGRAM_INVALID_RESPONSE');
    }

    const telegramFileId = telegramData.result.document.file_id;
    const telegramMessageId = telegramData.result.message_id;

    // Get file path (expires in 1 hour)
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`
    );

    if (!getFileResponse.ok) {
      throw new AppError('Failed to get Telegram file path', 'TELEGRAM_GETFILE_FAILED');
    }

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new AppError('Invalid getFile response', 'TELEGRAM_GETFILE_INVALID');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store chunk metadata with refresh capability
    const chunkKey = `${fileId}_chunk_${chunkIndex}`;
    const chunkMetadata = {
      telegramFileId,
      telegramMessageId,
      directUrl,
      urlExpiresAt: Date.now() + (50 * 60 * 1000), // Refresh before 1 hour
      size: chunkData.length,
      index: chunkIndex,
      parentFileId: fileId,
      kvNamespace: kvNamespace.name,
      kvIndex: kvNamespace.index,
      uploadedAt: Date.now(),
      lastRefreshed: Date.now(),
      refreshCount: 0,
      uploadId,
      version: '5.0'
    };

    await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

    console.log(`[${uploadId}] ✓ Chunk ${chunkIndex} uploaded to ${kvNamespace.name}`);

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
      throw new AppError('Telegram upload timeout (2 min)', 'TELEGRAM_TIMEOUT');
    }
    
    throw error;
  }
}

// Cleanup uploaded chunks on failure
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
      console.warn(`Failed to delete message ${chunk.telegramMessageId}:`, err.message);
    }
  });

  await Promise.allSettled(deletePromises);
}

// Generate checksum for integrity verification
async function generateChecksum(chunkResults) {
  const data = chunkResults.map(r => `${r.telegramFileId}:${r.size}`).join('|');
  const buffer = new TextEncoder().encode(data);
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
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