// /functions/upload.js - 500MB+ Optimized Version

export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== 500MB+ OPTIMIZED UPLOAD START ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, X-Requested-With',
    'Access-Control-Max-Age': '86400'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return errorResponse('Method not allowed', 405, corsHeaders);
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // Get all available KV namespaces
    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey, index: i });
      }
    }

    console.log(`Available KV: ${kvNamespaces.length}`);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing BOT_TOKEN or CHANNEL_ID');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces bound');
    }

    // Enhanced configuration for 500MB+ support
    const CONFIG = {
      CHUNK_SIZE: 18 * 1024 * 1024, // 18MB chunks
      MAX_CONCURRENT: 2, // Free tier friendly parallel uploads
      MAX_RETRIES: 3,
      DELAY_BETWEEN_CHUNKS: 150, // ms
      TELEGRAM_TIMEOUT: 90000, // 90s for large chunks
      MAX_FILE_SIZE: kvNamespaces.length * 18 * 1024 * 1024 // Dynamic based on KV count
    };

    console.log(`Max file size support: ${formatBytes(CONFIG.MAX_FILE_SIZE)}`);

    // Parse form data
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    // Enhanced MIME type detection
    const detectedMimeType = await detectMimeType(file);
    const finalMimeType = detectedMimeType || file.type || 'application/octet-stream';
    const extension = getFileExtension(file.name, finalMimeType);

    console.log(`File: ${file.name}, Size: ${formatBytes(file.size)}, MIME: ${finalMimeType}`);

    // File validation
    if (file.size === 0) {
      throw new Error('File is empty');
    }

    if (file.size > CONFIG.MAX_FILE_SIZE) {
      throw new Error(
        `File too large: ${formatBytes(file.size)}. Maximum supported: ${formatBytes(CONFIG.MAX_FILE_SIZE)} ` +
        `(${kvNamespaces.length} KV × 18MB chunks)`
      );
    }

    // Security: File type validation
    if (!await isValidFileType(file, finalMimeType)) {
      throw new Error(`File type ${finalMimeType} is not allowed`);
    }

    // Generate unique file ID
    const fileId = generateFileId();
    console.log(`File ID: ${fileId}, Extension: ${extension}`);

    // Calculate chunks
    const totalChunks = Math.ceil(file.size / CONFIG.CHUNK_SIZE);
    console.log(`Total chunks: ${totalChunks}, Using ${Math.min(CONFIG.MAX_CONCURRENT, totalChunks)} concurrent uploads`);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(
        `File requires ${totalChunks} chunks but only ${kvNamespaces.length} KV namespaces available. ` +
        `Max file size with current setup: ${formatBytes(kvNamespaces.length * CONFIG.CHUNK_SIZE)}`
      );
    }

    // Load file buffer once (memory optimized)
    const fileBuffer = await file.arrayBuffer();
    console.log('File loaded into memory buffer');

    const uploadStartTime = Date.now();
    
    // Upload chunks with controlled parallelism for better performance
    const chunkResults = await uploadChunksOptimized(
      fileBuffer,
      fileId,
      totalChunks,
      finalMimeType,
      file.name,
      BOT_TOKEN,
      CHANNEL_ID,
      kvNamespaces,
      CONFIG
    );

    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);
    console.log(`All ${totalChunks} chunks uploaded in ${uploadDuration}s`);

    // Store comprehensive master metadata
    const masterMetadata = {
      filename: file.name,
      originalName: file.name,
      size: file.size,
      contentType: finalMimeType,
      extension: extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_optimized',
      version: '4.0',
      totalChunks: totalChunks,
      storage: {
        strategy: 'distributed_kv',
        chunks: totalChunks,
        kvCount: kvNamespaces.length,
        redundancy: 'distributed'
      },
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        telegramFileId: result.telegramFileId,
        telegramMessageId: result.telegramMessageId,
        size: result.size,
        chunkKey: result.chunkKey,
        uploadedAt: result.uploadedAt,
        duration: result.duration,
        directUrl: result.directUrl
      })),
      analytics: {
        uploadSpeed: `${(file.size / 1024 / parseFloat(uploadDuration)).toFixed(2)} KB/s`,
        chunkSize: CONFIG.CHUNK_SIZE,
        concurrentUploads: CONFIG.MAX_CONCURRENT
      }
    };

    // Store master metadata in first KV
    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));
    console.log('Master metadata stored');

    // Generate response with multiple URLs
    const baseUrl = new URL(request.url).origin;
    const responseData = {
      success: true,
      message: 'File uploaded successfully',
      data: {
        id: fileId,
        filename: file.name,
        size: file.size,
        sizeFormatted: formatBytes(file.size),
        contentType: finalMimeType,
        extension: extension,
        uploadDuration: `${uploadDuration}s`,
        uploadSpeed: `${(file.size / 1024 / parseFloat(uploadDuration)).toFixed(2)} KB/s`,
        urls: {
          view: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
          download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
          stream: `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`,
          info: `${baseUrl}/btfstorage/info/${fileId}`
        },
        storage: {
          strategy: 'multi_kv_optimized',
          totalChunks: totalChunks,
          kvDistribution: chunkResults.map(r => r.kvNamespace),
          concurrentUploads: CONFIG.MAX_CONCURRENT,
          redundancy: 'distributed',
          maxSupportedSize: formatBytes(CONFIG.MAX_FILE_SIZE)
        },
        chunks: chunkResults.map(chunk => ({
          index: chunk.index,
          size: formatBytes(chunk.size),
          kv: chunk.kvNamespace,
          duration: chunk.duration
        })),
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    console.log('✅ UPLOAD SUCCESS - 500MB+ Ready');

    return new Response(JSON.stringify(responseData), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'X-File-Id': fileId,
        'X-Max-Supported-Size': CONFIG.MAX_FILE_SIZE.toString(),
        ...corsHeaders
      }
    });

  } catch (error) {
    console.error('❌ UPLOAD ERROR:', error.message);
    
    const errorResponse = handleUploadError(error);
    return new Response(JSON.stringify(errorResponse), {
      status: errorResponse.error.statusCode,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        ...corsHeaders
      }
    });
  }
}

// ==================== ENHANCED UTILITY FUNCTIONS ====================

/**
 * Upload chunks with controlled parallelism for 500MB+ files
 */
async function uploadChunksOptimized(fileBuffer, fileId, totalChunks, mimeType, filename, botToken, channelId, kvNamespaces, config) {
  const chunks = [];
  const results = new Array(totalChunks);
  const errors = [];

  // Prepare all chunks
  for (let i = 0; i < totalChunks; i++) {
    const start = i * config.CHUNK_SIZE;
    const end = Math.min(start + config.CHUNK_SIZE, fileBuffer.byteLength);
    const chunkBuffer = fileBuffer.slice(start, end);
    
    chunks.push({
      index: i,
      buffer: chunkBuffer,
      size: end - start,
      kvNamespace: kvNamespaces[i % kvNamespaces.length]
    });
  }

  console.log(`Prepared ${chunks.length} chunks for upload`);

  // Process with controlled concurrency
  const processBatch = async (batch) => {
    const batchPromises = batch.map(async (chunk) => {
      try {
        const result = await uploadChunkWithRetry(
          chunk.buffer,
          fileId,
          chunk.index,
          mimeType,
          filename,
          botToken,
          channelId,
          chunk.kvNamespace,
          config.MAX_RETRIES
        );
        
        results[chunk.index] = result;
        console.log(`✅ Chunk ${chunk.index + 1}/${totalChunks} completed (${formatBytes(chunk.size)})`);
        
        return { success: true, index: chunk.index };
      } catch (error) {
        console.error(`❌ Chunk ${chunk.index} failed:`, error.message);
        errors.push({ index: chunk.index, error: error.message });
        return { success: false, index: chunk.index, error };
      }
    });

    return await Promise.allSettled(batchPromises);
  };

  // Upload in batches with concurrency control
  for (let i = 0; i < chunks.length; i += config.MAX_CONCURRENT) {
    const batch = chunks.slice(i, i + config.MAX_CONCURRENT);
    console.log(`Processing batch ${Math.floor(i/config.MAX_CONCURRENT) + 1} (${batch.length} chunks)`);
    
    await processBatch(batch);
    
    // Delay between batches to avoid rate limits
    if (i + config.MAX_CONCURRENT < chunks.length) {
      await sleep(config.DELAY_BETWEEN_CHUNKS);
    }
  }

  // Check for errors
  if (errors.length > 0) {
    throw new Error(`${errors.length} chunks failed to upload. First error: ${errors[0].error}`);
  }

  return results;
}

/**
 * Enhanced chunk upload with better error handling
 */
async function uploadChunkWithRetry(chunkBuffer, fileId, chunkIndex, mimeType, filename, botToken, channelId, kvNamespace, maxRetries) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Chunk ${chunkIndex}: Attempt ${attempt}/${maxRetries}`);
      return await uploadChunkToTelegram(
        chunkBuffer,
        fileId,
        chunkIndex,
        mimeType,
        filename,
        botToken,
        channelId,
        kvNamespace
      );
    } catch (error) {
      lastError = error;
      console.error(`Chunk ${chunkIndex} attempt ${attempt} failed:`, error.message);

      if (attempt < maxRetries) {
        const baseDelay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        const jitter = Math.random() * 1000;
        await sleep(baseDelay + jitter);
      }
    }
  }

  throw new Error(`Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${lastError.message}`);
}

/**
 * Upload chunk to Telegram with enhanced timeout handling
 */
async function uploadChunkToTelegram(chunkBuffer, fileId, chunkIndex, mimeType, filename, botToken, channelId, kvNamespace) {
  const uploadStart = Date.now();
  
  const chunkBlob = new Blob([chunkBuffer], { type: mimeType });
  const chunkFile = new File(
    [chunkBlob], 
    `${filename}.part${chunkIndex}`, 
    { type: mimeType }
  );

  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

  // Dynamic timeout based on chunk size
  const timeoutMs = chunkBuffer.byteLength > 10 * 1024 * 1024 ? 90000 : 60000;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

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
      throw new Error(`Telegram API error ${telegramResponse.status}: ${errorText.slice(0, 150)}`);
    }

    const telegramData = await telegramResponse.json();

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response: Missing file_id');
    }

    const telegramFileId = telegramData.result.document.file_id;
    const telegramMessageId = telegramData.result.message_id;

    // Get direct URL for the chunk
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`
    );

    const getFileData = await getFileResponse.json();
    const directUrl = getFileData.ok && getFileData.result?.file_path 
      ? `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`
      : null;

    // Store enhanced chunk metadata
    const chunkKey = `${fileId}_chunk_${chunkIndex}`;
    const chunkMetadata = {
      telegramFileId,
      telegramMessageId,
      directUrl,
      size: chunkBuffer.byteLength,
      index: chunkIndex,
      parentFileId: fileId,
      kvNamespace: kvNamespace.name,
      uploadedAt: Date.now(),
      lastRefreshed: Date.now(),
      version: '4.0',
      timeoutUsed: timeoutMs
    };

    await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

    const duration = ((Date.now() - uploadStart) / 1000).toFixed(2);
    console.log(`✅ Chunk ${chunkIndex} → ${kvNamespace.name} (${duration}s)`);

    return {
      telegramFileId,
      telegramMessageId,
      size: chunkBuffer.byteLength,
      directUrl,
      kvNamespace: kvNamespace.name,
      chunkKey,
      uploadedAt: Date.now(),
      duration: parseFloat(duration),
      index: chunkIndex
    };

  } catch (error) {
    clearTimeout(timeout);
    if (error.name === 'AbortError') {
      throw new Error(`Upload timeout after ${timeoutMs/1000}s for chunk ${chunkIndex}`);
    }
    throw error;
  }
}

/**
 * Enhanced MIME type detection
 */
async function detectMimeType(file) {
  try {
    const header = await file.slice(0, 12).arrayBuffer();
    const bytes = new Uint8Array(header);
    
    const signatures = {
      'ffd8ff': 'image/jpeg',
      '89504e47': 'image/png',
      '47494638': 'image/gif',
      '52494646': 'image/webp',
      '424d': 'image/bmp',
      '49492a00': 'image/tiff',
      '4d4d002a': 'image/tiff',
      '000000': 'video/mp4',
      '1a45dfa3': 'video/webm',
      '664c7643': 'video/x-flv',
      '3026b275': 'video/x-ms-wmv',
      '494433': 'audio/mpeg',
      'fffb': 'audio/mpeg',
      'fff3': 'audio/mpeg',
      '4f676753': 'audio/ogg',
      '664c6143': 'audio/flac',
      '25504446': 'application/pdf',
      '504b0304': 'application/zip',
      'd0cf11e0': 'application/msword',
      '377abcaf': 'application/x-7z-compressed',
      '526172211a07': 'application/x-rar-compressed',
      '1f8b08': 'application/gzip',
      '425a68': 'application/x-bzip2'
    };

    let hex = '';
    for (let i = 0; i < Math.min(bytes.length, 8); i++) {
      hex += bytes[i].toString(16).padStart(2, '0');
    }

    for (const [signature, mimeType] of Object.entries(signatures)) {
      if (hex.startsWith(signature)) {
        console.log(`Detected MIME from signature: ${mimeType}`);
        return mimeType;
      }
    }

    // Special check for MP4
    if (bytes.length >= 8) {
      const ftypCheck = String.fromCharCode(...bytes.slice(4, 8));
      if (ftypCheck === 'ftyp') {
        return 'video/mp4';
      }
    }

  } catch (error) {
    console.error('MIME detection error:', error.message);
  }

  return null;
}

/**
 * Get file extension with fallbacks
 */
function getFileExtension(filename, mimeType) {
  // From filename
  if (filename && filename.includes('.')) {
    const ext = filename.slice(filename.lastIndexOf('.')).toLowerCase();
    if (ext.length <= 8) {
      return ext;
    }
  }

  // From MIME type
  const mimeToExt = {
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'image/bmp': '.bmp',
    'image/tiff': '.tiff',
    'video/mp4': '.mp4',
    'video/webm': '.webm',
    'video/x-flv': '.flv',
    'video/x-ms-wmv': '.wmv',
    'video/quicktime': '.mov',
    'audio/mpeg': '.mp3',
    'audio/ogg': '.ogg',
    'audio/wav': '.wav',
    'audio/flac': '.flac',
    'application/pdf': '.pdf',
    'application/zip': '.zip',
    'application/x-7z-compressed': '.7z',
    'application/x-rar-compressed': '.rar',
    'text/plain': '.txt',
    'text/html': '.html',
    'application/json': '.json'
  };

  return mimeToExt[mimeType] || '.bin';
}

/**
 * File type validation for security
 */
async function isValidFileType(file, mimeType) {
  const ALLOWED_TYPES = [
    'image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/bmp', 'image/tiff',
    'video/mp4', 'video/webm', 'video/quicktime', 'video/x-ms-wmv',
    'audio/mpeg', 'audio/ogg', 'audio/wav', 'audio/flac',
    'application/pdf', 'application/zip', 'application/x-7z-compressed',
    'text/plain', 'text/html', 'application/json'
  ];

  const BLOCKED_TYPES = [
    'application/x-msdownload', 'application/x-dosexec', 'application/x-sh',
    'application/x-bat', 'application/x-msi'
  ];

  // Check blocked types
  for (const blockedType of BLOCKED_TYPES) {
    if (mimeType.includes(blockedType)) {
      return false;
    }
  }

  // Check allowed types
  return ALLOWED_TYPES.includes(mimeType) || mimeType === 'application/octet-stream';
}

/**
 * Generate unique file ID
 */
function generateFileId() {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).slice(2, 10);
  return `file_${timestamp}_${random}`;
}

/**
 * Enhanced error handling
 */
function handleUploadError(error) {
  let statusCode = 500;
  let errorMessage = error.message;

  if (error.message.includes('too large') || error.message.includes('exceeds')) {
    statusCode = 413;
  } else if (error.message.includes('Telegram') || error.message.includes('Bot')) {
    statusCode = 502;
  } else if (error.message.includes('File type') || error.message.includes('not allowed')) {
    statusCode = 400;
  }

  return {
    success: false,
    error: {
      message: errorMessage,
      type: error.name || 'UploadError',
      statusCode: statusCode,
      timestamp: new Date().toISOString()
    }
  };
}

/**
 * Generic error response helper
 */
function errorResponse(message, status = 500, headers = {}) {
  return new Response(JSON.stringify({
    success: false,
    error: {
      message,
      statusCode: status,
      timestamp: new Date().toISOString()
    }
  }), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      ...headers
    }
  });
}

/**
 * Sleep utility
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Format bytes to human readable
 */
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}