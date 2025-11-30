// /functions/upload.js - Fixed Version with Better MIME Support
export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== OPTIMIZED UPLOAD START ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: { message: 'Method not allowed' }
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    // Get all KV namespaces
    const kvNamespaces = [];
    for (let i = 1; i <= 25; i++) {
      const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
      if (env[kvKey]) {
        kvNamespaces.push({ kv: env[kvKey], name: kvKey });
      }
    }

    console.log(`Available KV: ${kvNamespaces.length}`);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing BOT_TOKEN or CHANNEL_ID');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces bound');
    }

    // Check content-length header first to avoid loading large files
    const contentLength = parseInt(request.headers.get('content-length') || '0');
    const CHUNK_SIZE = 18 * 1024 * 1024; // 18MB per chunk
    const MAX_FILE_SIZE = kvNamespaces.length * CHUNK_SIZE;

    console.log(`Content-Length: ${formatBytes(contentLength)}`);

    if (contentLength > MAX_FILE_SIZE) {
      throw new Error(
        `File too large: ${formatBytes(contentLength)}. Max: ${formatBytes(MAX_FILE_SIZE)} ` +
        `(${kvNamespaces.length} KV × 18MB)`
      );
    }

    // Parse form data
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    // Enhanced MIME type detection
    const detectedMimeType = await detectMimeType(file);
    const finalMimeType = detectedMimeType || file.type || 'application/octet-stream';
    
    console.log(`File: ${file.name}, Size: ${file.size} bytes, MIME: ${finalMimeType}`);

    if (file.size === 0) {
      throw new Error('File is empty');
    }

    if (file.size > MAX_FILE_SIZE) {
      throw new Error(
        `File too large: ${formatBytes(file.size)}. Max: ${formatBytes(MAX_FILE_SIZE)}`
      );
    }

    // Generate file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = getFileExtension(file.name, finalMimeType);

    console.log(`File ID: ${fileId}, Extension: ${extension}`);

    // Calculate chunks
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    console.log(`Total chunks: ${totalChunks}`);

    if (totalChunks > kvNamespaces.length) {
      throw new Error(`Need ${totalChunks} KV, only ${kvNamespaces.length} available`);
    }

    // Load file buffer once
    const fileBuffer = await file.arrayBuffer();
    console.log('File buffered');

    const uploadStartTime = Date.now();
    const chunkResults = [];
    let consecutiveFailures = 0;

    // Sequential upload with better error handling
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);

      const chunkBuffer = fileBuffer.slice(start, end);
      const chunkBlob = new Blob([chunkBuffer], { type: finalMimeType });
      const chunkFile = new File(
        [chunkBlob], 
        `${file.name}.part${i}`, 
        { type: finalMimeType }
      );

      const targetKV = kvNamespaces[i % kvNamespaces.length];

      console.log(`Uploading chunk ${i + 1}/${totalChunks} (${formatBytes(chunkFile.size)}) to ${targetKV.name}`);

      try {
        const result = await uploadChunkWithRetry(
          chunkFile,
          fileId,
          i,
          BOT_TOKEN,
          CHANNEL_ID,
          targetKV,
          3
        );

        chunkResults.push(result);
        consecutiveFailures = 0;

        // Adaptive delay based on chunk size and success rate
        if (i < totalChunks - 1) {
          const delay = chunkFile.size > 10 * 1024 * 1024 ? 300 : 200;
          await sleep(delay);
        }

      } catch (error) {
        consecutiveFailures++;
        console.error(`Chunk ${i} failed:`, error.message);

        if (consecutiveFailures >= 3) {
          throw new Error(`Upload aborted: ${consecutiveFailures} consecutive failures`);
        }

        throw error;
      }
    }

    const uploadDuration = ((Date.now() - uploadStartTime) / 1000).toFixed(2);
    console.log(`Upload completed in ${uploadDuration}s`);

    // Store master metadata
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: finalMimeType,
      extension: extension,
      uploadedAt: Date.now(),
      uploadDuration: parseFloat(uploadDuration),
      type: 'multi_kv_sequential',
      version: '3.1',
      totalChunks: totalChunks,
      chunks: chunkResults.map((r, i) => ({
        index: i,
        kvNamespace: r.kvNamespace,
        telegramFileId: r.telegramFileId,
        telegramMessageId: r.telegramMessageId,
        size: r.size,
        chunkKey: r.chunkKey,
        uploadedAt: r.uploadedAt
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    // Generate URLs
    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
    const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?stream=1`;

    const result = {
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
          view: viewUrl,
          download: downloadUrl,
          stream: streamUrl
        },
        storage: {
          strategy: 'multi_kv_sequential',
          totalChunks: totalChunks,
          kvDistribution: chunkResults.map(r => r.kvNamespace),
          redundancy: 'distributed'
        },
        uploadedAt: new Date().toISOString()
      },
      timestamp: new Date().toISOString()
    };

    console.log('✅ UPLOAD SUCCESS');

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'X-File-Id': fileId,
        ...corsHeaders
      }
    });

  } catch (error) {
    console.error('❌ UPLOAD ERROR:', error.message);

    // Better error messages
    let statusCode = 500;
    let errorMessage = error.message;

    if (error.message.includes('too large') || error.message.includes('exceeds')) {
      statusCode = 413;
      errorMessage = error.message + ' - Use client-side chunking for large files';
    } else if (error.message.includes('Telegram')) {
      statusCode = 502;
    }

    return new Response(JSON.stringify({
      success: false,
      error: {
        message: errorMessage,
        type: error.name || 'UploadError',
        statusCode: statusCode,
        timestamp: new Date().toISOString()
      }
    }), {
      status: statusCode,
      headers: { 'Content-Type': 'application/json; charset=utf-8', ...corsHeaders }
    });
  }
}

// Enhanced MIME type detection from file header
async function detectMimeType(file) {
  try {
    const header = await file.slice(0, 12).arrayBuffer();
    const bytes = new Uint8Array(header);
    
    // Check file signatures
    const signatures = {
      // Images
      'ffd8ff': 'image/jpeg',
      '89504e47': 'image/png',
      '47494638': 'image/gif',
      '52494646': 'image/webp', // RIFF
      '424d': 'image/bmp',
      '49492a00': 'image/tiff',
      '4d4d002a': 'image/tiff',
      
      // Videos
      '000000': 'video/mp4', // ftyp
      '1a45dfa3': 'video/webm',
      '664c7643': 'video/x-flv',
      '3026b275': 'video/x-ms-wmv',
      
      // Audio
      '494433': 'audio/mpeg', // MP3 with ID3
      'fffb': 'audio/mpeg', // MP3
      'fff3': 'audio/mpeg', // MP3
      '4f676753': 'audio/ogg',
      '664c6143': 'audio/flac',
      
      // Documents
      '25504446': 'application/pdf',
      '504b0304': 'application/zip',
      'd0cf11e0': 'application/msword',
      '377abcaf': 'application/x-7z-compressed',
      '526172211a07': 'application/x-rar-compressed',
      
      // Archives
      '1f8b08': 'application/gzip',
      '425a68': 'application/x-bzip2'
    };

    // Convert bytes to hex string
    let hex = '';
    for (let i = 0; i < Math.min(bytes.length, 8); i++) {
      hex += bytes[i].toString(16).padStart(2, '0');
    }

    // Check signatures
    for (const [signature, mimeType] of Object.entries(signatures)) {
      if (hex.startsWith(signature)) {
        console.log(`Detected MIME from signature: ${mimeType}`);
        return mimeType;
      }
    }

    // Special check for MP4 (look for ftyp)
    if (bytes.length >= 8) {
      const ftypCheck = String.fromCharCode(...bytes.slice(4, 8));
      if (ftypCheck === 'ftyp') {
        console.log('Detected MIME from ftyp: video/mp4');
        return 'video/mp4';
      }
    }

  } catch (error) {
    console.error('MIME detection error:', error.message);
  }

  return null;
}

// Get file extension from name or MIME type
function getFileExtension(filename, mimeType) {
  // Try to get from filename first
  if (filename && filename.includes('.')) {
    return filename.slice(filename.lastIndexOf('.'));
  }

  // Fallback to MIME type
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

  return mimeToExt[mimeType] || '';
}

// Upload chunk with retry
async function uploadChunkWithRetry(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, maxRetries = 3) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Chunk ${chunkIndex}: Attempt ${attempt}/${maxRetries}`);
      return await uploadChunkToTelegram(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace);
    } catch (error) {
      console.error(`Chunk ${chunkIndex} attempt ${attempt} failed:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        // Exponential backoff with jitter
        const baseDelay = Math.min(1000 * Math.pow(2, attempt - 1), 5000);
        const jitter = Math.random() * 1000;
        const delay = baseDelay + jitter;
        console.log(`Retrying after ${delay.toFixed(0)}ms...`);
        await sleep(delay);
      }
    }
  }

  throw new Error(`Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${lastError.message}`);
}

// Upload chunk to Telegram
async function uploadChunkToTelegram(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  const uploadStart = Date.now();

  // Create form data
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);
  telegramForm.append('caption', `Chunk ${chunkIndex} - ${fileId}`);

  // Upload with timeout
  const controller = new AbortController();
  const timeoutMs = chunkFile.size > 10 * 1024 * 1024 ? 90000 : 60000; // 90s for large chunks
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
      throw new Error(`Telegram ${telegramResponse.status}: ${errorText.slice(0, 150)}`);
    }

    const telegramData = await telegramResponse.json();

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response structure');
    }

    const telegramFileId = telegramData.result.document.file_id;
    const telegramMessageId = telegramData.result.message_id;

    console.log(`Chunk ${chunkIndex} Telegram ID: ${telegramFileId}`);

    // Get file path
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`
    );

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Failed to get Telegram file path');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store in KV
    const chunkKey = `${fileId}_chunk_${chunkIndex}`;
    const chunkMetadata = {
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
      version: '3.1'
    };

    await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

    const duration = ((Date.now() - uploadStart) / 1000).toFixed(2);
    console.log(`✅ Chunk ${chunkIndex} → ${kvNamespace.name} (${duration}s)`);

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
    clearTimeout(timeout);
    if (error.name === 'AbortError') {
      throw new Error(`Chunk ${chunkIndex} upload timeout after ${timeoutMs/1000}s`);
    }
    throw error;
  }
}

// Helper: Sleep
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper: Format bytes
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}