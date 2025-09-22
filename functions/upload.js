
// functions/upload.js
// ðŸš€ MARYA VAULT - ULTIMATE ADVANCED FILE UPLOADER
// ðŸ’ª Challenge: Support up to 2GB files with 7 KV namespaces
// ðŸŽ¯ Features: Direct URL upload, Normal file upload, Advanced chunking

const CONFIG = {
  // File size limits
  MAX_FILE_SIZE: 2 * 1024 * 1024 * 1024, // 2GB maximum
  NORMAL_MAX_SIZE: 200 * 1024 * 1024,    // 200MB for normal upload

  // Chunking strategy
  CHUNK_SIZE: 20 * 1024 * 1024,          // 20MB per chunk
  MAX_CHUNKS_PER_KV: 15,                  // Max 15 chunks per KV (300MB per KV)
  TOTAL_KV_NAMESPACES: 7,                 // 7 KV namespaces available

  // Telegram limits
  TELEGRAM_MAX_FILE_SIZE: 50 * 1024 * 1024, // 50MB Telegram limit
  TELEGRAM_TIMEOUT: 120000,               // 2 minutes timeout

  // Advanced features
  PARALLEL_UPLOADS: 3,                    // Upload 3 chunks simultaneously
  RETRY_ATTEMPTS: 5,                      // Retry failed uploads
  PROGRESS_TRACKING: true,                // Track upload progress

  // Supported file types
  SUPPORTED_TYPES: {
    video: ['mp4', 'mkv', 'avi', 'mov', 'webm', 'flv', '3gp', 'm4v', 'wmv'],
    audio: ['mp3', 'wav', 'aac', 'm4a', 'ogg', 'flac', 'wma'],
    image: ['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'bmp', 'tiff'],
    document: ['pdf', 'txt', 'doc', 'docx', 'zip', 'rar', '7z', 'tar', 'gz'],
    archive: ['zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz']
  }
};

export async function onRequest(context) {
  const { request, env } = context;

  console.log('ðŸš€ MARYA VAULT ULTIMATE UPLOADER INITIATED');
  console.log('ðŸ“Š Request URL:', request.url);
  console.log('ðŸ”— Method:', request.method);
  console.log('ðŸ“… Timestamp:', new Date().toISOString());

  // CORS headers for cross-origin requests
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Upload-Type, X-File-URL',
    'Access-Control-Max-Age': '86400'
  };

  // Handle preflight requests
  if (request.method === 'OPTIONS') {
    console.log('âœ… CORS preflight handled');
    return new Response(null, { headers: corsHeaders });
  }

  // Only allow POST for uploads
  if (request.method !== 'POST') {
    return createErrorResponse('Method not allowed - Use POST for uploads', 405, corsHeaders);
  }

  try {
    // Validate environment setup
    const envValidation = validateEnvironment(env);
    if (!envValidation.valid) {
      throw new Error(`Environment validation failed: ${envValidation.error}`);
    }

    console.log(`ðŸ”§ Environment validated: ${envValidation.kvCount} KV namespaces, ${envValidation.botCount} bot tokens`);

    // Determine upload type from headers or content
    const uploadType = request.headers.get('X-Upload-Type') || 'auto-detect';
    const fileUrl = request.headers.get('X-File-URL');

    let uploadResult;

    if (fileUrl && fileUrl.trim()) {
      // URL-based upload
      console.log('ðŸŒ URL Upload Mode Detected');
      uploadResult = await handleUrlUpload(request, env, fileUrl, corsHeaders);
    } else {
      // Form-based file upload
      console.log('ðŸ“ File Upload Mode Detected');
      uploadResult = await handleFileUpload(request, env, corsHeaders);
    }

    return uploadResult;

  } catch (error) {
    console.error('âŒ Critical upload error:', error);
    console.error('ðŸ“ Error stack:', error.stack);

    return createErrorResponse(
      `Upload failed: ${error.message}`,
      500,
      corsHeaders
    );
  }
}

/**
 * Validate environment configuration
 */
function validateEnvironment(env) {
  const requiredEnvVars = ['BOT_TOKEN', 'CHANNEL_ID'];
  const missing = requiredEnvVars.filter(key => !env[key]);

  if (missing.length > 0) {
    return {
      valid: false,
      error: `Missing environment variables: ${missing.join(', ')}`
    };
  }

  // Count available KV namespaces
  const kvNamespaces = [];
  for (let i = 1; i <= CONFIG.TOTAL_KV_NAMESPACES; i++) {
    const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
    if (env[kvKey]) {
      kvNamespaces.push({ kv: env[kvKey], name: kvKey, index: i - 1 });
    }
  }

  // Count available bot tokens
  const botTokens = [];
  for (let i = 1; i <= 4; i++) {
    const tokenKey = i === 1 ? 'BOT_TOKEN' : `BOT_TOKEN${i}`;
    if (env[tokenKey]) {
      botTokens.push(env[tokenKey]);
    }
  }

  if (kvNamespaces.length === 0) {
    return {
      valid: false,
      error: 'No KV namespaces configured'
    };
  }

  return {
    valid: true,
    kvCount: kvNamespaces.length,
    botCount: botTokens.length,
    kvNamespaces,
    botTokens
  };
}

/**
 * Handle URL-based file upload
 * Downloads file from URL and uploads to storage
 */
async function handleUrlUpload(request, env, fileUrl, corsHeaders) {
  console.log('ðŸŒ URL Upload Processing:', fileUrl);

  try {
    // Validate URL
    let url;
    try {
      url = new URL(fileUrl);
    } catch {
      throw new Error('Invalid URL provided');
    }

    // Security check - only allow HTTP/HTTPS
    if (!['http:', 'https:'].includes(url.protocol)) {
      throw new Error('Only HTTP/HTTPS URLs are supported');
    }

    console.log('ðŸ” Fetching file metadata from URL...');

    // Fetch file with HEAD request first to check size
    let headResponse;
    try {
      headResponse = await fetch(fileUrl, {
        method: 'HEAD',
        signal: AbortSignal.timeout(30000)
      });
    } catch (error) {
      console.log('âš ï¸ HEAD request failed, trying GET request');
    }

    // Get file info
    let fileSize = 0;
    let contentType = 'application/octet-stream';
    let filename = '';

    if (headResponse && headResponse.ok) {
      fileSize = parseInt(headResponse.headers.get('Content-Length') || '0');
      contentType = headResponse.headers.get('Content-Type') || contentType;

      // Extract filename from URL or Content-Disposition
      const contentDisposition = headResponse.headers.get('Content-Disposition');
      if (contentDisposition) {
        const match = contentDisposition.match(/filename[*]?=([^;\n\r"']+)/);
        if (match) filename = match[1].replace(/['"]/g, '');
      }
    }

    if (!filename) {
      const urlPath = url.pathname;
      filename = urlPath.split('/').pop() || `download_${Date.now()}`;

      // If no extension, try to guess from content-type
      if (!filename.includes('.') && contentType) {
        const ext = getExtensionFromMimeType(contentType);
        if (ext) filename += ext;
      }
    }

    console.log(`ðŸ“Š URL File Info:
    ðŸ“ Name: ${filename}
    ðŸ“ Size: ${fileSize ? Math.round(fileSize/1024/1024) : 'Unknown'}MB
    ðŸ·ï¸ Type: ${contentType}`);

    // Validate file size
    if (fileSize > CONFIG.MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(fileSize/1024/1024)}MB (max 2GB)`);
    }

    // Download file with progress tracking
    console.log('ðŸ“¥ Downloading file from URL...');
    const downloadResponse = await fetch(fileUrl, {
      signal: AbortSignal.timeout(300000) // 5 minutes for download
    });

    if (!downloadResponse.ok) {
      throw new Error(`Download failed: ${downloadResponse.status} ${downloadResponse.statusText}`);
    }

    // Create file object from downloaded data
    const arrayBuffer = await downloadResponse.arrayBuffer();
    const actualSize = arrayBuffer.byteLength;

    console.log(`âœ… File downloaded: ${Math.round(actualSize/1024/1024)}MB`);

    const file = new File([arrayBuffer], filename, { type: contentType });

    // Process the downloaded file using standard upload logic
    return await processFileUpload(file, request, env, corsHeaders, 'url');

  } catch (error) {
    console.error('âŒ URL upload error:', error);
    throw new Error(`URL upload failed: ${error.message}`);
  }
}

/**
 * Handle form-based file upload
 */
async function handleFileUpload(request, env, corsHeaders) {
  console.log('ðŸ“ File Upload Processing...');

  try {
    const contentType = request.headers.get('Content-Type') || '';
    let file;

    if (contentType.includes('multipart/form-data')) {
      // Standard form upload
      const formData = await request.formData();
      file = formData.get('file');

      if (!file) {
        // Check for alternative field names
        for (const [key, value] of formData) {
          if (value instanceof File) {
            file = value;
            console.log(`ðŸ“ File found in field: ${key}`);
            break;
          }
        }
      }
    } else if (contentType.includes('application/octet-stream') || contentType.includes('application/binary')) {
      // Direct binary upload
      const arrayBuffer = await request.arrayBuffer();
      const filename = request.headers.get('X-Filename') || `upload_${Date.now()}.bin`;
      file = new File([arrayBuffer], filename, { type: contentType });
      console.log('ðŸ“ Binary upload detected');
    }

    if (!file) {
      throw new Error('No file provided in request');
    }

    console.log(`ðŸ“ File received:
    ðŸ“ Name: ${file.name}
    ðŸ“ Size: ${Math.round(file.size/1024/1024)}MB
    ðŸ·ï¸ Type: ${file.type}`);

    return await processFileUpload(file, request, env, corsHeaders, 'file');

  } catch (error) {
    console.error('âŒ File upload error:', error);
    throw new Error(`File upload failed: ${error.message}`);
  }
}

/**
 * Process file upload with advanced chunking strategy
 */
async function processFileUpload(file, request, env, corsHeaders, uploadType) {
  console.log('ðŸŽ¯ Processing file upload with advanced strategy...');

  const envValidation = validateEnvironment(env);
  const { kvNamespaces, botTokens } = envValidation;

  // Validate file size
  if (file.size > CONFIG.MAX_FILE_SIZE) {
    throw new Error(`File too large: ${Math.round(file.size/1024/1024)}MB (max ${Math.round(CONFIG.MAX_FILE_SIZE/1024/1024)}MB)`);
  }

  // Generate advanced file ID
  const fileId = generateAdvancedFileId(file);
  const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

  console.log(`ðŸ†” Generated file ID: ${fileId}${extension}`);

  // Determine upload strategy based on file size
  let strategy;
  if (file.size <= CONFIG.NORMAL_MAX_SIZE) {
    strategy = await handleNormalUpload(file, fileId, env, botTokens[0], kvNamespaces[0]);
  } else {
    strategy = await handleChunkedUpload(file, fileId, env, botTokens, kvNamespaces);
  }

  // Create response URLs
  const baseUrl = new URL(request.url).origin;
  const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
  const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
  const hlsUrl = file.type.startsWith('video/') ? `${baseUrl}/btfstorage/file/${fileId}.m3u8` : null;

  const result = {
    success: true,
    message: 'Upload completed successfully',

    // File information
    file: {
      id: fileId,
      filename: file.name,
      size: file.size,
      sizeFormatted: formatFileSize(file.size),
      contentType: file.type,
      extension: extension,
      category: getFileCategory(file.name)
    },

    // Upload details
    upload: {
      type: uploadType,
      strategy: strategy.type,
      timestamp: new Date().toISOString(),
      processingTime: strategy.processingTime,
      chunks: strategy.chunks || 0,
      kvDistribution: strategy.kvDistribution || []
    },

    // Access URLs
    urls: {
      stream: streamUrl,
      download: downloadUrl,
      hls: hlsUrl,
      embed: `${baseUrl}/embed/${fileId}${extension}`
    },

    // Advanced features
    features: {
      instantPlay: strategy.type === 'chunked',
      hlsStreaming: file.type.startsWith('video/') && strategy.type === 'chunked',
      rangeRequests: true,
      crossOrigin: true,
      caching: true
    }
  };

  console.log(`âœ… Upload completed successfully:
  ðŸ“ File: ${file.name}
  ðŸ“ Size: ${formatFileSize(file.size)}
  ðŸŽ¯ Strategy: ${strategy.type}
  â±ï¸ Time: ${strategy.processingTime}ms
  ðŸ”— URL: ${streamUrl}`);

  return new Response(JSON.stringify(result, null, 2), {
    headers: { 
      'Content-Type': 'application/json',
      'X-Upload-ID': fileId,
      'X-Upload-Strategy': strategy.type,
      ...corsHeaders 
    }
  });
}

/**
 * Handle normal upload (files â‰¤ 200MB)
 */
async function handleNormalUpload(file, fileId, env, botToken, kvNamespace) {
  console.log('ðŸ“¤ Normal upload strategy (single file)');
  const startTime = Date.now();

  try {
    // Upload directly to Telegram
    const telegramResult = await uploadToTelegram(file, botToken, env.CHANNEL_ID);

    // Store metadata in KV
    const metadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      uploadedAt: Date.now(),
      type: 'single',
      telegramFileId: telegramResult.fileId,
      directUrl: telegramResult.directUrl,
      kvNamespace: kvNamespace.name
    };

    await kvNamespace.kv.put(fileId, JSON.stringify(metadata));

    const processingTime = Date.now() - startTime;
    console.log(`âœ… Normal upload completed in ${processingTime}ms`);

    return {
      type: 'single',
      processingTime,
      kvDistribution: [kvNamespace.name]
    };

  } catch (error) {
    console.error('âŒ Normal upload failed:', error);
    throw new Error(`Normal upload failed: ${error.message}`);
  }
}

/**
 * Handle chunked upload (files > 200MB, up to 2GB)
 */
async function handleChunkedUpload(file, fileId, env, botTokens, kvNamespaces) {
  console.log('ðŸ§© Chunked upload strategy (advanced multi-KV)');
  const startTime = Date.now();

  try {
    // Calculate optimal chunking strategy
    const totalChunks = Math.ceil(file.size / CONFIG.CHUNK_SIZE);
    const chunksPerKv = Math.ceil(totalChunks / kvNamespaces.length);

    console.log(`ðŸ“Š Chunking Strategy:
    ðŸ§© Total chunks: ${totalChunks}
    ðŸ“¦ Chunk size: ${Math.round(CONFIG.CHUNK_SIZE/1024/1024)}MB
    ðŸ—ƒï¸ KV namespaces: ${kvNamespaces.length}
    ðŸ“‹ Chunks per KV: ${chunksPerKv}
    âš¡ Parallel uploads: ${CONFIG.PARALLEL_UPLOADS}`);

    if (totalChunks > kvNamespaces.length * CONFIG.MAX_CHUNKS_PER_KV) {
      throw new Error(`File requires ${totalChunks} chunks, exceeds capacity (${kvNamespaces.length * CONFIG.MAX_CHUNKS_PER_KV} max)`);
    }

    // Create chunk upload queue with optimal distribution
    const chunkQueue = [];
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CONFIG.CHUNK_SIZE;
      const end = Math.min(start + CONFIG.CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const targetKv = kvNamespaces[i % kvNamespaces.length];
      const botToken = botTokens[i % botTokens.length];

      chunkQueue.push({
        index: i,
        chunk,
        size: chunk.size,
        kvNamespace: targetKv,
        botToken,
        fileId,
        filename: `${file.name}.part${i}`
      });
    }

    console.log('ðŸš€ Starting parallel chunk uploads...');

    // Upload chunks in parallel batches
    const chunkResults = await uploadChunksInParallel(chunkQueue, env.CHANNEL_ID);

    console.log('âœ… All chunks uploaded successfully');

    // Store master metadata
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      uploadedAt: Date.now(),
      type: 'chunked',
      totalChunks,
      chunkSize: CONFIG.CHUNK_SIZE,
      chunks: chunkResults.map((result, index) => ({
        index,
        kvNamespace: result.kvNamespace,
        keyName: result.chunkKey,
        telegramFileId: result.telegramFileId,
        size: result.size,
        directUrl: result.directUrl
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    const processingTime = Date.now() - startTime;
    const kvDistribution = [...new Set(chunkResults.map(r => r.kvNamespace))];

    console.log(`âœ… Chunked upload completed in ${processingTime}ms`);

    return {
      type: 'chunked',
      processingTime,
      chunks: totalChunks,
      kvDistribution
    };

  } catch (error) {
    console.error('âŒ Chunked upload failed:', error);
    throw new Error(`Chunked upload failed: ${error.message}`);
  }
}

/**
 * Upload chunks in parallel with retry logic
 */
async function uploadChunksInParallel(chunkQueue, channelId) {
  const results = [];
  const failedChunks = [];

  // Process chunks in parallel batches
  for (let i = 0; i < chunkQueue.length; i += CONFIG.PARALLEL_UPLOADS) {
    const batch = chunkQueue.slice(i, i + CONFIG.PARALLEL_UPLOADS);
    console.log(`ðŸ”„ Processing batch ${Math.floor(i/CONFIG.PARALLEL_UPLOADS) + 1}/${Math.ceil(chunkQueue.length/CONFIG.PARALLEL_UPLOADS)} (${batch.length} chunks)`);

    const batchPromises = batch.map(async (chunkInfo) => {
      return await uploadChunkWithRetry(chunkInfo, channelId);
    });

    try {
      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults);

      console.log(`âœ… Batch completed: ${batchResults.length} chunks uploaded`);
    } catch (error) {
      console.error('âŒ Batch failed:', error);
      failedChunks.push(...batch);
    }

    // Add small delay between batches to respect rate limits
    if (i + CONFIG.PARALLEL_UPLOADS < chunkQueue.length) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  // Retry failed chunks individually
  if (failedChunks.length > 0) {
    console.log(`ðŸ”„ Retrying ${failedChunks.length} failed chunks...`);

    for (const chunkInfo of failedChunks) {
      try {
        const result = await uploadChunkWithRetry(chunkInfo, channelId);
        results.push(result);
        console.log(`âœ… Retry successful for chunk ${chunkInfo.index}`);
      } catch (error) {
        console.error(`âŒ Final retry failed for chunk ${chunkInfo.index}:`, error);
        throw new Error(`Chunk ${chunkInfo.index} upload failed after all retries`);
      }
    }
  }

  // Sort results by chunk index
  return results.sort((a, b) => a.index - b.index);
}

/**
 * Upload single chunk with retry logic
 */
async function uploadChunkWithRetry(chunkInfo, channelId) {
  const { index, chunk, kvNamespace, botToken, fileId, filename } = chunkInfo;

  for (let attempt = 0; attempt < CONFIG.RETRY_ATTEMPTS; attempt++) {
    try {
      console.log(`ðŸ“¤ Uploading chunk ${index} (attempt ${attempt + 1}/${CONFIG.RETRY_ATTEMPTS}) to ${kvNamespace.name}`);

      // Create file for this chunk
      const chunkFile = new File([chunk], filename, { type: 'application/octet-stream' });

      // Upload to Telegram
      const telegramResult = await uploadToTelegram(chunkFile, botToken, channelId);

      // Store chunk metadata in assigned KV namespace
      const chunkKey = `${fileId}_chunk_${index}`;
      const chunkMetadata = {
        telegramFileId: telegramResult.fileId,
        directUrl: telegramResult.directUrl,
        size: chunk.size,
        index,
        parentFileId: fileId,
        kvNamespace: kvNamespace.name,
        uploadedAt: Date.now(),
        lastRefreshed: Date.now()
      };

      await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

      console.log(`âœ… Chunk ${index} uploaded successfully to ${kvNamespace.name}`);

      return {
        index,
        telegramFileId: telegramResult.fileId,
        size: chunk.size,
        directUrl: telegramResult.directUrl,
        kvNamespace: kvNamespace.name,
        chunkKey
      };

    } catch (error) {
      console.error(`âŒ Chunk ${index} upload attempt ${attempt + 1} failed:`, error);

      if (attempt === CONFIG.RETRY_ATTEMPTS - 1) {
        throw new Error(`Chunk ${index} failed after ${CONFIG.RETRY_ATTEMPTS} attempts: ${error.message}`);
      }

      // Exponential backoff
      const delay = Math.pow(2, attempt) * 1000;
      console.log(`â³ Waiting ${delay}ms before retry...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

/**
 * Upload file/chunk to Telegram
 */
async function uploadToTelegram(file, botToken, channelId) {
  console.log(`ðŸ“¤ Uploading to Telegram: ${file.name} (${formatFileSize(file.size)})`);

  try {
    // Prepare form data
    const formData = new FormData();
    formData.append('chat_id', channelId);
    formData.append('document', file);

    // Upload to Telegram
    const response = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: formData,
      signal: AbortSignal.timeout(CONFIG.TELEGRAM_TIMEOUT)
    });

    if (!response.ok) {
      throw new Error(`Telegram API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();

    if (!data.ok || !data.result?.document?.file_id) {
      throw new Error(`Invalid Telegram response: ${data.error_code || 'Unknown error'}`);
    }

    const fileId = data.result.document.file_id;

    // Get direct URL
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`,
      { signal: AbortSignal.timeout(30000) }
    );

    if (!getFileResponse.ok) {
      throw new Error(`GetFile API error: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('No file path in Telegram response');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    console.log(`âœ… Telegram upload successful: ${file.name}`);

    return {
      fileId,
      directUrl,
      size: file.size
    };

  } catch (error) {
    console.error('âŒ Telegram upload error:', error);
    throw new Error(`Telegram upload failed: ${error.message}`);
  }
}

/**
 * Generate advanced file ID with metadata encoding
 */
function generateAdvancedFileId(file) {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).slice(2, 8);
  const sizeIndicator = file.size > CONFIG.NORMAL_MAX_SIZE ? 'L' : 'S'; // L = Large, S = Small
  const typeIndicator = getFileCategory(file.name).charAt(0).toUpperCase(); // V, A, I, D, O

  return `${typeIndicator}${sizeIndicator}${timestamp}${random}`;
}

/**
 * Get file category based on extension
 */
function getFileCategory(filename) {
  const ext = filename.toLowerCase().split('.').pop();

  if (CONFIG.SUPPORTED_TYPES.video.includes(ext)) return 'video';
  if (CONFIG.SUPPORTED_TYPES.audio.includes(ext)) return 'audio';
  if (CONFIG.SUPPORTED_TYPES.image.includes(ext)) return 'image';
  if (CONFIG.SUPPORTED_TYPES.document.includes(ext)) return 'document';
  if (CONFIG.SUPPORTED_TYPES.archive.includes(ext)) return 'archive';

  return 'other';
}

/**
 * Get file extension from MIME type
 */
function getExtensionFromMimeType(mimeType) {
  const mimeToExt = {
    'video/mp4': '.mp4',
    'video/webm': '.webm',
    'video/x-msvideo': '.avi',
    'audio/mpeg': '.mp3',
    'audio/wav': '.wav',
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'application/pdf': '.pdf',
    'application/zip': '.zip',
    'text/plain': '.txt'
  };

  return mimeToExt[mimeType] || '';
}

/**
 * Format file size for display
 */
function formatFileSize(bytes) {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * Create standardized error response
 */
function createErrorResponse(message, status = 500, additionalHeaders = {}) {
  const errorResponse = {
    success: false,
    error: {
      message,
      status,
      timestamp: new Date().toISOString(),
      service: 'MARYA VAULT UPLOADER'
    }
  };

  console.error(`âŒ Error Response: ${status} - ${message}`);

  return new Response(JSON.stringify(errorResponse, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...additionalHeaders
    }
  });
}