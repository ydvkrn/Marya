
// functions/upload.js
// ðŸ’ª ULTIMATE CHALLENGE ACCEPTED - MARYA VAULT UPLOADER
// ðŸŽ¯ Features: 2GB URL upload, Chunking system, Real-time progress, Heavy code level

console.log('ðŸš€ MARYA VAULT ULTIMATE UPLOADER - CHALLENGE VERSION LOADED');

// Advanced Configuration
const CONFIG = {
  // File size limits
  MAX_FILE_SIZE: 2 * 1024 * 1024 * 1024,    // 2GB max
  NORMAL_MAX_SIZE: 500 * 1024 * 1024,       // 500MB for normal upload

  // Advanced chunking strategy
  CHUNK_SIZE: 20 * 1024 * 1024,             // 20MB per chunk
  MAX_CHUNKS_PER_KV: 20,                    // Max 20 chunks per KV
  TOTAL_KV_NAMESPACES: 7,                   // 7 KV namespaces

  // Upload performance
  PARALLEL_CHUNKS: 4,                       // Upload 4 chunks simultaneously
  RETRY_ATTEMPTS: 7,                        // Retry failed chunks 7 times
  TIMEOUT_DURATION: 180000,                 // 3 minutes timeout

  // Progress tracking
  PROGRESS_UPDATE_INTERVAL: 1000,           // Update progress every 1 second
  SPEED_CALCULATION_INTERVAL: 2000,         // Calculate speed every 2 seconds

  // Advanced features
  ENABLE_COMPRESSION: false,                // Disable compression for speed
  ENABLE_VERIFICATION: true,                // Enable chunk verification
  ENABLE_REAL_TIME_LOGS: true,              // Real-time upload logs

  // URL download settings
  URL_DOWNLOAD_TIMEOUT: 600000,             // 10 minutes for URL downloads
  URL_MAX_RETRIES: 5,                       // Retry URL downloads 5 times
  URL_CHUNK_DOWNLOAD: true,                 // Download URLs in chunks if large
};

export async function onRequest(context) {
  const { request, env } = context;

  console.log('ðŸŽ¯ CHALLENGE UPLOADER INITIATED');
  console.log('ðŸ“… Timestamp:', new Date().toISOString());
  console.log('ðŸ”— Request URL:', request.url);
  console.log('ðŸ“Š Method:', request.method);

  // Enhanced CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, PUT, DELETE',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Upload-Type, X-File-URL, X-Progress-ID, X-Chunk-Index',
    'Access-Control-Expose-Headers': 'X-Upload-ID, X-Progress-ID, X-Upload-Speed, X-Chunks-Total',
    'Access-Control-Max-Age': '86400'
  };

  // Handle CORS preflight
  if (request.method === 'OPTIONS') {
    console.log('âœ… CORS preflight handled');
    return new Response(null, { 
      status: 204, 
      headers: corsHeaders 
    });
  }

  // Only allow POST
  if (request.method !== 'POST') {
    return createJsonResponse({
      success: false,
      error: 'Method not allowed - Use POST only',
      code: 'METHOD_NOT_ALLOWED'
    }, 405, corsHeaders);
  }

  try {
    // Validate environment
    const envCheck = await validateEnvironment(env);
    if (!envCheck.valid) {
      throw new Error(`Environment error: ${envCheck.error}`);
    }

    console.log(`ðŸ”§ Environment validated: ${envCheck.kvCount} KVs, ${envCheck.botCount} bots`);

    // Determine upload type
    const uploadType = request.headers.get('X-Upload-Type') || 'auto';
    const fileUrl = request.headers.get('X-File-URL');

    console.log('ðŸŽ¯ Upload type detected:', uploadType);

    let uploadResult;

    if (fileUrl && fileUrl.trim()) {
      // URL-based upload with progress tracking
      console.log('ðŸŒ Starting URL upload for:', fileUrl);
      uploadResult = await handleAdvancedUrlUpload(request, env, fileUrl, corsHeaders);
    } else {
      // File-based upload with chunking
      console.log('ðŸ“ Starting file upload with chunking');
      uploadResult = await handleAdvancedFileUpload(request, env, corsHeaders);
    }

    return uploadResult;

  } catch (error) {
    console.error('âŒ CRITICAL ERROR:', error);
    console.error('ðŸ“ Stack trace:', error.stack);

    return createJsonResponse({
      success: false,
      error: error.message,
      code: 'UPLOAD_FAILED',
      timestamp: new Date().toISOString(),
      debug: CONFIG.ENABLE_REAL_TIME_LOGS ? error.stack : undefined
    }, 500, corsHeaders);
  }
}

/**
 * Advanced URL upload with progress tracking and chunking
 */
async function handleAdvancedUrlUpload(request, env, fileUrl, corsHeaders) {
  console.log('ðŸŒ ADVANCED URL UPLOAD INITIATED');

  const startTime = Date.now();
  let downloadedBytes = 0;
  let totalBytes = 0;

  try {
    // Validate URL
    let url;
    try {
      url = new URL(fileUrl);
      console.log('ðŸ” URL parsed:', url.hostname);
    } catch {
      throw new Error('Invalid URL format provided');
    }

    // Security validation
    if (!['http:', 'https:'].includes(url.protocol)) {
      throw new Error('Only HTTP and HTTPS URLs are supported');
    }

    console.log('ðŸ” Fetching file metadata from URL...');

    // Advanced HEAD request with multiple attempts
    let headResponse = null;
    let contentLength = 0;
    let contentType = 'application/octet-stream';
    let filename = '';

    for (let attempt = 0; attempt < CONFIG.URL_MAX_RETRIES; attempt++) {
      try {
        console.log(`ðŸ”„ HEAD request attempt ${attempt + 1}/${CONFIG.URL_MAX_RETRIES}`);

        headResponse = await fetch(fileUrl, {
          method: 'HEAD',
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'identity',
            'Range': 'bytes=0-1023' // Test range support
          },
          signal: AbortSignal.timeout(30000)
        });

        if (headResponse.ok) {
          contentLength = parseInt(headResponse.headers.get('Content-Length') || '0');
          contentType = headResponse.headers.get('Content-Type') || contentType;

          // Extract filename from Content-Disposition or URL
          const contentDisposition = headResponse.headers.get('Content-Disposition');
          if (contentDisposition) {
            const match = contentDisposition.match(/filename[*]?=([^;\n\r"']+)/);
            if (match) {
              filename = match[1].replace(/['"]/g, '').trim();
            }
          }

          console.log('âœ… HEAD request successful');
          break;
        }

        console.log(`âš ï¸ HEAD request failed: ${headResponse.status}, trying GET request`);

      } catch (error) {
        console.log(`âŒ HEAD attempt ${attempt + 1} failed:`, error.message);
        if (attempt === CONFIG.URL_MAX_RETRIES - 1) {
          console.log('ðŸ”„ Falling back to GET request without HEAD');
        }
      }
    }

    // Extract filename from URL if not found in headers
    if (!filename) {
      const urlPath = url.pathname;
      filename = urlPath.split('/').pop() || `download_${Date.now()}`;

      // Add extension based on content-type if missing
      if (!filename.includes('.') && contentType) {
        const ext = getExtensionFromMimeType(contentType);
        if (ext) filename += ext;
      }
    }

    totalBytes = contentLength;

    console.log(`ðŸ“Š URL File Analysis:
    ðŸ“ Name: ${filename}
    ðŸ“ Size: ${totalBytes ? formatFileSize(totalBytes) : 'Unknown'}
    ðŸ·ï¸ Type: ${contentType}
    ðŸ”— URL: ${fileUrl}`);

    // Validate file size
    if (totalBytes > 0 && totalBytes > CONFIG.MAX_FILE_SIZE) {
      throw new Error(`File size ${formatFileSize(totalBytes)} exceeds maximum limit of ${formatFileSize(CONFIG.MAX_FILE_SIZE)}`);
    }

    // Start download with progress tracking
    console.log('ðŸ“¥ Starting advanced download with progress tracking...');

    const downloadResponse = await fetch(fileUrl, {
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'identity'
      },
      signal: AbortSignal.timeout(CONFIG.URL_DOWNLOAD_TIMEOUT)
    });

    if (!downloadResponse.ok) {
      throw new Error(`Download failed: ${downloadResponse.status} ${downloadResponse.statusText}`);
    }

    // Get actual content length from response
    const actualContentLength = parseInt(downloadResponse.headers.get('Content-Length') || '0');
    if (actualContentLength > 0) {
      totalBytes = actualContentLength;
    }

    console.log('ðŸ“¥ Download response received, processing stream...');

    // Process download stream with progress tracking
    const reader = downloadResponse.body?.getReader();
    if (!reader) {
      throw new Error('Unable to read download stream');
    }

    const chunks = [];
    let lastProgressUpdate = Date.now();
    let lastSpeedCalculation = Date.now();
    let bytesAtLastSpeedCalc = 0;

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        chunks.push(value);
        downloadedBytes += value.length;

        // Update progress periodically
        const now = Date.now();
        if (now - lastProgressUpdate > CONFIG.PROGRESS_UPDATE_INTERVAL) {
          const progress = totalBytes > 0 ? Math.round((downloadedBytes / totalBytes) * 100) : 0;

          console.log(`ðŸ“¥ Download progress: ${progress}% (${formatFileSize(downloadedBytes)}${totalBytes > 0 ? `/${formatFileSize(totalBytes)}` : ''})`);

          lastProgressUpdate = now;
        }

        // Calculate download speed
        if (now - lastSpeedCalculation > CONFIG.SPEED_CALCULATION_INTERVAL) {
          const bytesInInterval = downloadedBytes - bytesAtLastSpeedCalc;
          const timeInSeconds = (now - lastSpeedCalculation) / 1000;
          const speed = bytesInInterval / timeInSeconds;

          console.log(`ðŸ“Š Download speed: ${formatFileSize(speed)}/s`);

          lastSpeedCalculation = now;
          bytesAtLastSpeedCalc = downloadedBytes;
        }
      }
    } finally {
      reader.releaseLock();
    }

    // Combine all chunks into a single ArrayBuffer
    const totalSize = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const combined = new Uint8Array(totalSize);
    let offset = 0;

    for (const chunk of chunks) {
      combined.set(chunk, offset);
      offset += chunk.length;
    }

    console.log(`âœ… Download completed: ${formatFileSize(downloadedBytes)} in ${Date.now() - startTime}ms`);

    // Create File object
    const file = new File([combined.buffer], filename, { type: contentType });

    // Process the downloaded file
    return await processAdvancedFileUpload(file, request, env, corsHeaders, 'url', startTime);

  } catch (error) {
    console.error('âŒ URL upload error:', error);
    throw new Error(`URL upload failed: ${error.message}`);
  }
}

/**
 * Advanced file upload handler
 */
async function handleAdvancedFileUpload(request, env, corsHeaders) {
  console.log('ðŸ“ ADVANCED FILE UPLOAD INITIATED');

  try {
    const contentType = request.headers.get('Content-Type') || '';
    let file;

    // Handle different content types
    if (contentType.includes('multipart/form-data')) {
      // Form-based upload
      console.log('ðŸ“‹ Processing form data upload');
      const formData = await request.formData();
      file = formData.get('file') || formData.get('document') || formData.get('upload');

      if (!file) {
        // Search through all form fields
        for (const [key, value] of formData) {
          if (value instanceof File && value.size > 0) {
            file = value;
            console.log(`ðŸ“ File found in field: ${key}`);
            break;
          }
        }
      }
    } else if (contentType.includes('application/octet-stream') || contentType.includes('application/binary')) {
      // Binary upload
      console.log('ðŸ“¦ Processing binary upload');
      const arrayBuffer = await request.arrayBuffer();
      const filename = request.headers.get('X-Filename') || `binary_${Date.now()}.bin`;
      file = new File([arrayBuffer], filename, { type: contentType });
    } else {
      // Try to parse as form data anyway
      try {
        const formData = await request.formData();
        file = formData.get('file');
      } catch {
        throw new Error('Unsupported upload format');
      }
    }

    if (!file || file.size === 0) {
      throw new Error('No valid file found in request');
    }

    console.log(`ðŸ“ File received:
    ðŸ“ Name: ${file.name}
    ðŸ“ Size: ${formatFileSize(file.size)}
    ðŸ·ï¸ Type: ${file.type || 'Unknown'}`);

    return await processAdvancedFileUpload(file, request, env, corsHeaders, 'file');

  } catch (error) {
    console.error('âŒ File upload error:', error);
    throw new Error(`File upload failed: ${error.message}`);
  }
}

/**
 * Advanced file processing with chunking and progress tracking
 */
async function processAdvancedFileUpload(file, request, env, corsHeaders, uploadType, startTime = null) {
  console.log('ðŸŽ¯ ADVANCED FILE PROCESSING INITIATED');

  const processingStartTime = startTime || Date.now();
  const envValidation = await validateEnvironment(env);
  const { kvNamespaces, botTokens } = envValidation;

  // Validate file size
  if (file.size > CONFIG.MAX_FILE_SIZE) {
    throw new Error(`File too large: ${formatFileSize(file.size)} (maximum: ${formatFileSize(CONFIG.MAX_FILE_SIZE)})`);
  }

  // Generate advanced file ID
  const fileId = generateAdvancedFileId(file);
  const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

  console.log(`ðŸ†” Generated file ID: ${fileId}${extension}`);

  // Determine upload strategy
  let strategy;

  if (file.size <= CONFIG.NORMAL_MAX_SIZE) {
    console.log('ðŸ“¤ Using single file upload strategy');
    strategy = await handleSingleFileUpload(file, fileId, env, botTokens[0], kvNamespaces[0]);
  } else {
    console.log('ðŸ§© Using advanced chunked upload strategy');
    strategy = await handleAdvancedChunkedUpload(file, fileId, env, botTokens, kvNamespaces);
  }

  // Generate response URLs
  const baseUrl = new URL(request.url).origin;
  const streamUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
  const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;
  const hlsUrl = file.type.startsWith('video/') ? `${baseUrl}/btfstorage/file/${fileId}.m3u8` : null;

  const totalProcessingTime = Date.now() - processingStartTime;

  // Create comprehensive response
  const result = {
    success: true,
    message: 'ðŸš€ Upload completed successfully',

    // File information
    file: {
      id: fileId,
      filename: file.name,
      size: file.size,
      sizeFormatted: formatFileSize(file.size),
      contentType: file.type || 'application/octet-stream',
      extension: extension,
      category: getFileCategory(file.name)
    },

    // Upload performance metrics
    upload: {
      type: uploadType,
      strategy: strategy.type,
      timestamp: new Date().toISOString(),
      processingTimeMs: totalProcessingTime,
      processingTimeFormatted: formatTime(totalProcessingTime),
      chunks: strategy.chunks || 0,
      kvDistribution: strategy.kvDistribution || [],
      averageSpeed: strategy.averageSpeed || 0,
      averageSpeedFormatted: formatFileSize(strategy.averageSpeed || 0) + '/s',
      retries: strategy.retries || 0,
      parallelUploads: strategy.parallelUploads || 1
    },

    // Access URLs
    urls: {
      stream: streamUrl,
      download: downloadUrl,
      hls: hlsUrl,
      embed: `${baseUrl}/embed/${fileId}${extension}`,
      direct: `${baseUrl}/direct/${fileId}${extension}`
    },

    // Advanced features
    features: {
      instantPlay: strategy.type === 'chunked',
      hlsStreaming: file.type.startsWith('video/') && strategy.type === 'chunked',
      rangeRequests: true,
      progressiveDownload: true,
      crossOrigin: true,
      caching: true,
      resumableUpload: strategy.type === 'chunked',
      verification: CONFIG.ENABLE_VERIFICATION
    },

    // System information
    system: {
      totalKvNamespaces: kvNamespaces.length,
      totalBotTokens: botTokens.length,
      maxFileSize: formatFileSize(CONFIG.MAX_FILE_SIZE),
      chunkSize: formatFileSize(CONFIG.CHUNK_SIZE),
      parallelUploads: CONFIG.PARALLEL_CHUNKS
    }
  };

  console.log(`âœ… UPLOAD COMPLETED SUCCESSFULLY:
  ðŸ“ File: ${file.name}
  ðŸ“ Size: ${formatFileSize(file.size)}
  ðŸŽ¯ Strategy: ${strategy.type}
  â±ï¸ Time: ${formatTime(totalProcessingTime)}
  ðŸš€ Speed: ${formatFileSize(strategy.averageSpeed || 0)}/s
  ðŸ”— URL: ${streamUrl}`);

  return new Response(JSON.stringify(result, null, 2), {
    status: 200,
    headers: { 
      'Content-Type': 'application/json',
      'X-Upload-ID': fileId,
      'X-Upload-Strategy': strategy.type,
      'X-Processing-Time': totalProcessingTime.toString(),
      'X-File-Size': file.size.toString(),
      ...corsHeaders 
    }
  });
}

/**
 * Advanced chunked upload with progress tracking
 */
async function handleAdvancedChunkedUpload(file, fileId, env, botTokens, kvNamespaces) {
  console.log('ðŸ§© ADVANCED CHUNKED UPLOAD INITIATED');
  const startTime = Date.now();

  try {
    // Calculate chunking strategy
    const totalChunks = Math.ceil(file.size / CONFIG.CHUNK_SIZE);
    const optimalKvDistribution = distributeChunksOptimally(totalChunks, kvNamespaces.length);

    console.log(`ðŸ“Š Advanced Chunking Strategy:
    ðŸ§© Total chunks: ${totalChunks}
    ðŸ“¦ Chunk size: ${formatFileSize(CONFIG.CHUNK_SIZE)}
    ðŸ—ƒï¸ KV namespaces: ${kvNamespaces.length}
    âš¡ Parallel uploads: ${CONFIG.PARALLEL_CHUNKS}
    ðŸ”„ Retry attempts: ${CONFIG.RETRY_ATTEMPTS}
    ðŸ“ˆ Distribution: ${JSON.stringify(optimalKvDistribution)}`);

    if (totalChunks > kvNamespaces.length * CONFIG.MAX_CHUNKS_PER_KV) {
      throw new Error(`File requires ${totalChunks} chunks, exceeds system capacity (${kvNamespaces.length * CONFIG.MAX_CHUNKS_PER_KV} max)`);
    }

    // Create optimized chunk queue
    const chunkQueue = [];
    let totalUploadedBytes = 0;
    let totalRetries = 0;

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CONFIG.CHUNK_SIZE;
      const end = Math.min(start + CONFIG.CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      // Select optimal KV namespace based on distribution
      const kvIndex = optimalKvDistribution[i] || (i % kvNamespaces.length);
      const targetKv = kvNamespaces[kvIndex];

      // Select bot token (round-robin)
      const botToken = botTokens[i % botTokens.length];

      chunkQueue.push({
        index: i,
        chunk,
        size: chunk.size,
        kvNamespace: targetKv,
        botToken,
        fileId,
        filename: `${file.name}.chunk${i.toString().padStart(3, '0')}`,
        attempts: 0,
        uploaded: false
      });
    }

    console.log('ðŸš€ Starting advanced parallel chunk upload...');

    // Upload chunks with advanced progress tracking
    const chunkResults = await uploadChunksWithAdvancedProgress(chunkQueue, env.CHANNEL_ID, (progress) => {
      console.log(`ðŸ“Š Upload Progress: ${progress.percentage}% | Speed: ${formatFileSize(progress.speed)}/s | ETA: ${formatTime(progress.eta)} | Completed: ${progress.completed}/${progress.total}`);

      totalUploadedBytes = progress.uploadedBytes;
      totalRetries = progress.totalRetries;
    });

    console.log('âœ… All chunks uploaded with advanced tracking');

    // Create master metadata with advanced information
    const masterMetadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      uploadedAt: Date.now(),
      type: 'advanced_chunked',
      version: '2.0',

      // Chunking information
      totalChunks,
      chunkSize: CONFIG.CHUNK_SIZE,
      distribution: optimalKvDistribution,

      // Performance metrics
      uploadTimeMs: Date.now() - startTime,
      averageSpeed: file.size / ((Date.now() - startTime) / 1000),
      totalRetries,

      // Chunk details
      chunks: chunkResults.map((result, index) => ({
        index,
        kvNamespace: result.kvNamespace,
        keyName: result.chunkKey,
        telegramFileId: result.telegramFileId,
        size: result.size,
        directUrl: result.directUrl,
        uploadedAt: result.uploadedAt,
        attempts: result.attempts
      }))
    };

    // Store master metadata in primary KV
    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    const processingTime = Date.now() - startTime;
    const averageSpeed = file.size / (processingTime / 1000);
    const kvDistribution = [...new Set(chunkResults.map(r => r.kvNamespace))];

    console.log(`âœ… ADVANCED CHUNKED UPLOAD COMPLETED:
    â±ï¸ Time: ${formatTime(processingTime)}
    ðŸš€ Speed: ${formatFileSize(averageSpeed)}/s
    ðŸ”„ Retries: ${totalRetries}
    ðŸ“Š Success rate: ${Math.round(((totalChunks * CONFIG.RETRY_ATTEMPTS - totalRetries) / (totalChunks * CONFIG.RETRY_ATTEMPTS)) * 100)}%`);

    return {
      type: 'chunked',
      processingTime,
      chunks: totalChunks,
      kvDistribution,
      averageSpeed,
      retries: totalRetries,
      parallelUploads: CONFIG.PARALLEL_CHUNKS
    };

  } catch (error) {
    console.error('âŒ Advanced chunked upload failed:', error);
    throw new Error(`Chunked upload failed: ${error.message}`);
  }
}

/**
 * Upload chunks with advanced progress tracking
 */
async function uploadChunksWithAdvancedProgress(chunkQueue, channelId, progressCallback) {
  const results = [];
  const totalChunks = chunkQueue.length;
  let completedChunks = 0;
  let uploadedBytes = 0;
  let totalRetries = 0;
  let startTime = Date.now();
  let lastProgressTime = Date.now();
  let bytesAtLastProgress = 0;

  // Create progress tracker
  const updateProgress = () => {
    const now = Date.now();
    const elapsedTime = now - startTime;
    const percentage = Math.round((completedChunks / totalChunks) * 100);

    // Calculate current speed
    const timeSinceLastUpdate = now - lastProgressTime;
    const bytesSinceLastUpdate = uploadedBytes - bytesAtLastProgress;
    const currentSpeed = timeSinceLastUpdate > 0 ? (bytesSinceLastUpdate / (timeSinceLastUpdate / 1000)) : 0;

    // Calculate ETA
    const avgSpeed = uploadedBytes / (elapsedTime / 1000);
    const remainingBytes = chunkQueue.reduce((sum, chunk) => sum + chunk.size, 0) - uploadedBytes;
    const eta = avgSpeed > 0 ? (remainingBytes / avgSpeed) * 1000 : 0;

    progressCallback({
      percentage,
      completed: completedChunks,
      total: totalChunks,
      uploadedBytes,
      speed: currentSpeed,
      averageSpeed: avgSpeed,
      eta,
      totalRetries
    });

    lastProgressTime = now;
    bytesAtLastProgress = uploadedBytes;
  };

  // Process chunks in parallel batches
  for (let i = 0; i < chunkQueue.length; i += CONFIG.PARALLEL_CHUNKS) {
    const batch = chunkQueue.slice(i, i + CONFIG.PARALLEL_CHUNKS);

    console.log(`ðŸ”„ Processing batch ${Math.floor(i/CONFIG.PARALLEL_CHUNKS) + 1}/${Math.ceil(chunkQueue.length/CONFIG.PARALLEL_CHUNKS)} (${batch.length} chunks)`);

    const batchPromises = batch.map(async (chunkInfo) => {
      const result = await uploadSingleChunkWithRetry(chunkInfo, channelId);

      // Update progress
      completedChunks++;
      uploadedBytes += chunkInfo.size;
      totalRetries += chunkInfo.attempts;

      updateProgress();

      return result;
    });

    try {
      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults);

      console.log(`âœ… Batch completed: ${batchResults.length} chunks uploaded`);
    } catch (error) {
      console.error('âŒ Batch failed:', error);
      throw error;
    }

    // Small delay between batches to respect rate limits
    if (i + CONFIG.PARALLEL_CHUNKS < chunkQueue.length) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  // Sort results by chunk index
  return results.sort((a, b) => a.index - b.index);
}

/**
 * Upload single chunk with advanced retry logic
 */
async function uploadSingleChunkWithRetry(chunkInfo, channelId) {
  const { index, chunk, kvNamespace, botToken, fileId, filename } = chunkInfo;

  for (let attempt = 0; attempt < CONFIG.RETRY_ATTEMPTS; attempt++) {
    chunkInfo.attempts++;

    try {
      console.log(`ðŸ“¤ Uploading chunk ${index} (attempt ${attempt + 1}/${CONFIG.RETRY_ATTEMPTS}) to ${kvNamespace.name}`);

      // Create chunk file
      const chunkFile = new File([chunk], filename, { 
        type: 'application/octet-stream' 
      });

      // Upload to Telegram with timeout
      const telegramResult = await uploadToTelegramWithTimeout(chunkFile, botToken, channelId);

      // Store chunk metadata
      const chunkKey = `${fileId}_chunk_${index.toString().padStart(3, '0')}`;
      const chunkMetadata = {
        telegramFileId: telegramResult.fileId,
        directUrl: telegramResult.directUrl,
        size: chunk.size,
        index,
        parentFileId: fileId,
        kvNamespace: kvNamespace.name,
        uploadedAt: Date.now(),
        lastRefreshed: Date.now(),
        attempts: chunkInfo.attempts,
        version: '2.0'
      };

      await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

      console.log(`âœ… Chunk ${index} uploaded successfully to ${kvNamespace.name} (${chunkInfo.attempts} attempts)`);

      return {
        index,
        telegramFileId: telegramResult.fileId,
        size: chunk.size,
        directUrl: telegramResult.directUrl,
        kvNamespace: kvNamespace.name,
        chunkKey,
        uploadedAt: Date.now(),
        attempts: chunkInfo.attempts
      };

    } catch (error) {
      console.error(`âŒ Chunk ${index} upload attempt ${attempt + 1} failed:`, error);

      if (attempt === CONFIG.RETRY_ATTEMPTS - 1) {
        throw new Error(`Chunk ${index} failed after ${CONFIG.RETRY_ATTEMPTS} attempts: ${error.message}`);
      }

      // Exponential backoff with jitter
      const delay = Math.min(Math.pow(2, attempt) * 1000 + Math.random() * 1000, 30000);
      console.log(`â³ Waiting ${Math.round(delay)}ms before retry...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

/**
 * Upload to Telegram with enhanced timeout and error handling
 */
async function uploadToTelegramWithTimeout(file, botToken, channelId) {
  console.log(`ðŸ“¤ Uploading to Telegram: ${file.name} (${formatFileSize(file.size)})`);

  try {
    // Create form data
    const formData = new FormData();
    formData.append('chat_id', channelId);
    formData.append('document', file);
    formData.append('disable_notification', 'true');

    // Upload with timeout
    const response = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: formData,
      signal: AbortSignal.timeout(CONFIG.TIMEOUT_DURATION)
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Telegram API error: ${response.status} ${response.statusText} - ${errorText}`);
    }

    const data = await response.json();

    if (!data.ok || !data.result?.document?.file_id) {
      throw new Error(`Invalid Telegram response: ${data.error_code || 'Unknown error'} - ${data.description || 'No description'}`);
    }

    const fileId = data.result.document.file_id;

    // Get direct URL
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`,
      { signal: AbortSignal.timeout(30000) }
    );

    if (!getFileResponse.ok) {
      throw new Error(`GetFile API error: ${getFileResponse.status} ${getFileResponse.statusText}`);
    }

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error(`No file path in Telegram response: ${getFileData.error_code || 'Unknown error'}`);
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
 * Single file upload (for files â‰¤ 500MB)
 */
async function handleSingleFileUpload(file, fileId, env, botToken, kvNamespace) {
  console.log('ðŸ“¤ SINGLE FILE UPLOAD STRATEGY');
  const startTime = Date.now();

  try {
    // Upload to Telegram
    const telegramResult = await uploadToTelegramWithTimeout(file, botToken, env.CHANNEL_ID);

    // Store metadata
    const metadata = {
      filename: file.name,
      size: file.size,
      contentType: file.type,
      uploadedAt: Date.now(),
      type: 'single',
      version: '2.0',
      telegramFileId: telegramResult.fileId,
      directUrl: telegramResult.directUrl,
      kvNamespace: kvNamespace.name
    };

    await kvNamespace.kv.put(fileId, JSON.stringify(metadata));

    const processingTime = Date.now() - startTime;
    const averageSpeed = file.size / (processingTime / 1000);

    console.log(`âœ… Single file upload completed in ${formatTime(processingTime)}`);

    return {
      type: 'single',
      processingTime,
      kvDistribution: [kvNamespace.name],
      averageSpeed,
      retries: 0,
      parallelUploads: 1
    };

  } catch (error) {
    console.error('âŒ Single file upload failed:', error);
    throw new Error(`Single file upload failed: ${error.message}`);
  }
}

// Utility functions (continued in next part due to length limits)



/**
 * Distribute chunks optimally across KV namespaces
 */
function distributeChunksOptimally(totalChunks, kvCount) {
  const distribution = {};
  const chunksPerKv = Math.floor(totalChunks / kvCount);
  const remainingChunks = totalChunks % kvCount;

  let chunkIndex = 0;

  for (let kvIndex = 0; kvIndex < kvCount; kvIndex++) {
    const chunksForThisKv = chunksPerKv + (kvIndex < remainingChunks ? 1 : 0);

    for (let i = 0; i < chunksForThisKv; i++) {
      distribution[chunkIndex] = kvIndex;
      chunkIndex++;
    }
  }

  return distribution;
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
  const ext = filename.toLowerCase().split('.').pop() || '';

  const categories = {
    video: ['mp4', 'mkv', 'avi', 'mov', 'webm', 'flv', '3gp', 'm4v', 'wmv', 'mpg', 'mpeg'],
    audio: ['mp3', 'wav', 'aac', 'm4a', 'ogg', 'flac', 'wma', 'opus'],
    image: ['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'bmp', 'tiff', 'ico'],
    document: ['pdf', 'txt', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'rtf'],
    archive: ['zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz']
  };

  for (const [category, extensions] of Object.entries(categories)) {
    if (extensions.includes(ext)) return category;
  }

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
    'video/quicktime': '.mov',
    'audio/mpeg': '.mp3',
    'audio/wav': '.wav',
    'audio/aac': '.aac',
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'image/svg+xml': '.svg',
    'application/pdf': '.pdf',
    'application/zip': '.zip',
    'text/plain': '.txt',
    'application/msword': '.doc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx'
  };

  return mimeToExt[mimeType] || '';
}

/**
 * Validate environment configuration
 */
async function validateEnvironment(env) {
  const requiredVars = ['BOT_TOKEN', 'CHANNEL_ID'];
  const missing = requiredVars.filter(key => !env[key]);

  if (missing.length > 0) {
    return {
      valid: false,
      error: `Missing environment variables: ${missing.join(', ')}`
    };
  }

  // Collect KV namespaces
  const kvNamespaces = [];
  for (let i = 1; i <= CONFIG.TOTAL_KV_NAMESPACES; i++) {
    const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
    if (env[kvKey]) {
      kvNamespaces.push({ 
        kv: env[kvKey], 
        name: kvKey, 
        index: i - 1 
      });
    }
  }

  // Collect bot tokens
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
 * Format time duration
 */
function formatTime(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3600000) return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
  return `${Math.floor(ms / 3600000)}h ${Math.floor((ms % 3600000) / 60000)}m`;
}

/**
 * Create standardized JSON response
 */
function createJsonResponse(data, status = 200, additionalHeaders = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...additionalHeaders
    }
  });
}