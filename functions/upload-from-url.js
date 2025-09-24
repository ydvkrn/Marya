
// functions/upload-from-url.js
// ðŸš€ ULTIMATE MARYA VAULT - ADVANCED URL UPLOAD SYSTEM
// Enhanced to match the advanced upload system

export async function onRequest(context) {
  const { request, env } = context;

  console.log('ðŸŒ === ULTIMATE URL UPLOAD START === ðŸŒ');
  console.log('ðŸ“… Timestamp:', new Date().toISOString());

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, PUT, DELETE',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-URL-Type, X-Download-Options',
    'Access-Control-Expose-Headers': 'X-Download-ID, X-Processing-Time, X-Download-Stats',
    'Access-Control-Max-Age': '86400'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return createJsonResponse({
      success: false,
      error: 'Method not allowed - Use POST only'
    }, 405, corsHeaders);
  }

  const startTime = Date.now();
  const downloadId = generateAdvancedDownloadId();

  try {
    // ðŸ”§ Enhanced environment validation (same as upload)
    const envValidation = await validateAdvancedEnvironment(env);
    if (!envValidation.success) {
      throw new Error(`Environment validation failed: ${envValidation.error}`);
    }

    const { kvNamespaces, botTokens, channelId } = envValidation;

    // ðŸ“¥ Process URL input
    const { url } = await request.json();

    if (!url || !url.trim()) {
      throw new Error('No URL provided');
    }

    const cleanUrl = url.trim();
    console.log('ðŸ”— Processing URL:', cleanUrl.substring(0, 100) + '...');

    // ðŸ” Advanced URL validation
    const urlValidation = validateAdvancedUrl(cleanUrl);
    if (!urlValidation.valid) {
      throw new Error(urlValidation.error);
    }

    console.log('âœ… URL validation passed:', urlValidation.type);

    // ðŸ“Š Get file information
    const fileInfo = await getAdvancedFileInfo(cleanUrl);
    console.log('ðŸ“ File info retrieved:', {
      size: formatFileSize(fileInfo.size),
      type: fileInfo.contentType,
      filename: fileInfo.filename
    });

    // âœ… Validate file size
    const maxFileSize = calculateMaxFileSize(kvNamespaces.length);
    if (fileInfo.size > maxFileSize) {
      throw new Error(`File too large: ${formatFileSize(fileInfo.size)} (max: ${formatFileSize(maxFileSize)})`);
    }

    // ðŸ“¥ Enhanced download with progress tracking
    console.log('â¬‡ï¸ Starting enhanced download...');
    const downloadResult = await downloadAdvancedFile(cleanUrl, fileInfo);

    if (!downloadResult.success) {
      throw new Error(`Download failed: ${downloadResult.error}`);
    }

    const { arrayBuffer, actualSize, downloadTime } = downloadResult;
    console.log(`âœ… Download completed: ${formatFileSize(actualSize)} in ${formatTime(downloadTime)}`);

    // ðŸ“ Create enhanced file object
    const file = new File([arrayBuffer], fileInfo.filename, { 
      type: fileInfo.contentType,
      lastModified: Date.now()
    });

    // ðŸ“Š Generate advanced metadata
    const metadata = await generateAdvancedFileMetadata(file);
    metadata.sourceUrl = cleanUrl.length > 200 ? cleanUrl.substring(0, 200) + '...' : cleanUrl;
    metadata.downloadStats = {
      downloadTime: downloadTime,
      downloadSpeed: actualSize / (downloadTime / 1000),
      sourceServer: extractServerInfo(cleanUrl),
      userAgent: 'MARYA-VAULT/3.0 Advanced Downloader'
    };

    // ðŸ†” Generate advanced file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 10);
    const category = metadata.category.charAt(0).toUpperCase();
    const sizeIndicator = file.size > 500 * 1024 * 1024 ? 'L' : 'M';
    const fileId = `${category}${sizeIndicator}${timestamp}${random}`;
    const extension = fileInfo.filename.includes('.') ? fileInfo.filename.slice(fileInfo.filename.lastIndexOf('.')) : '';

    console.log(`ðŸ†” Advanced file ID: ${fileId}${extension}`);

    // ðŸ§© Calculate optimal chunking strategy
    const chunkingStrategy = calculateOptimalChunkingStrategy(file.size, kvNamespaces.length, metadata);
    console.log(`ðŸ§© Chunking strategy:`, {
      totalChunks: chunkingStrategy.totalChunks,
      chunkSize: formatFileSize(chunkingStrategy.chunkSize),
      distribution: chunkingStrategy.distribution,
      parallelUploads: chunkingStrategy.parallelUploads
    });

    // ðŸš€ Execute advanced chunked upload
    const uploadResult = await executeAdvancedChunkedUpload(
      file, 
      fileId, 
      chunkingStrategy, 
      kvNamespaces, 
      botTokens, 
      channelId,
      downloadId
    );

    // ðŸ“ˆ Create comprehensive metadata
    const masterMetadata = createAdvancedMasterMetadata(
      file, 
      fileId, 
      extension, 
      chunkingStrategy, 
      uploadResult, 
      metadata,
      startTime
    );

    // Add URL-specific metadata
    masterMetadata.source = {
      type: 'url_upload',
      originalUrl: cleanUrl,
      downloadTime: downloadTime,
      downloadSpeed: actualSize / (downloadTime / 1000),
      serverInfo: extractServerInfo(cleanUrl),
      downloadId: downloadId
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));
    console.log(`ðŸ’¾ Master metadata stored`);

    // ðŸ”— Generate advanced URLs
    const baseUrl = new URL(request.url).origin;
    const urls = generateAdvancedUrls(baseUrl, fileId, extension, metadata);

    // ðŸ“Š Calculate comprehensive metrics
    const totalProcessingTime = Date.now() - startTime;
    const overallSpeed = file.size / (totalProcessingTime / 1000);
    const efficiency = calculateUrlUploadEfficiency(downloadTime, uploadResult.processingTime, file.size);

    // ðŸŽ‰ Create ultimate response
    const response = {
      success: true,
      message: 'ðŸŒ Ultimate URL upload completed!',
      timestamp: new Date().toISOString(),
      downloadId: downloadId,

      // Source information
      source: {
        originalUrl: cleanUrl.length > 100 ? cleanUrl.substring(0, 100) + '...' : cleanUrl,
        urlType: urlValidation.type,
        serverInfo: extractServerInfo(cleanUrl),
        downloadTime: downloadTime,
        downloadTimeFormatted: formatTime(downloadTime),
        downloadSpeed: actualSize / (downloadTime / 1000),
        downloadSpeedFormatted: formatFileSize(actualSize / (downloadTime / 1000)) + '/s'
      },

      // File information
      file: {
        id: fileId,
        filename: fileInfo.filename,
        size: file.size,
        sizeFormatted: formatFileSize(file.size),
        contentType: fileInfo.contentType,
        extension: extension,
        category: metadata.category,
        hash: metadata.hash,
        isVideo: metadata.isVideo,
        isLarge: metadata.isLarge,
        quality: metadata.quality
      },

      // Processing information
      processing: {
        totalTime: totalProcessingTime,
        totalTimeFormatted: formatTime(totalProcessingTime),
        downloadTime: downloadTime,
        uploadTime: uploadResult.processingTime,
        overallSpeed: overallSpeed,
        overallSpeedFormatted: formatFileSize(overallSpeed) + '/s',
        efficiency: efficiency
      },

      // Chunking details
      chunking: {
        strategy: chunkingStrategy.strategy,
        totalChunks: chunkingStrategy.totalChunks,
        chunkSize: chunkingStrategy.chunkSize,
        chunkSizeFormatted: formatFileSize(chunkingStrategy.chunkSize),
        distribution: chunkingStrategy.distribution,
        parallelUploads: chunkingStrategy.parallelUploads,
        kvNamespaces: uploadResult.kvDistribution
      },

      // Performance metrics
      performance: {
        successRate: uploadResult.successRate,
        retries: uploadResult.totalRetries,
        bottlenecks: uploadResult.bottlenecks,
        performanceScore: calculatePerformanceScore(uploadResult, totalProcessingTime),
        downloadEfficiency: (downloadTime / totalProcessingTime) * 100,
        uploadEfficiency: (uploadResult.processingTime / totalProcessingTime) * 100
      },

      // Access URLs
      urls: urls,

      // Advanced features
      features: {
        instantStreaming: metadata.isVideo && chunkingStrategy.totalChunks > 1,
        rangeRequests: true,
        hlsStreaming: metadata.isVideo && file.size > 100 * 1024 * 1024,
        adaptiveBitrate: metadata.isVideo,
        resumableDownload: true,
        crossOriginSupport: true,
        cdnAcceleration: true,
        encryptionAtRest: true,
        analyticsTracking: true
      },

      // System info
      system: {
        version: '3.0.0-ultimate-url',
        infrastructure: 'Cloudflare Workers + KV + Telegram',
        maxFileSize: formatFileSize(maxFileSize),
        supportedProtocols: ['HTTP', 'HTTPS'],
        supportedDomains: 'All public domains',
        downloadTimeout: '10 minutes',
        retryPolicy: '5 attempts with exponential backoff'
      }
    };

    console.log(`ðŸŽ‰ URL upload completed:`, {
      downloadId: downloadId,
      filename: fileInfo.filename,
      size: formatFileSize(file.size),
      totalTime: formatTime(totalProcessingTime),
      overallSpeed: formatFileSize(overallSpeed) + '/s'
    });

    return new Response(JSON.stringify(response, null, 2), {
      status: 200,
      headers: { 
        'Content-Type': 'application/json',
        'X-Download-ID': downloadId,
        'X-Processing-Time': totalProcessingTime.toString(),
        'X-Download-Stats': JSON.stringify({
          downloadTime: downloadTime,
          downloadSpeed: actualSize / (downloadTime / 1000),
          efficiency: efficiency
        }),
        ...corsHeaders 
      }
    });

  } catch (error) {
    console.error('ðŸ’¥ URL UPLOAD ERROR:', error);

    const processingTime = Date.now() - startTime;

    return createJsonResponse({
      success: false,
      error: error.message,
      downloadId: downloadId,
      timestamp: new Date().toISOString(),
      processingTime: processingTime,
      errorCode: getErrorCode(error),
      troubleshooting: generateUrlTroubleshootingTips(error),
      support: {
        documentation: '/docs/url-upload-errors',
        contact: 'support@marya-vault.com'
      }
    }, 500, corsHeaders);
  }
}

/**
 * ðŸ” Advanced URL Validation
 */
function validateAdvancedUrl(url) {
  if (!url.startsWith('http://') && !url.startsWith('https://')) {
    return {
      valid: false,
      error: 'URL must start with http:// or https://'
    };
  }

  try {
    const parsedUrl = new URL(url);

    // Check for suspicious patterns
    const suspiciousPatterns = ['localhost', '127.0.0.1', '0.0.0.0', '::1'];
    if (suspiciousPatterns.some(pattern => parsedUrl.hostname.includes(pattern))) {
      return {
        valid: false,
        error: 'Local URLs are not allowed for security reasons'
      };
    }

    // Determine URL type
    let urlType = 'generic';
    if (parsedUrl.hostname.includes('workers.dev')) urlType = 'cloudflare_worker';
    else if (parsedUrl.hostname.includes('github')) urlType = 'github';
    else if (parsedUrl.hostname.includes('gdrive') || parsedUrl.hostname.includes('drive.google')) urlType = 'google_drive';
    else if (parsedUrl.hostname.includes('dropbox')) urlType = 'dropbox';
    else if (parsedUrl.hostname.includes('mega.')) urlType = 'mega';

    return {
      valid: true,
      type: urlType,
      hostname: parsedUrl.hostname,
      protocol: parsedUrl.protocol
    };
  } catch (urlError) {
    return {
      valid: false,
      error: `Invalid URL format: ${urlError.message}`
    };
  }
}

/**
 * ðŸ“Š Get Advanced File Information
 */
async function getAdvancedFileInfo(url) {
  console.log('ðŸ” Getting file info with HEAD request...');

  try {
    const headResponse = await fetch(url, { 
      method: 'HEAD',
      headers: {
        'User-Agent': 'MARYA-VAULT/3.0 Advanced Downloader',
        'Accept': '*/*',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache'
      },
      signal: AbortSignal.timeout(30000) // 30 seconds
    });

    let size = 0;
    let contentType = 'application/octet-stream';
    let filename = 'download';

    if (headResponse.ok) {
      size = parseInt(headResponse.headers.get('Content-Length') || '0');
      contentType = headResponse.headers.get('Content-Type') || contentType;

      // Extract filename from Content-Disposition
      const disposition = headResponse.headers.get('Content-Disposition');
      if (disposition && disposition.includes('filename')) {
        const match = disposition.match(/filename[*]?=([^;]+)/);
        if (match) {
          filename = match[1].replace(/['"]/g, '').trim();
          filename = decodeURIComponent(filename);
        }
      }
    }

    // If HEAD failed or no filename, extract from URL
    if (filename === 'download') {
      try {
        const urlPath = new URL(url).pathname;
        const urlFilename = urlPath.split('/').pop();
        if (urlFilename && urlFilename.includes('.')) {
          filename = decodeURIComponent(urlFilename);
        }
      } catch (e) {
        filename = `download_${Date.now()}`;
      }
    }

    // Clean filename
    filename = filename.replace(/[<>:"/\|?*]/g, '_').trim();

    // Add extension if missing
    if (!filename.includes('.') && contentType) {
      const ext = getExtensionFromMimeType(contentType);
      if (ext) filename += ext;
    }

    return {
      size: size,
      contentType: contentType,
      filename: filename,
      hasValidInfo: size > 0
    };

  } catch (error) {
    console.warn('âš ï¸ HEAD request failed, will determine info during download:', error.message);

    // Fallback: extract filename from URL
    let filename = 'download';
    try {
      const urlPath = new URL(url).pathname;
      const urlFilename = urlPath.split('/').pop();
      if (urlFilename && urlFilename.includes('.')) {
        filename = decodeURIComponent(urlFilename);
      }
    } catch (e) {
      filename = `download_${Date.now()}`;
    }

    return {
      size: 0,
      contentType: 'application/octet-stream',
      filename: filename.replace(/[<>:"/\|?*]/g, '_').trim(),
      hasValidInfo: false
    };
  }
}

/**
 * â¬‡ï¸ Advanced File Download
 */
async function downloadAdvancedFile(url, fileInfo) {
  const downloadStartTime = Date.now();

  console.log('â¬‡ï¸ Starting advanced file download...');

  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'User-Agent': 'MARYA-VAULT/3.0 Advanced Downloader',
        'Accept': '*/*',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      }
      // No timeout - let it download as long as needed
    });

    if (!response.ok) {
      throw new Error(`Download failed: ${response.status} ${response.statusText}`);
    }

    // Update content info if we have better data
    const actualContentType = response.headers.get('Content-Type') || fileInfo.contentType;
    const actualSize = parseInt(response.headers.get('Content-Length') || '0') || fileInfo.size;

    console.log('ðŸ“¥ Response received, reading stream...');

    // Read the response as stream with progress tracking
    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('Cannot read response stream');
    }

    const chunks = [];
    let receivedLength = 0;
    let lastProgressTime = Date.now();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      chunks.push(value);
      receivedLength += value.length;

      // Log progress every 100MB or 10 seconds
      const now = Date.now();
      if (receivedLength % (100 * 1024 * 1024) < value.length || now - lastProgressTime > 10000) {
        const speed = receivedLength / ((now - downloadStartTime) / 1000);
        console.log(`ðŸ“¥ Downloaded: ${formatFileSize(receivedLength)} at ${formatFileSize(speed)}/s`);
        lastProgressTime = now;
      }

      // Check size limits during download
      if (receivedLength > 1050 * 1024 * 1024) { // Max 1050MB
        reader.releaseLock();
        throw new Error(`File too large during download: ${formatFileSize(receivedLength)} (max 1050MB)`);
      }
    }

    reader.releaseLock();

    // Combine all chunks
    const arrayBuffer = new Uint8Array(receivedLength);
    let position = 0;
    for (const chunk of chunks) {
      arrayBuffer.set(chunk, position);
      position += chunk.length;
    }

    const downloadTime = Date.now() - downloadStartTime;
    const downloadSpeed = receivedLength / (downloadTime / 1000);

    console.log(`âœ… Download completed: ${formatFileSize(receivedLength)} in ${formatTime(downloadTime)} at ${formatFileSize(downloadSpeed)}/s`);

    return {
      success: true,
      arrayBuffer: arrayBuffer.buffer,
      actualSize: receivedLength,
      downloadTime: downloadTime,
      downloadSpeed: downloadSpeed,
      actualContentType: actualContentType
    };

  } catch (error) {
    console.error('âŒ Download failed:', error);
    return {
      success: false,
      error: error.message,
      downloadTime: Date.now() - downloadStartTime
    };
  }
}

// Additional utility functions for URL upload
function generateAdvancedDownloadId() {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).slice(2, 12);
  return `download_${timestamp}_${random}`;
}

function extractServerInfo(url) {
  try {
    const parsedUrl = new URL(url);
    return {
      hostname: parsedUrl.hostname,
      protocol: parsedUrl.protocol,
      port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? '443' : '80'),
      path: parsedUrl.pathname.substring(0, 50) + '...'
    };
  } catch (e) {
    return {
      hostname: 'unknown',
      protocol: 'unknown',
      port: 'unknown',
      path: 'unknown'
    };
  }
}

function getExtensionFromMimeType(mimeType) {
  const mimeMap = {
    'video/mp4': '.mp4',
    'video/webm': '.webm',
    'video/x-msvideo': '.avi',
    'video/quicktime': '.mov',
    'video/x-matroska': '.mkv',
    'audio/mpeg': '.mp3',
    'audio/wav': '.wav',
    'audio/aac': '.aac',
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'application/pdf': '.pdf',
    'application/zip': '.zip',
    'application/x-rar-compressed': '.rar'
  };

  return mimeMap[mimeType.toLowerCase()] || '';
}

function calculateUrlUploadEfficiency(downloadTime, uploadTime, fileSize) {
  const totalTime = downloadTime + uploadTime;
  const baselineTime = (fileSize / (10 * 1024 * 1024)) * 1000; // 10MB/s baseline
  return Math.round((baselineTime / totalTime) * 100);
}

function generateUrlTroubleshootingTips(error) {
  const message = error.message.toLowerCase();
  if (message.includes('download failed')) {
    return [
      'Check if the URL is accessible',
      'Verify the file still exists at the source',
      'Try the URL in a browser to confirm it works',
      'Check if the server requires authentication'
    ];
  }
  if (message.includes('timeout') || message.includes('slow')) {
    return [
      'The source server may be slow or overloaded',
      'Try downloading during off-peak hours',
      'Check if there are alternative download URLs',
      'Contact the file host about server performance'
    ];
  }
  if (message.includes('too large')) {
    return [
      'File exceeds maximum size limit',
      'Try compressing the file at source',
      'Use a file splitter to break into parts',
      'Consider using direct upload instead'
    ];
  }
  return [
    'Verify the URL is correct and accessible',
    'Check your internet connection',
    'Try the upload again after a few minutes',
    'Contact support if the issue persists'
  ];
}

// Include all utility functions from the main upload system
${utility_functions}