// 🚀 ULTIMATE MARYA VAULT - ADVANCED URL UPLOAD SYSTEM
// Enhanced to match the advanced upload system (FIXED & COMPLETE)

// Main handler for the request
export async function onRequest(context) {
  const { request, env } = context;

  console.log('🌐 === ULTIMATE URL UPLOAD START === 🌐');
  console.log('📅 Timestamp:', new Date().toISOString());

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
    // 🔧 Enhanced environment validation
    const envValidation = await validateAdvancedEnvironment(env);
    if (!envValidation.success) {
      throw new Error(`Environment validation failed: ${envValidation.error}`);
    }

    const { kvNamespaces, botTokens, channelId } = envValidation;

    // 📥 Process URL input
    const { url } = await request.json();

    if (!url || !url.trim()) {
      throw new Error('No URL provided');
    }

    const cleanUrl = url.trim();
    console.log('🔗 Processing URL:', cleanUrl.substring(0, 100) + '...');

    // 🔍 Advanced URL validation
    const urlValidation = validateAdvancedUrl(cleanUrl);
    if (!urlValidation.valid) {
      throw new Error(urlValidation.error);
    }

    console.log('✅ URL validation passed:', urlValidation.type);

    // 📊 Get file information
    const fileInfo = await getAdvancedFileInfo(cleanUrl);
    console.log('📁 File info retrieved:', {
      size: formatFileSize(fileInfo.size),
      type: fileInfo.contentType,
      filename: fileInfo.filename
    });

    // ✅ Validate file size
    const maxFileSize = calculateMaxFileSize(kvNamespaces.length);
    if (fileInfo.size > 0 && fileInfo.size > maxFileSize) {
        throw new Error(`File too large: ${formatFileSize(fileInfo.size)} (max: ${formatFileSize(maxFileSize)})`);
    }

    // ⬇️ Enhanced download with progress tracking
    console.log('⬇️ Starting enhanced download...');
    const downloadResult = await downloadAdvancedFile(cleanUrl, fileInfo);

    if (!downloadResult.success) {
      throw new Error(`Download failed: ${downloadResult.error}`);
    }

    const { arrayBuffer, actualSize, downloadTime } = downloadResult;
    console.log(`✅ Download completed: ${formatFileSize(actualSize)} in ${formatTime(downloadTime)}`);

    // 📁 Create enhanced file object
    const file = new File([arrayBuffer], fileInfo.filename, { 
      type: fileInfo.contentType,
      lastModified: Date.now()
    });

    // 📊 Generate advanced metadata
    const metadata = await generateAdvancedFileMetadata(file);
    metadata.sourceUrl = cleanUrl.length > 200 ? cleanUrl.substring(0, 200) + '...' : cleanUrl;
    metadata.downloadStats = {
      downloadTime: downloadTime,
      downloadSpeed: actualSize / (downloadTime / 1000),
      sourceServer: extractServerInfo(cleanUrl),
      userAgent: 'MARYA-VAULT/3.0 Advanced Downloader'
    };

    // 🆔 Generate advanced file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 10);
    const category = metadata.category.charAt(0).toUpperCase();
    const sizeIndicator = file.size > 500 * 1024 * 1024 ? 'L' : 'M';
    const fileId = `${category}${sizeIndicator}${timestamp}${random}`;
    const extension = fileInfo.filename.includes('.') ? fileInfo.filename.slice(fileInfo.filename.lastIndexOf('.')) : '';

    console.log(`🆔 Advanced file ID: ${fileId}${extension}`);

    // 🧩 Calculate optimal chunking strategy
    const chunkingStrategy = calculateOptimalChunkingStrategy(file.size, kvNamespaces.length, metadata);
    console.log(`🧩 Chunking strategy:`, {
      totalChunks: chunkingStrategy.totalChunks,
      chunkSize: formatFileSize(chunkingStrategy.chunkSize),
      distribution: chunkingStrategy.distribution,
      parallelUploads: chunkingStrategy.parallelUploads
    });

    // 🚀 Execute advanced chunked upload
    const uploadResult = await executeAdvancedChunkedUpload(
      file, 
      fileId, 
      chunkingStrategy, 
      kvNamespaces, 
      botTokens, 
      channelId,
      downloadId
    );

    // 📈 Create comprehensive metadata
    const masterMetadata = createAdvancedMasterMetadata(
      file, 
      fileId, 
      extension, 
      chunkingStrategy, 
      uploadResult, 
      metadata,
      startTime
    );

    masterMetadata.source = {
      type: 'url_upload',
      originalUrl: cleanUrl,
      downloadTime: downloadTime,
      downloadSpeed: actualSize / (downloadTime / 1000),
      serverInfo: extractServerInfo(cleanUrl),
      downloadId: downloadId
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));
    console.log(`💾 Master metadata stored`);

    // 🔗 Generate advanced URLs
    const baseUrl = new URL(request.url).origin;
    const urls = generateAdvancedUrls(baseUrl, fileId, extension, metadata);

    // 📊 Calculate comprehensive metrics
    const totalProcessingTime = Date.now() - startTime;
    const overallSpeed = file.size / (totalProcessingTime / 1000);
    const efficiency = calculateUrlUploadEfficiency(downloadTime, uploadResult.processingTime, file.size);

    // 🎉 Create ultimate response
    const responseData = {
      success: true,
      message: '🌐 Ultimate URL upload completed!',
      source: {
        originalUrl: cleanUrl.length > 100 ? cleanUrl.substring(0, 100) + '...' : cleanUrl,
        downloadTimeFormatted: formatTime(downloadTime),
        downloadSpeedFormatted: formatFileSize(actualSize / (downloadTime / 1000)) + '/s'
      },
      file: {
        id: fileId,
        filename: fileInfo.filename,
        size: file.size,
        sizeFormatted: formatFileSize(file.size),
        extension: extension
      },
      processing: {
        totalTimeFormatted: formatTime(totalProcessingTime),
        overallSpeedFormatted: formatFileSize(overallSpeed) + '/s',
      },
      urls: urls,
    };

    return new Response(JSON.stringify(responseData, null, 2), {
      status: 200,
      headers: { 
        'Content-Type': 'application/json',
        ...corsHeaders 
      }
    });

  } catch (error) {
    console.error('💥 URL UPLOAD ERROR:', error);
    const processingTime = Date.now() - startTime;
    return createJsonResponse({
      success: false,
      error: error.message,
      downloadId: downloadId,
      timestamp: new Date().toISOString(),
      processingTime: processingTime,
      errorCode: getErrorCode(error),
      troubleshooting: generateUrlTroubleshootingTips(error),
    }, 500, corsHeaders);
  }
}


// #############################################################
// #################### UTILITY FUNCTIONS ######################
// #############################################################

/**
 * 🔧 Advanced Environment Validation
 */
async function validateAdvancedEnvironment(env) {
    const requiredVars = ['BOT_TOKEN', 'CHANNEL_ID'];
    const missing = requiredVars.filter(key => !env[key]);
    if (missing.length > 0) {
        return { success: false, error: `Missing env variables: ${missing.join(', ')}` };
    }
    const kvNamespaces = [];
    for (let i = 1; i <= 7; i++) {
        const kvKey = i === 1 ? 'FILES_KV' : `FILES_KV${i}`;
        if (env[kvKey]) {
            kvNamespaces.push({ kv: env[kvKey], name: kvKey, index: i - 1 });
        }
    }
    const botTokens = [];
    for (let i = 1; i <= 4; i++) {
        const tokenKey = i === 1 ? 'BOT_TOKEN' : `BOT_TOKEN${i}`;
        if (env[tokenKey]) {
            botTokens.push({ token: env[tokenKey], name: tokenKey, index: i - 1 });
        }
    }
    if (kvNamespaces.length === 0) {
        return { success: false, error: 'No KV namespaces configured' };
    }
    return {
        success: true,
        kvNamespaces,
        botTokens,
        channelId: env.CHANNEL_ID,
    };
}

/**
 * 🔍 Advanced URL Validation
 */
function validateAdvancedUrl(url) {
    if (!url.startsWith('http://') && !url.startsWith('https://')) {
        return { valid: false, error: 'URL must start with http:// or https://' };
    }
    try {
        const parsedUrl = new URL(url);
        const suspiciousPatterns = ['localhost', '127.0.0.1'];
        if (suspiciousPatterns.some(pattern => parsedUrl.hostname.includes(pattern))) {
            return { valid: false, error: 'Local URLs are not allowed' };
        }
        let urlType = 'generic';
        if (parsedUrl.hostname.includes('workers.dev')) urlType = 'cloudflare_worker';
        return { valid: true, type: urlType, hostname: parsedUrl.hostname };
    } catch (urlError) {
        return { valid: false, error: `Invalid URL format: ${urlError.message}` };
    }
}

/**
 * 📊 Get Advanced File Information from URL
 */
async function getAdvancedFileInfo(url) {
    try {
        const headResponse = await fetch(url, {
            method: 'HEAD',
            headers: { 'User-Agent': 'MARYA-VAULT/3.0' },
            signal: AbortSignal.timeout(30000)
        });
        let size = 0;
        let contentType = 'application/octet-stream';
        let filename = 'download';
        if (headResponse.ok) {
            size = parseInt(headResponse.headers.get('Content-Length') || '0');
            contentType = headResponse.headers.get('Content-Type') || contentType;
            const disposition = headResponse.headers.get('Content-Disposition');
            if (disposition && disposition.includes('filename')) {
                const match = disposition.match(/filename[*]?=([^;]+)/);
                if (match) filename = decodeURIComponent(match[1].replace(/['"]/g, '').trim());
            }
        }
        if (filename === 'download') {
            const urlPath = new URL(url).pathname;
            const urlFilename = urlPath.split('/').pop();
            if (urlFilename) filename = decodeURIComponent(urlFilename);
        }
        filename = filename.replace(/[<>:"/\\|?*]/g, '_').trim();
        if (!filename.includes('.') && contentType) {
            const ext = getExtensionFromMimeType(contentType);
            if (ext) filename += ext;
        }
        return { size, contentType, filename };
    } catch (error) {
        console.warn('⚠️ HEAD request failed, proceeding without file info:', error.message);
        let filename = 'download';
        try {
            const urlPath = new URL(url).pathname;
            const urlFilename = urlPath.split('/').pop();
            if (urlFilename) filename = decodeURIComponent(urlFilename);
        } catch (e) {
            filename = `download_${Date.now()}`;
        }
        return { size: 0, contentType: 'application/octet-stream', filename: filename.replace(/[<>:"/\\|?*]/g, '_').trim() };
    }
}

/**
 * ⬇️ Advanced File Download from URL
 */
async function downloadAdvancedFile(url, fileInfo) {
    const downloadStartTime = Date.now();
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: { 'User-Agent': 'MARYA-VAULT/3.0' }
        });
        if (!response.ok) {
            throw new Error(`Download failed: ${response.status} ${response.statusText}`);
        }
        const arrayBuffer = await response.arrayBuffer();
        const downloadTime = Date.now() - downloadStartTime;
        return {
            success: true,
            arrayBuffer,
            actualSize: arrayBuffer.byteLength,
            downloadTime
        };
    } catch (error) {
        return { success: false, error: error.message, downloadTime: Date.now() - downloadStartTime };
    }
}

/**
 * 📊 Generate Advanced File Metadata
 */
async function generateAdvancedFileMetadata(file) {
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.') + 1).toLowerCase() : '';
    const categories = {
        video: ['mp4', 'mkv', 'webm'],
        audio: ['mp3', 'wav', 'ogg'],
        image: ['jpg', 'jpeg', 'png', 'gif'],
        document: ['pdf', 'doc', 'docx', 'txt'],
        archive: ['zip', 'rar', '7z']
    };
    let category = 'other';
    for (const cat in categories) {
        if (categories[cat].includes(extension)) {
            category = cat;
            break;
        }
    }
    const hash = generateFileHash(file.name, file.size);
    return {
        category,
        extension,
        hash,
        isVideo: category === 'video',
        isLarge: file.size > 500 * 1024 * 1024,
        quality: determineFileQuality(file.name, file.size),
    };
}

/**
 * 🧩 Calculate Optimal Chunking Strategy
 */
function calculateOptimalChunkingStrategy(fileSize, kvCount, metadata) {
    let baseChunkSize = 20 * 1024 * 1024; // Default 20MB
    if (metadata.isVideo) baseChunkSize = 25 * 1024 * 1024;
    else if (fileSize > 1024 * 1024 * 1024) baseChunkSize = 30 * 1024 * 1024;
    const totalChunks = Math.ceil(fileSize / baseChunkSize);
    const distribution = {};
    for (let i = 0; i < totalChunks; i++) {
        const kvIndex = i % kvCount;
        distribution[kvIndex] = (distribution[kvIndex] || 0) + 1;
    }
    return {
        strategy: totalChunks === 1 ? 'single' : 'chunked',
        chunkSize: baseChunkSize,
        totalChunks,
        distribution,
        parallelUploads: Math.min(5, kvCount),
    };
}

/**
 * 🚀 Execute Advanced Chunked Upload
 */
async function executeAdvancedChunkedUpload(file, fileId, strategy, kvNamespaces, botTokens, channelId, uploadId) {
    const startTime = Date.now();
    const results = [];
    const chunkQueue = [];
    for (let i = 0; i < strategy.totalChunks; i++) {
        const start = i * strategy.chunkSize;
        const end = Math.min(start + strategy.chunkSize, file.size);
        const chunk = file.slice(start, end);
        const chunkFile = new File([chunk], `${file.name}.chunk${i.toString().padStart(3, '0')}`);
        chunkQueue.push({
            index: i,
            file: chunkFile,
            kvNamespace: kvNamespaces[i % kvNamespaces.length],
            botToken: botTokens[i % botTokens.length].token,
        });
    }
    for (let i = 0; i < chunkQueue.length; i += strategy.parallelUploads) {
        const batch = chunkQueue.slice(i, i + strategy.parallelUploads);
        const batchPromises = batch.map(info => uploadAdvancedChunkWithRetry(info.file, fileId, info.index, info.botToken, channelId, info.kvNamespace, 3));
        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults);
    }
    const totalTime = Date.now() - startTime;
    return {
        results,
        totalRetries: 0, // Simplified for this fix
        bottlenecks: [], // Simplified
        processingTime: totalTime,
        successRate: (results.length / strategy.totalChunks) * 100,
        kvDistribution: [...new Set(results.map(r => r.kvNamespace))],
    };
}

/**
 * 🔄 Advanced Chunk Upload with Retry Logic
 */
async function uploadAdvancedChunkWithRetry(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, maxRetries = 3) {
    let lastError;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await uploadSingleAdvancedChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace);
        } catch (error) {
            lastError = error;
            console.warn(`⚠️ Chunk ${chunkIndex} attempt ${attempt} failed: ${error.message}`);
            if (attempt < maxRetries) {
                await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
            }
        }
    }
    throw new Error(`Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${lastError.message}`);
}

/**
 * 📤 Upload Single Advanced Chunk
 */
async function uploadSingleAdvancedChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);
    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
        method: 'POST',
        body: telegramForm
    });
    if (!telegramResponse.ok) throw new Error(`Telegram API error ${telegramResponse.status}`);
    const telegramData = await telegramResponse.json();
    if (!telegramData.ok) throw new Error(`Telegram API rejected: ${telegramData.description}`);
    const telegramFileId = telegramData.result.document.file_id;
    const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
    if (!getFileResponse.ok) throw new Error(`GetFile API error ${getFileResponse.status}`);
    const getFileData = await getFileResponse.json();
    if (!getFileData.ok) throw new Error(`GetFile failed: ${getFileData.description}`);
    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
    const chunkKey = `${fileId}_chunk_${chunkIndex.toString().padStart(3, '0')}`;
    const chunkMetadata = {
        telegramFileId,
        directUrl,
        size: chunkFile.size,
        index: chunkIndex,
        parentFileId: fileId,
        kvNamespace: kvNamespace.name,
        uploadedAt: Date.now()
    };
    await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));
    return {
        telegramFileId,
        directUrl,
        size: chunkFile.size,
        chunkKey,
        kvNamespace: kvNamespace.name
    };
}

/**
 * 📋 Create Advanced Master Metadata
 */
function createAdvancedMasterMetadata(file, fileId, extension, chunkingStrategy, uploadResult, metadata, startTime) {
    const processingTime = Date.now() - startTime;
    return {
        fileId,
        filename: file.name,
        extension,
        size: file.size,
        contentType: file.type,
        category: metadata.category,
        hash: metadata.hash,
        type: 'advanced_multi_kv_chunked',
        totalChunks: chunkingStrategy.totalChunks,
        chunkSize: chunkingStrategy.chunkSize,
        uploadedAt: Date.now(),
        processingTime,
        chunks: uploadResult.results.map((result, index) => ({
            index,
            kvNamespace: result.kvNamespace,
            chunkKey: result.chunkKey,
            telegramFileId: result.telegramFileId,
            size: result.size,
        })),
        version: '3.0.0'
    };
}

/**
 * 🔗 Generate Advanced URLs
 */
function generateAdvancedUrls(baseUrl, fileId, extension, metadata) {
    const urls = {
        stream: `${baseUrl}/btfstorage/file/${fileId}${extension}`,
        download: `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`,
        info: `${baseUrl}/btfstorage/info/${fileId}`,
    };
    if (metadata.isVideo) {
        urls.hls = `${baseUrl}/btfstorage/hls/${fileId}/master.m3u8`;
        urls.thumbnail = `${baseUrl}/btfstorage/thumb/${fileId}.jpg`;
    }
    return urls;
}

// Other Helper Functions
function createJsonResponse(data, status = 200, headers = {}) {
    return new Response(JSON.stringify(data, null, 2), {
        status,
        headers: { 'Content-Type': 'application/json', ...headers }
    });
}
function generateAdvancedDownloadId() { return `download_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 12)}`; }
function calculateMaxFileSize(kvCount) { return kvCount * 150 * 1024 * 1024; }
function formatFileSize(bytes) { if (bytes === 0) return '0 Bytes'; const i = Math.floor(Math.log(bytes) / Math.log(1024)); return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + ['Bytes', 'KB', 'MB', 'GB', 'TB'][i]; }
function formatTime(ms) { if (ms < 1000) return `${ms}ms`; if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`; return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`; }
function generateFileHash(filename, size) { let hash = 0; const str = filename + size; for (let i = 0; i < str.length; i++) { hash = ((hash << 5) - hash) + str.charCodeAt(i); hash |= 0; } return Math.abs(hash).toString(36); }
function determineFileQuality(filename) { const name = filename.toLowerCase(); if (name.includes('1080p')) return 'high'; if (name.includes('720p')) return 'medium'; if (name.includes('480p')) return 'standard'; return 'standard'; }
function extractServerInfo(url) { try { const u = new URL(url); return { hostname: u.hostname, protocol: u.protocol }; } catch { return { hostname: 'unknown' }; } }
function getExtensionFromMimeType(mimeType) { const map = { 'video/mp4': '.mp4', 'video/webm': '.webm', 'image/jpeg': '.jpg', 'application/zip': '.zip' }; return map[mimeType.toLowerCase()] || ''; }
function calculateUrlUploadEfficiency(downloadTime, uploadTime, fileSize) { const totalTime = downloadTime + uploadTime; const baseline = (fileSize / (10 * 1024 * 1024)) * 1000; return Math.round((baseline / totalTime) * 100); }
function getErrorCode(error) { const msg = error.message.toLowerCase(); if (msg.includes('file too large')) return 'FILE_TOO_LARGE'; if (msg.includes('telegram')) return 'TELEGRAM_API_ERROR'; if (msg.includes('timeout')) return 'TIMEOUT_ERROR'; return 'UNKNOWN_ERROR'; }
function generateUrlTroubleshootingTips(error) { const code = getErrorCode(error); if (code === 'FILE_TOO_LARGE') return ['File exceeds max size limit']; if (code === 'TELEGRAM_API_ERROR') return ['Check bot token and channel ID']; if (code === 'TIMEOUT_ERROR') return ['Source server may be too slow']; return ['Check if URL is correct and public']; }
function calculatePerformanceScore(uploadResult, processingTime) { const reliabilityScore = uploadResult.successRate; return Math.round(reliabilityScore); }
