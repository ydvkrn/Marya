
// functions/upload-from-url.js
// Enhanced URL upload with better error handling for complex URLs

export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== ENHANCED URL UPLOAD START ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed'
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing bot credentials');
    }

    if (kvNamespaces.length === 0) {
      throw new Error('No KV namespaces available');
    }

    const { url } = await request.json();

    if (!url) {
      throw new Error('No URL provided');
    }

    console.log('Downloading from URL (truncated):', url.substring(0, 100) + '...');

    // Enhanced URL validation and cleaning
    let cleanUrl = url.trim();

    if (!cleanUrl.startsWith('http://') && !cleanUrl.startsWith('https://')) {
      throw new Error('Invalid URL - must start with http:// or https://');
    }

    // Handle special characters and encoding
    try {
      // Test if URL is parseable
      new URL(cleanUrl);
    } catch {
      throw new Error('Invalid URL format - contains unsupported characters');
    }

    // Enhanced fetch with better timeout and error handling
    console.log('Starting download with enhanced error handling...');

    let response;
    try {
      // First try a HEAD request to get file info
      const headResponse = await fetch(cleanUrl, { 
        method: 'HEAD',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
          'Accept': '*/*',
          'Accept-Encoding': 'identity'
        },
        signal: AbortSignal.timeout(30000) // 30 second timeout for HEAD
      });

      if (headResponse.ok) {
        const contentLength = parseInt(headResponse.headers.get('Content-Length') || '0');
        console.log(`File size from HEAD request: ${Math.round(contentLength/1024/1024)}MB`);

        // Check size limit early
        if (contentLength > 2 * 1024 * 1024 * 1024) {
          throw new Error(`File too large: ${Math.round(contentLength/1024/1024)}MB (max 2048MB)`);
        }
      }
    } catch (headError) {
      console.log('HEAD request failed, proceeding with GET request:', headError.message);
    }

    // Main download with extended timeout for large files
    response = await fetch(cleanUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache'
      },
      signal: AbortSignal.timeout(600000) // 10 minutes timeout for large files
    });

    if (!response.ok) {
      throw new Error(`Download failed: ${response.status} ${response.statusText}`);
    }

    // Get filename with enhanced extraction
    let filename = 'download';

    // Try Content-Disposition header first
    const contentDisposition = response.headers.get('Content-Disposition');
    if (contentDisposition) {
      const matches = contentDisposition.match(/filename[*]?=([^;\n\r"']+)/);
      if (matches && matches[1]) {
        filename = decodeURIComponent(matches[1].replace(/['"]/g, '').trim());
      }
    }

    // If no filename from header, extract from URL
    if (filename === 'download') {
      try {
        const urlPath = new URL(cleanUrl).pathname;
        const urlFilename = urlPath.split('/').pop();
        if (urlFilename && urlFilename.includes('.')) {
          filename = decodeURIComponent(urlFilename);
        }
      } catch {
        // Fallback to timestamp-based filename
        filename = `download_${Date.now()}`;
      }
    }

    // Clean filename (remove unsafe characters)
    filename = filename.replace(/[<>:"/\|?*]/g, '_').trim();

    console.log(`Downloading file: ${filename}`);

    // Convert to ArrayBuffer with progress logging
    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('Cannot read response stream');
    }

    const chunks = [];
    let receivedLength = 0;
    const contentLength = parseInt(response.headers.get('Content-Length') || '0');

    console.log(`Starting stream download, expected size: ${Math.round(contentLength/1024/1024)}MB`);

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      chunks.push(value);
      receivedLength += value.length;

      // Log progress every 50MB
      if (receivedLength % (50 * 1024 * 1024) < value.length) {
        console.log(`Downloaded: ${Math.round(receivedLength/1024/1024)}MB${contentLength > 0 ? `/${Math.round(contentLength/1024/1024)}MB` : ''}`);
      }

      // Check size limit during download
      if (receivedLength > 2 * 1024 * 1024 * 1024) {
        reader.releaseLock();
        throw new Error(`File too large: ${Math.round(receivedLength/1024/1024)}MB (max 2048MB)`);
      }
    }

    reader.releaseLock();

    // Combine chunks
    const arrayBuffer = new Uint8Array(receivedLength);
    let position = 0;
    for (const chunk of chunks) {
      arrayBuffer.set(chunk, position);
      position += chunk.length;
    }

    console.log(`Download completed: ${filename} (${Math.round(receivedLength/1024/1024)}MB)`);

    // Create file object
    const contentType = response.headers.get('Content-Type') || 'application/octet-stream';
    const file = new File([arrayBuffer.buffer], filename, { type: contentType });

    // Process with same chunking logic as regular upload
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB per chunk
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > kvNamespaces.length * 25) {
      throw new Error(`File requires ${totalChunks} chunks, but only ${kvNamespaces.length * 25} chunks supported`);
    }

    console.log(`Processing ${totalChunks} chunks for URL download`);

    // Upload chunks with enhanced error handling
    const chunkPromises = [];
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const chunkFile = new File([chunk], `${filename}.part${i}`, { type: contentType });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      // Add retry logic for chunk uploads
      const chunkPromise = uploadChunkWithRetry(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV, 3);
      chunkPromises.push(chunkPromise);
    }

    const chunkResults = await Promise.all(chunkPromises);

    // Store master metadata
    const masterMetadata = {
      filename: filename,
      size: file.size,
      contentType: contentType,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'url_upload_chunked',
      totalChunks: totalChunks,
      chunkSize: CHUNK_SIZE,
      sourceUrl: url.substring(0, 200) + '...', // Truncate long URL in metadata
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.chunkKey,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;

    const result = {
      success: true,
      filename: filename,
      size: file.size,
      contentType: contentType,
      url: customUrl,
      download: downloadUrl,
      id: fileId,
      strategy: 'url_upload_chunked',
      chunks: totalChunks,
      sourceUrl: url.substring(0, 100) + '...', // Truncated for response
      kvDistribution: chunkResults.map(r => r.kvNamespace)
    };

    console.log('Enhanced URL upload completed:', {
      filename: result.filename,
      size: `${Math.round(result.size/1024/1024)}MB`,
      chunks: result.chunks
    });

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Enhanced URL upload error:', error);

    // Enhanced error messages
    let errorMessage = error.message;
    if (error.name === 'TimeoutError') {
      errorMessage = 'Download timeout - file too large or server too slow';
    } else if (error.message.includes('fetch')) {
      errorMessage = 'Failed to download file - check if URL is accessible';
    } else if (error.message.includes('AbortError')) {
      errorMessage = 'Download was cancelled due to timeout';
    }

    return new Response(JSON.stringify({
      success: false,
      error: errorMessage
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Enhanced chunk upload with retry logic
async function uploadChunkWithRetry(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace, maxRetries = 3) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Uploading chunk ${chunkIndex} to ${kvNamespace.name} (attempt ${attempt}/${maxRetries})`);

      return await uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace);
    } catch (error) {
      lastError = error;
      console.error(`Chunk ${chunkIndex} upload attempt ${attempt} failed:`, error.message);

      if (attempt < maxRetries) {
        // Wait before retry (exponential backoff)
        const waitTime = Math.pow(2, attempt) * 1000;
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
    }
  }

  throw new Error(`Chunk ${chunkIndex} failed after ${maxRetries} attempts: ${lastError.message}`);
}

// Standard chunk upload function
async function uploadChunkToKV(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  // Upload to Telegram
  const telegramForm = new FormData();
  telegramForm.append('chat_id', channelId);
  telegramForm.append('document', chunkFile);

  const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: telegramForm
  });

  if (!telegramResponse.ok) {
    const errorText = await telegramResponse.text();
    throw new Error(`Telegram upload failed for chunk ${chunkIndex}: ${telegramResponse.status} - ${errorText}`);
  }

  const telegramData = await telegramResponse.json();
  if (!telegramData.ok || !telegramData.result?.document?.file_id) {
    throw new Error(`Invalid Telegram response for chunk ${chunkIndex}: ${JSON.stringify(telegramData)}`);
  }

  const telegramFileId = telegramData.result.document.file_id;

  // Get file URL
  const getFileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
  if (!getFileResponse.ok) {
    throw new Error(`GetFile API failed for chunk ${chunkIndex}: ${getFileResponse.status}`);
  }

  const getFileData = await getFileResponse.json();
  if (!getFileData.ok || !getFileData.result?.file_path) {
    throw new Error(`No file_path for chunk ${chunkIndex}: ${JSON.stringify(getFileData)}`);
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  // Store chunk metadata
  const chunkKey = `${fileId}_chunk_${chunkIndex}`;
  const chunkMetadata = {
    telegramFileId: telegramFileId,
    directUrl: directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    uploadedAt: Date.now(),
    lastRefreshed: Date.now()
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

  return {
    telegramFileId: telegramFileId,
    size: chunkFile.size,
    directUrl: directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey: chunkKey
  };
}