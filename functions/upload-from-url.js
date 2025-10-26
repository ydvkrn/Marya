// functions/upload-from-url.js
// ðŸŒ ULTRA-ROBUST URL UPLOAD - HTTP 500 FIX

export async function onRequest(context) {
  const { request, env } = context;

  console.log('ðŸŒ URL UPLOAD STARTED:', new Date().toISOString());

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return jsonResponse({ success: false, error: 'Only POST allowed' }, 405, corsHeaders);
  }

  try {
    // Validate environment
    if (!env.BOT_TOKEN || !env.CHAT_ID || !env.FILES_KV) {
      throw new Error('Server configuration error - Missing credentials');
    }

    // Get URL from request
    let body;
    try {
      body = await request.json();
    } catch (error) {
      throw new Error('Invalid JSON in request body');
    }

    const { url } = body;

    if (!url || !url.trim()) {
      throw new Error('No URL provided');
    }

    console.log('ðŸŒ Downloading from:', url);

    // Validate URL
    let urlObj;
    try {
      urlObj = new URL(url);
    } catch {
      throw new Error('Invalid URL format');
    }

    if (!['http:', 'https:'].includes(urlObj.protocol)) {
      throw new Error('Only HTTP/HTTPS URLs are supported');
    }

    // Download with retry
    let downloadResponse;
    let lastError;
    const maxDownloadRetries = 3;

    for (let attempt = 1; attempt <= maxDownloadRetries; attempt++) {
      try {
        console.log(`ðŸ”„ Download attempt ${attempt}/${maxDownloadRetries}`);

        const downloadPromise = fetch(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*'
          }
        });

        const timeoutPromise = new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Download timeout (120s)')), 120000)
        );

        downloadResponse = await Promise.race([downloadPromise, timeoutPromise]);

        if (downloadResponse.ok) {
          break;
        }

        throw new Error(`HTTP ${downloadResponse.status}`);

      } catch (error) {
        console.error(`âŒ Download attempt ${attempt} failed:`, error.message);
        lastError = error;

        if (attempt < maxDownloadRetries) {
          await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
        }
      }
    }

    if (!downloadResponse || !downloadResponse.ok) {
      throw new Error(`Download failed: ${lastError?.message || 'Unknown error'}`);
    }

    // Get filename
    let filename = 'download';
    const contentDisposition = downloadResponse.headers.get('Content-Disposition');

    if (contentDisposition) {
      const match = contentDisposition.match(/filename[*]?=([^;\n\r"']+)/);
      if (match) {
        filename = match[1].replace(/['"]/g, '').trim();
      }
    }

    if (filename === 'download') {
      const urlPath = urlObj.pathname;
      const urlFilename = urlPath.split('/').pop();
      if (urlFilename && urlFilename.length > 0) {
        filename = urlFilename;
      }
    }

    // Get content type
    const contentType = downloadResponse.headers.get('Content-Type') || 'application/octet-stream';

    // Add extension if missing
    if (!filename.includes('.')) {
      const ext = getExtFromMimeType(contentType);
      if (ext) filename += ext;
    }

    // Download content
    console.log('ðŸ“¥ Downloading content...');

    const arrayBuffer = await downloadResponse.arrayBuffer();
    const fileSize = arrayBuffer.byteLength;

    console.log(`âœ… Downloaded: ${filename} (${formatBytes(fileSize)})`);

    // Validate size
    if (fileSize === 0) {
      throw new Error('Downloaded file is empty');
    }

    if (fileSize > 2147483648) {
      throw new Error(`File too large: ${formatBytes(fileSize)} (max 2GB)`);
    }

    // Generate unique filename
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const fileId = `${timestamp}${random}`;

    const ext = filename.includes('.') ? filename.substring(filename.lastIndexOf('.')) : '';
    const baseName = filename.substring(0, filename.lastIndexOf('.') || filename.length);
    const sanitized = baseName.toLowerCase().replace(/[^a-z0-9-]/g, '_').substring(0, 40);
    const finalFilename = `${sanitized}_${fileId}${ext}`;

    console.log('ðŸ†” Generated filename:', finalFilename);

    // Create File object
    const file = new File([arrayBuffer], filename, { type: contentType });

    // Determine upload strategy
    const CHUNK_THRESHOLD = 50 * 1024 * 1024;
    const CHUNK_SIZE = 10 * 1024 * 1024;
    const needsChunking = fileSize > CHUNK_THRESHOLD;

    console.log(`ðŸ“Š Upload strategy: ${needsChunking ? 'CHUNKED' : 'SINGLE'}`);

    let result;

    if (needsChunking) {
      result = await uploadChunked(file, finalFilename, env, CHUNK_SIZE);
    } else {
      result = await uploadSingle(file, finalFilename, env);
    }

    console.log('âœ… URL upload completed');

    return jsonResponse({
      success: true,
      filename: finalFilename,
      id: fileId,
      originalName: filename,
      size: fileSize,
      contentType: contentType,
      uploadType: needsChunking ? 'chunked' : 'single',
      chunks: result.chunks || 0,
      sourceUrl: url,
      uploadedAt: new Date().toISOString()
    }, 200, corsHeaders);

  } catch (error) {
    console.error('âŒ URL UPLOAD ERROR:', error.message);
    console.error('Stack:', error.stack);

    return jsonResponse({
      success: false,
      error: error.message || 'URL upload failed',
      details: error.stack?.split('\n')[0]
    }, 500, corsHeaders);
  }
}

// Single upload
async function uploadSingle(file, filename, env) {
  console.log('ðŸ“¤ Starting SINGLE upload...');

  const maxRetries = 3;
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`ðŸ”„ Upload attempt ${attempt}/${maxRetries}`);

      const formData = new FormData();
      formData.append('chat_id', env.CHAT_ID);
      formData.append('document', file);
      formData.append('caption', `ðŸŒ URL: ${file.name}\nSize: ${formatBytes(file.size)}`);

      const response = await fetch(
        `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
        {
          method: 'POST',
          body: formData
        }
      );

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();

      if (!data.ok || !data.result?.document?.file_id) {
        throw new Error('Invalid Telegram response');
      }

      const telegramFileId = data.result.document.file_id;

      const metadata = {
        filename: filename,
        originalName: file.name,
        size: file.size,
        contentType: file.type || 'application/octet-stream',
        uploadType: 'single',
        telegramFileId: telegramFileId,
        uploadedAt: Date.now()
      };

      await env.FILES_KV.put(filename, JSON.stringify(metadata));

      console.log('âœ… Single upload successful');
      return { chunks: 0 };

    } catch (error) {
      console.error(`âŒ Attempt ${attempt} failed:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt)));
      }
    }
  }

  throw new Error(`Upload failed after ${maxRetries} attempts: ${lastError.message}`);
}

// Chunked upload
async function uploadChunked(file, filename, env, chunkSize) {
  console.log('ðŸ§© Starting CHUNKED upload...');

  const totalChunks = Math.ceil(file.size / chunkSize);
  console.log(`ðŸ“Š Total chunks: ${totalChunks}`);

  if (totalChunks > 200) {
    throw new Error(`Too many chunks: ${totalChunks}`);
  }

  const chunks = [];

  for (let i = 0; i < totalChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, file.size);
    const chunkBlob = file.slice(start, end);

    console.log(`ðŸ“¤ Uploading chunk ${i + 1}/${totalChunks}`);

    const chunkFilename = `${filename}.chunk${String(i).padStart(4, '0')}`;
    const chunkFile = new File([chunkBlob], chunkFilename, { type: 'application/octet-stream' });

    const chunkResult = await uploadChunkWithRetry(chunkFile, i, filename, env, 3);
    chunks.push(chunkResult);

    if (i < totalChunks - 1) {
      await new Promise(resolve => setTimeout(resolve, 300));
    }
  }

  const masterMetadata = {
    filename: filename,
    originalName: file.name,
    size: file.size,
    contentType: file.type || 'application/octet-stream',
    uploadType: 'chunked',
    totalChunks: totalChunks,
    chunkSize: chunkSize,
    chunks: chunks,
    uploadedAt: Date.now()
  };

  await env.FILES_KV.put(filename, JSON.stringify(masterMetadata));

  console.log('âœ… All chunks uploaded');
  return { chunks: totalChunks };
}

// Upload chunk with retry
async function uploadChunkWithRetry(chunkFile, index, parentFilename, env, maxRetries) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const formData = new FormData();
      formData.append('chat_id', env.CHAT_ID);
      formData.append('document', chunkFile);
      formData.append('caption', `ðŸ§© Chunk ${index + 1}`);

      const response = await fetch(
        `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
        {
          method: 'POST',
          body: formData
        }
      );

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();

      if (!data.ok || !data.result?.document?.file_id) {
        throw new Error('Invalid response');
      }

      const telegramFileId = data.result.document.file_id;

      const chunkKey = `${parentFilename}_chunk_${String(index).padStart(4, '0')}`;
      const chunkMeta = {
        parentFile: parentFilename,
        index: index,
        size: chunkFile.size,
        telegramFileId: telegramFileId,
        uploadedAt: Date.now()
      };

      await env.FILES_KV.put(chunkKey, JSON.stringify(chunkMeta));

      console.log(`âœ… Chunk ${index + 1} uploaded`);

      return {
        index: index,
        size: chunkFile.size,
        telegramFileId: telegramFileId
      };

    } catch (error) {
      console.error(`âŒ Chunk ${index + 1} attempt ${attempt} failed:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
      }
    }
  }

  throw new Error(`Chunk ${index + 1} failed: ${lastError.message}`);
}

// Utilities
function getExtFromMimeType(mimeType) {
  const map = {
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'video/mp4': '.mp4',
    'video/webm': '.webm',
    'video/x-matroska': '.mkv',
    'audio/mpeg': '.mp3',
    'audio/wav': '.wav',
    'application/pdf': '.pdf',
    'application/zip': '.zip',
    'text/plain': '.txt',
  };
  return map[mimeType] || '';
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function jsonResponse(data, status, headers) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { ...headers, 'Content-Type': 'application/json' }
  });
}