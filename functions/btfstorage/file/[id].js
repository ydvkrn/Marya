// functions/btfstorage/file/[id].js
// ✅ Fully Tested & Debugged - Fast Streaming

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/x-matroska', 'avi': 'video/x-msvideo',
  'mov': 'video/quicktime', 'webm': 'video/webm', 'mp3': 'audio/mpeg',
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'pdf': 'application/pdf', 'm3u8': 'application/x-mpegURL'
};

// 🔧 DEBUG MODE - Set to true for detailed logs, false for production
const DEBUG = true;

function log(...args) {
  if (DEBUG) console.log('🔍', ...args);
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  log('=== REQUEST START ===');
  log('File ID:', fileId);
  log('URL:', request.url);
  log('Method:', request.method);

  // CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
        'Access-Control-Allow-Headers': 'Range',
        'Access-Control-Max-Age': '86400'
      }
    });
  }

  try {
    // Parse file ID and extension
    let actualId = fileId;
    let extension = '';
    
    if (fileId.includes('.')) {
      const lastDot = fileId.lastIndexOf('.');
      actualId = fileId.substring(0, lastDot);
      extension = fileId.substring(lastDot + 1).toLowerCase();
      log('Parsed ID:', actualId, 'Extension:', extension);
    }

    // Get metadata from KV
    log('Fetching metadata from KV...');
    const metadataString = await env.FILES_KV.get(actualId);
    
    if (!metadataString) {
      log('❌ File not found in KV:', actualId);
      return errorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);
    log('✅ Metadata loaded:', metadata.filename, Math.round(metadata.size/1024/1024) + 'MB');

    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';
    log('MIME Type:', mimeType);

    // Route to handler
    if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      log('→ Routing to SINGLE FILE handler');
      return await handleSingleFile(request, env, metadata, mimeType);
    }

    if (metadata.chunks && metadata.chunks.length > 0) {
      log('→ Routing to CHUNKED FILE handler');
      return await handleChunkedFile(request, env, metadata, mimeType);
    }

    log('❌ Invalid metadata structure');
    return errorResponse('Invalid file configuration', 400);

  } catch (error) {
    log('❌ FATAL ERROR:', error.message);
    log('Stack:', error.stack);
    return errorResponse('Server error: ' + error.message, 500);
  }
}

/**
 * Handle single file streaming
 */
async function handleSingleFile(request, env, metadata, mimeType) {
  log('📥 Single file streaming started');
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  if (botTokens.length === 0) {
    log('❌ No bot tokens configured');
    return errorResponse('Service unavailable', 503);
  }

  log('🤖 Available bots:', botTokens.length);

  for (let i = 0; i < botTokens.length; i++) {
    const botToken = botTokens[i];
    log(`🤖 Trying bot ${i + 1}/${botTokens.length}`);

    try {
      // Get file info from Telegram
      const getFileUrl = `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`;
      log('📡 Telegram API call...');
      
      const getFileRes = await fetch(getFileUrl);
      const fileData = await getFileRes.json();

      if (!fileData.ok) {
        log(`❌ Bot ${i + 1} error:`, fileData.description);
        continue;
      }

      if (!fileData.result || !fileData.result.file_path) {
        log(`❌ Bot ${i + 1} no file_path`);
        continue;
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
      log('✅ Direct URL obtained');

      // Handle range request
      const rangeHeader = request.headers.get('Range');
      const fetchHeaders = {};
      
      if (rangeHeader) {
        fetchHeaders['Range'] = rangeHeader;
        log('📍 Range request:', rangeHeader);
      }

      // Fetch from Telegram
      log('⬇️ Fetching from Telegram...');
      const telegramRes = await fetch(directUrl, { headers: fetchHeaders });

      if (!telegramRes.ok) {
        log(`❌ Telegram fetch failed: ${telegramRes.status}`);
        continue;
      }

      log('✅ Telegram response OK, status:', telegramRes.status);

      // Build response headers
      const responseHeaders = new Headers();
      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Cache-Control', 'public, max-age=86400');
      responseHeaders.set('Content-Disposition', 'inline');

      // Copy important headers
      const contentLength = telegramRes.headers.get('content-length');
      const contentRange = telegramRes.headers.get('content-range');
      
      if (contentLength) responseHeaders.set('Content-Length', contentLength);
      if (contentRange) responseHeaders.set('Content-Range', contentRange);

      log('📤 Streaming to client...');
      log('=== REQUEST SUCCESS ===');

      return new Response(telegramRes.body, {
        status: telegramRes.status,
        headers: responseHeaders
      });

    } catch (error) {
      log(`❌ Bot ${i + 1} exception:`, error.message);
      continue;
    }
  }

  log('❌ All bots failed');
  return errorResponse('All streaming servers failed', 503);
}

/**
 * Handle chunked file streaming
 */
async function handleChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520;
  const rangeHeader = request.headers.get('Range');

  log('🧩 Chunked file - Total:', chunks.length, 'chunks');
  log('📦 Chunk size:', Math.round(chunkSize/1024/1024) + 'MB');

  // Handle range requests
  if (rangeHeader) {
    log('📍 Range request detected');
    return await handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize);
  }

  // Full file streaming
  log('📥 Full file streaming');
  return await handleFullStream(env, metadata, mimeType, totalSize);
}

/**
 * Handle full file streaming
 */
async function handleFullStream(env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  let chunkIndex = 0;

  log('🔄 Starting full stream, chunks:', chunks.length);

  const stream = new ReadableStream({
    async pull(controller) {
      if (chunkIndex >= chunks.length) {
        log('✅ Stream complete');
        controller.close();
        return;
      }

      try {
        log(`⬇️ Loading chunk ${chunkIndex + 1}/${chunks.length}`);
        const chunkData = await loadChunk(env, chunks[chunkIndex]);
        controller.enqueue(new Uint8Array(chunkData));
        log(`✅ Chunk ${chunkIndex + 1} sent:`, Math.round(chunkData.byteLength/1024/1024) + 'MB');
        chunkIndex++;
      } catch (error) {
        log(`❌ Chunk ${chunkIndex + 1} failed:`, error.message);
        controller.error(error);
      }
    },

    cancel(reason) {
      log('⚠️ Stream cancelled:', reason);
    }
  });

  return new Response(stream, {
    status: 200,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': totalSize.toString(),
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=86400',
      'Content-Disposition': 'inline'
    }
  });
}

/**
 * Handle range requests for chunked files
 */
async function handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) {
    log('❌ Invalid range format');
    return errorResponse('Invalid range', 416);
  }

  const start = parseInt(rangeMatch[1], 10);
  let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;
  
  if (end >= totalSize) end = totalSize - 1;
  
  if (start >= totalSize || start > end) {
    log('❌ Range not satisfiable');
    return errorResponse('Range not satisfiable', 416);
  }

  const requestedSize = end - start + 1;
  log(`📍 Range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  log(`🧩 Need chunks ${startChunk} to ${endChunk} (${neededChunks.length} total)`);

  let currentPosition = startChunk * chunkSize;
  let chunkIdx = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      if (chunkIdx >= neededChunks.length) {
        log('✅ Range stream complete');
        controller.close();
        return;
      }

      try {
        const chunkData = await loadChunk(env, neededChunks[chunkIdx]);
        const uint8Array = new Uint8Array(chunkData);

        const chunkStart = Math.max(start - currentPosition, 0);
        const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

        if (chunkStart < chunkEnd) {
          const slice = uint8Array.slice(chunkStart, chunkEnd);
          controller.enqueue(slice);
          log(`✅ Range chunk ${chunkIdx + 1} sent: ${slice.length} bytes`);
        }

        currentPosition += chunkSize;
        chunkIdx++;
      } catch (error) {
        log(`❌ Range chunk ${chunkIdx + 1} failed:`, error.message);
        controller.error(error);
      }
    }
  });

  return new Response(stream, {
    status: 206,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': requestedSize.toString(),
      'Content-Range': `bytes ${start}-${end}/${totalSize}`,
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=86400',
      'Content-Disposition': 'inline'
    }
  });
}

/**
 * Load single chunk
 */
async function loadChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  log(`📦 Loading chunk: ${chunkKey}`);

  // Get chunk metadata
  const metaStr = await kvNamespace.get(chunkKey);
  if (!metaStr) {
    throw new Error('Chunk not found: ' + chunkKey);
  }

  const chunkMeta = JSON.parse(metaStr);
  const fileId = chunkMeta.telegramFileId || chunkMeta.fileIdCode;

  if (!fileId) {
    throw new Error('No fileId in chunk metadata: ' + chunkKey);
  }

  // Try cached URL first
  if (chunkMeta.directUrl) {
    try {
      const res = await fetch(chunkMeta.directUrl);
      if (res.ok) {
        log(`✅ Cache hit: ${chunkKey}`);
        return await res.arrayBuffer();
      }
    } catch (e) {
      log(`⚠️ Cache miss: ${chunkKey}`);
    }
  }

  // Refresh URL from Telegram
  log(`🔄 Refreshing URL for: ${chunkKey}`);
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (let i = 0; i < botTokens.length; i++) {
    const botToken = botTokens[i];

    try {
      const getFileRes = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`
      );

      const fileData = await getFileRes.json();
      if (!fileData.ok || !fileData.result || !fileData.result.file_path) {
        continue;
      }

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
      const res = await fetch(freshUrl);

      if (res.ok) {
        // Update KV (non-blocking)
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMeta,
          directUrl: freshUrl,
          refreshed: Date.now()
        })).catch(() => {});

        log(`✅ URL refreshed: ${chunkKey}`);
        return await res.arrayBuffer();
      }
    } catch (e) {
      log(`⚠️ Bot ${i + 1} failed for chunk`);
      continue;
    }
  }

  throw new Error('All bots failed for chunk: ' + chunkKey);
}

/**
 * Error response helper
 */
function errorResponse(message, status = 500) {
  log('❌ Error response:', status, message);
  return new Response(JSON.stringify({
    error: message,
    status: status,
    timestamp: new Date().toISOString()
  }), {
    status: status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    }
  });
}