// functions/btfstorage/file/[id].js
// 🎬 PRODUCTION READY - Fixed Video Streaming

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/x-matroska', 'avi': 'video/x-msvideo',
  'mov': 'video/quicktime', 'webm': 'video/webm', 'flv': 'video/x-flv',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'aac': 'audio/mp4',
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'zip': 'application/zip'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  
  console.log('🎬 REQUEST:', request.method, request.url);
  
  // CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
        'Access-Control-Allow-Headers': 'Range, Content-Type',
        'Access-Control-Max-Age': '86400',
        'Access-Control-Expose-Headers': 'Content-Length, Content-Range, Accept-Ranges'
      }
    });
  }

  try {
    const fileId = params.id;
    
    // Parse ID and extension
    let actualId = fileId;
    let extension = '';
    
    if (fileId.includes('.')) {
      const parts = fileId.split('.');
      extension = parts.pop().toLowerCase();
      actualId = parts.join('.');
    }

    console.log('📂 ID:', actualId, 'EXT:', extension);

    // Get metadata
    const metadataString = await env.FILES_KV.get(actualId);
    
    if (!metadataString) {
      console.error('❌ NOT FOUND:', actualId);
      return errorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);
    console.log('📦 FILE:', metadata.filename);
    console.log('📊 SIZE:', Math.round(metadata.size/1024/1024) + 'MB');
    console.log('🧩 CHUNKS:', metadata.chunks?.length || 0);

    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    // Route to handler
    const hasSingleFile = metadata.telegramFileId || metadata.fileIdCode;
    const hasChunks = metadata.chunks && metadata.chunks.length > 0;

    if (hasSingleFile && !hasChunks) {
      console.log('🚀 ROUTING: Single file');
      return await streamSingleFile(request, env, metadata, mimeType);
    }

    if (hasChunks) {
      console.log('🚀 ROUTING: Chunked file');
      return await streamChunkedFile(request, env, metadata, mimeType);
    }

    console.error('❌ INVALID CONFIG');
    return errorResponse('Invalid file configuration', 400);

  } catch (error) {
    console.error('❌ FATAL ERROR:', error.message);
    console.error('📍 STACK:', error.stack);
    return errorResponse('Server error: ' + error.message, 500);
  }
}

/**
 * Single file streaming (< 20MB)
 */
async function streamSingleFile(request, env, metadata, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
  const telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

  console.log('🤖 BOTS AVAILABLE:', botTokens.length);

  for (let i = 0; i < botTokens.length; i++) {
    try {
      console.log('🤖 TRYING BOT:', i + 1);
      
      const getFileUrl = `https://api.telegram.org/bot${botTokens[i]}/getFile?file_id=${encodeURIComponent(telegramFileId)}`;
      
      const fileInfoRes = await fetch(getFileUrl, { 
        signal: AbortSignal.timeout(10000) 
      });
      
      if (!fileInfoRes.ok) {
        console.error('❌ BOT API FAILED:', fileInfoRes.status);
        continue;
      }

      const fileInfo = await fileInfoRes.json();
      
      if (!fileInfo.ok || !fileInfo.result?.file_path) {
        console.error('❌ NO FILE PATH');
        continue;
      }

      const directUrl = `https://api.telegram.org/file/bot${botTokens[i]}/${fileInfo.result.file_path}`;
      console.log('📡 FETCHING FROM TELEGRAM');

      // Prepare headers
      const fetchHeaders = {};
      const rangeHeader = request.headers.get('Range');
      if (rangeHeader) {
        fetchHeaders['Range'] = rangeHeader;
        console.log('🎯 RANGE:', rangeHeader);
      }

      const telegramRes = await fetch(directUrl, {
        headers: fetchHeaders,
        signal: AbortSignal.timeout(30000)
      });

      if (!telegramRes.ok) {
        console.error('❌ TELEGRAM FETCH FAILED:', telegramRes.status);
        continue;
      }

      // Build response headers
      const headers = new Headers();
      headers.set('Content-Type', mimeType);
      headers.set('Access-Control-Allow-Origin', '*');
      headers.set('Accept-Ranges', 'bytes');
      headers.set('Cache-Control', 'public, max-age=31536000');
      headers.set('Content-Disposition', 'inline');

      // CRITICAL: Copy exact content headers
      const contentLength = telegramRes.headers.get('content-length');
      const contentRange = telegramRes.headers.get('content-range');
      
      if (contentLength) {
        headers.set('Content-Length', contentLength);
        console.log('📏 LENGTH:', contentLength);
      }
      
      if (contentRange) {
        headers.set('Content-Range', contentRange);
        console.log('🎯 RANGE:', contentRange);
      }

      console.log('✅ SINGLE FILE SUCCESS');

      return new Response(telegramRes.body, {
        status: telegramRes.status,
        headers: headers
      });

    } catch (error) {
      console.error('❌ BOT ERROR:', error.message);
      continue;
    }
  }

  console.error('❌ ALL BOTS FAILED');
  return errorResponse('All streaming sources failed', 503);
}

/**
 * Chunked file streaming (> 20MB)
 */
async function streamChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520;

  const rangeHeader = request.headers.get('Range');
  
  console.log('🧩 CHUNKS:', chunks.length);
  console.log('📏 TOTAL:', Math.round(totalSize/1024/1024) + 'MB');
  console.log('🎯 RANGE:', rangeHeader || 'FULL');

  // Range request
  if (rangeHeader) {
    return handleRangeStream(request, env, metadata, rangeHeader, mimeType, chunkSize);
  }

  // Full stream
  return handleFullChunkStream(request, env, metadata, mimeType);
}

/**
 * Handle range requests
 */
async function handleRangeStream(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  // Parse range
  const match = rangeHeader.match(/bytes=(d+)-(d*)/);
  if (!match) {
    console.error('❌ INVALID RANGE FORMAT');
    return errorResponse('Invalid range', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }

  const start = parseInt(match[1], 10);
  let end = match[2] ? parseInt(match[2], 10) : totalSize - 1;

  // Fix end boundary
  if (end >= totalSize) end = totalSize - 1;

  if (start >= totalSize || start > end) {
    console.error('❌ RANGE NOT SATISFIABLE');
    return errorResponse('Range not satisfiable', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }

  const requestedSize = end - start + 1;
  
  console.log('🎯 START:', start, 'END:', end);
  console.log('📏 REQUESTED:', Math.round(requestedSize/1024/1024) + 'MB');

  // Calculate chunks needed
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);

  console.log('🧩 CHUNKS:', startChunk, 'to', endChunk);

  // Stream state
  let chunkIdx = startChunk;
  let position = startChunk * chunkSize;
  let closed = false;

  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();

  // Start streaming in background
  (async () => {
    try {
      while (chunkIdx <= endChunk && !closed) {
        console.log(`🧩 CHUNK ${chunkIdx + 1}/${chunks.length}`);

        const chunkData = await loadChunk(env, chunks[chunkIdx]);
        const bytes = new Uint8Array(chunkData);

        // Calculate slice
        const sliceStart = Math.max(start - position, 0);
        const sliceEnd = Math.min(bytes.length, end - position + 1);

        if (sliceStart < sliceEnd) {
          const slice = bytes.slice(sliceStart, sliceEnd);
          await writer.write(slice);
          console.log(`✅ SENT ${slice.length} bytes`);
        }

        position += chunkSize;
        chunkIdx++;

        if (position > end) break;
      }

      console.log('✅ RANGE COMPLETE');
      await writer.close();

    } catch (error) {
      console.error('❌ STREAM ERROR:', error.message);
      try {
        await writer.abort(error);
      } catch (e) {
        console.error('❌ ABORT FAILED:', e.message);
      }
    }
  })();

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000');

  return new Response(readable, { status: 206, headers });
}

/**
 * Handle full stream
 */
async function handleFullChunkStream(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;

  console.log('📥 FULL STREAM');

  let chunkIdx = 0;
  let closed = false;

  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();

  // Stream in background
  (async () => {
    try {
      while (chunkIdx < chunks.length && !closed) {
        console.log(`🧩 CHUNK ${chunkIdx + 1}/${chunks.length}`);

        const chunkData = await loadChunk(env, chunks[chunkIdx]);
        const bytes = new Uint8Array(chunkData);

        await writer.write(bytes);
        console.log(`✅ SENT ${Math.round(bytes.length/1024/1024)}MB`);

        chunkIdx++;
      }

      console.log('✅ FULL STREAM COMPLETE');
      await writer.close();

    } catch (error) {
      console.error('❌ STREAM ERROR:', error.message);
      try {
        await writer.abort(error);
      } catch (e) {
        console.error('❌ ABORT FAILED:', e.message);
      }
    }
  })();

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000');

  return new Response(readable, { status: 200, headers });
}

/**
 * Load chunk with auto URL refresh
 */
async function loadChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  console.log('📥 LOADING:', chunkKey);

  const metadataStr = await kvNamespace.get(chunkKey);
  if (!metadataStr) {
    throw new Error(`Chunk not found: ${chunkKey}`);
  }

  const chunkMeta = JSON.parse(metadataStr);
  const fileId = chunkMeta.telegramFileId || chunkMeta.fileIdCode;

  // Try cached URL
  if (chunkMeta.directUrl) {
    try {
      const res = await fetch(chunkMeta.directUrl, { 
        signal: AbortSignal.timeout(20000) 
      });
      
      if (res.ok) {
        console.log('✅ CACHED URL OK');
        return res.arrayBuffer();
      }
      
      console.log('🔄 CACHED URL EXPIRED');
    } catch (e) {
      console.log('🔄 CACHED URL FAILED');
    }
  }

  // Refresh URL
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);

  for (const botToken of botTokens) {
    try {
      const getFileUrl = `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId)}`;
      const fileInfoRes = await fetch(getFileUrl, { signal: AbortSignal.timeout(10000) });
      const fileInfo = await fileInfoRes.json();

      if (!fileInfo.ok || !fileInfo.result?.file_path) {
        continue;
      }

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${fileInfo.result.file_path}`;
      const res = await fetch(freshUrl, { signal: AbortSignal.timeout(20000) });

      if (res.ok) {
        // Update KV (non-blocking)
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMeta,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        })).catch(() => {});

        console.log('✅ URL REFRESHED');
        return res.arrayBuffer();
      }

    } catch (e) {
      continue;
    }
  }

  throw new Error(`All bots failed for: ${chunkKey}`);
}

/**
 * Error response
 */
function errorResponse(message, status = 500, additionalHeaders = {}) {
  console.error('❌ ERROR RESPONSE:', status, message);
  
  const headers = new Headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    ...additionalHeaders
  });

  return new Response(JSON.stringify({
    error: message,
    status: status,
    timestamp: new Date().toISOString()
  }, null, 2), { status, headers });
}