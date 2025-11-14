// functions/btfstorage/file/[id].js
// ⚡ ULTRA-FAST Cloudflare Pages Functions - Optimized File Streaming

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/x-matroska', 'avi': 'video/x-msvideo',
  'mov': 'video/quicktime', 'webm': 'video/webm', 'mp3': 'audio/mpeg',
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'pdf': 'application/pdf', 'm3u8': 'application/x-mpegURL'
};

const CONFIG = {
  ENABLE_LOGS: false, // ⚡ Disable logs for production speed
  PARALLEL_CHUNKS: 3, // Load 3 chunks simultaneously
  INITIAL_BUFFER_SIZE: 100 * 1024 * 1024, // 100MB initial buffer
  CHUNK_RETRY_ATTEMPTS: 2, // Reduced retries
  REQUEST_TIMEOUT: 20000, // 20s timeout
  CACHE_TTL: 86400 // 24 hours
};

function log(...args) {
  if (CONFIG.ENABLE_LOGS) console.log(...args);
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  // ⚡ Fast CORS preflight
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
    let actualId = fileId;
    let extension = '';
    
    if (fileId.includes('.')) {
      const parts = fileId.split('.');
      extension = parts.pop().toLowerCase();
      actualId = parts.join('.');
    }

    // ⚡ Single KV read with caching
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      return errorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);
    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    log('⚡ File:', metadata.filename, Math.round(metadata.size/1024/1024) + 'MB');

    // Route to appropriate handler
    if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      return await streamSingleFile(request, env, metadata, mimeType);
    }

    if (metadata.chunks && metadata.chunks.length > 0) {
      return await streamChunkedFile(request, env, metadata, mimeType);
    }

    return errorResponse('Invalid file', 400);

  } catch (error) {
    log('❌ Error:', error.message);
    return errorResponse(error.message, 500);
  }
}

/**
 * ⚡ ULTRA-FAST Single File Streaming
 */
async function streamSingleFile(request, env, metadata, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  if (botTokens.length === 0) {
    return errorResponse('No bot tokens', 503);
  }

  // ⚡ Try only first bot, if fails move to next (no retry loops)
  for (const botToken of botTokens) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), CONFIG.REQUEST_TIMEOUT);

      // Get file path from Telegram
      const getFileRes = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${metadata.telegramFileId}`,
        { signal: controller.signal }
      );
      clearTimeout(timeoutId);

      const fileData = await getFileRes.json();
      if (!fileData.ok || !fileData.result?.file_path) continue;

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
      
      // ⚡ Direct proxy to Telegram
      const rangeHeader = request.headers.get('Range');
      const headers = rangeHeader ? { 'Range': rangeHeader } : {};
      
      const telegramRes = await fetch(directUrl, { headers });
      if (!telegramRes.ok) continue;

      // ⚡ Minimal response headers
      const responseHeaders = new Headers({
        'Content-Type': mimeType,
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': `public, max-age=${CONFIG.CACHE_TTL}, immutable`,
        'Content-Disposition': 'inline'
      });

      // Copy essential headers
      ['content-length', 'content-range'].forEach(h => {
        const v = telegramRes.headers.get(h);
        if (v) responseHeaders.set(h, v);
      });

      log('✅ Streaming via Telegram');
      return new Response(telegramRes.body, {
        status: telegramRes.status,
        headers: responseHeaders
      });

    } catch (e) {
      log('⚠️ Bot failed:', e.message);
      continue;
    }
  }

  return errorResponse('All bots failed', 503);
}

/**
 * ⚡ ULTRA-FAST Chunked File Streaming with Parallel Loading
 */
async function streamChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520;
  const rangeHeader = request.headers.get('Range');

  // ⚡ Handle range requests efficiently
  if (rangeHeader) {
    return await streamRange(request, env, metadata, rangeHeader, mimeType, chunkSize);
  }

  // ⚡ Default: Smart buffered streaming
  return await streamBuffered(env, metadata, mimeType, totalSize);
}

/**
 * ⚡ Smart Buffered Streaming - Loads chunks in parallel
 */
async function streamBuffered(env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  const maxBufferChunks = Math.min(CONFIG.PARALLEL_CHUNKS, chunks.length);
  
  log(`⚡ Buffered streaming: ${maxBufferChunks} parallel chunks`);

  let chunkIndex = 0;
  const chunkQueue = []; // Pre-loaded chunks

  // ⚡ Preload initial chunks in parallel
  async function preloadChunks(startIdx, count) {
    const promises = [];
    for (let i = 0; i < count && (startIdx + i) < chunks.length; i++) {
      promises.push(loadChunkFast(env, chunks[startIdx + i]));
    }
    return await Promise.all(promises);
  }

  const stream = new ReadableStream({
    async start(controller) {
      try {
        // ⚡ Load first batch in parallel
        const initialChunks = await preloadChunks(0, maxBufferChunks);
        chunkQueue.push(...initialChunks);
        chunkIndex = maxBufferChunks;
        log('⚡ Initial buffer loaded:', chunkQueue.length, 'chunks');
      } catch (e) {
        controller.error(e);
      }
    },

    async pull(controller) {
      try {
        if (chunkQueue.length > 0) {
          // Send buffered chunk
          const chunkData = chunkQueue.shift();
          controller.enqueue(new Uint8Array(chunkData));
          
          // ⚡ Prefetch next chunk while streaming current
          if (chunkIndex < chunks.length) {
            loadChunkFast(env, chunks[chunkIndex])
              .then(data => chunkQueue.push(data))
              .catch(e => log('⚠️ Prefetch failed:', e.message));
            chunkIndex++;
          }
        } else if (chunkIndex >= chunks.length) {
          log('✅ Stream complete');
          controller.close();
        } else {
          // Fallback: load next chunk synchronously
          const chunkData = await loadChunkFast(env, chunks[chunkIndex]);
          controller.enqueue(new Uint8Array(chunkData));
          chunkIndex++;
        }
      } catch (e) {
        log('❌ Stream error:', e.message);
        controller.error(e);
      }
    },

    cancel() {
      log('⚠️ Stream cancelled');
    }
  });

  return new Response(stream, {
    status: 200,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': totalSize.toString(),
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': `public, max-age=${CONFIG.CACHE_TTL}`,
      'Content-Disposition': 'inline',
      'X-Streaming-Mode': 'buffered-parallel'
    }
  });
}

/**
 * ⚡ Optimized Range Streaming
 */
async function streamRange(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) {
    return errorResponse('Invalid range', 416);
  }

  const start = parseInt(rangeMatch[1], 10);
  let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;
  if (end >= totalSize) end = totalSize - 1;

  if (start >= totalSize || start > end) {
    return errorResponse('Range not satisfiable', 416);
  }

  const requestedSize = end - start + 1;
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  log(`⚡ Range: ${Math.round(requestedSize/1024/1024)}MB, Chunks: ${neededChunks.length}`);

  let currentPosition = startChunk * chunkSize;
  let chunkIdx = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      if (chunkIdx >= neededChunks.length) {
        controller.close();
        return;
      }

      try {
        const chunkData = await loadChunkFast(env, neededChunks[chunkIdx]);
        const uint8Array = new Uint8Array(chunkData);

        const chunkStart = Math.max(start - currentPosition, 0);
        const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

        if (chunkStart < chunkEnd) {
          controller.enqueue(uint8Array.slice(chunkStart, chunkEnd));
        }

        currentPosition += chunkSize;
        chunkIdx++;
      } catch (e) {
        controller.error(e);
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
      'Cache-Control': `public, max-age=${CONFIG.CACHE_TTL}`,
      'Content-Disposition': 'inline'
    }
  });
}

/**
 * ⚡ ULTRA-FAST Chunk Loading (Optimized)
 */
async function loadChunkFast(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  // ⚡ Try cached URL first (no KV read if URL is fresh)
  if (chunkInfo.directUrl) {
    try {
      const res = await fetch(chunkInfo.directUrl, { 
        signal: AbortSignal.timeout(CONFIG.REQUEST_TIMEOUT) 
      });
      if (res.ok) {
        log('⚡ Cache hit:', chunkKey);
        return res.arrayBuffer();
      }
    } catch (e) {
      log('⚠️ Cache miss:', chunkKey);
    }
  }

  // ⚡ Refresh URL from KV
  const metaStr = await kvNamespace.get(chunkKey);
  if (!metaStr) throw new Error('Chunk not found: ' + chunkKey);

  const chunkMeta = JSON.parse(metaStr);
  const fileId = chunkMeta.telegramFileId || chunkMeta.fileIdCode;

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  // ⚡ Fast bot token rotation (no retries)
  for (const botToken of botTokens) {
    try {
      const getFileRes = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${fileId}`,
        { signal: AbortSignal.timeout(10000) }
      );

      const fileData = await getFileRes.json();
      if (!fileData.ok || !fileData.result?.file_path) continue;

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${fileData.result.file_path}`;
      const res = await fetch(freshUrl, { signal: AbortSignal.timeout(CONFIG.REQUEST_TIMEOUT) });

      if (res.ok) {
        // ⚡ Update KV asynchronously (non-blocking)
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMeta,
          directUrl: freshUrl,
          refreshed: Date.now()
        })).catch(() => {});

        log('✅ URL refreshed:', chunkKey);
        return res.arrayBuffer();
      }
    } catch (e) {
      continue;
    }
  }

  throw new Error('All bots failed for chunk: ' + chunkKey);
}

/**
 * ⚡ Fast Error Response
 */
function errorResponse(message, status = 500) {
  return new Response(JSON.stringify({ error: message, status }), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    }
  });
}