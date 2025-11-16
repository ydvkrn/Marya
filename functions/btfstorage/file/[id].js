// functions/btfstorage/file/[id].js
// 🎬 FIXED: Cloudflare Pages Functions - Video Streaming Handler

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/x-matroska', 'avi': 'video/x-msvideo',
  'mov': 'video/quicktime', 'webm': 'video/webm', 'mp3': 'audio/mpeg',
  'wav': 'audio/wav', 'aac': 'audio/mp4', 'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'zip': 'application/zip'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 Streaming request:', fileId);

  // CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
        'Access-Control-Allow-Headers': 'Range, Content-Type',
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
    }

    console.log('📂 File ID:', actualId, 'Extension:', extension);

    // Get metadata from KV
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      return errorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);
    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    console.log('📦 File:', metadata.filename, 'Size:', Math.round(metadata.size/1024/1024) + 'MB');

    // Determine streaming type
    const hasSingleFile = metadata.telegramFileId || metadata.fileIdCode;
    const hasChunks = metadata.chunks && metadata.chunks.length > 0;

    if (hasSingleFile && !hasChunks) {
      return await streamSingleFile(request, env, metadata, mimeType);
    }

    if (hasChunks) {
      return await streamChunkedFile(request, env, metadata, mimeType);
    }

    return errorResponse('Invalid file configuration', 400);

  } catch (error) {
    console.error('❌ Error:', error.message);
    return errorResponse(error.message, 500);
  }
}

/**
 * Stream single file from Telegram (< 20MB files)
 */
async function streamSingleFile(request, env, metadata, mimeType) {
  console.log('🚀 Single file streaming');

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
  const telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

  for (const botToken of botTokens) {
    try {
      // Get file path
      const getFileUrl = `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`;
      const fileInfoResponse = await fetch(getFileUrl, { signal: AbortSignal.timeout(10000) });
      const fileInfo = await fileInfoResponse.json();

      if (!fileInfo.ok || !fileInfo.result?.file_path) {
        continue;
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${fileInfo.result.file_path}`;
      
      // Forward range header if present
      const headers = {};
      const rangeHeader = request.headers.get('Range');
      if (rangeHeader) {
        headers['Range'] = rangeHeader;
      }

      // Fetch from Telegram
      const telegramResponse = await fetch(directUrl, { 
        headers,
        signal: AbortSignal.timeout(30000)
      });

      if (!telegramResponse.ok) {
        continue;
      }

      // Build response headers
      const responseHeaders = new Headers();
      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Cache-Control', 'public, max-age=31536000');
      responseHeaders.set('Content-Disposition', 'inline');

      // Copy content headers
      if (telegramResponse.headers.get('content-length')) {
        responseHeaders.set('Content-Length', telegramResponse.headers.get('content-length'));
      }
      if (telegramResponse.headers.get('content-range')) {
        responseHeaders.set('Content-Range', telegramResponse.headers.get('content-range'));
      }

      console.log('✅ Single file streaming successful');

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: responseHeaders
      });

    } catch (error) {
      console.log('⚠️ Bot token failed:', error.message);
      continue;
    }
  }

  return errorResponse('All streaming attempts failed', 503);
}

/**
 * Stream chunked file (> 20MB files split into chunks)
 */
async function streamChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520;

  console.log('🎬 Chunked streaming:', chunks.length, 'chunks');

  const rangeHeader = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');

  // Handle range request
  if (rangeHeader) {
    return await handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize);
  }

  // Handle full stream
  return await handleFullStream(request, env, metadata, mimeType, isDownload);
}

/**
 * Handle range requests for seeking/partial content
 */
async function handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  // Parse range
  const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) {
    return errorResponse('Invalid range', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }

  const start = parseInt(rangeMatch[1], 10);
  let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;

  if (end >= totalSize) end = totalSize - 1;
  if (start >= totalSize || start > end) {
    return errorResponse('Range not satisfiable', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }

  const requestedSize = end - start + 1;
  console.log('🎯 Range:', start, '-', end, '=', Math.round(requestedSize/1024/1024) + 'MB');

  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);

  console.log('🧩 Need chunks:', startChunk, 'to', endChunk);

  let currentChunkIndex = startChunk;
  let currentPosition = startChunk * chunkSize;
  let streamClosed = false;

  const stream = new ReadableStream({
    async pull(controller) {
      if (streamClosed || currentChunkIndex > endChunk) {
        controller.close();
        streamClosed = true;
        return;
      }

      try {
        const chunkInfo = chunks[currentChunkIndex];
        console.log(`🎯 Loading chunk ${currentChunkIndex + 1}/${chunks.length}`);

        const chunkData = await loadChunk(env, chunkInfo);
        const uint8Array = new Uint8Array(chunkData);

        // Calculate slice boundaries
        const chunkStart = Math.max(start - currentPosition, 0);
        const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

        if (chunkStart < chunkEnd) {
          const slice = uint8Array.slice(chunkStart, chunkEnd);
          controller.enqueue(slice);
          console.log(`✅ Chunk ${currentChunkIndex + 1} sent:`, slice.length, 'bytes');
        }

        currentPosition += chunkSize;
        currentChunkIndex++;

        // Close if done
        if (currentChunkIndex > endChunk || currentPosition > end) {
          controller.close();
          streamClosed = true;
          console.log('🎯 Range streaming complete');
        }

      } catch (error) {
        console.error('❌ Chunk error:', error);
        if (!streamClosed) {
          controller.error(error);
          streamClosed = true;
        }
      }
    },

    cancel(reason) {
      streamClosed = true;
      console.log('🛑 Stream cancelled:', reason);
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000');

  return new Response(stream, { status: 206, headers });
}

/**
 * Handle full file stream (for download or initial play)
 */
async function handleFullStream(request, env, metadata, mimeType, isDownload) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;

  console.log('📥', isDownload ? 'Download' : 'Stream', 'mode');

  let currentChunkIndex = 0;
  let streamClosed = false;

  const stream = new ReadableStream({
    async pull(controller) {
      if (streamClosed || currentChunkIndex >= chunks.length) {
        if (!streamClosed) {
          controller.close();
          streamClosed = true;
          console.log('✅ Streaming complete');
        }
        return;
      }

      try {
        const chunkInfo = chunks[currentChunkIndex];
        console.log(`📦 Loading chunk ${currentChunkIndex + 1}/${chunks.length}`);

        const chunkData = await loadChunk(env, chunkInfo);
        const uint8Array = new Uint8Array(chunkData);

        controller.enqueue(uint8Array);
        console.log(`✅ Chunk ${currentChunkIndex + 1} sent:`, Math.round(uint8Array.byteLength/1024/1024) + 'MB');

        currentChunkIndex++;

      } catch (error) {
        console.error('❌ Chunk error:', error);
        if (!streamClosed) {
          controller.error(error);
          streamClosed = true;
        }
      }
    },

    cancel(reason) {
      streamClosed = true;
      console.log('🛑 Stream cancelled:', reason);
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', isDownload ? `attachment; filename="${metadata.filename}"` : 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000');

  return new Response(stream, { status: 200, headers });
}

/**
 * Load single chunk with URL refresh
 */
async function loadChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  // Get chunk metadata
  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk not found: ${chunkKey}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  const telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

  // Try cached URL first
  if (chunkMetadata.directUrl) {
    try {
      const response = await fetch(chunkMetadata.directUrl, { 
        signal: AbortSignal.timeout(20000) 
      });
      
      if (response.ok) {
        console.log('✅ Loaded from cached URL');
        return response.arrayBuffer();
      }
    } catch (error) {
      console.log('🔄 Cached URL failed, refreshing...');
    }
  }

  // Refresh URL
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);

  for (const botToken of botTokens) {
    try {
      const getFileUrl = `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`;
      const fileInfoResponse = await fetch(getFileUrl, { signal: AbortSignal.timeout(10000) });
      const fileInfo = await fileInfoResponse.json();

      if (!fileInfo.ok || !fileInfo.result?.file_path) {
        continue;
      }

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${fileInfo.result.file_path}`;
      const response = await fetch(freshUrl, { signal: AbortSignal.timeout(20000) });

      if (response.ok) {
        // Update KV with fresh URL (non-blocking)
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        })).catch(() => {});

        console.log('✅ URL refreshed and loaded');
        return response.arrayBuffer();
      }

    } catch (error) {
      continue;
    }
  }

  throw new Error(`Failed to load chunk: ${chunkKey}`);
}

/**
 * Error response helper
 */
function errorResponse(message, status = 500, additionalHeaders = {}) {
  const headers = new Headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    ...additionalHeaders
  });

  return new Response(JSON.stringify({ 
    error: message, 
    status,
    timestamp: new Date().toISOString()
  }), { status, headers });
}