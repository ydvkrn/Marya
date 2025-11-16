// functions/btfstorage/file/[id].js
// 🚀 Premium Fast Streaming - Production Ready

const MIME_TYPES = {
  'mp4': 'video/mp4',
  'mkv': 'video/x-matroska',
  'avi': 'video/x-msvideo',
  'mov': 'video/quicktime',
  'webm': 'video/webm',
  'flv': 'video/x-flv',
  '3gp': 'video/3gpp',
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'aac': 'audio/mp4',
  'ogg': 'audio/ogg',
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp',
  'pdf': 'application/pdf',
  'zip': 'application/zip',
  'txt': 'text/plain',
  'm3u8': 'application/x-mpegURL',
  'ts': 'video/mp2t'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 Request:', fileId);

  // CORS
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
    // Parse file ID
    let actualId = fileId;
    let extension = '';
    let isHlsPlaylist = false;
    let isHlsSegment = false;
    let segmentIndex = -1;

    if (fileId.includes('.')) {
      const lastDot = fileId.lastIndexOf('.');
      extension = fileId.substring(lastDot + 1).toLowerCase();
      actualId = fileId.substring(0, lastDot);

      // HLS playlist check
      if (extension === 'm3u8') {
        isHlsPlaylist = true;
      }
      // HLS segment check (format: id-0.ts, id-1.ts)
      else if (extension === 'ts' && actualId.includes('-')) {
        const parts = actualId.split('-');
        const lastPart = parts[parts.length - 1];
        if (!isNaN(parseInt(lastPart))) {
          segmentIndex = parseInt(lastPart);
          parts.pop();
          actualId = parts.join('-');
          isHlsSegment = true;
        }
      }
    }

    console.log('📂 Parsed - ID:', actualId, 'Ext:', extension);

    // Get metadata from KV
    const metadataString = await env.FILES_KV.get(actualId);
    
    if (!metadataString) {
      console.error('❌ File not found in KV:', actualId);
      return errorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);

    // Validate metadata
    if (!metadata.filename || !metadata.size) {
      console.error('❌ Invalid metadata');
      return errorResponse('Invalid file metadata', 400);
    }

    // Backward compatibility
    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

    if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      console.error('❌ No file source');
      return errorResponse('Missing file source', 400);
    }

    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    console.log('✅ File:', metadata.filename, 'Size:', Math.round(metadata.size / 1024 / 1024) + 'MB');

    // Route to handlers
    if (isHlsPlaylist) {
      return generateHlsPlaylist(request, metadata, actualId);
    }

    if (isHlsSegment && segmentIndex >= 0) {
      return streamHlsSegment(env, metadata, segmentIndex);
    }

    // Single file (< 20MB)
    if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      return streamSingleFile(request, env, metadata, mimeType);
    }

    // Chunked file (> 20MB)
    if (metadata.chunks && metadata.chunks.length > 0) {
      return streamChunkedFile(request, env, metadata, mimeType);
    }

    return errorResponse('Invalid file configuration', 400);

  } catch (error) {
    console.error('❌ Fatal error:', error.message);
    console.error(error.stack);
    return errorResponse('Server error: ' + error.message, 500);
  }
}

/**
 * Generate HLS playlist for chunked files
 */
function generateHlsPlaylist(request, metadata, actualId) {
  console.log('📼 Generating HLS playlist');

  if (!metadata.chunks || metadata.chunks.length === 0) {
    return errorResponse('HLS not supported for this file', 400);
  }

  const baseUrl = new URL(request.url).origin;
  const chunks = metadata.chunks;

  let playlist = '#EXTM3U
';
  playlist += '#EXT-X-VERSION:3
';
  playlist += '#EXT-X-TARGETDURATION:6
';
  playlist += '#EXT-X-MEDIA-SEQUENCE:0
';
  playlist += '#EXT-X-PLAYLIST-TYPE:VOD
';

  for (let i = 0; i < chunks.length; i++) {
    playlist += '#EXTINF:6.0,
';
    playlist += baseUrl + '/btfstorage/file/' + actualId + '-' + i + '.ts
';
  }

  playlist += '#EXT-X-ENDLIST
';

  console.log('✅ HLS playlist generated:', chunks.length, 'segments');

  return new Response(playlist, {
    status: 200,
    headers: {
      'Content-Type': 'application/x-mpegURL',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'no-cache'
    }
  });
}

/**
 * Stream HLS segment
 */
async function streamHlsSegment(env, metadata, segmentIndex) {
  console.log('📼 Streaming HLS segment:', segmentIndex);

  if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
    console.error('❌ Invalid segment index');
    return errorResponse('Segment not found', 404);
  }

  try {
    const chunkInfo = metadata.chunks[segmentIndex];
    const chunkData = await loadChunkData(env, chunkInfo);

    console.log('✅ HLS segment loaded:', Math.round(chunkData.byteLength / 1024 / 1024) + 'MB');

    return new Response(chunkData, {
      status: 200,
      headers: {
        'Content-Type': 'video/mp2t',
        'Content-Length': chunkData.byteLength.toString(),
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=31536000, immutable',
        'Accept-Ranges': 'bytes'
      }
    });

  } catch (error) {
    console.error('❌ HLS segment error:', error.message);
    return errorResponse('Segment loading failed: ' + error.message, 500);
  }
}

/**
 * Stream single file from Telegram (< 20MB)
 */
async function streamSingleFile(request, env, metadata, mimeType) {
  console.log('🚀 Single file streaming');

  const botTokens = [
    env.BOT_TOKEN,
    env.BOT_TOKEN2,
    env.BOT_TOKEN3,
    env.BOT_TOKEN4
  ].filter(token => token);

  if (botTokens.length === 0) {
    console.error('❌ No bot tokens configured');
    return errorResponse('Service unavailable', 503);
  }

  console.log('🤖 Available bots:', botTokens.length);

  // Try each bot
  for (let i = 0; i < botTokens.length; i++) {
    const botToken = botTokens[i];
    console.log('🤖 Trying bot', i + 1);

    try {
      // Get file path from Telegram
      const getFileUrl = 'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(metadata.telegramFileId);
      
      const fileInfoResponse = await fetch(getFileUrl, {
        signal: AbortSignal.timeout(10000)
      });

      if (!fileInfoResponse.ok) {
        console.error('Bot API failed:', fileInfoResponse.status);
        continue;
      }

      const fileInfo = await fileInfoResponse.json();

      if (!fileInfo.ok || !fileInfo.result || !fileInfo.result.file_path) {
        console.error('Invalid response from bot');
        continue;
      }

      // Construct direct URL
      const directUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + fileInfo.result.file_path;
      console.log('📡 Got direct URL');

      // Prepare request headers
      const headers = {};
      const rangeHeader = request.headers.get('Range');
      
      if (rangeHeader) {
        headers['Range'] = rangeHeader;
        console.log('🎯 Range request:', rangeHeader);
      }

      // Fetch from Telegram
      const telegramResponse = await fetch(directUrl, {
        headers: headers,
        signal: AbortSignal.timeout(30000)
      });

      if (!telegramResponse.ok) {
        console.error('Telegram fetch failed:', telegramResponse.status);
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
      const contentLength = telegramResponse.headers.get('content-length');
      const contentRange = telegramResponse.headers.get('content-range');

      if (contentLength) {
        responseHeaders.set('Content-Length', contentLength);
      }

      if (contentRange) {
        responseHeaders.set('Content-Range', contentRange);
      }

      console.log('✅ Single file streaming success');

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: responseHeaders
      });

    } catch (error) {
      console.error('Bot', i + 1, 'error:', error.message);
      continue;
    }
  }

  console.error('❌ All bots failed');
  return errorResponse('All streaming sources failed', 503);
}

/**
 * Stream chunked file (> 20MB)
 */
async function streamChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520; // 20MB default

  const rangeHeader = request.headers.get('Range');

  console.log('🧩 Chunked streaming - Chunks:', chunks.length, 'Total:', Math.round(totalSize / 1024 / 1024) + 'MB');

  // Handle range requests
  if (rangeHeader) {
    return streamRange(request, env, metadata, rangeHeader, mimeType, chunkSize);
  }

  // Full stream
  return streamFull(env, metadata, mimeType);
}

/**
 * Handle range requests for chunked files
 */
async function streamRange(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  console.log('🎯 Range request:', rangeHeader);

  // Parse range header
  const match = rangeHeader.match(/bytes=(d+)-(d*)/);
  
  if (!match) {
    console.error('Invalid range format');
    return errorResponse('Invalid range', 416, {
      'Content-Range': 'bytes */' + totalSize
    });
  }

  const start = parseInt(match[1], 10);
  let end = match[2] ? parseInt(match[2], 10) : totalSize - 1;

  // Validate range
  if (end >= totalSize) {
    end = totalSize - 1;
  }

  if (start >= totalSize || start > end) {
    console.error('Range not satisfiable');
    return errorResponse('Range not satisfiable', 416, {
      'Content-Range': 'bytes */' + totalSize
    });
  }

  const requestedSize = end - start + 1;

  console.log('🎯 Serving bytes', start, 'to', end, '(' + Math.round(requestedSize / 1024 / 1024) + 'MB)');

  // Calculate which chunks we need
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);

  console.log('🧩 Need chunks', startChunk, 'to', endChunk);

  // Stream state
  let currentChunkIndex = startChunk;
  let currentPosition = startChunk * chunkSize;

  // Use TransformStream for proper streaming
  const transformStream = new TransformStream();
  const writer = transformStream.writable.getWriter();

  // Stream chunks in background
  (async () => {
    try {
      while (currentChunkIndex <= endChunk) {
        console.log('📦 Loading chunk', currentChunkIndex + 1, '/', chunks.length);

        const chunkInfo = chunks[currentChunkIndex];
        const chunkData = await loadChunkData(env, chunkInfo);
        const chunkBytes = new Uint8Array(chunkData);

        // Calculate what part of this chunk we need
        const chunkStart = Math.max(start - currentPosition, 0);
        const chunkEnd = Math.min(chunkBytes.length, end - currentPosition + 1);

        if (chunkStart < chunkEnd) {
          const slice = chunkBytes.slice(chunkStart, chunkEnd);
          await writer.write(slice);
          console.log('✅ Sent', slice.length, 'bytes from chunk', currentChunkIndex + 1);
        }

        currentPosition += chunkSize;
        currentChunkIndex++;

        // Stop if we've passed the end
        if (currentPosition > end) {
          break;
        }
      }

      console.log('✅ Range streaming complete');
      await writer.close();

    } catch (error) {
      console.error('❌ Range streaming error:', error.message);
      try {
        await writer.abort(error);
      } catch (abortError) {
        console.error('Abort error:', abortError.message);
      }
    }
  })();

  // Return response immediately with stream
  return new Response(transformStream.readable, {
    status: 206,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': requestedSize.toString(),
      'Content-Range': 'bytes ' + start + '-' + end + '/' + totalSize,
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=31536000',
      'Content-Disposition': 'inline'
    }
  });
}

/**
 * Stream full chunked file
 */
async function streamFull(env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;

  console.log('📥 Full stream - Total chunks:', chunks.length);

  let currentChunkIndex = 0;

  // Use TransformStream
  const transformStream = new TransformStream();
  const writer = transformStream.writable.getWriter();

  // Stream all chunks in background
  (async () => {
    try {
      while (currentChunkIndex < chunks.length) {
        console.log('📦 Chunk', currentChunkIndex + 1, '/', chunks.length);

        const chunkInfo = chunks[currentChunkIndex];
        const chunkData = await loadChunkData(env, chunkInfo);
        const chunkBytes = new Uint8Array(chunkData);

        await writer.write(chunkBytes);
        console.log('✅ Sent chunk', currentChunkIndex + 1, '-', Math.round(chunkBytes.length / 1024 / 1024) + 'MB');

        currentChunkIndex++;
      }

      console.log('✅ Full streaming complete');
      await writer.close();

    } catch (error) {
      console.error('❌ Full streaming error:', error.message);
      try {
        await writer.abort(error);
      } catch (abortError) {
        console.error('Abort error:', abortError.message);
      }
    }
  })();

  // Return response immediately
  return new Response(transformStream.readable, {
    status: 200,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': totalSize.toString(),
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=31536000',
      'Content-Disposition': 'inline'
    }
  });
}

/**
 * Load chunk data from KV storage with URL refresh
 */
async function loadChunkData(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  console.log('📥 Loading chunk:', chunkKey);

  // Get chunk metadata
  const chunkMetadataString = await kvNamespace.get(chunkKey);
  
  if (!chunkMetadataString) {
    throw new Error('Chunk not found in KV: ' + chunkKey);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  const telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

  // Try cached URL first
  if (chunkMetadata.directUrl) {
    try {
      console.log('🔗 Trying cached URL');
      const response = await fetch(chunkMetadata.directUrl, {
        signal: AbortSignal.timeout(20000)
      });

      if (response.ok) {
        console.log('✅ Loaded from cached URL');
        return response.arrayBuffer();
      }

      console.log('🔄 Cached URL expired');
    } catch (error) {
      console.log('🔄 Cached URL failed:', error.message);
    }
  }

  // Refresh URL from Telegram
  console.log('🔄 Refreshing URL from Telegram');

  const botTokens = [
    env.BOT_TOKEN,
    env.BOT_TOKEN2,
    env.BOT_TOKEN3,
    env.BOT_TOKEN4
  ].filter(token => token);

  for (let i = 0; i < botTokens.length; i++) {
    const botToken = botTokens[i];

    try {
      console.log('🤖 Trying bot', i + 1, 'for chunk refresh');

      const getFileUrl = 'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(telegramFileId);
      
      const fileInfoResponse = await fetch(getFileUrl, {
        signal: AbortSignal.timeout(10000)
      });

      if (!fileInfoResponse.ok) {
        console.error('Bot API failed for chunk');
        continue;
      }

      const fileInfo = await fileInfoResponse.json();

      if (!fileInfo.ok || !fileInfo.result || !fileInfo.result.file_path) {
        console.error('Invalid response for chunk');
        continue;
      }

      const freshUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + fileInfo.result.file_path;

      const response = await fetch(freshUrl, {
        signal: AbortSignal.timeout(20000)
      });

      if (response.ok) {
        // Update KV with fresh URL (non-blocking)
        const updatedMetadata = {
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        };

        kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata)).catch(error => {
          console.warn('Failed to update KV:', error.message);
        });

        console.log('✅ URL refreshed and chunk loaded');
        return response.arrayBuffer();
      }

      console.error('Fresh URL failed for chunk');

    } catch (error) {
      console.error('Bot', i + 1, 'failed for chunk:', error.message);
      continue;
    }
  }

  throw new Error('All bots failed to load chunk: ' + chunkKey);
}

/**
 * Create error response
 */
function errorResponse(message, status = 500, additionalHeaders = {}) {
  console.error('❌ Error response:', status, '-', message);

  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    ...additionalHeaders
  };

  const body = JSON.stringify({
    error: message,
    status: status,
    timestamp: new Date().toISOString()
  }, null, 2);

  return new Response(body, {
    status: status,
    headers: headers
  });
}