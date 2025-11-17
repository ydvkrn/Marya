// functions/btfstorage/files/[id].js
// ðŸš€ WORLD-CLASS Cloudflare Pages Functions - Ultra-Optimized Streaming Handler

const MIME_TYPES = {
  'mp4': 'video/mp4',
  'mkv': 'video/x-matroska',
  'avi': 'video/x-msvideo',
  'mov': 'video/quicktime',
  'm4v': 'video/mp4',
  'wmv': 'video/x-ms-wmv',
  'flv': 'video/x-flv',
  '3gp': 'video/3gpp',
  'webm': 'video/webm',
  'ogv': 'video/ogg',
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'aac': 'audio/mp4',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg',
  'flac': 'audio/flac',
  'wma': 'audio/x-ms-wma',
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp',
  'svg': 'image/svg+xml',
  'bmp': 'image/bmp',
  'tiff': 'image/tiff',
  'pdf': 'application/pdf',
  'doc': 'application/msword',
  'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'txt': 'text/plain',
  'zip': 'application/zip',
  'rar': 'application/x-rar-compressed',
  'm3u8': 'application/x-mpegURL',
  'ts': 'video/mp2t',
  'mpd': 'application/dash+xml'
};

const MAX_PARALLEL_CHUNKS = 3;
const CACHE_TTL_LONG = 31536000;
const FETCH_TIMEOUT = 25000;
const MAX_RETRIES = 3;

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('ðŸš€ Streaming started:', fileId);

  if (request.method === 'OPTIONS') {
    return new Response(null, { 
      status: 204, 
      headers: createCorsHeaders() 
    });
  }

  try {
    let actualId = fileId;
    let extension = '';
    let isHlsPlaylist = false;
    let isHlsSegment = false;
    let segmentIndex = -1;

    if (fileId.includes('.')) {
      const parts = fileId.split('.');
      extension = parts.pop().toLowerCase();
      actualId = parts.join('.');

      if (extension === 'm3u8') {
        isHlsPlaylist = true;
      } else if (extension === 'ts' && actualId.includes('-')) {
        const segParts = actualId.split('-');
        const lastPart = segParts[segParts.length - 1];

        if (!isNaN(parseInt(lastPart))) {
          segmentIndex = parseInt(segParts.pop(), 10);
          actualId = segParts.join('-');
          isHlsSegment = true;
        }
      } else {
        actualId = fileId.substring(0, fileId.lastIndexOf('.'));
        extension = fileId.substring(fileId.lastIndexOf('.') + 1).toLowerCase();
      }
    }

    const metadataString = await env.FILES_KV.get(actualId);

    if (!metadataString) {
      return createErrorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);

    if (!metadata.filename || !metadata.size) {
      return createErrorResponse('Invalid file metadata', 400);
    }

    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

    if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      return createErrorResponse('Missing file source data', 400);
    }

    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    if (isHlsPlaylist) {
      return await handleHlsPlaylist(request, env, metadata, actualId);
    }

    if (isHlsSegment && segmentIndex >= 0) {
      return await handleHlsSegment(request, env, metadata, segmentIndex);
    }

    if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      return await handleSingleFile(request, env, metadata, mimeType);
    }

    if (metadata.chunks && metadata.chunks.length > 0) {
      return await handleChunkedFile(request, env, metadata, mimeType, extension);
    }

    return createErrorResponse('Invalid file format', 400);

  } catch (error) {
    console.error('Error:', error);
    return createErrorResponse('Streaming error: ' + error.message, 500);
  }
}

function createCorsHeaders() {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
  headers.set('Access-Control-Max-Age', '86400');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
  return headers;
}

async function handleHlsPlaylist(request, env, metadata, actualId) {
  if (!metadata.chunks || metadata.chunks.length === 0) {
    return createErrorResponse('HLS not supported for single files', 400);
  }

  const chunks = metadata.chunks;
  const segmentDuration = 6;
  const baseUrl = new URL(request.url).origin;

  const playlistLines = ['#EXTM3U', '#EXT-X-VERSION:3', '#EXT-X-TARGETDURATION:' + segmentDuration, '#EXT-X-MEDIA-SEQUENCE:0', '#EXT-X-PLAYLIST-TYPE:VOD'];

  for (let i = 0; i < chunks.length; i++) {
    playlistLines.push('#EXTINF:' + segmentDuration.toFixed(1) + ',');
    playlistLines.push(baseUrl + '/btfstorage/files/' + actualId + '-' + i + '.ts');
  }

  playlistLines.push('#EXT-X-ENDLIST');

  const playlist = playlistLines.join('\n') + '\n';

  const headers = new Headers();
  headers.set('Content-Type', 'application/x-mpegURL');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'no-cache, no-store, must-revalidate');

  return new Response(playlist, { status: 200, headers });
}

async function handleHlsSegment(request, env, metadata, segmentIndex) {
  if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
    return createErrorResponse('Segment not found', 404);
  }

  try {
    const chunkInfo = metadata.chunks[segmentIndex];
    const chunkData = await loadSingleChunk(env, chunkInfo);

    const headers = new Headers();
    headers.set('Content-Type', 'video/mp2t');
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=' + CACHE_TTL_LONG + ', immutable');
    headers.set('Accept-Ranges', 'bytes');

    return new Response(chunkData, { status: 200, headers });

  } catch (error) {
    return createErrorResponse('Segment loading failed: ' + error.message, 500);
  }
}

async function handleSingleFile(request, env, metadata, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  if (botTokens.length === 0) {
    return createErrorResponse('Service configuration error', 503);
  }

  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];

    try {
      const getFileResponse = await fetchWithRetry(
        'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(metadata.telegramFileId),
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();

      if (!getFileData.ok || !getFileData.result?.file_path) {
        continue;
      }

      const directUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + getFileData.result.file_path;

      const requestHeaders = {};
      const rangeHeader = request.headers.get('Range');

      if (rangeHeader) {
        requestHeaders['Range'] = rangeHeader;
      }

      const telegramResponse = await fetchWithRetry(directUrl, {
        headers: requestHeaders,
        signal: AbortSignal.timeout(FETCH_TIMEOUT)
      });

      if (!telegramResponse.ok) {
        continue;
      }

      const responseHeaders = new Headers();

      ['content-length', 'content-range', 'accept-ranges'].forEach(header => {
        const value = telegramResponse.headers.get(header);
        if (value) {
          responseHeaders.set(header, value);
        }
      });

      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Cache-Control', 'public, max-age=' + CACHE_TTL_LONG + ', immutable');

      const url = new URL(request.url);
      if (url.searchParams.has('dl') || url.searchParams.has('download')) {
        responseHeaders.set('Content-Disposition', 'attachment; filename="' + metadata.filename + '"');
      } else {
        responseHeaders.set('Content-Disposition', 'inline');
      }

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: responseHeaders
      });

    } catch (botError) {
      continue;
    }
  }

  return createErrorResponse('All streaming servers failed', 503);
}

async function handleChunkedFile(request, env, metadata, mimeType, extension) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520;

  const rangeHeader = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');

  if (rangeHeader) {
    return await handleParallelRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload);
  }

  if (isDownload) {
    return await handleParallelFullDownload(request, env, metadata, mimeType);
  }

  return await handleOptimizedInstantPlay(request, env, metadata, mimeType, totalSize);
}

async function handleOptimizedInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;

  try {
    const maxInitialChunks = Math.min(2, chunks.length);

    const chunkPromises = [];
    for (let i = 0; i < maxInitialChunks; i++) {
      chunkPromises.push(loadSingleChunk(env, chunks[i]));
    }

    const loadedChunks = await Promise.all(chunkPromises);

    let loadedBytes = 0;
    const stream = new ReadableStream({
      start(controller) {
        for (let i = 0; i < loadedChunks.length; i++) {
          const uint8Array = new Uint8Array(loadedChunks[i]);
          controller.enqueue(uint8Array);
          loadedBytes += uint8Array.byteLength;
        }
        controller.close();
      }
    });

    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', Math.min(loadedBytes, totalSize).toString());
    headers.set('Content-Range', 'bytes 0-' + (Math.min(loadedBytes, totalSize) - 1) + '/' + totalSize);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=' + CACHE_TTL_LONG + ', immutable');

    return new Response(stream, { status: 206, headers });

  } catch (error) {
    return createErrorResponse('Instant play failed: ' + error.message, 500);
  }
}

async function handleParallelRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) {
    return createErrorResponse('Invalid range format', 416, {
      'Content-Range': 'bytes */' + totalSize
    });
  }

  const start = parseInt(rangeMatch[1], 10);
  let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;

  if (end >= totalSize) end = totalSize - 1;
  if (start >= totalSize || start > end) {
    return createErrorResponse('Range not satisfiable', 416, {
      'Content-Range': 'bytes */' + totalSize
    });
  }

  const requestedSize = end - start + 1;

  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  try {
    const chunkPromises = neededChunks.map(chunkInfo => loadSingleChunk(env, chunkInfo));
    const loadedChunks = await Promise.all(chunkPromises);

    let currentPosition = startChunk * chunkSize;
    const rangeData = [];

    for (let i = 0; i < loadedChunks.length; i++) {
      const uint8Array = new Uint8Array(loadedChunks[i]);

      const chunkStart = Math.max(start - currentPosition, 0);
      const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

      if (chunkStart < chunkEnd) {
        const chunkSlice = uint8Array.slice(chunkStart, chunkEnd);
        rangeData.push(chunkSlice);
      }

      currentPosition += chunkSize;
      if (currentPosition > end) break;
    }

    const totalLength = rangeData.reduce((sum, chunk) => sum + chunk.length, 0);
    const combinedData = new Uint8Array(totalLength);
    let offset = 0;

    for (const chunk of rangeData) {
      combinedData.set(chunk, offset);
      offset += chunk.length;
    }

    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', requestedSize.toString());
    headers.set('Content-Range', 'bytes ' + start + '-' + end + '/' + totalSize);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', isDownload ? ('attachment; filename="' + metadata.filename + '"') : 'inline');
    headers.set('Cache-Control', 'public, max-age=' + CACHE_TTL_LONG + ', immutable');

    return new Response(combinedData, { status: 206, headers });

  } catch (error) {
    return createErrorResponse('Range request failed: ' + error.message, 500);
  }
}

async function handleParallelFullDownload(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const filename = metadata.filename;
  const totalSize = metadata.size;

  let chunkIndex = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      while (chunkIndex < chunks.length) {
        try {
          const batchSize = Math.min(MAX_PARALLEL_CHUNKS, chunks.length - chunkIndex);
          const batchPromises = [];

          for (let i = 0; i < batchSize; i++) {
            batchPromises.push(loadSingleChunk(env, chunks[chunkIndex + i]));
          }

          const batchChunks = await Promise.all(batchPromises);

          for (let i = 0; i < batchChunks.length; i++) {
            const uint8Array = new Uint8Array(batchChunks[i]);
            controller.enqueue(uint8Array);
          }

          chunkIndex += batchSize;

        } catch (error) {
          controller.error(error);
          return;
        }
      }

      controller.close();
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Content-Disposition', 'attachment; filename="' + filename + '"');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=' + CACHE_TTL_LONG + ', immutable');

  return new Response(stream, { status: 200, headers });
}

async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error('Chunk metadata not found: ' + chunkKey);
  }

  const chunkMetadata = JSON.parse(metadataString);
  chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

  if (chunkMetadata.directUrl) {
    try {
      const response = await fetchWithRetry(chunkMetadata.directUrl, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT)
      });

      if (response.ok) {
        return response.arrayBuffer();
      }
    } catch (error) {
      console.log('Cached URL failed, refreshing...');
    }
  }

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];

    try {
      const getFileResponse = await fetchWithRetry(
        'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(chunkMetadata.telegramFileId),
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();

      if (!getFileData.ok || !getFileData.result?.file_path) {
        continue;
      }

      const freshUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + getFileData.result.file_path;

      const response = await fetchWithRetry(freshUrl, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT)
      });

      if (response.ok) {
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        })).catch(() => {});

        return response.arrayBuffer();
      }

    } catch (botError) {
      continue;
    }
  }

  throw new Error('All refresh attempts failed for chunk: ' + chunkKey);
}

async function fetchWithRetry(url, options, retries) {
  if (!retries) retries = MAX_RETRIES;

  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetch(url, options);

      if (response.ok) {
        return response;
      }

      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After')) || Math.pow(2, attempt);
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
        continue;
      }

      if (response.status >= 500 && attempt < retries - 1) {
        const delay = Math.min(Math.pow(2, attempt) * 1000, 8000);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }

      if (response.status >= 400 && response.status < 500) {
        return response;
      }

    } catch (error) {
      if (attempt === retries - 1) {
        throw error;
      }
    }

    if (attempt < retries - 1) {
      const delay = Math.min(Math.pow(2, attempt) * 1000, 8000);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  throw new Error('All fetch attempts failed');
}

function createErrorResponse(message, status, additionalHeaders) {
  const headers = new Headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    ...additionalHeaders
  });

  const errorResponse = {
    error: message,
    status: status || 500,
    timestamp: new Date().toISOString()
  };

  return new Response(JSON.stringify(errorResponse, null, 2), {
    status: status || 500,
    headers: headers
  });
}