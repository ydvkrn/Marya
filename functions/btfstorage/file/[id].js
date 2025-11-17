// functions/btfstorage/file/[id].js
// 🎬 Cloudflare Pages Functions - Optimized Advanced File Streaming Handler
// URL: marya-hosting.pages.dev/btfstorage/file/MSM221-48U91C62-no.mp4

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

const MAX_RETRY_ATTEMPTS = 3;
const INITIAL_RETRY_DELAY = 500;
const MAX_RETRY_DELAY = 5000;
const FETCH_TIMEOUT = 30000;
const API_TIMEOUT = 10000;

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;
  const startTime = Date.now();

  console.log('🎬 TOP TIER STREAMING STARTED:', fileId);
  console.log('📍 Request URL:', request.url);
  console.log('🔗 Method:', request.method);

  if (request.method === 'OPTIONS') {
    return handleCORSPreflight();
  }

  try {
    const fileInfo = parseFileId(fileId);
    
    console.log('📁 File parsed:', fileInfo.actualId);

    const metadataString = await Promise.race([
      env.FILES_KV.get(fileInfo.actualId),
      timeout(5000, 'Metadata fetch timeout')
    ]);

    if (!metadataString) {
      console.error('❌ File not found:', fileInfo.actualId);
      return createErrorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);

    if (!metadata.filename || !metadata.size) {
      console.error('❌ Invalid metadata');
      return createErrorResponse('Invalid file metadata', 400);
    }

    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

    if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      console.error('❌ No file source data');
      return createErrorResponse('Missing file source data', 400);
    }

    const mimeType = metadata.contentType || MIME_TYPES[fileInfo.extension] || 'application/octet-stream';

    console.log('📁 File:', metadata.filename, '- Size:', Math.round(metadata.size/1024/1024), 'MB');

    let response;
    if (fileInfo.isHlsPlaylist) {
      response = await handleHlsPlaylist(request, env, metadata, fileInfo.actualId);
    } else if (fileInfo.isHlsSegment && fileInfo.segmentIndex >= 0) {
      response = await handleHlsSegment(request, env, metadata, fileInfo.segmentIndex);
    } else if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      response = await handleSingleFile(request, env, metadata, mimeType);
    } else if (metadata.chunks && metadata.chunks.length > 0) {
      response = await handleChunkedFile(request, env, metadata, mimeType, fileInfo.extension);
    } else {
      response = createErrorResponse('Invalid file format', 400);
    }

    const duration = Date.now() - startTime;
    console.log('✅ Request completed in', duration, 'ms');
    
    return response;

  } catch (error) {
    console.error('❌ Critical error:', error.message);
    return createErrorResponse('Streaming error: ' + error.message, 500);
  }
}

function parseFileId(fileId) {
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

  return { actualId, extension, isHlsPlaylist, isHlsSegment, segmentIndex };
}

function handleCORSPreflight() {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
  headers.set('Access-Control-Max-Age', '86400');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
  return new Response(null, { status: 204, headers });
}

async function handleHlsPlaylist(request, env, metadata, actualId) {
  if (!metadata.chunks || metadata.chunks.length === 0) {
    return createErrorResponse('HLS not supported for single files', 400);
  }

  const chunks = metadata.chunks;
  const segmentDuration = 6;
  const baseUrl = new URL(request.url).origin;

  let playlist = '#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:' + segmentDuration + '
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
';

  for (let i = 0; i < chunks.length; i++) {
    playlist += '#EXTINF:' + segmentDuration.toFixed(1) + ',
';
    playlist += baseUrl + '/btfstorage/file/' + actualId + '-' + i + '.ts
';
  }

  playlist += '#EXT-X-ENDLIST
';

  const headers = new Headers();
  headers.set('Content-Type', 'application/x-mpegURL');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=300');
  headers.set('CDN-Cache-Control', 'public, max-age=3600');

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
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    headers.set('Content-Disposition', 'inline');
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
      const getFileResponse = await fetchWithTimeout(
        'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(metadata.telegramFileId),
        { method: 'GET' },
        API_TIMEOUT
      );

      const getFileData = await getFileResponse.json();

      if (!getFileData.ok || !getFileData.result || !getFileData.result.file_path) {
        continue;
      }

      const directUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + getFileData.result.file_path;

      const requestHeaders = {};
      const rangeHeader = request.headers.get('Range');
      if (rangeHeader) {
        requestHeaders['Range'] = rangeHeader;
      }

      const telegramResponse = await fetchWithTimeout(directUrl, { headers: requestHeaders }, FETCH_TIMEOUT);

      if (!telegramResponse.ok) {
        continue;
      }

      const responseHeaders = new Headers();
      ['content-length', 'content-range', 'accept-ranges'].forEach(header => {
        const value = telegramResponse.headers.get(header);
        if (value) responseHeaders.set(header, value);
      });

      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Cache-Control', 'public, max-age=31536000, immutable');
      responseHeaders.set('CDN-Cache-Control', 'public, max-age=31536000');

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
    return await handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload);
  }

  if (isDownload) {
    return await handleFullStreamDownload(request, env, metadata, mimeType);
  }

  return await handleInstantPlay(request, env, metadata, mimeType, totalSize);
}

async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;

  try {
    const maxInitialBytes = 30 * 1024 * 1024;
    const maxInitialChunks = Math.min(2, chunks.length);

    let loadedBytes = 0;
    let chunkIndex = 0;

    const chunkPromises = [];
    for (let i = 0; i < maxInitialChunks; i++) {
      chunkPromises.push(loadSingleChunk(env, chunks[i]));
    }

    const chunkResults = await Promise.all(chunkPromises);

    const stream = new ReadableStream({
      pull(controller) {
        try {
          while (chunkIndex < chunkResults.length && loadedBytes < maxInitialBytes) {
            const chunkData = chunkResults[chunkIndex];
            const uint8Array = new Uint8Array(chunkData);
            controller.enqueue(uint8Array);
            loadedBytes += uint8Array.byteLength;
            chunkIndex++;
          }
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      }
    });

    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', Math.min(loadedBytes, totalSize).toString());
    headers.set('Content-Range', 'bytes 0-' + (Math.min(loadedBytes, totalSize) - 1) + '/' + totalSize);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=3600');
    headers.set('X-Streaming-Mode', 'instant-play');

    return new Response(stream, { status: 206, headers });

  } catch (error) {
    return createErrorResponse('Instant play failed: ' + error.message, 500);
  }
}

async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
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

  let currentPosition = startChunk * chunkSize;
  let chunkIndex = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      try {
        while (chunkIndex < neededChunks.length) {
          const chunkInfo = neededChunks[chunkIndex];
          const chunkData = await loadSingleChunk(env, chunkInfo);
          const uint8Array = new Uint8Array(chunkData);

          const chunkStart = Math.max(start - currentPosition, 0);
          const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

          if (chunkStart < chunkEnd) {
            const chunkSlice = uint8Array.slice(chunkStart, chunkEnd);
            controller.enqueue(chunkSlice);
          }

          currentPosition += chunkSize;
          chunkIndex++;
          
          if (currentPosition > end) break;
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', 'bytes ' + start + '-' + end + '/' + totalSize);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', isDownload ? 'attachment; filename="' + metadata.filename + '"' : 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('X-Streaming-Mode', 'range-request');

  return new Response(stream, { status: 206, headers });
}

async function handleFullStreamDownload(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const filename = metadata.filename;
  const totalSize = metadata.size;

  let chunkIndex = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      try {
        while (chunkIndex < chunks.length) {
          const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
          const uint8Array = new Uint8Array(chunkData);
          controller.enqueue(uint8Array);
          chunkIndex++;
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Content-Disposition', 'attachment; filename="' + filename + '"');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('X-Download-Mode', 'full-stream');
  headers.set('Cache-Control', 'public, max-age=31536000');

  return new Response(stream, { status: 200, headers });
}

async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  const metadataString = await Promise.race([
    kvNamespace.get(chunkKey),
    timeout(3000, 'Chunk metadata timeout')
  ]);

  if (!metadataString) {
    throw new Error('Chunk metadata not found: ' + chunkKey);
  }

  const chunkMetadata = JSON.parse(metadataString);
  chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

  if (chunkMetadata.directUrl) {
    try {
      const response = await fetchWithTimeout(chunkMetadata.directUrl, { method: 'GET' }, 15000);
      if (response.ok) {
        return response.arrayBuffer();
      }
    } catch (error) {
      // Continue to refresh
    }
  }

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];

    try {
      const getFileResponse = await fetchWithTimeout(
        'https://api.telegram.org/bot' + botToken + '/getFile?file_id=' + encodeURIComponent(chunkMetadata.telegramFileId),
        { method: 'GET' },
        API_TIMEOUT
      );

      const getFileData = await getFileResponse.json();

      if (!getFileData.ok || !getFileData.result || !getFileData.result.file_path) {
        continue;
      }

      const freshUrl = 'https://api.telegram.org/file/bot' + botToken + '/' + getFileData.result.file_path;
      const response = await fetchWithTimeout(freshUrl, { method: 'GET' }, 20000);

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

  throw new Error('All refresh attempts failed for: ' + chunkKey);
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === 'AbortError') {
      throw new Error('Request timeout after ' + timeoutMs + 'ms');
    }
    throw error;
  }
}

function timeout(ms, message) {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(message)), ms);
  });
}

function createErrorResponse(message, status, additionalHeaders) {
  const headers = new Headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    ...additionalHeaders
  });

  const errorResponse = {
    error: message,
    status: status,
    timestamp: new Date().toISOString(),
    service: 'BTF Storage Streaming'
  };

  return new Response(JSON.stringify(errorResponse, null, 2), {
    status: status,
    headers: headers
  });
}