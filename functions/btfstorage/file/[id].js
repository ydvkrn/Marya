// functions/btfstorage/file/[id].js
// ⚡ Production-Ready Fast Streaming Handler

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/x-matroska', 'avi': 'video/x-msvideo',
  'mov': 'video/quicktime', 'm4v': 'video/mp4', 'wmv': 'video/x-ms-wmv',
  'flv': 'video/x-flv', '3gp': 'video/3gpp', 'webm': 'video/webm',
  'ogv': 'video/ogg', 'mp3': 'audio/mpeg', 'wav': 'audio/wav',
  'aac': 'audio/mp4', 'm4a': 'audio/mp4', 'ogg': 'audio/ogg',
  'flac': 'audio/flac', 'wma': 'audio/x-ms-wma', 'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif',
  'webp': 'image/webp', 'svg': 'image/svg+xml', 'bmp': 'image/bmp',
  'tiff': 'image/tiff', 'pdf': 'application/pdf',
  'doc': 'application/msword', 'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'txt': 'text/plain', 'zip': 'application/zip',
  'rar': 'application/x-rar-compressed', 'm3u8': 'application/x-mpegURL',
  'ts': 'video/mp2t', 'mpd': 'application/dash+xml'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

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
      return errorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);
    if (!metadata.filename || !metadata.size) {
      return errorResponse('Invalid metadata', 400);
    }

    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

    if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      return errorResponse('Missing file source', 400);
    }

    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    if (isHlsPlaylist) {
      return handleHlsPlaylist(request, env, metadata, actualId);
    }

    if (isHlsSegment && segmentIndex >= 0) {
      return handleHlsSegment(request, env, metadata, segmentIndex);
    }

    if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      return handleSingleFile(request, env, metadata, mimeType);
    }

    if (metadata.chunks && metadata.chunks.length > 0) {
      return handleChunkedFile(request, env, metadata, mimeType);
    }

    return errorResponse('Invalid configuration', 400);

  } catch (error) {
    return errorResponse(error.message, 500);
  }
}

async function handleHlsPlaylist(request, env, metadata, actualId) {
  if (!metadata.chunks || metadata.chunks.length === 0) {
    return errorResponse('HLS not supported', 400);
  }

  const chunks = metadata.chunks;
  const segmentDuration = 6;
  const baseUrl = new URL(request.url).origin;

  let playlist = '#EXTM3U
#EXT-X-VERSION:3
';
  playlist += `#EXT-X-TARGETDURATION:${segmentDuration}
`;
  playlist += '#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
';

  for (let i = 0; i < chunks.length; i++) {
    playlist += `#EXTINF:${segmentDuration.toFixed(1)},
`;
    playlist += `${baseUrl}/btfstorage/file/${actualId}-${i}.ts
`;
  }

  playlist += '#EXT-X-ENDLIST
';

  return new Response(playlist, {
    status: 200,
    headers: {
      'Content-Type': 'application/x-mpegURL',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'no-cache'
    }
  });
}

async function handleHlsSegment(request, env, metadata, segmentIndex) {
  if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
    return errorResponse('Segment not found', 404);
  }

  try {
    const chunkData = await loadChunk(env, metadata.chunks[segmentIndex]);

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
    return errorResponse(error.message, 500);
  }
}

async function handleSingleFile(request, env, metadata, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  if (botTokens.length === 0) {
    return errorResponse('Service unavailable', 503);
  }

  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      
      const requestHeaders = {};
      const rangeHeader = request.headers.get('Range');
      if (rangeHeader) requestHeaders['Range'] = rangeHeader;

      const telegramResponse = await fetch(directUrl, {
        headers: requestHeaders,
        signal: AbortSignal.timeout(30000)
      });

      if (!telegramResponse.ok) continue;

      const responseHeaders = new Headers();
      ['content-length', 'content-range', 'accept-ranges'].forEach(header => {
        const value = telegramResponse.headers.get(header);
        if (value) responseHeaders.set(header, value);
      });

      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Cache-Control', 'public, max-age=31536000');

      const url = new URL(request.url);
      if (url.searchParams.has('dl') || url.searchParams.has('download')) {
        responseHeaders.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
      } else {
        responseHeaders.set('Content-Disposition', 'inline');
      }

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: responseHeaders
      });

    } catch (error) {
      continue;
    }
  }

  return errorResponse('All servers failed', 503);
}

async function handleChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520;
  const rangeHeader = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');

  if (rangeHeader) {
    return handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize);
  }

  if (isDownload) {
    return handleFullDownload(env, metadata, mimeType, totalSize);
  }

  return handleSmartStream(env, metadata, mimeType, totalSize);
}

async function handleSmartStream(env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  const maxInitialChunks = Math.min(3, chunks.length);
  let chunkIndex = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      if (chunkIndex >= maxInitialChunks) {
        controller.close();
        return;
      }

      try {
        const chunkData = await loadChunk(env, chunks[chunkIndex]);
        controller.enqueue(new Uint8Array(chunkData));
        chunkIndex++;
      } catch (error) {
        controller.error(error);
      }
    }
  });

  const estimatedSize = Math.min(maxInitialChunks * (metadata.chunkSize || 20971520), totalSize);

  return new Response(stream, {
    status: 206,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': estimatedSize.toString(),
      'Content-Range': `bytes 0-${estimatedSize - 1}/${totalSize}`,
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=31536000',
      'Content-Disposition': 'inline'
    }
  });
}

async function handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize) {
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

  let currentPosition = startChunk * chunkSize;
  let chunkIdx = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      if (chunkIdx >= neededChunks.length) {
        controller.close();
        return;
      }

      try {
        const chunkData = await loadChunk(env, neededChunks[chunkIdx]);
        const uint8Array = new Uint8Array(chunkData);

        const chunkStart = Math.max(start - currentPosition, 0);
        const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

        if (chunkStart < chunkEnd) {
          controller.enqueue(uint8Array.slice(chunkStart, chunkEnd));
        }

        currentPosition += chunkSize;
        chunkIdx++;
      } catch (error) {
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
      'Cache-Control': 'public, max-age=31536000',
      'Content-Disposition': 'inline'
    }
  });
}

async function handleFullDownload(env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  let chunkIndex = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      if (chunkIndex >= chunks.length) {
        controller.close();
        return;
      }

      try {
        const chunkData = await loadChunk(env, chunks[chunkIndex]);
        controller.enqueue(new Uint8Array(chunkData));
        chunkIndex++;
      } catch (error) {
        controller.error(error);
      }
    }
  });

  return new Response(stream, {
    status: 200,
    headers: {
      'Content-Type': mimeType,
      'Content-Length': totalSize.toString(),
      'Content-Disposition': `attachment; filename="${metadata.filename}"`,
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=31536000'
    }
  });
}

async function loadChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error('Chunk not found');
  }

  const chunkMetadata = JSON.parse(metadataString);
  chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

  if (chunkMetadata.directUrl) {
    try {
      const response = await fetch(chunkMetadata.directUrl, { 
        signal: AbortSignal.timeout(25000) 
      });
      if (response.ok) {
        return response.arrayBuffer();
      }
    } catch (error) {
      // URL expired, continue to refresh
    }
  }

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(12000) }
      );

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      const response = await fetch(freshUrl, { signal: AbortSignal.timeout(25000) });

      if (response.ok) {
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        })).catch(() => {});

        return response.arrayBuffer();
      }
    } catch (error) {
      continue;
    }
  }

  throw new Error('All bots failed');
}

function errorResponse(message, status = 500) {
  return new Response(JSON.stringify({
    error: message,
    status: status
  }), {
    status: status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    }
  });
}