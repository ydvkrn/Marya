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
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp',
  'svg': 'image/svg+xml',
  'pdf': 'application/pdf'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  // Handle OPTIONS preflight CORS
  if (request.method === 'OPTIONS') {
    const headers = new Headers();
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Range, Content-Type');
    headers.set('Access-Control-Max-Age', '86400');
    return new Response(null, { status: 204, headers });
  }

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.') + 1) : '';
    const mimeType = MIME_TYPES[extension.toLowerCase()] || 'application/octet-stream';

    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { status: 404 });
    }
    
    const metadata = JSON.parse(metadataString);
    if (!metadata.filename || !metadata.size || (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0))) {
      return new Response('Invalid file metadata', { status: 400 });
    }

    // Support HEAD requests - return headers without body
    if (request.method === 'HEAD') {
      const headers = new Headers();
      headers.set('Content-Type', mimeType);
      headers.set('Content-Length', metadata.size.toString());
      headers.set('Accept-Ranges', 'bytes');
      headers.set('Access-Control-Allow-Origin', '*');
      headers.set('Cache-Control', 'public, max-age=3600');
      headers.set('Content-Disposition', request.url.includes('dl') ? `attachment; filename="${metadata.filename}"` : 'inline');
      return new Response(null, { status: 200, headers });
    }

    if (metadata.telegramFileId && !metadata.chunks) {
      // Single file on Telegram
      return await handleSingleFile(request, env, metadata, mimeType);
    }
    
    if (metadata.chunks && metadata.chunks.length > 0) {
      // Chunked file
      return await handleChunkedFile(request, env, metadata, mimeType, extension);
    }

    return new Response('Invalid file format', { status: 400 });

  } catch (error) {
    return new Response(`Streaming error: ${error.message}`, { status: 500 });
  }
}

async function handleSingleFile(request, env, metadata, mimeType) {
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      const resp = await fetchWithRetry(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );
      
      const data = await resp.json();
      if (!data.ok || !data.result?.file_path) {
        continue;
      }
      
      const directUrl = `https://api.telegram.org/file/bot${botToken}/${data.result.file_path}`;

      const headersReq = {};
      if (request.headers.get('Range')) headersReq['Range'] = request.headers.get('Range');

      const fileResp = await fetchWithRetry(directUrl, { 
        headers: headersReq, 
        signal: AbortSignal.timeout(45000) 
      });

      if (!fileResp.ok) continue;

      const headers = new Headers();
      ['content-length', 'content-range', 'accept-ranges'].forEach(h => {
        if (fileResp.headers.get(h)) headers.set(h, fileResp.headers.get(h));
      });

      headers.set('Content-Type', mimeType);
      headers.set('Accept-Ranges', 'bytes');
      headers.set('Access-Control-Allow-Origin', '*');
      headers.set('Cache-Control', 'public, max-age=3600');
      headers.set('Content-Disposition', request.url.includes('dl') ? 
        `attachment; filename="${metadata.filename}"` : 'inline');

      return new Response(fileResp.body, {
        status: fileResp.status,
        headers: headers
      });

    } catch (e) {
      continue;
    }
  }
  
  return new Response('All streaming servers failed', { status: 503 });
}

async function handleChunkedFile(request, env, metadata, mimeType) {
  const { chunks, size } = metadata;
  const chunkSize = metadata.chunkSize || 20 * 1024 * 1024;
  const range = request.headers.get('Range');

  // Full download param check
  const isDownload = new URL(request.url).searchParams.has('dl');

  if (range) {
    return await handleSmartRange(request, env, metadata, range, mimeType, chunkSize, isDownload);
  }

  if (isDownload) {
    return await handleFullStreamDownload(request, env, metadata, mimeType);
  }

  return await handleInstantPlay(request, env, metadata, mimeType, size);
}

async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;

  const maxInitialBytes = 100 * 1024 * 1024; // 100MB max to start playback
  let loadedBytes = 0;
  let chunkIndex = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      while (chunkIndex < chunks.length && loadedBytes < maxInitialBytes) {
        try {
          const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
          const uint8Array = new Uint8Array(chunkData);
          controller.enqueue(uint8Array);
          loadedBytes += uint8Array.byteLength;
          chunkIndex++;
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
  headers.set('Content-Length', Math.min(loadedBytes, totalSize).toString());
  headers.set('Content-Range', `bytes 0-${Math.min(loadedBytes, totalSize) - 1}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');
  headers.set('Cache-Control', 'public, max-age=3600');
  headers.set('X-Streaming-Mode', 'instant-play');

  return new Response(stream, { status: 206, headers });
}

async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload = false) {
  const size = metadata.size;
  const chunks = metadata.chunks;

  const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) {
    return new Response('Invalid range', { status: 416 });
  }

  const start = parseInt(rangeMatch[1], 10);
  let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;
  if (end >= size) end = size - 1;
  
  if (start >= size || start > end) {
    return new Response('Range not satisfiable', {
      status: 416,
      headers: { 'Content-Range': `bytes */${size}` }
    });
  }

  const requestedSize = end - start + 1;

  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  let currentPosition = startChunk * chunkSize;

  const stream = new ReadableStream({
    async pull(controller) {
      for (let i = 0; i < neededChunks.length; i++) {
        const chunkInfo = neededChunks[i];
        try {
          const chunkData = await loadSingleChunk(env, chunkInfo);
          const uint8Array = new Uint8Array(chunkData);

          const chunkStart = Math.max(start - currentPosition, 0);
          const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

          if (chunkStart < chunkEnd) {
            controller.enqueue(uint8Array.slice(chunkStart, chunkEnd));
          }

          currentPosition += chunkSize;
          if (currentPosition > end) break;
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
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', isDownload ? 
    `attachment; filename="${metadata.filename}"` : 'inline');
  headers.set('Cache-Control', 'public, max-age=3600');

  return new Response(stream, { status: 206, headers });
}

async function handleFullStreamDownload(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const filename = metadata.filename;
  const totalSize = metadata.size;

  let chunkIndex = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      while (chunkIndex < chunks.length) {
        try {
          const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
          const uint8Array = new Uint8Array(chunkData);
          controller.enqueue(uint8Array);
          chunkIndex++;
        } catch (error) {
          controller.error(error);
          return;
        }
      }
      controller.close();
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', 'application/octet-stream');
  headers.set('Content-Length', totalSize.toString());
  headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=3600');
  headers.set('X-Download-Mode', 'full-stream');

  return new Response(stream, { status: 200, headers });
}

async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkKey}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  let response = await fetchWithRetry(chunkMetadata.directUrl, { 
    signal: AbortSignal.timeout(30000) 
  });

  if (response.ok) {
    return response.arrayBuffer();
  }

  // Refresh expired URL and retry
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetchWithRetry(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );
      
      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      response = await fetchWithRetry(freshUrl, { signal: AbortSignal.timeout(30000) });

      if (response.ok) {
        // Update KV with fresh URL asynchronously
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        })).catch(() => {});

        return response.arrayBuffer();
      }
    } catch { 
      continue; 
    }
  }
  
  throw new Error(`All refresh attempts failed: ${chunkKey}`);
}

async function fetchWithRetry(url, options, retries = 5) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response;
      
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After')) || 5;
        await new Promise(res => setTimeout(res, retryAfter * 1000));
      }
    } catch { }
    
    if (i < retries - 1) {
      await new Promise(res => setTimeout(res, 2000));
    }
  }
  
  throw new Error(`Failed to fetch: ${url}`);
}