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
  'pdf': 'application/pdf',
  'm3u8': 'application/x-mpegURL',
  'ts': 'video/mp2t'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 TOP TIER STREAMING:', fileId);

  if (request.method === 'OPTIONS') {
    const headers = new Headers();
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Range');
    headers.set('Access-Control-Max-Age', '86400');
    return new Response(null, { status: 204, headers });
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
        if (segParts.length > 1 && !isNaN(parseInt(segParts[segParts.length - 1]))) {
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
      console.error('File not found in KV:', actualId);
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    if (!metadata.filename || !metadata.size) {
      console.error('Invalid metadata:', metadata);
      return new Response('Invalid file metadata', { status: 400 });
    }

    // Adapt to possible fileIdCode or telegramFileId
    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

    // Handle chunks if present
    if (!metadata.telegramFileId && !metadata.chunks) {
      console.error('No telegramFileId or chunks in metadata:', metadata);
      return new Response('Missing file ID or chunks', { status: 400 });
    }

    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';
    console.log(`📁 ${metadata.filename} | Size: ${Math.round(metadata.size/1024/1024)}MB | MIME: ${mimeType} | Chunks: ${metadata.chunks?.length || 0} | UploadedAt: ${metadata.uploadedAt || 'N/A'} | HLS Playlist: ${isHlsPlaylist} | HLS Segment: ${isHlsSegment} Index: ${segmentIndex}`);

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

    return new Response('Invalid file format', { status: 400 });

  } catch (error) {
    console.error('❌ Streaming error:', error);
    return new Response(`Streaming error: ${error.message}`, { status: 500 });
  }
}

async function handleHlsPlaylist(request, env, metadata, actualId) {
  console.log('📼 Generating HLS playlist for:', actualId);

  if (!metadata.chunks || metadata.chunks.length === 0) {
    return new Response('HLS not supported for single files', { status: 400 });
  }

  const chunks = metadata.chunks;
  const segmentDuration = 5;
  const baseUrl = new URL(request.url).origin;

  let playlist = '#EXTM3U\n';
  playlist += '#EXT-X-VERSION:3\n';
  playlist += `#EXT-X-TARGETDURATION:${segmentDuration}\n`;
  playlist += '#EXT-X-MEDIA-SEQUENCE:0\n';
  playlist += '#EXT-X-PLAYLIST-TYPE:VOD\n';

  for (let i = 0; i < chunks.length; i++) {
    playlist += `#EXTINF:${segmentDuration.toFixed(1)},\n`;
    playlist += `${baseUrl}/btfstorage/file/${actualId}-${i}.ts\n`;
  }

  playlist += '#EXT-X-ENDLIST\n';

  const headers = new Headers();
  headers.set('Content-Type', 'application/x-mpegURL');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'no-cache');

  console.log('📼 HLS playlist generated with', chunks.length, 'segments');

  return new Response(playlist, { status: 200, headers });
}

async function handleHlsSegment(request, env, metadata, segmentIndex) {
  console.log('📼 Serving HLS segment:', segmentIndex);

  if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
    return new Response('Segment not found', { status: 404 });
  }

  try {
    const chunkInfo = metadata.chunks[segmentIndex];
    const chunkData = await loadSingleChunk(env, chunkInfo);

    const headers = new Headers();
    headers.set('Content-Type', 'video/mp2t');
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=31536000');
    headers.set('Content-Disposition', 'inline');

    console.log('📼 HLS segment served:', segmentIndex, 'Size:', Math.round(chunkData.byteLength/1024/1024), 'MB');

    return new Response(chunkData, { status: 200, headers });
  } catch (error) {
    console.error('❌ HLS segment error:', error);
    return new Response(`Segment error: ${error.message}`, { status: 500 });
  }
}

async function handleSingleFile(request, env, metadata, mimeType) {
  console.log('🚀 Single file streaming');

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetchWithRetry(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) {
        console.error(`Telegram API error: ${getFileData.error_code} - ${getFileData.description}`);
        continue;
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      const telegramResponse = await fetchWithRetry(directUrl, {
        headers: request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {},
        signal: AbortSignal.timeout(45000)
      });

      if (!telegramResponse.ok) {
        console.error(`Telegram file fetch failed: ${telegramResponse.status}`);
        continue;
      }

      const headers = new Headers();
      ['content-length', 'content-range', 'accept-ranges'].forEach(header => {
        if (telegramResponse.headers.get(header)) {
          headers.set(header, telegramResponse.headers.get(header));
        }
      });

      headers.set('Content-Type', mimeType);
      headers.set('Accept-Ranges', 'bytes');
      headers.set('Access-Control-Allow-Origin', '*');
      headers.set('Cache-Control', 'public, max-age=31536000');

      const url = new URL(request.url);
      if (url.searchParams.has('dl')) {
        headers.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
      } else {
        headers.set('Content-Disposition', 'inline');
      }

      console.log(`🚀 Single file streaming: ${metadata.filename} (Status: ${telegramResponse.status})`);

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: headers
      });

    } catch (botError) {
      console.error(`❌ Bot failed:`, botError);
      continue;
    }
  }

  return new Response('All streaming servers failed', { status: 503 });
}

async function handleChunkedFile(request, env, metadata, mimeType, extension) {
  const chunks = metadata.chunks;
  const size = metadata.size;
  const filename = metadata.filename;
  const chunkSize = metadata.chunkSize || 20971520;

  const range = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');

  console.log(`🎬 Chunked streaming: ${chunks.length} chunks | Range: ${range} | Download: ${isDownload}`);

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
  
  console.log('⚡ INSTANT PLAY: Streaming initial chunks...');

  try {
    const maxInitialBytes = 50 * 1024 * 1024;
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
            console.log(`⚡ Streamed initial chunk ${chunkIndex + 1}: ${Math.round(uint8Array.byteLength/1024/1024)}MB`);
            chunkIndex++;
          } catch (error) {
            console.error(`❌ Initial chunk ${chunkIndex + 1} failed:`, error);
            controller.error(error);
            return;
          }
        }
        controller.close();
      },
      cancel() {
        console.log('⚡ Stream cancelled');
      }
    });

    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', Math.min(loadedBytes || maxInitialBytes, totalSize).toString());
    headers.set('Content-Range', `bytes 0-${Math.min(loadedBytes || maxInitialBytes, totalSize) - 1}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=31536000');
    headers.set('X-Streaming-Mode', 'instant-play');

    console.log(`⚡ INSTANT PLAY READY: ${Math.round((loadedBytes || maxInitialBytes)/1024/1024)}MB streamed`);

    return new Response(stream, { status: 206, headers });

  } catch (error) {
    console.error('⚡ Instant play error:', error);
    return new Response(`Instant play error: ${error.message}`, { status: 500 });
  }
}

async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload = false) {
  const size = metadata.size;
  const chunks = metadata.chunks;

  console.log('🎯 SMART RANGE:', rangeHeader);

  const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);
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
  console.log(`🎯 Range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`🎯 Need chunks: ${startChunk}-${endChunk} (${neededChunks.length})`);

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
            console.log(`🎯 Streamed range chunk ${startChunk + i + 1}: ${chunkEnd - chunkStart} bytes`);
          }

          currentPosition += chunkSize;
          if (currentPosition > end) break;
        } catch (error) {
          console.error(`❌ Range chunk ${startChunk + i + 1} failed:`, error);
          controller.error(error);
          return;
        }
      }
      controller.close();
    },
    cancel() {
      console.log('🎯 Range stream cancelled');
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', isDownload ? `attachment; filename="${metadata.filename}"` : 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000');

  console.log(`✅ RANGE RESPONSE: ${requestedSize} bytes streamed`);

  return new Response(stream, { status: 206, headers });
}

async function handleFullStreamDownload(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const filename = metadata.filename;
  const totalSize = metadata.size;

  console.log('📥 FULL STREAM DOWNLOAD: Streaming all chunks...');

  let chunkIndex = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      while (chunkIndex < chunks.length) {
        try {
          const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
          const uint8Array = new Uint8Array(chunkData);
          controller.enqueue(uint8Array);
          console.log(`📥 Streamed download chunk ${chunkIndex + 1}: ${Math.round(uint8Array.byteLength/1024/1024)}MB`);
          chunkIndex++;
        } catch (error) {
          console.error(`❌ Download chunk ${chunkIndex + 1} failed:`, error);
          controller.error(error);
          return;
        }
      }
      controller.close();
    },
    cancel() {
      console.log('📥 Download stream cancelled');
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('X-Download-Mode', 'full-stream');
  headers.set('Cache-Control', 'public, max-age=31536000');

  console.log(`📥 Full download stream started: Total ${Math.round(totalSize/1024/1024)}MB`);

  return new Response(stream, { status: 200, headers });
}

async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;
  
  console.log(`📥 Loading: ${chunkKey}`);
  
  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkKey}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

  let response = await fetchWithRetry(chunkMetadata.directUrl, { 
    signal: AbortSignal.timeout(30000) 
  });
  
  if (response.ok) {
    return response.arrayBuffer();
  }

  console.log(`🔄 URL expired, refreshing: ${chunkKey}`);
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetchWithRetry(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok) {
        console.error(`Telegram API error: ${getFileData.error_code} - ${getFileData.description}`);
        continue;
      }

      if (!getFileData.result?.file_path) {
        console.error('No file_path in Telegram response');
        continue;
      }

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      response = await fetchWithRetry(freshUrl, { signal: AbortSignal.timeout(30000) });
      
      if (response.ok) {
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        })).catch(() => {});

        console.log(`✅ URL refreshed: ${chunkKey}`);
        return response.arrayBuffer();
      }
      
    } catch (botError) {
      console.error(`❌ Bot failed:`, botError);
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
        const retryAfter = response.headers.get('Retry-After') || 5;
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
      }
      console.error(`Attempt ${i + 1} failed: ${response.status}`);
    } catch (error) {
      console.error(`Attempt ${i + 1} error:`, error);
    }
    if (i < retries - 1) await new Promise(resolve => setTimeout(resolve, 2000));
  }
  throw new Error(`All retries failed for ${url}`);
}