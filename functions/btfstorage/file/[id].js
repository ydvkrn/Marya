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
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.') + 1) : '';

    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      console.error('File not found in KV:', actualId);
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    if (!metadata.filename || !metadata.size || (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0))) {
      console.error('Invalid metadata:', metadata);
      return new Response('Invalid file metadata', { status: 400 });
    }

    const mimeType = MIME_TYPES[extension.toLowerCase()] || 'application/octet-stream';
    console.log(`📁 ${metadata.filename} | Size: ${Math.round(metadata.size/1024/1024)}MB | MIME: ${mimeType} | Chunks: ${metadata.chunks?.length || 0}`);

    if (metadata.telegramFileId && !metadata.chunks) {
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
      headers.set('Cache-Control', 'public, max-age=3600');

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

  if (range && !isDownload) {
    return await handleSmartRange(request, env, metadata, range, mimeType, chunkSize);
  }

  if (isDownload) {
    return await handleSmartDownload(request, env, metadata, mimeType);
  }

  return await handleInstantPlay(request, env, metadata, mimeType, size);
}

async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  
  console.log('⚡ INSTANT PLAY: Loading initial chunks for large file...');

  try {
    // Load up to 5 chunks or until we have enough data for MP4 moov atom (100MB max)
    const maxInitialBytes = 100 * 1024 * 1024; // 100MB
    const initialChunkData = [];
    let totalLoaded = 0;
    let i = 0;

    while (i < chunks.length && totalLoaded < maxInitialBytes) {
      try {
        const chunkData = await loadSingleChunk(env, chunks[i]);
        initialChunkData.push(new Uint8Array(chunkData));
        totalLoaded += chunkData.byteLength;
        console.log(`⚡ Initial chunk ${i + 1} loaded: ${Math.round(chunkData.byteLength/1024/1024)}MB`);
        i++;
      } catch (chunkError) {
        console.error(`❌ Initial chunk ${i + 1} failed:`, chunkError);
        break;
      }
    }

    if (initialChunkData.length === 0) {
      throw new Error('No initial chunks could be loaded');
    }

    // Stream chunks instead of combining in memory
    const stream = new ReadableStream({
      async pull(controller) {
        for (const chunkData of initialChunkData) {
          controller.enqueue(chunkData);
        }
        controller.close();
      }
    });

    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', totalLoaded.toString());
    headers.set('Content-Range', `bytes 0-${totalLoaded - 1}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=3600');
    headers.set('X-Streaming-Mode', 'instant-play');
    headers.set('X-Initial-Chunks', initialChunkData.length.toString());

    console.log(`⚡ INSTANT PLAY READY: ${Math.round(totalLoaded/1024/1024)}MB buffered`);

    return new Response(stream, { status: 206, headers });

  } catch (error) {
    console.error('⚡ Instant play error:', error);
    return new Response(`Instant play error: ${error.message}`, { status: 500 });
  }
}

async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const size = metadata.size;
  const chunks = metadata.chunks;

  console.log('🎯 SMART RANGE:', rangeHeader);

  const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) {
    return new Response('Invalid range', { status: 416 });
  }

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : Math.min(start + chunkSize * 4 - 1, size - 1);
  
  if (start >= size || end >= size || start > end) {
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

  // Stream chunks to avoid memory issues
  const stream = new ReadableStream({
    async pull(controller) {
      let currentOffset = startChunk * chunkSize;
      for (let i = 0; i < neededChunks.length; i++) {
        const chunkInfo = neededChunks[i];
        try {
          const chunkData = await loadSingleChunk(env, chunkInfo);
          const chunkArray = new Uint8Array(chunkData);
          
          // Trim chunk if necessary
          const chunkStart = i === 0 ? start - currentOffset : 0;
          const chunkEnd = i === neededChunks.length - 1 ? Math.min(chunkArray.length, end - currentOffset + 1) : chunkArray.length;
          
          if (chunkStart < chunkArray.length && chunkEnd > chunkStart) {
            controller.enqueue(chunkArray.slice(chunkStart, chunkEnd));
          }
          
          currentOffset += chunkSize;
        } catch (error) {
          console.error(`❌ Range chunk ${startChunk + i + 1} failed:`, error);
          controller.error(error);
          return;
        }
      }
      controller.close();
    }
  });

  return createRangeResponse(stream, start, end, size, mimeType);
}

async function handleSmartDownload(request, env, metadata, mimeType) {
  console.log('📥 SMART DOWNLOAD: Starting...');

  const chunks = metadata.chunks;
  const filename = metadata.filename;

  try {
    const firstChunk = await loadSingleChunk(env, chunks[0]);
    
    const headers = new Headers();
    headers.set('Content-Type', 'application/octet-stream');
    headers.set('Content-Length', firstChunk.byteLength.toString());
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('X-Download-Mode', 'progressive');
    headers.set('X-Total-Chunks', chunks.length.toString());

    console.log(`📥 Smart download started: ${Math.round(firstChunk.byteLength/1024/1024)}MB`);

    return new Response(firstChunk, { status: 200, headers });

  } catch (error) {
    console.error('📥 Download error:', error);
    return new Response(`Download error: ${error.message}`, { status: 500 });
  }
}

function createRangeResponse(stream, start, end, totalSize, mimeType) {
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', (end - start + 1).toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');

  console.log(`✅ RANGE RESPONSE: ${end - start + 1} bytes delivered`);
  return new Response(stream, { status: 206, headers });
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

async function fetchWithRetry(url, options, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response;
      console.error(`Attempt ${i + 1} failed: ${response.status}`);
    } catch (error) {
      console.error(`Attempt ${i + 1} error:`, error);
    }
    if (i < retries - 1) await new Promise(resolve => setTimeout(resolve, 1000));
  }
  throw new Error(`All retries failed for ${url}`);
}