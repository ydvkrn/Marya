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

  // Handle CORS preflight
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

    // Get metadata
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      console.error('File not found in KV:', actualId);
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    // Validate metadata0
    if (!metadata.filename || !metadata.size || (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0))) {
      console.error('Invalid metadata:', metadata);
      return new Response('Invalid file metadata', { status: 400 });
    }

    const mimeType = MIME_TYPES[extension.toLowerCase()] || 'application/octet-stream';
    console.log(`📁 ${metadata.filename} | Size: ${Math.round(metadata.size/1024/1024)}MB | MIME: ${mimeType} | Chunks: ${metadata.chunks?.length || 0}`);

    // Handle based on file type
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

// Handle single files (Direct proxy - fastest)
async function handleSingleFile(request, env, metadata, mimeType) {
  console.log('🚀 Single file streaming');

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      // Get fresh URL with retry
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

      // Direct proxy
      const telegramResponse = await fetchWithRetry(directUrl, {
        headers: request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {},
        signal: AbortSignal.timeout(45000)
      });

      if (!telegramResponse.ok) {
        console.error(`Telegram file fetch failed: ${telegramResponse.status}`);
        continue;
      }

      // Perfect streaming headers
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

// Handle chunked files (Smart streaming - Netflix style)
async function handleChunkedFile(request, env, metadata, mimeType, extension) {
  const chunks = metadata.chunks;
  const size = metadata.size;
  const filename = metadata.filename;
  const chunkSize = metadata.chunkSize || 20971520; // 20MB default

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

// Instant play strategy (Netflix/YouTube approach)
async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  
  console.log('⚡ INSTANT PLAY: Loading initial chunks...');

  try {
    const initialChunks = Math.min(3, chunks.length);
    const initialChunkData = [];
    let totalLoaded = 0;

    for (let i = 0; i < initialChunks; i++) {
      try {
        const chunkData = await loadSingleChunk(env, chunks[i]);
        initialChunkData.push(new Uint8Array(chunkData));
        totalLoaded += chunkData.byteLength;
        console.log(`⚡ Initial chunk ${i + 1} loaded: ${Math.round(chunkData.byteLength/1024/1024)}MB`);
      } catch (chunkError) {
        console.error(`❌ Initial chunk ${i + 1} failed:`, chunkError);
        break;
      }
    }

    if (initialChunkData.length === 0) {
      throw new Error('No initial chunks could be loaded');
    }

    const combinedBuffer = new Uint8Array(totalLoaded);
    let offset = 0;
    for (const chunkData of initialChunkData) {
      combinedBuffer.set(chunkData, offset);
      offset += chunkData.byteLength;
    }

    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', combinedBuffer.byteLength.toString());
    headers.set('Content-Range', `bytes 0-${combinedBuffer.byteLength - 1}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=3600');
    headers.set('X-Streaming-Mode', 'instant-play');
    headers.set('X-Initial-Chunks', initialChunks.toString());

    console.log(`⚡ INSTANT PLAY READY: ${Math.round(totalLoaded/1024/1024)}MB buffered`);

    return new Response(combinedBuffer, { status: 206, headers });

  } catch (error) {
    console.error('⚡ Instant play error:', error);
    return new Response(`Instant play error: ${error.message}`, { status: 500 });
  }
}

// Smart Range handling (for video seeking)
async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const size = metadata.size;
  const chunks = metadata.chunks;

  console.log('🎯 SMART RANGE:', rangeHeader);

  // Parse range
  const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) {
    return new Response('Invalid range', { status: 416 });
  }

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;
  
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

  if (neededChunks.length > 4) {
    console.log('⚠️ Range too large, serving partial');
    const limitedChunks = neededChunks.slice(0, 4);
    const limitedEndChunk = startChunk + 3;
    const limitedEnd = Math.min(end, (limitedEndChunk + 1) * chunkSize - 1);
    
    const rangeData = await loadRangeChunks(env, limitedChunks, start, limitedEnd, startChunk, chunkSize);
    return createRangeResponse(rangeData, start, limitedEnd, size, mimeType);
  }

  const rangeData = await loadRangeChunks(env, neededChunks, start, end, startChunk, chunkSize);
  return createRangeResponse(rangeData, start, end, size, mimeType);
}

// Smart download (Progressive download like IDM)
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

// Load chunks for range
async function loadRangeChunks(env, chunkInfos, rangeStart, rangeEnd, startChunk, chunkSize) {
  console.log(`🎯 Loading ${chunkInfos.length} range chunks...`);
  
  const parts = [];
  let totalSize = 0;

  for (let i = 0; i < chunkInfos.length; i++) {
    const chunkInfo = chunkInfos[i];
    const chunkIndex = startChunk + i;
    
    try {
      console.log(`🎯 Loading range chunk ${chunkIndex + 1}...`);
      const chunkData = await loadSingleChunk(env, chunkInfo);
      parts.push(new Uint8Array(chunkData));
      totalSize += chunkData.byteLength;
      console.log(`✅ Range chunk ${chunkIndex + 1}: ${Math.round(chunkData.byteLength/1024)}KB`);
    } catch (error) {
      console.error(`❌ Range chunk ${chunkIndex + 1} failed:`, error);
      throw error;
    }
  }

  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.byteLength;
  }

  const rangeStartInBuffer = rangeStart - (startChunk * chunkSize);
  const requestedSize = rangeEnd - rangeStart + 1;
  const exactRange = combined.slice(rangeStartInBuffer, rangeStartInBuffer + requestedSize);

  console.log(`🎯 EXACT RANGE: ${exactRange.byteLength} bytes extracted`);
  return exactRange;
}

// Create Range response
function createRangeResponse(data, start, end, totalSize, mimeType) {
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', data.byteLength.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');

  console.log(`✅ RANGE RESPONSE: ${data.byteLength} bytes delivered`);
  return new Response(data, { status: 206, headers });
}

// Load single chunk with 4-bot fallback + auto-refresh
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

// Fetch with retry logic
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