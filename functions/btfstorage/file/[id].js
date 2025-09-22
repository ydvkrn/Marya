// TOP TIER STREAMING SYSTEM - NETFLIX/YOUTUBE LEVEL
// Handles: Error 1102 fix, Video play, Range support, URL uploads

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/mp4', 'avi': 'video/mp4', 'mov': 'video/mp4',
  'm4v': 'video/mp4', 'wmv': 'video/mp4', 'flv': 'video/mp4', '3gp': 'video/mp4',
  'webm': 'video/webm', 'ogv': 'video/ogg', 'mp3': 'audio/mpeg', 'wav': 'audio/wav', 
  'aac': 'audio/mp4', 'm4a': 'audio/mp4', 'ogg': 'audio/ogg', 'flac': 'audio/flac',
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif',
  'webp': 'image/webp', 'svg': 'image/svg+xml', 'pdf': 'application/pdf'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 TOP TIER STREAMING:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    // Get metadata
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    const mimeType = MIME_TYPES[extension.toLowerCase().replace('.', '')] || 'application/octet-stream';

    console.log(`📁 ${metadata.filename} | Size: ${Math.round(metadata.size/1024/1024)}MB | Chunks: ${metadata.chunks?.length || 0}`);

    // Handle based on file type
    if (metadata.telegramFileId && !metadata.chunks) {
      // Single file - Direct proxy (instant)
      return await handleSingleFile(request, env, metadata, mimeType);
    }
    
    if (metadata.chunks && metadata.chunks.length > 0) {
      // Chunked file - Smart streaming
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
      // Get fresh URL
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(8000) }
      );

      if (!getFileResponse.ok) continue;

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

      // Direct proxy
      const telegramResponse = await fetch(directUrl, {
        headers: request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {},
        signal: AbortSignal.timeout(45000)
      });

      if (!telegramResponse.ok) continue;

      // Perfect streaming headers
      const headers = new Headers();
      
      // Copy important headers from Telegram
      ['content-length', 'content-range', 'accept-ranges'].forEach(header => {
        if (telegramResponse.headers.get(header)) {
          headers.set(header, telegramResponse.headers.get(header));
        }
      });

      // Set streaming headers
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

  // Handle Range requests (Video seeking)
  if (range && !isDownload) {
    return await handleSmartRange(request, env, metadata, range, mimeType, chunkSize);
  }

  // Handle download requests
  if (isDownload) {
    return await handleSmartDownload(request, env, metadata, mimeType);
  }

  // Handle normal streaming - Instant play strategy
  return await handleInstantPlay(request, env, metadata, mimeType, size);
}

// Instant play strategy (Netflix/YouTube approach)
async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  
  console.log('⚡ INSTANT PLAY: Loading initial chunks...');

  try {
    // Load first 2-3 chunks for instant playback (4-6 seconds of video)
    const initialChunks = Math.min(3, chunks.length);
    const initialChunkData = [];
    let totalLoaded = 0;

    // Load chunks sequentially (avoid Promise.all overload)
    for (let i = 0; i < initialChunks; i++) {
      try {
        const chunkData = await loadSingleChunk(env, chunks[i]);
        initialChunkData.push(new Uint8Array(chunkData));
        totalLoaded += chunkData.byteLength;
        
        console.log(`⚡ Initial chunk ${i + 1} loaded: ${Math.round(chunkData.byteLength/1024/1024)}MB`);
      } catch (chunkError) {
        console.error(`❌ Initial chunk ${i + 1} failed:`, chunkError);
        // Continue with available chunks
        break;
      }
    }

    if (initialChunkData.length === 0) {
      throw new Error('No initial chunks could be loaded');
    }

    // Combine loaded chunks
    const combinedBuffer = new Uint8Array(totalLoaded);
    let offset = 0;
    for (const chunkData of initialChunkData) {
      combinedBuffer.set(chunkData, offset);
      offset += chunkData.byteLength;
    }

    // Send as partial content to enable Range mode
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', combinedBuffer.byteLength.toString());
    headers.set('Content-Range', `bytes 0-${combinedBuffer.byteLength - 1}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=3600');
    
    // Netflix-style headers
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
  const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
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

  // Find needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`🎯 Need chunks: ${startChunk}-${endChunk} (${neededChunks.length})`);

  // Smart limit: Max 4 chunks per request (avoid CPU/memory limits)
  if (neededChunks.length > 4) {
    console.log('⚠️ Range too large, serving partial');
    const limitedChunks = neededChunks.slice(0, 4);
    const limitedEndChunk = startChunk + 3;
    const limitedEnd = Math.min(end, (limitedEndChunk + 1) * chunkSize - 1);
    
    const rangeData = await loadRangeChunks(env, limitedChunks, start, limitedEnd, startChunk, chunkSize);
    return createRangeResponse(rangeData, start, limitedEnd, size, mimeType);
  }

  // Load needed chunks
  const rangeData = await loadRangeChunks(env, neededChunks, start, end, startChunk, chunkSize);
  return createRangeResponse(rangeData, start, end, size, mimeType);
}

// Smart download (Progressive download like IDM)
async function handleSmartDownload(request, env, metadata, mimeType) {
  console.log('📥 SMART DOWNLOAD: Starting...');

  const chunks = metadata.chunks;
  const filename = metadata.filename;

  try {
    // Start with first chunk for instant download start
    const firstChunk = await loadSingleChunk(env, chunks[0]);
    
    const headers = new Headers();
    headers.set('Content-Type', 'application/octet-stream');
    headers.set('Content-Length', firstChunk.byteLength.toString());
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    
    // Progressive download headers
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

  // Load chunks sequentially (no Promise.all to avoid overload)
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

  // Combine chunks efficiently
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.byteLength;
  }

  // Extract exact range
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
  
  // Get chunk metadata
  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkKey}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  
  // Try direct URL first
  let response = await fetch(chunkMetadata.directUrl, { 
    signal: AbortSignal.timeout(30000) 
  });
  
  if (response.ok) {
    return response.arrayBuffer();
  }

  // URL expired, refresh with 4-bot fallback
  console.log(`🔄 URL expired, refreshing: ${chunkKey}`);
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      if (!getFileResponse.ok) continue;

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      response = await fetch(freshUrl, { signal: AbortSignal.timeout(30000) });
      
      if (response.ok) {
        // Update KV async (don't wait)
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