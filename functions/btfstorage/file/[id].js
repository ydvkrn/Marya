// ULTIMATE SIMPLE - NO CPU/MEMORY LIMITS
// Direct Telegram URL serve (like a CDN redirect)

const MIME_TYPES = {
  'mp4': 'video/mp4',
  'mkv': 'video/mp4', 
  'avi': 'video/mp4',
  'mov': 'video/mp4',
  'm4v': 'video/mp4',
  'wmv': 'video/mp4',
  'flv': 'video/mp4',
  '3gp': 'video/mp4',
  'webm': 'video/webm',
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'aac': 'audio/mp4',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🔥 ULTIMATE SIMPLE:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    if (!actualId.startsWith('MSM')) {
      return new Response('Not found', { status: 404 });
    }

    // Get metadata
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    const mimeType = MIME_TYPES[extension.toLowerCase().replace('.', '')] || 'application/octet-stream';

    console.log(`File: ${metadata.filename} (${Math.round(metadata.size/1024/1024)}MB)`);

    // Check if it's a single Telegram file (best case)
    if (metadata.telegramFileId && !metadata.chunks) {
      console.log('🚀 Single file - Direct serve');
      return await serveSingleTelegramFile(request, env, metadata.telegramFileId, mimeType, metadata.filename);
    }

    // Handle chunked files with streaming
    if (metadata.chunks && metadata.chunks.length > 0) {
      console.log(`📦 Chunked file: ${metadata.chunks.length} chunks`);
      return await serveChunkedFile(request, env, metadata, mimeType);
    }

    return new Response('Invalid file format', { status: 400 });

  } catch (error) {
    console.error('Error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

// Single Telegram file - ZERO CPU usage (direct proxy)
async function serveSingleTelegramFile(request, env, telegramFileId, mimeType, filename) {
  console.log('🚀 Serving single Telegram file directly');
  
  const botToken = env.BOT_TOKEN || env.BOT_TOKEN2 || env.BOT_TOKEN3 || env.BOT_TOKEN4;
  if (!botToken) {
    return new Response('No bot token available', { status: 500 });
  }

  try {
    // Get fresh Telegram URL
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
      { signal: AbortSignal.timeout(10000) }
    );

    if (!getFileResponse.ok) {
      throw new Error(`Telegram API error: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Invalid Telegram response');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
    
    // Option 1: Direct redirect (ZERO CPU usage)
    const url = new URL(request.url);
    if (!url.searchParams.has('proxy')) {
      console.log('↗️ Direct redirect to Telegram');
      return new Response(null, {
        status: 302,
        headers: {
          'Location': directUrl,
          'Cache-Control': 'no-cache'
        }
      });
    }

    // Option 2: Proxy with headers (minimal CPU)
    console.log('🔄 Proxying with proper headers');
    const telegramResponse = await fetch(directUrl, {
      headers: request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {}
    });

    const headers = new Headers(telegramResponse.headers);
    headers.set('Content-Type', mimeType);
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    
    const url2 = new URL(request.url);
    if (url2.searchParams.has('dl')) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      headers.set('Content-Disposition', 'inline');
    }

    return new Response(telegramResponse.body, {
      status: telegramResponse.status,
      headers: headers
    });

  } catch (error) {
    console.error('Single file serve error:', error);
    return new Response(`File serve error: ${error.message}`, { status: 500 });
  }
}

// Chunked files - Smart streaming
async function serveChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const size = metadata.size;
  const filename = metadata.filename;

  console.log(`📦 Serving chunked file: ${chunks.length} chunks`);

  // Handle Range requests (for video seeking)
  const range = request.headers.get('Range');
  if (range) {
    return await handleChunkedRange(request, env, metadata, range, mimeType);
  }

  // For non-range requests, send first chunk only to force Range mode
  console.log('🎯 Sending first chunk to force Range mode');
  return await serveFirstChunkOnly(request, env, metadata, mimeType);
}

// Serve only first chunk to force browser into Range mode
async function serveFirstChunkOnly(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const size = metadata.size;
  const filename = metadata.filename;
  
  if (chunks.length === 0) {
    return new Response('No chunks available', { status: 404 });
  }

  const firstChunk = chunks[0];
  const kvNamespace = env[firstChunk.kvNamespace] || env.FILES_KV;

  try {
    console.log('📦 Loading first chunk only...');
    const chunkData = await loadSingleChunk(kvNamespace, firstChunk, env);
    
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Content-Range', `bytes 0-${chunkData.byteLength - 1}/${size}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    
    const url = new URL(request.url);
    if (url.searchParams.has('dl')) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    } else {
      headers.set('Content-Disposition', 'inline');
    }

    console.log(`✅ First chunk served: ${Math.round(chunkData.byteLength/1024)}KB`);
    return new Response(chunkData, { status: 206, headers });

  } catch (error) {
    console.error('First chunk error:', error);
    return new Response(`First chunk error: ${error.message}`, { status: 500 });
  }
}

// Handle Range for chunked files (load only needed chunks)
async function handleChunkedRange(request, env, metadata, rangeHeader, mimeType) {
  console.log('📺 Chunked Range request:', rangeHeader);
  
  const size = metadata.size;
  const chunks = metadata.chunks;
  const chunkSize = metadata.chunkSize || Math.ceil(size / chunks.length);

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
  console.log(`📺 Range: ${start}-${end} (${Math.round(requestedSize/1024)}KB)`);

  // Find needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Need chunks: ${startChunk}-${endChunk} (${neededChunks.length})`);

  // Limit to maximum 5 chunks to avoid CPU limits
  if (neededChunks.length > 5) {
    console.log('⚠️ Too many chunks, reducing range');
    const limitedChunks = neededChunks.slice(0, 5);
    const limitedEndChunk = startChunk + 4;
    const limitedEnd = Math.min(end, (limitedEndChunk + 1) * chunkSize - 1);
    
    const chunkData = await loadAndCombineChunks(env, limitedChunks, startChunk);
    const rangeData = extractRange(chunkData, start, limitedEnd, startChunk, chunkSize);

    return createRangeResponse(rangeData, start, limitedEnd, size, mimeType);
  }

  // Load needed chunks
  const chunkData = await loadAndCombineChunks(env, neededChunks, startChunk);
  const rangeData = extractRange(chunkData, start, end, startChunk, chunkSize);

  return createRangeResponse(rangeData, start, end, size, mimeType);
}

// Load and combine chunks efficiently
async function loadAndCombineChunks(env, chunkInfos, startIndex) {
  console.log(`📥 Loading ${chunkInfos.length} chunks...`);
  
  const parts = [];
  let totalSize = 0;

  // Load chunks sequentially to avoid overload
  for (let i = 0; i < chunkInfos.length; i++) {
    const chunkInfo = chunkInfos[i];
    const actualIndex = startIndex + i;
    const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
    
    try {
      const data = await loadSingleChunk(kvNamespace, chunkInfo, env);
      parts.push(new Uint8Array(data));
      totalSize += data.byteLength;
      
      console.log(`✅ Chunk ${actualIndex} loaded: ${Math.round(data.byteLength/1024)}KB`);
    } catch (err) {
      console.error(`❌ Chunk ${actualIndex} failed:`, err);
      throw new Error(`Chunk ${actualIndex} load failed`);
    }
  }

  // Combine efficiently
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.byteLength;
  }

  console.log(`🔗 Combined: ${Math.round(totalSize/1024)}KB`);
  return combined;
}

// Extract exact range from combined data
function extractRange(combinedData, start, end, startChunk, chunkSize) {
  const rangeStartInData = start - (startChunk * chunkSize);
  const requestedSize = end - start + 1;
  
  return combinedData.slice(rangeStartInData, rangeStartInData + requestedSize);
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

  console.log(`✅ Range response: ${data.byteLength} bytes`);
  return new Response(data, { status: 206, headers });
}

// Load single chunk with refresh
async function loadSingleChunk(kvNamespace, chunkInfo, env) {
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;
  
  console.log(`📥 Loading: ${chunkKey}`);
  
  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkKey}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  
  // Try direct URL
  let response = await fetch(chunkMetadata.directUrl, { signal: AbortSignal.timeout(30000) });
  
  if (response.ok) {
    return response.arrayBuffer();
  }

  // Refresh URL
  console.log(`🔄 Refreshing URL for: ${chunkKey}`);
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  if (botTokens.length === 0) {
    throw new Error('No bot tokens available');
  }

  const botToken = botTokens[0]; // Use first available
  
  try {
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
      { signal: AbortSignal.timeout(15000) }
    );

    const getFileData = await getFileResponse.json();
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Telegram getFile failed');
    }

    const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
    response = await fetch(freshUrl, { signal: AbortSignal.timeout(30000) });
    
    if (!response.ok) {
      throw new Error(`Fresh URL failed: ${response.status}`);
    }

    // Update KV async
    kvNamespace.put(chunkKey, JSON.stringify({
      ...chunkMetadata,
      directUrl: freshUrl,
      refreshed: Date.now()
    })).catch(() => {});

    return response.arrayBuffer();

  } catch (refreshError) {
    throw new Error(`URL refresh failed: ${refreshError.message}`);
  }
}
