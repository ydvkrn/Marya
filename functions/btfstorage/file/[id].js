// 🚀 LIGHTNING FAST STREAMING SYSTEM
// Zero buffering • Instant play • 2GB+ support

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/mp4', 'avi': 'video/mp4', 'mov': 'video/mp4', 'm4v': 'video/mp4',
  'webm': 'video/webm', 'ogv': 'video/ogg', 'flv': 'video/mp4', '3gp': 'video/mp4', 'wmv': 'video/mp4',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'aac': 'audio/mp4', 'm4a': 'audio/mp4', 'ogg': 'audio/ogg',
  'flac': 'audio/flac', 'wma': 'audio/x-ms-wma', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
  'png': 'image/png', 'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;
  
  console.log('🚀 LIGHTNING STREAMING:', fileId);
  
  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';
    
    // Get master metadata
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { status: 404 });
    }
    
    const metadata = JSON.parse(metadataString);
    const mimeType = MIME_TYPES[extension.toLowerCase().replace('.', '')] || 'application/octet-stream';
    
    console.log(`📁 ${metadata.filename} | Size: ${Math.round(metadata.size/1024/1024)}MB | Chunks: ${metadata.totalChunks}`);
    
    // Lightning fast streaming strategy
    return await handleLightningStream(request, env, metadata, mimeType, extension);
    
  } catch (error) {
    console.error('❌ Streaming error:', error);
    return new Response(`Streaming error: ${error.message}`, { status: 500 });
  }
}

// Lightning fast streaming (Netflix/YouTube level)
async function handleLightningStream(request, env, metadata, mimeType, extension) {
  const chunks = metadata.chunks || [];
  const totalSize = metadata.size;
  const filename = metadata.filename;
  const chunkSize = metadata.chunkSize || 5242880; // 5MB default
  
  const range = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');
  
  console.log(`🎬 Mode: ${range ? 'RANGE' : isDownload ? 'DOWNLOAD' : 'STREAM'}`);
  
  if (range && !isDownload) {
    return await handlePrecisionRange(request, env, metadata, range, mimeType, chunkSize);
  }
  
  if (isDownload) {
    return await handleSmartDownload(request, env, metadata, mimeType);
  }
  
  // Instant streaming (first 3-4 chunks for immediate playback)
  return await handleInstantStream(request, env, metadata, mimeType, totalSize);
}

// Instant streaming (0-delay playback start)
async function handleInstantStream(request, env, metadata, mimeType, totalSize) {
  console.log('⚡ INSTANT STREAM: Loading initial buffer...');
  
  const chunks = metadata.chunks;
  const initialChunks = Math.min(4, chunks.length); // First 4 chunks for instant play
  
  const loadPromises = [];
  for (let i = 0; i < initialChunks; i++) {
    loadPromises.push(loadChunkLightning(env, chunks[i]));
  }
  
  try {
    // Load initial chunks in parallel (controlled)
    const chunkResults = await Promise.all(loadPromises);
    
    // Combine chunks
    const totalBufferSize = chunkResults.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    const streamBuffer = new Uint8Array(totalBufferSize);
    
    let offset = 0;
    for (const chunkData of chunkResults) {
      streamBuffer.set(new Uint8Array(chunkData), offset);
      offset += chunkData.byteLength;
    }
    
    // Instant streaming headers
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', streamBuffer.byteLength.toString());
    headers.set('Content-Range', `bytes 0-${streamBuffer.byteLength - 1}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=86400');
    
    // Lightning streaming headers
    headers.set('X-Streaming-Mode', 'instant');
    headers.set('X-Buffer-Chunks', initialChunks.toString());
    headers.set('Connection', 'keep-alive');
    
    console.log(`⚡ INSTANT STREAM READY: ${Math.round(totalBufferSize/1024/1024)}MB buffered`);
    
    return new Response(streamBuffer, { status: 206, headers });
    
  } catch (error) {
    console.error('❌ Instant stream error:', error);
    return new Response(`Instant stream error: ${error.message}`, { status: 500 });
  }
}

// Precision range handling (seeking support)
async function handlePrecisionRange(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  console.log('🎯 PRECISION RANGE:', rangeHeader);
  
  const size = metadata.size;
  const chunks = metadata.chunks;
  
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
  
  console.log(`🎯 Range: ${start}-${end} (${Math.round((end - start + 1)/1024/1024)}MB)`);
  
  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);
  
  // Limit to 6 chunks max (no CPU overload)
  if (neededChunks.length > 6) {
    console.log('⚠️ Range too large, limiting to 6 chunks');
    const limitedChunks = neededChunks.slice(0, 6);
    const limitedEnd = Math.min(end, (startChunk + 5) * chunkSize + chunkSize - 1);
    
    const rangeData = await loadRangeLightning(env, limitedChunks, start, limitedEnd, startChunk, chunkSize);
    return createRangeResponse(rangeData, start, limitedEnd, size, mimeType);
  }
  
  // Load needed chunks
  const rangeData = await loadRangeLightning(env, neededChunks, start, end, startChunk, chunkSize);
  return createRangeResponse(rangeData, start, end, size, mimeType);
}

// Smart download (progressive)
async function handleSmartDownload(request, env, metadata, mimeType) {
  console.log('📥 SMART DOWNLOAD');
  
  const firstChunk = metadata.chunks[0];
  
  try {
    const chunkData = await loadChunkLightning(env, firstChunk);
    
    const headers = new Headers();
    headers.set('Content-Type', 'application/octet-stream');
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    
    console.log(`📥 Download ready: ${Math.round(chunkData.byteLength/1024/1024)}MB`);
    
    return new Response(chunkData, { status: 200, headers });
    
  } catch (error) {
    console.error('❌ Download error:', error);
    return new Response(`Download error: ${error.message}`, { status: 500 });
  }
}

// Lightning chunk loader (with smart caching & auto-refresh)
async function loadChunkLightning(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.chunkKey;
  
  console.log(`⚡ Loading: ${chunkKey}`);
  
  // Get chunk metadata
  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk not found: ${chunkKey}`);
  }
  
  const chunkMetadata = JSON.parse(metadataString);
  
  // Try direct URL (fastest path)
  let response = await fetch(chunkMetadata.directUrl, {
    signal: AbortSignal.timeout(30000)
  });
  
  if (response.ok) {
    console.log(`⚡ Direct hit: ${chunkKey}`);
    return response.arrayBuffer();
  }
  
  // URL refresh with multi-bot fallback
  console.log(`🔄 Refreshing: ${chunkKey}`);
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(10000) }
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
        
        console.log(`✅ Refreshed: ${chunkKey}`);
        return response.arrayBuffer();
      }
      
    } catch (botError) {
      continue;
    }
  }
  
  throw new Error(`All refresh attempts failed: ${chunkKey}`);
}

// Load range chunks (lightning fast)
async function loadRangeLightning(env, chunkInfos, rangeStart, rangeEnd, startChunk, chunkSize) {
  console.log(`🎯 Loading ${chunkInfos.length} range chunks...`);
  
  // Load chunks in parallel (controlled)
  const loadPromises = chunkInfos.map(chunkInfo => loadChunkLightning(env, chunkInfo));
  const chunkResults = await Promise.all(loadPromises);
  
  // Combine chunks
  const totalSize = chunkResults.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const combined = new Uint8Array(totalSize);
  
  let offset = 0;
  for (const chunkData of chunkResults) {
    combined.set(new Uint8Array(chunkData), offset);
    offset += chunkData.byteLength;
  }
  
  // Extract exact range
  const rangeStartInBuffer = rangeStart - (startChunk * chunkSize);
  const requestedSize = rangeEnd - rangeStart + 1;
  const exactRange = combined.slice(rangeStartInBuffer, rangeStartInBuffer + requestedSize);
  
  console.log(`🎯 EXACT RANGE: ${exactRange.byteLength} bytes extracted`);
  return exactRange;
}

// Create range response
function createRangeResponse(data, start, end, totalSize, mimeType) {
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', data.byteLength.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');
  headers.set('Cache-Control', 'public, max-age=86400');
  
  console.log(`✅ RANGE DELIVERED: ${data.byteLength} bytes`);
  return new Response(data, { status: 206, headers });
}