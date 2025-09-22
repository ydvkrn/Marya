// WORKING FILE SERVER - NO ERROR 1102
// Simple strategy: First chunk only for instant play

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/mp4', 'avi': 'video/mp4', 'mov': 'video/mp4',
  'm4v': 'video/mp4', 'wmv': 'video/mp4', 'flv': 'video/mp4', '3gp': 'video/mp4',
  'webm': 'video/webm', 'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'aac': 'audio/mp4',
  'm4a': 'audio/mp4', 'ogg': 'audio/ogg', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
  'png': 'image/png', 'gif': 'image/gif', 'pdf': 'application/pdf'
};

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('📁 WORKING FILE SERVE:', fileId);

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

    console.log(`File: ${metadata.filename} (${metadata.chunks?.length || 0} chunks)`);

    // Handle chunked files with SIMPLE strategy
    if (metadata.chunks && metadata.chunks.length > 0) {
      return await serveChunkedFile(request, env, metadata, mimeType);
    }

    return new Response('Invalid file format', { status: 400 });

  } catch (error) {
    console.error('Serve error:', error);
    return new Response(`Serve error: ${error.message}`, { status: 500 });
  }
}

// Serve chunked file with SIMPLE strategy (No 1102 error)
async function serveChunkedFile(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const size = metadata.size;
  const filename = metadata.filename;
  const chunkSize = metadata.chunkSize || 20971520; // 20MB default

  const range = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');

  console.log(`Serving: ${filename} | Range: ${range} | Download: ${isDownload}`);

  // Handle Range requests (for video seeking)
  if (range && !isDownload) {
    return await handleRangeRequest(request, env, metadata, range, mimeType, chunkSize);
  }

  // Handle download requests - serve first chunk with proper headers
  if (isDownload) {
    return await handleDownload(request, env, metadata, mimeType);
  }

  // Handle normal requests - serve first chunk for instant play
  return await serveFirstChunk(request, env, metadata, mimeType, size);
}

// Serve first chunk only (instant play, no memory issues)
async function serveFirstChunk(request, env, metadata, mimeType, totalSize) {
  const firstChunk = metadata.chunks[0];
  
  console.log('⚡ Serving first chunk for instant play');

  try {
    const chunkData = await loadSingleChunk(env, firstChunk);
    
    // Serve as partial content to force video players into range mode
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Content-Range', `bytes 0-${chunkData.byteLength - 1}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=3600');

    console.log(`✅ First chunk served: ${Math.round(chunkData.byteLength/1024/1024)}MB`);
    
    return new Response(chunkData, { status: 206, headers });

  } catch (error) {
    console.error('First chunk error:', error);
    return new Response(`First chunk error: ${error.message}`, { status: 500 });
  }
}

// Handle Range requests (for video seeking) - Load max 3 chunks
async function handleRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const size = metadata.size;
  const chunks = metadata.chunks;

  console.log('📺 Range request:', rangeHeader);

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

  console.log(`Range: ${start}-${end} (${Math.round((end - start + 1)/1024)}KB)`);

  // Find needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`Need chunks: ${startChunk}-${endChunk} (${neededChunks.length})`);

  // LIMIT: Max 3 chunks to avoid CPU limits
  if (neededChunks.length > 3) {
    console.log('⚠️ Too many chunks, limiting to 3');
    const limitedChunks = neededChunks.slice(0, 3);
    const limitedEndChunk = startChunk + 2;
    const limitedEnd = Math.min(end, (limitedEndChunk + 1) * chunkSize - 1);
    
    const rangeData = await loadAndCombineChunks(env, limitedChunks, start, limitedEnd, startChunk, chunkSize);
    return createRangeResponse(rangeData, start, limitedEnd, size, mimeType);
  }

  // Load needed chunks
  const rangeData = await loadAndCombineChunks(env, neededChunks, start, end, startChunk, chunkSize);
  return createRangeResponse(rangeData, start, end, size, mimeType);
}

// Handle download - serve first chunk with download headers
async function handleDownload(request, env, metadata, mimeType) {
  const firstChunk = metadata.chunks[0];
  
  console.log('📥 Download request - serving first chunk');

  try {
    const chunkData = await loadSingleChunk(env, firstChunk);
    
    const headers = new Headers();
    headers.set('Content-Type', 'application/octet-stream');
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
    headers.set('Access-Control-Allow-Origin', '*');

    console.log(`📥 Download chunk served: ${Math.round(chunkData.byteLength/1024/1024)}MB`);
    
    return new Response(chunkData, { status: 200, headers });

  } catch (error) {
    console.error('Download error:', error);
    return new Response(`Download error: ${error.message}`, { status: 500 });
  }
}

// Load and combine chunks (max 3 chunks)
async function loadAndCombineChunks(env, chunkInfos, rangeStart, rangeEnd, startChunk, chunkSize) {
  console.log(`Loading ${chunkInfos.length} chunks...`);
  
  const parts = [];
  let totalSize = 0;

  // Load chunks sequentially
  for (let i = 0; i < chunkInfos.length; i++) {
    const chunkInfo = chunkInfos[i];
    
    try {
      const chunkData = await loadSingleChunk(env, chunkInfo);
      parts.push(new Uint8Array(chunkData));
      totalSize += chunkData.byteLength;
      
      console.log(`✅ Chunk ${i} loaded: ${Math.round(chunkData.byteLength/1024)}KB`);
    } catch (err) {
      console.error(`❌ Chunk ${i} failed:`, err);
      throw err;
    }
  }

  // Combine chunks
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

  console.log(`Range extracted: ${exactRange.byteLength} bytes`);
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

  console.log(`✅ Range response: ${data.byteLength} bytes`);
  return new Response(data, { status: 206, headers });
}

// Load single chunk with refresh
async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName;
  
  console.log(`Loading chunk: ${chunkKey}`);
  
  // Get chunk metadata
  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkKey}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  
  // Try direct URL first
  let response = await fetch(chunkMetadata.directUrl, { signal: AbortSignal.timeout(30000) });
  
  if (response.ok) {
    return response.arrayBuffer();
  }

  // URL expired, refresh it
  console.log(`🔄 Refreshing URL for: ${chunkKey}`);
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      response = await fetch(freshUrl, { signal: AbortSignal.timeout(30000) });
      
      if (response.ok) {
        // Update KV async
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

  throw new Error(`All refresh attempts failed for: ${chunkKey}`);
}