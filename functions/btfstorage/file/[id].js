// functions/btfstorage/files/[id].js
// 🚀 WORLD-CLASS Cloudflare Pages Functions - Ultra-Optimized Streaming Handler
// URL: marya-hosting.pages.dev/btfstorage/files/MSM221-48U91C62-no.mp4

const MIME_TYPES = {
  // Video formats
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
  
  // Audio formats
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'aac': 'audio/mp4',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg',
  'flac': 'audio/flac',
  'wma': 'audio/x-ms-wma',
  
  // Image formats
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp',
  'svg': 'image/svg+xml',
  'bmp': 'image/bmp',
  'tiff': 'image/tiff',
  
  // Document formats
  'pdf': 'application/pdf',
  'doc': 'application/msword',
  'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'txt': 'text/plain',
  'zip': 'application/zip',
  'rar': 'application/x-rar-compressed',
  
  // Streaming formats
  'm3u8': 'application/x-mpegURL',
  'ts': 'video/mp2t',
  'mpd': 'application/dash+xml'
};

// 🔥 PERFORMANCE OPTIMIZATION CONSTANTS
const MAX_PARALLEL_CHUNKS = 3; // Parallel chunk fetching
const INSTANT_PLAY_SIZE = 30 * 1024 * 1024; // 30MB for instant playback
const CACHE_TTL_LONG = 31536000; // 1 year for immutable content
const CACHE_TTL_SHORT = 300; // 5 minutes for dynamic content
const FETCH_TIMEOUT = 25000; // 25 seconds timeout
const MAX_RETRIES = 3; // Maximum fetch retries

/**
 * 🎯 Main request handler for Cloudflare Pages Functions
 * Ultra-optimized for parallel streaming and caching
 */
export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;
  
  console.log('🚀 WORLD-CLASS STREAMING STARTED:', fileId);
  console.log('📍 Request URL:', request.url);
  console.log('🔗 Method:', request.method);
  console.log('📊 User-Agent:', request.headers.get('User-Agent') || 'Unknown');
  
  // Handle CORS preflight requests
  if (request.method === 'OPTIONS') {
    return new Response(null, { 
      status: 204, 
      headers: createCorsHeaders() 
    });
  }
  
  try {
    // Parse file ID and extract components
    let actualId = fileId;
    let extension = '';
    let isHlsPlaylist = false;
    let isHlsSegment = false;
    let segmentIndex = -1;
    
    // Handle file extensions and special formats
    if (fileId.includes('.')) {
      const parts = fileId.split('.');
      extension = parts.pop().toLowerCase();
      actualId = parts.join('.');
      
      // HLS Playlist (.m3u8)
      if (extension === 'm3u8') {
        isHlsPlaylist = true;
        console.log('📼 HLS Playlist requested:', actualId);
      }
      // HLS Segment (.ts with index)
      else if (extension === 'ts' && actualId.includes('-')) {
        const segParts = actualId.split('-');
        const lastPart = segParts[segParts.length - 1];
        
        if (!isNaN(parseInt(lastPart))) {
          segmentIndex = parseInt(segParts.pop(), 10);
          actualId = segParts.join('-');
          isHlsSegment = true;
          console.log('📼 HLS Segment requested:', actualId, 'Index:', segmentIndex);
        }
      }
      // Regular file with extension
      else {
        actualId = fileId.substring(0, fileId.lastIndexOf('.'));
        extension = fileId.substring(fileId.lastIndexOf('.') + 1).toLowerCase();
        console.log('📄 Regular file requested:', actualId, 'Extension:', extension);
      }
    }
    
    // Fetch metadata from KV storage
    console.log('🔍 Fetching metadata for:', actualId);
    const metadataString = await env.FILES_KV.get(actualId);
    
    if (!metadataString) {
      console.error('❌ File not found in KV storage:', actualId);
      return createErrorResponse('File not found', 404);
    }
    
    const metadata = JSON.parse(metadataString);
    
    // Validate metadata structure
    if (!metadata.filename || !metadata.size) {
      console.error('❌ Invalid metadata structure:', metadata);
      return createErrorResponse('Invalid file metadata', 400);
    }
    
    // Handle backward compatibility for field names
    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;
    
    // Validate file source (either single file or chunks)
    if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      console.error('❌ No telegramFileId or chunks in metadata:', actualId);
      return createErrorResponse('Missing file source data', 400);
    }
    
    // Determine MIME type
    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';
    
    // Log file information
    console.log(`📦 File Info:
📝 Name: ${metadata.filename}
📏 Size: ${Math.round(metadata.size/1024/1024)}MB (${metadata.size} bytes)
🏷️ MIME: ${mimeType}
🧩 Chunks: ${metadata.chunks?.length || 0}
📅 Uploaded: ${metadata.uploadedAt || 'N/A'}
🎵 HLS Playlist: ${isHlsPlaylist}
📼 HLS Segment: ${isHlsSegment} (Index: ${segmentIndex})
🔗 Has Telegram ID: ${!!metadata.telegramFileId}`);
    
    // Route to appropriate handler based on request type
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
    
    return createErrorResponse('Invalid file format or configuration', 400);
    
  } catch (error) {
    console.error('❌ Critical streaming error:', error);
    console.error('📍 Error stack:', error.stack);
    return createErrorResponse(`Streaming error: ${error.message}`, 500);
  }
}

/**
 * 🎨 Create CORS headers
 */
function createCorsHeaders() {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
  headers.set('Access-Control-Max-Age', '86400');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
  return headers;
}

/**
 * 📼 Handle HLS playlist generation (.m3u8)
 * Generates dynamic playlist from chunked file segments
 */
async function handleHlsPlaylist(request, env, metadata, actualId) {
  console.log('📼 Generating HLS playlist for:', actualId);
  
  if (!metadata.chunks || metadata.chunks.length === 0) {
    console.error('❌ HLS playlist requested for non-chunked file');
    return createErrorResponse('HLS not supported for single files', 400);
  }
  
  const chunks = metadata.chunks;
  const segmentDuration = 6; // seconds per segment
  const baseUrl = new URL(request.url).origin;
  
  // Generate M3U8 playlist content
  let playlist = '#EXTM3U
';
  playlist += '#EXT-X-VERSION:3
';
  playlist += `#EXT-X-TARGETDURATION:${segmentDuration}
`;
  playlist += '#EXT-X-MEDIA-SEQUENCE:0
';
  playlist += '#EXT-X-PLAYLIST-TYPE:VOD
';
  
  // Add each chunk as a segment
  for (let i = 0; i < chunks.length; i++) {
    playlist += `#EXTINF:${segmentDuration.toFixed(1)},
`;
    playlist += `${baseUrl}/btfstorage/files/${actualId}-${i}.ts
`;
  }
  
  playlist += '#EXT-X-ENDLIST
';
  
  const headers = new Headers();
  headers.set('Content-Type', 'application/x-mpegURL');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  headers.set('Pragma', 'no-cache');
  headers.set('Expires', '0');
  
  console.log(`📼 HLS playlist generated successfully:
🧩 Segments: ${chunks.length}
⏱️ Duration per segment: ${segmentDuration}s
📊 Total estimated duration: ${Math.round(chunks.length * segmentDuration / 60)}min`);
  
  return new Response(playlist, { status: 200, headers });
}

/**
 * 📼 Handle HLS segment serving (.ts)
 * Serves individual video segments for HLS playback
 */
async function handleHlsSegment(request, env, metadata, segmentIndex) {
  console.log('📼 Serving HLS segment:', segmentIndex);
  
  // Validate segment index
  if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
    console.error('❌ Invalid segment index:', segmentIndex, 'Available:', metadata.chunks?.length);
    return createErrorResponse('Segment not found', 404);
  }
  
  try {
    const chunkInfo = metadata.chunks[segmentIndex];
    console.log('📥 Loading segment chunk:', chunkInfo.keyName || chunkInfo.chunkKey);
    
    const chunkData = await loadSingleChunk(env, chunkInfo);
    
    const headers = new Headers();
    headers.set('Content-Type', 'video/mp2t');
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', `public, max-age=${CACHE_TTL_LONG}, immutable`);
    headers.set('Content-Disposition', 'inline');
    headers.set('Accept-Ranges', 'bytes');
    
    console.log(`📼 HLS segment served successfully:
📍 Index: ${segmentIndex}
📏 Size: ${Math.round(chunkData.byteLength/1024/1024)}MB
🏷️ Type: video/mp2t`);
    
    return new Response(chunkData, { status: 200, headers });
    
  } catch (error) {
    console.error('❌ HLS segment error:', error);
    return createErrorResponse(`Segment loading failed: ${error.message}`, 500);
  }
}

/**
 * 🚀 Handle single file streaming
 * Direct streaming from Telegram with range request support
 */
async function handleSingleFile(request, env, metadata, mimeType) {
  console.log('🚀 Single file streaming initiated');
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  if (botTokens.length === 0) {
    console.error('❌ No bot tokens configured');
    return createErrorResponse('Service configuration error', 503);
  }
  
  console.log(`🤖 Available bot tokens: ${botTokens.length}`);
  
  // Try each bot token until one works
  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];
    console.log(`🤖 Trying bot ${botIndex + 1}/${botTokens.length}`);
    
    try {
      // Get file information from Telegram
      const getFileResponse = await fetchWithRetry(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );
      
      const getFileData = await getFileResponse.json();
      
      if (!getFileData.ok || !getFileData.result?.file_path) {
        console.error(`🤖 Bot ${botIndex + 1} API error: ${getFileData.error_code} - ${getFileData.description}`);
        continue;
      }
      
      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      console.log('📡 Telegram direct URL obtained');
      
      // Prepare headers for range request if needed
      const requestHeaders = {};
      const rangeHeader = request.headers.get('Range');
      
      if (rangeHeader) {
        requestHeaders['Range'] = rangeHeader;
        console.log('🎯 Range request:', rangeHeader);
      }
      
      // Fetch file from Telegram
      const telegramResponse = await fetchWithRetry(directUrl, {
        headers: requestHeaders,
        signal: AbortSignal.timeout(FETCH_TIMEOUT)
      });
      
      if (!telegramResponse.ok) {
        console.error(`📡 Telegram file fetch failed: ${telegramResponse.status} ${telegramResponse.statusText}`);
        continue;
      }
      
      // Prepare response headers
      const responseHeaders = new Headers();
      
      // Copy relevant headers from Telegram response
      ['content-length', 'content-range', 'accept-ranges'].forEach(header => {
        const value = telegramResponse.headers.get(header);
        if (value) {
          responseHeaders.set(header, value);
        }
      });
      
      // Set standard headers
      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Cache-Control', `public, max-age=${CACHE_TTL_LONG}, immutable`);
      
      // Handle download vs inline display
      const url = new URL(request.url);
      if (url.searchParams.has('dl') || url.searchParams.has('download')) {
        responseHeaders.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
        console.log('📥 Download mode enabled');
      } else {
        responseHeaders.set('Content-Disposition', 'inline');
        console.log('👁️ Inline display mode');
      }
      
      console.log(`🚀 Single file streaming successful:
📝 File: ${metadata.filename}
📊 Status: ${telegramResponse.status}
🤖 Bot: ${botIndex + 1}
📏 Content-Length: ${telegramResponse.headers.get('content-length') || 'Unknown'}
🎯 Range: ${rangeHeader || 'Full file'}`);
      
      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: responseHeaders
      });
      
    } catch (botError) {
      console.error(`❌ Bot ${botIndex + 1} failed:`, botError.message);
      continue;
    }
  }
  
  console.error('❌ All bot tokens failed');
  return createErrorResponse('All streaming servers failed', 503);
}

/**
 * 🎬 Handle chunked file streaming
 * Ultra-optimized with parallel chunk loading
 */
async function handleChunkedFile(request, env, metadata, mimeType, extension) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520; // Default 20MB chunks
  
  const rangeHeader = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');
  
  console.log(`🎬 Chunked file streaming:
🧩 Total chunks: ${chunks.length}
📏 Total size: ${Math.round(totalSize/1024/1024)}MB
📦 Chunk size: ${Math.round(chunkSize/1024/1024)}MB
🎯 Range request: ${rangeHeader || 'None'}
📥 Download mode: ${isDownload}`);
  
  // Handle different streaming modes
  if (rangeHeader) {
    return await handleParallelRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload);
  }
  
  if (isDownload) {
    return await handleParallelFullDownload(request, env, metadata, mimeType);
  }
  
  return await handleOptimizedInstantPlay(request, env, metadata, mimeType, totalSize);
}

/**
 * ⚡ Handle optimized instant play streaming
 * Streams initial chunks with parallel loading for ultra-fast start
 */
async function handleOptimizedInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  console.log('⚡ OPTIMIZED INSTANT PLAY: Parallel streaming for lightning-fast start...');
  
  try {
    const maxInitialChunks = Math.min(2, chunks.length); // First 2 chunks for instant play
    
    // 🔥 PARALLEL CHUNK LOADING
    const chunkPromises = [];
    for (let i = 0; i < maxInitialChunks; i++) {
      chunkPromises.push(loadSingleChunk(env, chunks[i]));
    }
    
    console.log(`⚡ Loading ${maxInitialChunks} chunks in parallel...`);
    const loadedChunks = await Promise.all(chunkPromises);
    
    let loadedBytes = 0;
    const stream = new ReadableStream({
      start(controller) {
        for (let i = 0; i < loadedChunks.length; i++) {
          const uint8Array = new Uint8Array(loadedChunks[i]);
          controller.enqueue(uint8Array);
          loadedBytes += uint8Array.byteLength;
          console.log(`⚡ Streamed chunk ${i + 1}: ${Math.round(uint8Array.byteLength/1024/1024)}MB`);
        }
        console.log('⚡ Instant play chunks streaming completed');
        controller.close();
      },
      
      cancel(reason) {
        console.log('⚡ Stream cancelled:', reason);
      }
    });
    
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', Math.min(loadedBytes, totalSize).toString());
    headers.set('Content-Range', `bytes 0-${Math.min(loadedBytes, totalSize) - 1}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', `public, max-age=${CACHE_TTL_LONG}, immutable`);
    headers.set('X-Streaming-Mode', 'optimized-instant-play');
    headers.set('X-Parallel-Chunks', maxInitialChunks.toString());
    
    console.log(`⚡ INSTANT PLAY READY: ${Math.round(loadedBytes/1024/1024)}MB loaded in parallel for ultra-fast start`);
    
    return new Response(stream, { status: 206, headers });
    
  } catch (error) {
    console.error('⚡ Instant play error:', error);
    return createErrorResponse(`Instant play failed: ${error.message}`, 500);
  }
}

/**
 * 🎯 Handle parallel range requests
 * Ultra-optimized with parallel chunk loading across byte ranges
 */
async function handleParallelRangeRequest(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload = false) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;
  
  console.log('🎯 PARALLEL RANGE REQUEST:', rangeHeader);
  
  // Parse range header (format: bytes=start-end)
  const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) {
    console.error('❌ Invalid range format:', rangeHeader);
    return createErrorResponse('Invalid range format', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }
  
  const start = parseInt(rangeMatch[1], 10);
  let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;
  
  // Validate range bounds
  if (end >= totalSize) end = totalSize - 1;
  if (start >= totalSize || start > end) {
    console.error('❌ Range not satisfiable:', `${start}-${end}/${totalSize}`);
    return createErrorResponse('Range not satisfiable', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }
  
  const requestedSize = end - start + 1;
  console.log(`🎯 Range details:
📍 Start: ${start} (${Math.round(start/1024/1024)}MB)
📍 End: ${end} (${Math.round(end/1024/1024)}MB)
📏 Requested: ${Math.round(requestedSize/1024/1024)}MB
📊 Total size: ${Math.round(totalSize/1024/1024)}MB`);
  
  // Calculate which chunks are needed
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);
  
  console.log(`🧩 Chunk analysis:
🎯 Start chunk: ${startChunk}
🎯 End chunk: ${endChunk}
📦 Chunks needed: ${neededChunks.length}
📊 Chunk size: ${Math.round(chunkSize/1024/1024)}MB`);
  
  // 🔥 PARALLEL CHUNK LOADING
  try {
    console.log(`🔥 Loading ${neededChunks.length} chunks in parallel...`);
    const chunkPromises = neededChunks.map(chunkInfo => loadSingleChunk(env, chunkInfo));
    const loadedChunks = await Promise.all(chunkPromises);
    
    // Process loaded chunks and extract requested range
    let currentPosition = startChunk * chunkSize;
    const rangeData = [];
    
    for (let i = 0; i < loadedChunks.length; i++) {
      const uint8Array = new Uint8Array(loadedChunks[i]);
      
      // Calculate what portion of this chunk we need
      const chunkStart = Math.max(start - currentPosition, 0);
      const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);
      
      if (chunkStart < chunkEnd) {
        const chunkSlice = uint8Array.slice(chunkStart, chunkEnd);
        rangeData.push(chunkSlice);
        console.log(`🎯 Chunk ${startChunk + i + 1} processed: ${chunkSlice.length} bytes`);
      }
      
      currentPosition += chunkSize;
      if (currentPosition > end) break;
    }
    
    // Combine all range data
    const totalLength = rangeData.reduce((sum, chunk) => sum + chunk.length, 0);
    const combinedData = new Uint8Array(totalLength);
    let offset = 0;
    
    for (const chunk of rangeData) {
      combinedData.set(chunk, offset);
      offset += chunk.length;
    }
    
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', requestedSize.toString());
    headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', isDownload ? `attachment; filename="${metadata.filename}"` : 'inline');
    headers.set('Cache-Control', `public, max-age=${CACHE_TTL_LONG}, immutable`);
    headers.set('X-Streaming-Mode', 'parallel-range-request');
    headers.set('X-Parallel-Chunks', neededChunks.length.toString());
    
    console.log(`✅ PARALLEL RANGE RESPONSE READY: ${Math.round(requestedSize/1024/1024)}MB loaded in parallel`);
    
    return new Response(combinedData, { status: 206, headers });
    
  } catch (error) {
    console.error('❌ Parallel range request error:', error);
    return createErrorResponse(`Range request failed: ${error.message}`, 500);
  }
}

/**
 * 📥 Handle parallel full download streaming
 * Ultra-optimized with batch parallel chunk loading
 */
async function handleParallelFullDownload(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const filename = metadata.filename;
  const totalSize = metadata.size;
  
  console.log(`📥 PARALLEL FULL DOWNLOAD: Streaming complete file
📝 File: ${filename}
📏 Size: ${Math.round(totalSize/1024/1024)}MB
🧩 Chunks: ${chunks.length}`);
  
  let chunkIndex = 0;
  let streamedBytes = 0;
  
  const stream = new ReadableStream({
    async pull(controller) {
      while (chunkIndex < chunks.length) {
        try {
          // 🔥 BATCH PARALLEL LOADING
          const batchSize = Math.min(MAX_PARALLEL_CHUNKS, chunks.length - chunkIndex);
          const batchPromises = [];
          
          console.log(`📥 Loading batch of ${batchSize} chunks (${chunkIndex + 1}-${chunkIndex + batchSize}/${chunks.length})`);
          
          for (let i = 0; i < batchSize; i++) {
            batchPromises.push(loadSingleChunk(env, chunks[chunkIndex + i]));
          }
          
          const batchChunks = await Promise.all(batchPromises);
          
          // Stream all batch chunks
          for (let i = 0; i < batchChunks.length; i++) {
            const uint8Array = new Uint8Array(batchChunks[i]);
            controller.enqueue(uint8Array);
            streamedBytes += uint8Array.byteLength;
            console.log(`📥 Chunk ${chunkIndex + i + 1} streamed: ${Math.round(uint8Array.byteLength/1024/1024)}MB`);
          }
          
          chunkIndex += batchSize;
          console.log(`📥 Progress: ${Math.round((chunkIndex/chunks.length)*100)}% (${Math.round(streamedBytes/1024/1024)}MB)`);
          
        } catch (error) {
          console.error(`❌ Batch download failed at chunk ${chunkIndex + 1}:`, error);
          controller.error(error);
          return;
        }
      }
      
      console.log(`📥 Download completed: ${Math.round(streamedBytes/1024/1024)}MB streamed with parallel loading`);
      controller.close();
    },
    
    cancel(reason) {
      console.log('📥 Download stream cancelled:', reason);
    }
  });
  
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('X-Download-Mode', 'parallel-full-stream');
  headers.set('X-Batch-Size', MAX_PARALLEL_CHUNKS.toString());
  headers.set('Cache-Control', `public, max-age=${CACHE_TTL_LONG}, immutable`);
  
  console.log(`📥 Parallel full download stream initiated with batch size: ${MAX_PARALLEL_CHUNKS}`);
  
  return new Response(stream, { status: 200, headers });
}

/**
 * 📥 Load a single chunk from storage
 * Handles URL refresh if expired with optimized caching
 */
async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;
  
  console.log(`📥 Loading chunk: ${chunkKey}`);
  
  // Get chunk metadata from KV
  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkKey}`);
  }
  
  const chunkMetadata = JSON.parse(metadataString);
  chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;
  
  // Try existing direct URL first
  if (chunkMetadata.directUrl) {
    try {
      const response = await fetchWithRetry(chunkMetadata.directUrl, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT)
      });
      
      if (response.ok) {
        console.log(`✅ Chunk loaded from cached URL: ${chunkKey}`);
        return response.arrayBuffer();
      }
      
      console.log(`🔄 Cached URL expired for chunk: ${chunkKey}`);
    } catch (error) {
      console.log(`🔄 Cached URL failed for chunk: ${chunkKey}`, error.message);
    }
  }
  
  // URL expired or failed, refresh it
  console.log(`🔄 Refreshing URL for chunk: ${chunkKey}`);
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];
    
    try {
      console.log(`🤖 Refreshing with bot ${botIndex + 1}/${botTokens.length} for chunk: ${chunkKey}`);
      
      const getFileResponse = await fetchWithRetry(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );
      
      const getFileData = await getFileResponse.json();
      
      if (!getFileData.ok) {
        console.error(`🤖 Bot ${botIndex + 1} API error for chunk ${chunkKey}: ${getFileData.error_code} - ${getFileData.description}`);
        continue;
      }
      
      if (!getFileData.result?.file_path) {
        console.error(`🤖 Bot ${botIndex + 1} no file_path for chunk: ${chunkKey}`);
        continue;
      }
      
      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      
      const response = await fetchWithRetry(freshUrl, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT)
      });
      
      if (response.ok) {
        // Update KV store with fresh URL (non-blocking)
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now(),
          refreshedBy: `bot${botIndex + 1}`
        })).catch(error => {
          console.warn(`⚠️ Failed to update KV for chunk ${chunkKey}:`, error.message);
        });
        
        console.log(`✅ URL refreshed successfully for chunk: ${chunkKey} using bot ${botIndex + 1}`);
        return response.arrayBuffer();
      }
      
      console.error(`📡 Fresh URL failed for chunk ${chunkKey} with bot ${botIndex + 1}: ${response.status}`);
      
    } catch (botError) {
      console.error(`❌ Bot ${botIndex + 1} failed for chunk ${chunkKey}:`, botError.message);
      continue;
    }
  }
  
  throw new Error(`All refresh attempts failed for chunk: ${chunkKey}`);
}

/**
 * 🔄 Fetch with retry logic and intelligent rate limit handling
 */
async function fetchWithRetry(url, options = {}, retries = MAX_RETRIES) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetch(url, options);
      
      if (response.ok) {
        return response;
      }
      
      // Handle rate limiting with exponential backoff
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After')) || Math.pow(2, attempt);
        console.warn(`⏳ Rate limited, waiting ${retryAfter}s before retry ${attempt + 1}/${retries}`);
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
        continue;
      }
      
      // Handle server errors (5xx) - retry with exponential backoff
      if (response.status >= 500) {
        console.error(`🔄 Server error ${response.status} on attempt ${attempt + 1}/${retries}`);
        if (attempt < retries - 1) {
          const delay = Math.min(Math.pow(2, attempt) * 1000, 8000); // Max 8s delay
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
      }
      
      // Client errors (4xx) - don't retry
      if (response.status >= 400 && response.status < 500) {
        console.error(`❌ Client error ${response.status}: ${response.statusText}`);
        return response;
      }
      
      console.error(`❌ Attempt ${attempt + 1}/${retries} failed: ${response.status} ${response.statusText}`);
      
    } catch (error) {
      console.error(`❌ Attempt ${attempt + 1}/${retries} error:`, error.message);
      
      // If it's the last attempt, throw the error
      if (attempt === retries - 1) {
        throw error;
      }
    }
    
    // Wait before retry (exponential backoff)
    if (attempt < retries - 1) {
      const delay = Math.min(Math.pow(2, attempt) * 1000, 8000); // Max 8s delay
      console.log(`⏳ Waiting ${delay}ms before retry ${attempt + 2}/${retries}`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw new Error(`All ${retries} fetch attempts failed for ${url}`);
}

/**
 * ❌ Create standardized error response
 */
function createErrorResponse(message, status = 500, additionalHeaders = {}) {
  const headers = new Headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    ...additionalHeaders
  });
  
  const errorResponse = {
    error: message,
    status: status,
    timestamp: new Date().toISOString(),
    service: 'BTF Storage - World-Class Streaming'
  };
  
  console.error(`❌ Error response: ${status} - ${message}`);
  
  return new Response(JSON.stringify(errorResponse, null, 2), {
    status: status,
    headers: headers
  });
}