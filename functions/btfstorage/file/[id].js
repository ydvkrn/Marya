// functions/btfstorage/file/[id].js
// 🎬 Cloudflare Pages Functions - Optimized Advanced File Streaming Handler
// URL: marya-hosting.pages.dev/btfstorage/file/MSM221-48U91C62-no.mp4

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

// Performance optimization constants
const MAX_RETRY_ATTEMPTS = 3; // Reduced from 5 for faster fail
const INITIAL_RETRY_DELAY = 500; // Reduced from 1000ms
const MAX_RETRY_DELAY = 5000; // Reduced from 10000ms
const FETCH_TIMEOUT = 30000; // Reduced from 45000ms for faster detection
const API_TIMEOUT = 10000; // Reduced from 15000ms

/**
 * Main request handler for Cloudflare Pages Functions
 * Handles dynamic file streaming with multiple formats and protocols
 */
export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;
  const startTime = Date.now();

  console.log('🎬 TOP TIER STREAMING STARTED:', fileId);
  console.log('📍 Request URL:', request.url);
  console.log('🔗 Method:', request.method);
  console.log('📊 User-Agent:', request.headers.get('User-Agent') || 'Unknown');

  // Handle CORS preflight requests
  if (request.method === 'OPTIONS') {
    return handleCORSPreflight();
  }

  try {
    // Parse file ID and extract components
    const fileInfo = parseFileId(fileId);
    
    console.log(`📁 File Info Parsed:
    🆔 ID: ${fileInfo.actualId}
    📝 Extension: ${fileInfo.extension}
    📼 HLS Playlist: ${fileInfo.isHlsPlaylist}
    📼 HLS Segment: ${fileInfo.isHlsSegment} (Index: ${fileInfo.segmentIndex})`);

    // Fetch metadata from KV storage with timeout
    console.log('📂 Fetching metadata for:', fileInfo.actualId);
    const metadataString = await Promise.race([
      env.FILES_KV.get(fileInfo.actualId),
      timeout(5000, 'Metadata fetch timeout')
    ]);

    if (!metadataString) {
      console.error('❌ File not found in KV storage:', fileInfo.actualId);
      return createErrorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);

    // Validate metadata structure
    if (!metadata.filename || !metadata.size) {
      console.error('❌ Invalid metadata structure:', metadata);
      return createErrorResponse('Invalid file metadata', 400);
    }

    // Handle backward compatibility
    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

    // Validate file source
    if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      console.error('❌ No telegramFileId or chunks in metadata:', fileInfo.actualId);
      return createErrorResponse('Missing file source data', 400);
    }

    // Determine MIME type
    const mimeType = metadata.contentType || MIME_TYPES[fileInfo.extension] || 'application/octet-stream';

    // Log file information
    console.log(`📁 File Details:
    📝 Name: ${metadata.filename}
    📏 Size: ${Math.round(metadata.size/1024/1024)}MB (${metadata.size} bytes)
    🏷️ MIME: ${mimeType}
    🧩 Chunks: ${metadata.chunks?.length || 0}
    📅 Uploaded: ${metadata.uploadedAt || 'N/A'}
    🔗 Has Telegram ID: ${!!metadata.telegramFileId}`);

    // Route to appropriate handler
    let response;
    if (fileInfo.isHlsPlaylist) {
      response = await handleHlsPlaylist(request, env, metadata, fileInfo.actualId);
    } else if (fileInfo.isHlsSegment && fileInfo.segmentIndex >= 0) {
      response = await handleHlsSegment(request, env, metadata, fileInfo.segmentIndex);
    } else if (metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      response = await handleSingleFile(request, env, metadata, mimeType);
    } else if (metadata.chunks && metadata.chunks.length > 0) {
      response = await handleChunkedFile(request, env, metadata, mimeType, fileInfo.extension);
    } else {
      response = createErrorResponse('Invalid file format or configuration', 400);
    }

    const duration = Date.now() - startTime;
    console.log(`✅ Request completed in ${duration}ms`);
    
    return response;

  } catch (error) {
    console.error('❌ Critical streaming error:', error);
    console.error('🔍 Error stack:', error.stack);
    return createErrorResponse(`Streaming error: ${error.message}`, 500);
  }
}

/**
 * Parse file ID and extract components
 */
function parseFileId(fileId) {
  let actualId = fileId;
  let extension = '';
  let isHlsPlaylist = false;
  let isHlsSegment = false;
  let segmentIndex = -1;

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

  return { actualId, extension, isHlsPlaylist, isHlsSegment, segmentIndex };
}

/**
 * Handle CORS preflight
 */
function handleCORSPreflight() {
  const corsHeaders = new Headers();
  corsHeaders.set('Access-Control-Allow-Origin', '*');
  corsHeaders.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  corsHeaders.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
  corsHeaders.set('Access-Control-Max-Age', '86400');
  corsHeaders.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');

  console.log('✅ CORS preflight handled');
  return new Response(null, { status: 204, headers: corsHeaders });
}

/**
 * Handle HLS playlist generation (.m3u8)
 */
async function handleHlsPlaylist(request, env, metadata, actualId) {
  console.log('📼 Generating HLS playlist for:', actualId);

  if (!metadata.chunks || metadata.chunks.length === 0) {
    console.error('❌ HLS playlist requested for non-chunked file');
    return createErrorResponse('HLS not supported for single files', 400);
  }

  const chunks = metadata.chunks;
  const segmentDuration = 6;
  const baseUrl = new URL(request.url).origin;

  // Generate M3U8 playlist
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

  for (let i = 0; i < chunks.length; i++) {
    playlist += `#EXTINF:${segmentDuration.toFixed(1)},
`;
    playlist += `${baseUrl}/btfstorage/file/${actualId}-${i}.ts
`;
  }

  playlist += '#EXT-X-ENDLIST
';

  const headers = new Headers();
  headers.set('Content-Type', 'application/x-mpegURL');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=300'); // 5 min cache
  headers.set('CDN-Cache-Control', 'public, max-age=3600'); // 1 hour edge cache

  console.log(`📼 HLS playlist generated: ${chunks.length} segments`);

  return new Response(playlist, { status: 200, headers });
}

/**
 * Handle HLS segment serving (.ts)
 */
async function handleHlsSegment(request, env, metadata, segmentIndex) {
  console.log('📼 Serving HLS segment:', segmentIndex);

  if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
    console.error('❌ Invalid segment index:', segmentIndex);
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
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    headers.set('Content-Disposition', 'inline');
    headers.set('Accept-Ranges', 'bytes');

    console.log(`📼 HLS segment ${segmentIndex} served: ${Math.round(chunkData.byteLength/1024/1024)}MB`);

    return new Response(chunkData, { status: 200, headers });

  } catch (error) {
    console.error('❌ HLS segment error:', error);
    return createErrorResponse(`Segment loading failed: ${error.message}`, 500);
  }
}

/**
 * Handle single file streaming
 */
async function handleSingleFile(request, env, metadata, mimeType) {
  console.log('🚀 Single file streaming initiated');

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  if (botTokens.length === 0) {
    console.error('❌ No bot tokens configured');
    return createErrorResponse('Service configuration error', 503);
  }

  console.log(`🤖 Available bot tokens: ${botTokens.length}`);

  // Try each bot token
  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];
    console.log(`🤖 Trying bot ${botIndex + 1}/${botTokens.length}`);

    try {
      // Get file information from Telegram with reduced timeout
      const getFileResponse = await fetchWithTimeout(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
        { method: 'GET' },
        API_TIMEOUT
      );

      const getFileData = await getFileResponse.json();

      if (!getFileData.ok || !getFileData.result?.file_path) {
        console.error(`🤖 Bot ${botIndex + 1} failed: ${getFileData.description || 'Unknown error'}`);
        continue;
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      console.log('📡 Telegram direct URL obtained');

      // Prepare headers
      const requestHeaders = {};
      const rangeHeader = request.headers.get('Range');

      if (rangeHeader) {
        requestHeaders['Range'] = rangeHeader;
        console.log('🎯 Range request:', rangeHeader);
      }

      // Fetch file from Telegram with optimized timeout
      const telegramResponse = await fetchWithTimeout(
        directUrl,
        { headers: requestHeaders },
        FETCH_TIMEOUT
      );

      if (!telegramResponse.ok) {
        console.error(`📡 Telegram fetch failed: ${telegramResponse.status}`);
        continue;
      }

      // Prepare response headers
      const responseHeaders = new Headers();

      // Copy relevant headers
      ['content-length', 'content-range', 'accept-ranges'].forEach(header => {
        const value = telegramResponse.headers.get(header);
        if (value) responseHeaders.set(header, value);
      });

      // Set optimized headers
      responseHeaders.set('Content-Type', mimeType);
      responseHeaders.set('Accept-Ranges', 'bytes');
      responseHeaders.set('Access-Control-Allow-Origin', '*');
      responseHeaders.set('Cache-Control', 'public, max-age=31536000, immutable');
      responseHeaders.set('CDN-Cache-Control', 'public, max-age=31536000');

      // Handle download vs inline
      const url = new URL(request.url);
      if (url.searchParams.has('dl') || url.searchParams.has('download')) {
        responseHeaders.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
        console.log('📥 Download mode enabled');
      } else {
        responseHeaders.set('Content-Disposition', 'inline');
        console.log('👁️ Inline display mode');
      }

      console.log(`✅ Single file streaming successful via bot ${botIndex + 1}`);

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: responseHeaders
      });

    } catch (botError) {
      console.error(`❌ Bot ${botIndex + 1} failed:`, botError.message);
      // Try next bot immediately without delay
      continue;
    }
  }

  console.error('❌ All bot tokens failed');
  return createErrorResponse('All streaming servers failed', 503);
}

/**
 * Handle chunked file streaming
 */
async function handleChunkedFile(request, env, metadata, mimeType, extension) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520;

  const rangeHeader = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');

  console.log(`🎬 Chunked file streaming:
  🧩 Chunks: ${chunks.length}
  📏 Size: ${Math.round(totalSize/1024/1024)}MB
  🎯 Range: ${rangeHeader || 'None'}
  📥 Download: ${isDownload}`);

  // Handle different streaming modes
  if (rangeHeader) {
    return await handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload);
  }

  if (isDownload) {
    return await handleFullStreamDownload(request, env, metadata, mimeType);
  }

  return await handleInstantPlay(request, env, metadata, mimeType, totalSize);
}

/**
 * Handle instant play - OPTIMIZED
 */
async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  console.log('⚡ INSTANT PLAY: Quick start streaming...');

  try {
    // Optimized: Load only first 2 chunks or 30MB (whichever is smaller)
    const maxInitialBytes = 30 * 1024 * 1024; // Reduced from 50MB
    const maxInitialChunks = Math.min(2, chunks.length); // Reduced from 3

    let loadedBytes = 0;
    let chunkIndex = 0;
    const loadedChunks = [];

    // Pre-load chunks in parallel for faster start
    const chunkPromises = [];
    for (let i = 0; i < maxInitialChunks; i++) {
      chunkPromises.push(loadSingleChunk(env, chunks[i]));
    }

    console.log(`⚡ Pre-loading ${maxInitialChunks} chunks in parallel...`);
    const chunkResults = await Promise.all(chunkPromises);

    const stream = new ReadableStream({
      type: 'bytes', // Optimized for byte streaming
      pull(controller) {
        try {
          while (chunkIndex < chunkResults.length && loadedBytes < maxInitialBytes) {
            const chunkData = chunkResults[chunkIndex];
            const uint8Array = new Uint8Array(chunkData);

            controller.enqueue(uint8Array);
            loadedBytes += uint8Array.byteLength;

            console.log(`⚡ Streamed chunk ${chunkIndex + 1}: ${Math.round(uint8Array.byteLength/1024/1024)}MB`);
            chunkIndex++;
          }

          console.log('⚡ Initial chunks streaming completed');
          controller.close();
        } catch (error) {
          console.error('❌ Stream error:', error);
          controller.error(error);
        }
      },

      cancel(reason) {
        console.log('⚡ Stream cancelled:', reason);
      }
    }, { highWaterMark: 65536 }); // Optimized buffer size

    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', Math.min(loadedBytes, totalSize).toString());
    headers.set('Content-Range', `bytes 0-${Math.min(loadedBytes, totalSize) - 1}/${totalSize}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=3600');
    headers.set('X-Streaming-Mode', 'instant-play');

    console.log(`⚡ INSTANT PLAY READY: ${Math.round(loadedBytes/1024/1024)}MB`);

    return new Response(stream, { status: 206, headers });

  } catch (error) {
    console.error('⚡ Instant play error:', error);
    return createErrorResponse(`Instant play failed: ${error.message}`, 500);
  }
}

/**
 * Handle smart range requests - OPTIMIZED
 */
async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload = false) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  console.log('🎯 SMART RANGE REQUEST:', rangeHeader);

  const rangeMatch = rangeHeader.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) {
    console.error('❌ Invalid range format:', rangeHeader);
    return createErrorResponse('Invalid range format', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }

  const start = parseInt(rangeMatch[1], 10);
  let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;

  if (end >= totalSize) end = totalSize - 1;
  if (start >= totalSize || start > end) {
    console.error('❌ Range not satisfiable:', `${start}-${end}/${totalSize}`);
    return createErrorResponse('Range not satisfiable', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }

  const requestedSize = end - start + 1;
  console.log(`🎯 Range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`🧩 Loading chunks ${startChunk} to ${endChunk} (${neededChunks.length} total)`);

  let currentPosition = startChunk * chunkSize;
  let chunkIndex = 0;

  const stream = new ReadableStream({
    type: 'bytes',
    async pull(controller) {
      try {
        while (chunkIndex < neededChunks.length) {
          const chunkInfo = neededChunks[chunkIndex];
          const chunkNumber = startChunk + chunkIndex;

          console.log(`🎯 Loading chunk ${chunkNumber + 1}/${chunks.length}`);
          const chunkData = await loadSingleChunk(env, chunkInfo);
          const uint8Array = new Uint8Array(chunkData);

          const chunkStart = Math.max(start - currentPosition, 0);
          const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

          if (chunkStart < chunkEnd) {
            const chunkSlice = uint8Array.slice(chunkStart, chunkEnd);
            controller.enqueue(chunkSlice);
            console.log(`🎯 Chunk ${chunkNumber + 1} streamed: ${chunkSlice.length} bytes`);
          }

          currentPosition += chunkSize;
          chunkIndex++;
          
          if (currentPosition > end) break;
        }

        console.log('🎯 Range streaming completed');
        controller.close();
      } catch (error) {
        console.error('❌ Range chunk error:', error);
        controller.error(error);
      }
    },

    cancel(reason) {
      console.log('🎯 Range stream cancelled:', reason);
    }
  }, { highWaterMark: 65536 });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', isDownload ? `attachment; filename="${metadata.filename}"` : 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('X-Streaming-Mode', 'range-request');

  console.log(`✅ RANGE RESPONSE READY: ${Math.round(requestedSize/1024/1024)}MB`);

  return new Response(stream, { status: 206, headers });
}

/**
 * Handle full file download streaming
 */
async function handleFullStreamDownload(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const filename = metadata.filename;
  const totalSize = metadata.size;

  console.log(`📥 FULL DOWNLOAD: ${filename} (${Math.round(totalSize/1024/1024)}MB, ${chunks.length} chunks)`);

  let chunkIndex = 0;
  let streamedBytes = 0;

  const stream = new ReadableStream({
    type: 'bytes',
    async pull(controller) {
      try {
        while (chunkIndex < chunks.length) {
          console.log(`📥 Download chunk ${chunkIndex + 1}/${chunks.length}`);

          const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
          const uint8Array = new Uint8Array(chunkData);

          controller.enqueue(uint8Array);
          streamedBytes += uint8Array.byteLength;

          console.log(`📥 Chunk ${chunkIndex + 1}: ${Math.round(uint8Array.byteLength/1024/1024)}MB (Total: ${Math.round(streamedBytes/1024/1024)}MB)`);
          chunkIndex++;
        }

        console.log(`📥 Download completed: ${Math.round(streamedBytes/1024/1024)}MB`);
        controller.close();
      } catch (error) {
        console.error('❌ Download error:', error);
        controller.error(error);
      }
    },

    cancel(reason) {
      console.log('📥 Download cancelled:', reason);
    }
  }, { highWaterMark: 65536 });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('X-Download-Mode', 'full-stream');
  headers.set('Cache-Control', 'public, max-age=31536000');

  return new Response(stream, { status: 200, headers });
}

/**
 * Load a single chunk - OPTIMIZED
 */
async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  console.log(`📥 Loading chunk: ${chunkKey}`);

  // Get chunk metadata with timeout
  const metadataString = await Promise.race([
    kvNamespace.get(chunkKey),
    timeout(3000, 'Chunk metadata timeout')
  ]);

  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkKey}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  chunkMetadata.telegramFileId = chunkMetadata.telegramFileId || chunkMetadata.fileIdCode;

  // Try existing URL first (with shorter timeout)
  if (chunkMetadata.directUrl) {
    try {
      const response = await fetchWithTimeout(
        chunkMetadata.directUrl,
        { method: 'GET' },
        15000 // Reduced timeout
      );

      if (response.ok) {
        console.log(`✅ Chunk loaded from cached URL: ${chunkKey}`);
        return response.arrayBuffer();
      }

      console.log(`🔄 Cached URL expired: ${chunkKey}`);
    } catch (error) {
      console.log(`🔄 Cached URL failed: ${chunkKey}`);
    }
  }

  // Refresh URL
  console.log(`🔄 Refreshing URL: ${chunkKey}`);

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];

    try {
      console.log(`🤖 Refreshing with bot ${botIndex + 1} for: ${chunkKey}`);

      const getFileResponse = await fetchWithTimeout(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { method: 'GET' },
        API_TIMEOUT
      );

      const getFileData = await getFileResponse.json();

      if (!getFileData.ok || !getFileData.result?.file_path) {
        console.error(`🤖 Bot ${botIndex + 1} failed for chunk`);
        continue;
      }

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

      const response = await fetchWithTimeout(freshUrl, { method: 'GET' }, 20000);

      if (response.ok) {
        // Update KV asynchronously (don't wait)
        env.ctx?.waitUntil?.(
          kvNamespace.put(chunkKey, JSON.stringify({
            ...chunkMetadata,
            directUrl: freshUrl,
            lastRefreshed: Date.now()
          }))
        );

        console.log(`✅ URL refreshed for: ${chunkKey}`);
        return response.arrayBuffer();
      }

    } catch (botError) {
      console.error(`❌ Bot ${botIndex + 1} failed:`, botError.message);
      continue;
    }
  }

  throw new Error(`All refresh attempts failed for: ${chunkKey}`);
}

/**
 * Fetch with timeout - OPTIMIZED
 */
async function fetchWithTimeout(url, options = {}, timeoutMs = FETCH_TIMEOUT) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeoutMs}ms`);
    }
    throw error;
  }
}

/**
 * Fetch with retry - OPTIMIZED
 */
async function fetchWithRetry(url, options = {}, retries = MAX_RETRY_ATTEMPTS) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetchWithTimeout(url, options, FETCH_TIMEOUT);

      if (response.ok) {
        return response;
      }

      // Rate limiting
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After')) || 3;
        console.warn(`⏳ Rate limited, waiting ${retryAfter}s`);
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
        continue;
      }

      // Server errors - retry
      if (response.status >= 500) {
        console.error(`🔄 Server error ${response.status}, attempt ${attempt + 1}`);
        if (attempt < retries - 1) {
          const delay = Math.min(INITIAL_RETRY_DELAY * Math.pow(2, attempt), MAX_RETRY_DELAY);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
      }

      // Client errors - don't retry
      if (response.status >= 400 && response.status < 500) {
        console.error(`❌ Client error ${response.status}`);
        return response;
      }

    } catch (error) {
      console.error(`❌ Attempt ${attempt + 1} error:`, error.message);

      if (attempt === retries - 1) {
        throw error;
      }

      const delay = Math.min(INITIAL_RETRY_DELAY * Math.pow(2, attempt), MAX_RETRY_DELAY);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  throw new Error(`All ${retries} attempts failed for ${url}`);
}

/**
 * Timeout helper
 */
function timeout(ms, message = 'Operation timeout') {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(message)), ms);
  });
}

/**
 * Create error response
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
    service: 'BTF Storage Streaming'
  };

  console.error(`❌ Error response: ${status} - ${message}`);

  return new Response(JSON.stringify(errorResponse, null, 2), {
    status: status,
    headers: headers
  });
}