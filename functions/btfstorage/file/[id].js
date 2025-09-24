
// functions/btfstorage/file/[id].js
// ðŸŽ¬ Cloudflare Pages Functions - Advanced File Streaming Handler
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

/**
 * Main request handler for Cloudflare Pages Functions
 * Handles dynamic file streaming with multiple formats and protocols
 */
export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id; // Gets MSM221-48U91C62-no.mp4 from URL

  console.log('ðŸŽ¬ TOP TIER STREAMING STARTED:', fileId);
  console.log('ðŸ“ Request URL:', request.url);
  console.log('ðŸ”— Method:', request.method);
  console.log('ðŸ“Š User-Agent:', request.headers.get('User-Agent') || 'Unknown');

  // Handle CORS preflight requests
  if (request.method === 'OPTIONS') {
    const corsHeaders = new Headers();
    corsHeaders.set('Access-Control-Allow-Origin', '*');
    corsHeaders.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    corsHeaders.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
    corsHeaders.set('Access-Control-Max-Age', '86400');
    corsHeaders.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');

    console.log('âœ… CORS preflight handled');
    return new Response(null, { status: 204, headers: corsHeaders });
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
        console.log('ðŸ“¼ HLS Playlist requested:', actualId);
      } 
      // HLS Segment (.ts with index)
      else if (extension === 'ts' && actualId.includes('-')) {
        const segParts = actualId.split('-');
        const lastPart = segParts[segParts.length - 1];

        if (!isNaN(parseInt(lastPart))) {
          segmentIndex = parseInt(segParts.pop(), 10);
          actualId = segParts.join('-');
          isHlsSegment = true;
          console.log('ðŸ“¼ HLS Segment requested:', actualId, 'Index:', segmentIndex);
        }
      } 
      // Regular file with extension
      else {
        actualId = fileId.substring(0, fileId.lastIndexOf('.'));
        extension = fileId.substring(fileId.lastIndexOf('.') + 1).toLowerCase();
        console.log('ðŸ“ Regular file requested:', actualId, 'Extension:', extension);
      }
    }

    // Fetch metadata from KV storage
    console.log('ðŸ” Fetching metadata for:', actualId);
    const metadataString = await env.FILES_KV.get(actualId);

    if (!metadataString) {
      console.error('âŒ File not found in KV storage:', actualId);
      return createErrorResponse('File not found', 404);
    }

    const metadata = JSON.parse(metadataString);

    // Validate metadata structure
    if (!metadata.filename || !metadata.size) {
      console.error('âŒ Invalid metadata structure:', metadata);
      return createErrorResponse('Invalid file metadata', 400);
    }

    // Handle backward compatibility for field names
    metadata.telegramFileId = metadata.telegramFileId || metadata.fileIdCode;

    // Validate file source (either single file or chunks)
    if (!metadata.telegramFileId && (!metadata.chunks || metadata.chunks.length === 0)) {
      console.error('âŒ No telegramFileId or chunks in metadata:', actualId);
      return createErrorResponse('Missing file source data', 400);
    }

    // Determine MIME type
    const mimeType = metadata.contentType || MIME_TYPES[extension] || 'application/octet-stream';

    // Log file information
    console.log(`ðŸ“ File Info:
    ðŸ“ Name: ${metadata.filename}
    ðŸ“ Size: ${Math.round(metadata.size/1024/1024)}MB (${metadata.size} bytes)
    ðŸ·ï¸ MIME: ${mimeType}
    MaryaUploader Chunks: ${metadata.chunks?.length || 0}
    ðŸ“… Uploaded: ${metadata.uploadedAt || 'N/A'}
    ðŸŽµ HLS Playlist: ${isHlsPlaylist}
    ðŸ“¼ HLS Segment: ${isHlsSegment} (Index: ${segmentIndex})
    ðŸ”— Has Telegram ID: ${!!metadata.telegramFileId}`);

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
    console.error('âŒ Critical streaming error:', error);
    console.error('ðŸ“ Error stack:', error.stack);
    return createErrorResponse(`Streaming error: ${error.message}`, 500);
  }
}

/**
 * Handle HLS playlist generation (.m3u8)
 * Generates dynamic playlist from chunked file segments
 */
async function handleHlsPlaylist(request, env, metadata, actualId) {
  console.log('ðŸ“¼ Generating HLS playlist for:', actualId);

  if (!metadata.chunks || metadata.chunks.length === 0) {
    console.error('âŒ HLS playlist requested for non-chunked file');
    return createErrorResponse('HLS not supported for single files', 400);
  }

  const chunks = metadata.chunks;
  const segmentDuration = 6; // seconds per segment
  const baseUrl = new URL(request.url).origin;

  // Generate M3U8 playlist content
  let playlist = '#EXTM3U\n';
  playlist += '#EXT-X-VERSION:3\n';
  playlist += `#EXT-X-TARGETDURATION:${segmentDuration}\n`;
  playlist += '#EXT-X-MEDIA-SEQUENCE:0\n';
  playlist += '#EXT-X-PLAYLIST-TYPE:VOD\n';

  // Add each chunk as a segment
  for (let i = 0; i < chunks.length; i++) {
    playlist += `#EXTINF:${segmentDuration.toFixed(1)},\n`;
    playlist += `${baseUrl}/btfstorage/file/${actualId}-${i}.ts\n`;
  }

  playlist += '#EXT-X-ENDLIST\n';

  const headers = new Headers();
  headers.set('Content-Type', 'application/x-mpegURL');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  headers.set('Pragma', 'no-cache');
  headers.set('Expires', '0');

  console.log(`ðŸ“¼ HLS playlist generated successfully:
  ðŸ§© Segments: ${chunks.length}
  â±ï¸ Duration per segment: ${segmentDuration}s
  ðŸ“ Total estimated duration: ${Math.round(chunks.length * segmentDuration / 60)}min`);

  return new Response(playlist, { status: 200, headers });
}

/**
 * Handle HLS segment serving (.ts)
 * Serves individual video segments for HLS playback
 */
async function handleHlsSegment(request, env, metadata, segmentIndex) {
  console.log('ðŸ“¼ Serving HLS segment:', segmentIndex);

  // Validate segment index
  if (!metadata.chunks || segmentIndex >= metadata.chunks.length || segmentIndex < 0) {
    console.error('âŒ Invalid segment index:', segmentIndex, 'Available:', metadata.chunks?.length);
    return createErrorResponse('Segment not found', 404);
  }

  try {
    const chunkInfo = metadata.chunks[segmentIndex];
    console.log('ðŸ“¥ Loading segment chunk:', chunkInfo.keyName || chunkInfo.chunkKey);

    const chunkData = await loadSingleChunk(env, chunkInfo);

    const headers = new Headers();
    headers.set('Content-Type', 'video/mp2t');
    headers.set('Content-Length', chunkData.byteLength.toString());
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    headers.set('Content-Disposition', 'inline');
    headers.set('Accept-Ranges', 'bytes');

    console.log(`ðŸ“¼ HLS segment served successfully:
    ðŸ“ Index: ${segmentIndex}
    ðŸ“ Size: ${Math.round(chunkData.byteLength/1024/1024)}MB
    ðŸ·ï¸ Type: video/mp2t`);

    return new Response(chunkData, { status: 200, headers });

  } catch (error) {
    console.error('âŒ HLS segment error:', error);
    return createErrorResponse(`Segment loading failed: ${error.message}`, 500);
  }
}

/**
 * Handle single file streaming
 * Direct streaming from Telegram with range request support
 */
async function handleSingleFile(request, env, metadata, mimeType) {
  console.log('ðŸš€ Single file streaming initiated');

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  if (botTokens.length === 0) {
    console.error('âŒ No bot tokens configured');
    return createErrorResponse('Service configuration error', 503);
  }

  console.log(`ðŸ¤– Available bot tokens: ${botTokens.length}`);

  // Try each bot token until one works
  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];
    console.log(`ðŸ¤– Trying bot ${botIndex + 1}/${botTokens.length}`);

    try {
      // Get file information from Telegram
      const getFileResponse = await fetchWithRetry(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(metadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();

      if (!getFileData.ok || !getFileData.result?.file_path) {
        console.error(`ðŸ¤– Bot ${botIndex + 1} API error: ${getFileData.error_code} - ${getFileData.description}`);
        continue;
      }

      const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      console.log('ðŸ“¡ Telegram direct URL obtained');

      // Prepare headers for range request if needed
      const requestHeaders = {};
      const rangeHeader = request.headers.get('Range');

      if (rangeHeader) {
        requestHeaders['Range'] = rangeHeader;
        console.log('ðŸŽ¯ Range request:', rangeHeader);
      }

      // Fetch file from Telegram
      const telegramResponse = await fetchWithRetry(directUrl, {
        headers: requestHeaders,
        signal: AbortSignal.timeout(45000)
      });

      if (!telegramResponse.ok) {
        console.error(`ðŸ“¡ Telegram file fetch failed: ${telegramResponse.status} ${telegramResponse.statusText}`);
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
      responseHeaders.set('Cache-Control', 'public, max-age=31536000');

      // Handle download vs inline display
      const url = new URL(request.url);
      if (url.searchParams.has('dl') || url.searchParams.has('download')) {
        responseHeaders.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
        console.log('ðŸ“¥ Download mode enabled');
      } else {
        responseHeaders.set('Content-Disposition', 'inline');
        console.log('ðŸ‘ï¸ Inline display mode');
      }

      console.log(`ðŸš€ Single file streaming successful:
      ðŸ“ File: ${metadata.filename}
      ðŸ“Š Status: ${telegramResponse.status}
      ðŸ¤– Bot: ${botIndex + 1}
      ðŸ“ Content-Length: ${telegramResponse.headers.get('content-length') || 'Unknown'}
      ðŸŽ¯ Range: ${rangeHeader || 'Full file'}`);

      return new Response(telegramResponse.body, {
        status: telegramResponse.status,
        headers: responseHeaders
      });

    } catch (botError) {
      console.error(`âŒ Bot ${botIndex + 1} failed:`, botError.message);
      continue;
    }
  }

  console.error('âŒ All bot tokens failed');
  return createErrorResponse('All streaming servers failed', 503);
}

/**
 * Handle chunked file streaming
 * Combines multiple chunks into a single stream
 */
async function handleChunkedFile(request, env, metadata, mimeType, extension) {
  const chunks = metadata.chunks;
  const totalSize = metadata.size;
  const chunkSize = metadata.chunkSize || 20971520; // Default 20MB chunks

  const rangeHeader = request.headers.get('Range');
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') || url.searchParams.has('download');

  console.log(`ðŸŽ¬ Chunked file streaming:
  ðŸ§© Total chunks: ${chunks.length}
  ðŸ“ Total size: ${Math.round(totalSize/1024/1024)}MB
  ðŸ“¦ Chunk size: ${Math.round(chunkSize/1024/1024)}MB
  ðŸŽ¯ Range request: ${rangeHeader || 'None'}
  ðŸ“¥ Download mode: ${isDownload}`);

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
 * Handle instant play streaming
 * Streams initial chunks for quick video start
 */
async function handleInstantPlay(request, env, metadata, mimeType, totalSize) {
  const chunks = metadata.chunks;
  console.log('âš¡ INSTANT PLAY: Streaming initial chunks for quick start...');

  try {
    const maxInitialBytes = 50 * 1024 * 1024; // 50MB for instant play
    const maxInitialChunks = Math.min(3, chunks.length); // First 3 chunks max

    let loadedBytes = 0;
    let chunkIndex = 0;

    const stream = new ReadableStream({
      async pull(controller) {
        while (chunkIndex < maxInitialChunks && loadedBytes < maxInitialBytes) {
          try {
            console.log(`âš¡ Loading initial chunk ${chunkIndex + 1}/${maxInitialChunks}`);
            const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
            const uint8Array = new Uint8Array(chunkData);

            controller.enqueue(uint8Array);
            loadedBytes += uint8Array.byteLength;

            console.log(`âš¡ Streamed chunk ${chunkIndex + 1}: ${Math.round(uint8Array.byteLength/1024/1024)}MB (Total: ${Math.round(loadedBytes/1024/1024)}MB)`);
            chunkIndex++;

          } catch (error) {
            console.error(`âŒ Initial chunk ${chunkIndex + 1} failed:`, error);
            controller.error(error);
            return;
          }
        }

        console.log('âš¡ Initial chunks streaming completed');
        controller.close();
      },

      cancel(reason) {
        console.log('âš¡ Stream cancelled:', reason);
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

    console.log(`âš¡ INSTANT PLAY READY: ${Math.round((loadedBytes || maxInitialBytes)/1024/1024)}MB streamed for quick start`);

    return new Response(stream, { status: 206, headers });

  } catch (error) {
    console.error('âš¡ Instant play error:', error);
    return createErrorResponse(`Instant play failed: ${error.message}`, 500);
  }
}

/**
 * Handle smart range requests
 * Efficiently streams requested byte ranges across multiple chunks
 */
async function handleSmartRange(request, env, metadata, rangeHeader, mimeType, chunkSize, isDownload = false) {
  const totalSize = metadata.size;
  const chunks = metadata.chunks;

  console.log('ðŸŽ¯ SMART RANGE REQUEST:', rangeHeader);

  // Parse range header (format: bytes=start-end)
  const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) {
    console.error('âŒ Invalid range format:', rangeHeader);
    return createErrorResponse('Invalid range format', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }

  const start = parseInt(rangeMatch[1], 10);
  let end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : totalSize - 1;

  // Validate range bounds
  if (end >= totalSize) end = totalSize - 1;
  if (start >= totalSize || start > end) {
    console.error('âŒ Range not satisfiable:', `${start}-${end}/${totalSize}`);
    return createErrorResponse('Range not satisfiable', 416, {
      'Content-Range': `bytes */${totalSize}`
    });
  }

  const requestedSize = end - start + 1;
  console.log(`ðŸŽ¯ Range details:
  ðŸ“ Start: ${start} (${Math.round(start/1024/1024)}MB)
  ðŸ“ End: ${end} (${Math.round(end/1024/1024)}MB)
  ðŸ“ Requested: ${Math.round(requestedSize/1024/1024)}MB
  ðŸ“Š Total size: ${Math.round(totalSize/1024/1024)}MB`);

  // Calculate which chunks are needed
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`ðŸ§© Chunk analysis:
  ðŸŽ¯ Start chunk: ${startChunk}
  ðŸŽ¯ End chunk: ${endChunk}
  ðŸ“¦ Chunks needed: ${neededChunks.length}
  ðŸ“Š Chunk size: ${Math.round(chunkSize/1024/1024)}MB`);

  let currentPosition = startChunk * chunkSize;

  const stream = new ReadableStream({
    async pull(controller) {
      for (let i = 0; i < neededChunks.length; i++) {
        const chunkInfo = neededChunks[i];
        const chunkNumber = startChunk + i;

        try {
          console.log(`ðŸŽ¯ Loading range chunk ${chunkNumber + 1}/${chunks.length} (${i + 1}/${neededChunks.length})`);
          const chunkData = await loadSingleChunk(env, chunkInfo);
          const uint8Array = new Uint8Array(chunkData);

          // Calculate what portion of this chunk we need
          const chunkStart = Math.max(start - currentPosition, 0);
          const chunkEnd = Math.min(uint8Array.length, end - currentPosition + 1);

          if (chunkStart < chunkEnd) {
            const chunkSlice = uint8Array.slice(chunkStart, chunkEnd);
            controller.enqueue(chunkSlice);

            console.log(`ðŸŽ¯ Range chunk ${chunkNumber + 1} streamed: ${chunkSlice.length} bytes (${chunkStart}-${chunkEnd-1} of chunk)`);
          }

          currentPosition += chunkSize;
          if (currentPosition > end) break;

        } catch (error) {
          console.error(`âŒ Range chunk ${chunkNumber + 1} failed:`, error);
          controller.error(error);
          return;
        }
      }

      console.log('ðŸŽ¯ Range streaming completed');
      controller.close();
    },

    cancel(reason) {
      console.log('ðŸŽ¯ Range stream cancelled:', reason);
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', isDownload ? `attachment; filename="${metadata.filename}"` : 'inline');
  headers.set('Cache-Control', 'public, max-age=31536000');
  headers.set('X-Streaming-Mode', 'range-request');

  console.log(`âœ… RANGE RESPONSE READY: ${Math.round(requestedSize/1024/1024)}MB will be streamed`);

  return new Response(stream, { status: 206, headers });
}

/**
 * Handle full file download streaming
 * Streams complete file by combining all chunks
 */
async function handleFullStreamDownload(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const filename = metadata.filename;
  const totalSize = metadata.size;

  console.log(`ðŸ“¥ FULL DOWNLOAD: Streaming complete file
  ðŸ“ File: ${filename}
  ðŸ“ Size: ${Math.round(totalSize/1024/1024)}MB
  ðŸ§© Chunks: ${chunks.length}`);

  let chunkIndex = 0;
  let streamedBytes = 0;

  const stream = new ReadableStream({
    async pull(controller) {
      while (chunkIndex < chunks.length) {
        try {
          console.log(`ðŸ“¥ Download chunk ${chunkIndex + 1}/${chunks.length} (${Math.round((chunkIndex/chunks.length)*100)}%)`);

          const chunkData = await loadSingleChunk(env, chunks[chunkIndex]);
          const uint8Array = new Uint8Array(chunkData);

          controller.enqueue(uint8Array);
          streamedBytes += uint8Array.byteLength;

          console.log(`ðŸ“¥ Download chunk ${chunkIndex + 1} completed: ${Math.round(uint8Array.byteLength/1024/1024)}MB (Total: ${Math.round(streamedBytes/1024/1024)}MB)`);
          chunkIndex++;

        } catch (error) {
          console.error(`âŒ Download chunk ${chunkIndex + 1} failed:`, error);
          controller.error(error);
          return;
        }
      }

      console.log(`ðŸ“¥ Download completed: ${Math.round(streamedBytes/1024/1024)}MB streamed`);
      controller.close();
    },

    cancel(reason) {
      console.log('ðŸ“¥ Download stream cancelled:', reason);
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

  console.log(`ðŸ“¥ Full download stream initiated: ${Math.round(totalSize/1024/1024)}MB total`);

  return new Response(stream, { status: 200, headers });
}

/**
 * Load a single chunk from storage
 * Handles URL refresh if expired
 */
async function loadSingleChunk(env, chunkInfo) {
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;

  console.log(`ðŸ“¥ Loading chunk: ${chunkKey}`);

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
        signal: AbortSignal.timeout(30000) 
      });

      if (response.ok) {
        console.log(`âœ… Chunk loaded from cached URL: ${chunkKey}`);
        return response.arrayBuffer();
      }

      console.log(`ðŸ”„ Cached URL expired for chunk: ${chunkKey}`);
    } catch (error) {
      console.log(`ðŸ”„ Cached URL failed for chunk: ${chunkKey}`, error.message);
    }
  }

  // URL expired or failed, refresh it
  console.log(`ðŸ”„ Refreshing URL for chunk: ${chunkKey}`);

  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);

  for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
    const botToken = botTokens[botIndex];

    try {
      console.log(`ðŸ¤– Refreshing with bot ${botIndex + 1}/${botTokens.length} for chunk: ${chunkKey}`);

      const getFileResponse = await fetchWithRetry(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();

      if (!getFileData.ok) {
        console.error(`ðŸ¤– Bot ${botIndex + 1} API error for chunk ${chunkKey}: ${getFileData.error_code} - ${getFileData.description}`);
        continue;
      }

      if (!getFileData.result?.file_path) {
        console.error(`ðŸ¤– Bot ${botIndex + 1} no file_path for chunk: ${chunkKey}`);
        continue;
      }

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

      const response = await fetchWithRetry(freshUrl, { 
        signal: AbortSignal.timeout(30000) 
      });

      if (response.ok) {
        // Update KV store with fresh URL (don't block on this)
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now(),
          refreshedBy: `bot${botIndex + 1}`
        })).catch(error => {
          console.warn(`âš ï¸ Failed to update KV for chunk ${chunkKey}:`, error.message);
        });

        console.log(`âœ… URL refreshed successfully for chunk: ${chunkKey} using bot ${botIndex + 1}`);
        return response.arrayBuffer();
      }

      console.error(`ðŸ“¡ Fresh URL failed for chunk ${chunkKey} with bot ${botIndex + 1}: ${response.status}`);

    } catch (botError) {
      console.error(`âŒ Bot ${botIndex + 1} failed for chunk ${chunkKey}:`, botError.message);
      continue;
    }
  }

  throw new Error(`All refresh attempts failed for chunk: ${chunkKey}`);
}

/**
 * Fetch with retry logic and rate limit handling
 */
async function fetchWithRetry(url, options = {}, retries = 5) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetch(url, options);

      if (response.ok) {
        return response;
      }

      // Handle rate limiting
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After')) || 5;
        console.warn(`â³ Rate limited, waiting ${retryAfter}s before retry ${attempt + 1}/${retries}`);
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
        continue;
      }

      // Handle server errors (5xx) - retry
      if (response.status >= 500) {
        console.error(`ðŸ”„ Server error ${response.status} on attempt ${attempt + 1}/${retries}`);
        if (attempt < retries - 1) {
          await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
          continue;
        }
      }

      // Client errors (4xx) - don't retry
      if (response.status >= 400 && response.status < 500) {
        console.error(`âŒ Client error ${response.status}: ${response.statusText}`);
        return response; // Return even if not ok, let caller handle
      }

      console.error(`âŒ Attempt ${attempt + 1}/${retries} failed: ${response.status} ${response.statusText}`);

    } catch (error) {
      console.error(`âŒ Attempt ${attempt + 1}/${retries} error:`, error.message);

      // If it's the last attempt, throw the error
      if (attempt === retries - 1) {
        throw error;
      }
    }

    // Wait before retry (exponential backoff)
    if (attempt < retries - 1) {
      const delay = Math.min(Math.pow(2, attempt) * 1000, 10000); // Max 10s delay
      console.log(`â³ Waiting ${delay}ms before retry ${attempt + 2}/${retries}`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  throw new Error(`All ${retries} fetch attempts failed for ${url}`);
}

/**
 * Create standardized error response
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

  console.error(`âŒ Error response: ${status} - ${message}`);

  return new Response(JSON.stringify(errorResponse, null, 2), {
    status: status,
    headers: headers
  });
}