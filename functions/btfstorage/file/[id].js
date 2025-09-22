// Enhanced MIME type mapping for streaming
const MIME_TYPES = {
  // Video formats (streamable)
  'mp4': 'video/mp4',
  'webm': 'video/webm', 
  'mkv': 'video/x-matroska',
  'mov': 'video/quicktime',
  'avi': 'video/x-msvideo',
  'm4v': 'video/x-m4v',
  'wmv': 'video/x-ms-wmv',
  'flv': 'video/x-flv',
  '3gp': 'video/3gpp',
  'mpg': 'video/mpeg',
  'mpeg': 'video/mpeg',
  
  // Audio formats (streamable)
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'flac': 'audio/flac',
  'aac': 'audio/aac',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg',
  'wma': 'audio/x-ms-wma',
  
  // Image formats (viewable)
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
  'txt': 'text/plain',
  'json': 'application/json',
  'html': 'text/html',
  'css': 'text/css',
  'js': 'application/javascript',
  
  // Archive formats
  'zip': 'application/zip',
  'rar': 'application/vnd.rar',
  '7z': 'application/x-7z-compressed',
  'tar': 'application/x-tar',
  'gz': 'application/gzip'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

// Check if file type is streamable
function isStreamable(mimeType) {
  return mimeType.startsWith('video/') || 
         mimeType.startsWith('audio/') || 
         mimeType.startsWith('image/') ||
         mimeType === 'application/pdf' ||
         mimeType.startsWith('text/');
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 ULTIMATE STREAM/DOWNLOAD SERVE:', fileId);

  try {
    // Extract file ID and extension
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    // Validate MSM ID format
    if (!actualId.startsWith('MSM')) {
      return new Response('❌ Invalid file ID format', { status: 404 });
    }

    // All KV namespaces
    const kvNamespaces = {
      FILES_KV: env.FILES_KV,
      FILES_KV2: env.FILES_KV2,
      FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4,
      FILES_KV5: env.FILES_KV5,
      FILES_KV6: env.FILES_KV6,
      FILES_KV7: env.FILES_KV7
    };

    // Get master metadata
    const masterMetadataString = await kvNamespaces.FILES_KV.get(actualId);
    if (!masterMetadataString) {
      return new Response('🔍 File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    const { filename, size, totalChunks, contentType } = masterMetadata;
    
    console.log(`📁 File: ${filename} (${Math.round(size/1024/1024)}MB, ${totalChunks} chunks)`);

    // Handle chunked files (new format)
    if (masterMetadata.type === 'chunked_upload' || masterMetadata.type === 'url_import' || masterMetadata.type === 'ultimate_streaming') {
      return await handleStreamingFile(request, kvNamespaces, masterMetadata, extension, env);
    } 
    // Handle legacy formats
    else if (masterMetadata.type === 'multi_kv_chunked') {
      return await handleLegacyChunkedFile(request, kvNamespaces, masterMetadata, extension, env);
    } 
    // Handle single files
    else {
      return await handleSingleFile(request, kvNamespaces.FILES_KV, actualId, extension, masterMetadata, env);
    }

  } catch (error) {
    console.error('💥 File serve error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

// Handle streaming files with perfect browser compatibility
async function handleStreamingFile(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size, contentType } = masterMetadata;
  const mimeType = contentType || getMimeType(extension);
  
  console.log(`🎬 Streaming: ${filename} (Type: ${mimeType})`);

  // Check URL parameters
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');
  const isStream = url.searchParams.has('stream') || !isDownload;

  // Handle Range requests for streaming (YouTube-style)
  const range = request.headers.get('Range');
  if (range && isStream && isStreamable(mimeType)) {
    console.log('📺 Range request for streaming:', range);
    return await handleRangeRequestStreaming(request, kvNamespaces, masterMetadata, extension, range, env, mimeType);
  }

  // For large files (>100MB), use streaming response
  if (size > 100 * 1024 * 1024 && isStream) {
    console.log('🌊 Large file streaming mode');
    return await handleLargeFileStreaming(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload);
  }

  // Regular file serving for smaller files
  console.log('📄 Regular file serving');
  return await handleRegularFileServing(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload);
}

// Handle Range requests for perfect video streaming
async function handleRangeRequestStreaming(request, kvNamespaces, masterMetadata, extension, range, env, mimeType) {
  const { size, chunks } = masterMetadata;
  const chunkSize = masterMetadata.chunkSize || Math.ceil(size / chunks.length);
  
  // Parse range header
  const ranges = parseRange(range, size);
  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: { 'Content-Range': `bytes */${size}` }
    });
  }

  const { start, end } = ranges[0];
  const requestedSize = end - start + 1;

  console.log(`📺 Streaming range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  // Determine which chunks are needed
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Need chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // Get needed chunks with auto-refresh
  const chunkPromises = neededChunks.map(async (chunkInfo, index) => {
    const actualIndex = startChunk + index;
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    return await getChunkWithAutoRefresh(kvNamespace, chunkInfo.keyName, chunkInfo, env, actualIndex);
  });

  const chunkResults = await Promise.all(chunkPromises);
  chunkResults.sort((a, b) => a.index - b.index);

  // Combine chunks efficiently
  const combinedSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(combinedSize);

  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  // Extract exact range
  const rangeStart = start - (startChunk * chunkSize);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + requestedSize);

  // Perfect streaming headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('Content-Disposition', 'inline'); // Always inline for streaming

  console.log(`✅ Streaming ${Math.round(requestedSize/1024/1024)}MB range`);
  return new Response(rangeBuffer, { status: 206, headers });
}

// Handle large file streaming without memory limits
async function handleLargeFileStreaming(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`🌊 Large file streaming: ${filename} (${Math.round(size/1024/1024)}MB)`);

  // Create streaming response
  const readable = new ReadableStream({
    async start(controller) {
      try {
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          console.log(`🌊 Streaming chunk ${i + 1}/${chunks.length}...`);
          
          const chunkData = await getChunkWithAutoRefresh(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
          controller.enqueue(new Uint8Array(chunkData.data));
        }
        
        console.log('✅ All chunks streamed successfully');
        controller.close();
        
      } catch (error) {
        console.error('💥 Streaming error:', error);
        controller.error(error);
      }
    }
  });

  // Perfect streaming headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  return new Response(readable, { status: 200, headers });
}

// Handle regular file serving for smaller files
async function handleRegularFileServing(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`📄 Regular serving: ${filename} (${chunks.length} chunks)`);

  // Get all chunks in parallel
  const chunkPromises = chunks.map(async (chunkInfo, index) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    return await getChunkWithAutoRefresh(kvNamespace, chunkInfo.keyName, chunkInfo, env, index);
  });

  const chunkResults = await Promise.all(chunkPromises);
  chunkResults.sort((a, b) => a.index - b.index);

  // Combine all chunks
  const totalSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalSize);

  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  // Perfect headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log(`✅ File served: ${Math.round(totalSize/1024/1024)}MB`);
  return new Response(combinedBuffer, { status: 200, headers });
}

// Get chunk with auto-refresh and multi-bot fallback
async function getChunkWithAutoRefresh(kvNamespace, keyName, chunkInfo, env, index) {
  console.log(`📦 Getting chunk ${index}: ${keyName}`);

  const chunkMetadataString = await kvNamespace.get(keyName);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${keyName} not found`);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;

  // Try to fetch chunk
  let response = await fetch(directUrl);

  // If URL expired, refresh with multi-bot fallback
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${index}, refreshing...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    // Try refresh with multiple bot tokens
    for (const botToken of botTokens) {
      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { signal: AbortSignal.timeout(15000) }
        );

        if (!getFileResponse.ok) continue;

        const getFileData = await getFileResponse.json();
        if (!getFileData.ok || !getFileData.result?.file_path) continue;

        // Create fresh URL
        const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

        // Update KV with fresh URL
        const updatedMetadata = {
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now(),
          refreshCount: (chunkMetadata.refreshCount || 0) + 1
        };

        await kvNamespace.put(keyName, JSON.stringify(updatedMetadata));

        // Try with fresh URL
        response = await fetch(freshUrl);
        if (response.ok) {
          console.log(`✅ URL refreshed for chunk ${index} using bot ending ...${botToken.slice(-4)}`);
          break;
        }

      } catch (refreshError) {
        console.error(`Failed to refresh chunk ${index} with bot ${botToken.slice(-4)}:`, refreshError);
        continue;
      }
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${index}: ${response.status}`);
  }

  return {
    index: index,
    data: await response.arrayBuffer()
  };
}

// Handle legacy chunked files
async function handleLegacyChunkedFile(request, kvNamespaces, masterMetadata, extension, env) {
  const { totalChunks, chunks, filename, size } = masterMetadata;
  console.log(`📂 Legacy chunked file: ${filename} (${totalChunks} chunks)`);

  // Handle Range requests
  const range = request.headers.get('Range');
  if (range) {
    return await handleLegacyRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env);
  }

  // Get all chunks
  const chunkPromises = chunks.map(async (chunkInfo, index) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.chunkKey || `${masterMetadata.id}_chunk_${index}`;
    return await getChunkWithAutoRefresh(kvNamespace, chunkKey, chunkInfo, env, index);
  });

  const chunkResults = await Promise.all(chunkPromises);
  chunkResults.sort((a, b) => a.index - b.index);

  const totalSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalSize);

  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  const headers = new Headers();
  const mimeType = getMimeType(extension);
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log('✅ Legacy chunked file served successfully');
  return new Response(combinedBuffer, { status: 200, headers });
}

// Handle legacy range requests
async function handleLegacyRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env) {
  console.log('📺 Legacy Range request:', range);

  const { size } = masterMetadata;
  const ranges = parseRange(range, size);
  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { status: 416 });
  }

  const { start, end } = ranges[0];
  const chunkSize = end - start + 1;

  // Simple implementation for legacy files
  const CHUNK_SIZE = 20 * 1024 * 1024;
  const startChunk = Math.floor(start / CHUNK_SIZE);
  const endChunk = Math.floor(end / CHUNK_SIZE);
  const neededChunks = masterMetadata.chunks.slice(startChunk, endChunk + 1);

  const chunkPromises = neededChunks.map(async (chunkInfo) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.chunkKey;
    return await getChunkWithAutoRefresh(kvNamespace, chunkKey, chunkInfo, env, chunkInfo.index);
  });

  const chunkResults = await Promise.all(chunkPromises);
  chunkResults.sort((a, b) => a.index - b.index);

  const combinedSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(combinedSize);

  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  const rangeStart = start - (startChunk * CHUNK_SIZE);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + chunkSize);

  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', chunkSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');

  return new Response(rangeBuffer, { status: 206, headers });
}

// Handle single files
async function handleSingleFile(request, kvNamespace, actualId, extension, metadata, env) {
  console.log('📄 Serving single file (legacy)');

  const directUrl = await kvNamespace.get(actualId);
  if (!directUrl) {
    return new Response('File not found', { status: 404 });
  }

  let response = await fetch(directUrl);

  // Auto-refresh single file URL if expired
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log('🔄 Single file URL expired, refreshing...');

    const BOT_TOKEN = env.BOT_TOKEN;
    const telegramFileId = metadata?.telegramFileId;

    if (BOT_TOKEN && telegramFileId) {
      try {
        const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
        if (getFileResponse.ok) {
          const getFileData = await getFileResponse.json();
          if (getFileData.ok && getFileData.result?.file_path) {
            const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
            await kvNamespace.put(actualId, freshUrl);
            console.log('✅ Single file URL refreshed');
            response = await fetch(freshUrl);
          }
        }
      } catch (refreshError) {
        console.error('Failed to refresh single file URL:', refreshError);
      }
    }
  }

  if (!response.ok) {
    return new Response(`File not accessible: ${response.status}`, { status: response.status });
  }

  const headers = new Headers();
  const mimeType = getMimeType(extension);
  headers.set('Content-Type', mimeType);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=3600');

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');
  const filename = metadata?.filename || 'download';

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  return new Response(response.body, { status: response.status, headers });
}

// Parse Range header
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) return null;

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}