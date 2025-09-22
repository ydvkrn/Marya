// Enhanced MIME type mapping
const MIME_TYPES = {
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
  'mov': 'video/quicktime', 'avi': 'video/x-msvideo', 'm4v': 'video/x-m4v',
  'wmv': 'video/x-ms-wmv', 'flv': 'video/x-flv', '3gp': 'video/3gpp',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'aac': 'audio/aac', 'm4a': 'audio/mp4', 'ogg': 'audio/ogg',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json',
  'zip': 'application/zip', 'rar': 'application/vnd.rar',
  '7z': 'application/x-7z-compressed', 'tar': 'application/x-tar',
  'doc': 'application/msword', 'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'xls': 'application/vnd.ms-excel', 'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('=== FINAL 2GB STREAMING FILE SERVE ===');
  console.log('File ID:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    // Validate MSM ID format
    if (!actualId.startsWith('MSM')) {
      return new Response('Invalid file ID format', { status: 404 });
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
      return new Response('File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    console.log(`File: ${masterMetadata.filename} (${Math.round(masterMetadata.size/1024/1024)}MB, ${masterMetadata.totalChunks} chunks)`);

    // Handle chunked files with streaming
    if (masterMetadata.type === 'chunked_upload' || masterMetadata.type === 'url_import') {
      return await handleStreamingChunkedFile(request, kvNamespaces, masterMetadata, extension, env);
    } else {
      // Legacy support
      return await handleLegacyFile(request, kvNamespaces, masterMetadata, extension, env);
    }

  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

// Handle streaming for large files (up to 2GB)
async function handleStreamingChunkedFile(request, kvNamespaces, masterMetadata, extension, env) {
  const { totalChunks, chunks, filename, size, contentType } = masterMetadata;
  
  console.log(`Streaming ${filename}: ${Math.round(size/1024/1024)}MB in ${totalChunks} chunks`);

  // Enhanced MIME type detection
  const mimeType = contentType || getMimeType(extension);

  // Handle Range requests for large file streaming
  const range = request.headers.get('Range');
  if (range) {
    return await handleRangeRequestStreaming(request, kvNamespaces, masterMetadata, extension, range, env, mimeType);
  }

  // For large files (>100MB), use streaming response to avoid memory limits
  if (size > 100 * 1024 * 1024) {
    return await handleLargeFileStreaming(request, kvNamespaces, masterMetadata, extension, env, mimeType);
  }

  // For smaller files, use regular combining
  return await handleRegularChunkedFile(request, kvNamespaces, masterMetadata, extension, env, mimeType);
}

// Stream large files (>100MB) without loading into memory
async function handleLargeFileStreaming(request, kvNamespaces, masterMetadata, extension, env, mimeType) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`Large file streaming mode for ${filename} (${Math.round(size/1024/1024)}MB)`);

  // Create a ReadableStream that fetches chunks on demand
  const readable = new ReadableStream({
    async start(controller) {
      try {
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          console.log(`Streaming chunk ${i + 1}/${chunks.length}...`);
          
          const chunkData = await getChunkWithAutoRefresh(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
          
          // Enqueue chunk data to stream
          controller.enqueue(new Uint8Array(chunkData.data));
        }
        
        console.log('✅ All chunks streamed successfully');
        controller.close();
        
      } catch (error) {
        console.error('Streaming error:', error);
        controller.error(error);
      }
    }
  });

  // Response headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (mimeType.startsWith('video/') || mimeType.startsWith('audio/') || 
        mimeType.startsWith('image/') || mimeType === 'application/pdf') {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  return new Response(readable, { status: 200, headers });
}

// Handle Range requests for large file streaming (YouTube-like)
async function handleRangeRequestStreaming(request, kvNamespaces, masterMetadata, extension, range, env, mimeType) {
  console.log('Range request for large file streaming:', range);

  const { size, chunkSize = 8 * 1024 * 1024, chunks } = masterMetadata;
  const ranges = parseRange(range, size);

  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { status: 416 });
  }

  const { start, end } = ranges[0];
  const requestedSize = end - start + 1;

  // Determine which chunks are needed
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`Range request needs chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // Get needed chunks in parallel
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

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=31536000');

  return new Response(rangeBuffer, { status: 206, headers });
}

// Handle regular chunked files (<100MB)
async function handleRegularChunkedFile(request, kvNamespaces, masterMetadata, extension, env, mimeType) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`Regular chunked file: ${filename} (${chunks.length} chunks)`);

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

  // Response headers
  const headers = new Headers();
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
    if (mimeType.startsWith('video/') || mimeType.startsWith('audio/') || 
        mimeType.startsWith('image/') || mimeType === 'application/pdf') {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  return new Response(combinedBuffer, { status: 200, headers });
}

// Get chunk with auto-refresh and enhanced error handling
async function getChunkWithAutoRefresh(kvNamespace, keyName, chunkInfo, env, index) {
  console.log(`Getting chunk ${index}: ${keyName}`);

  const chunkMetadataString = await kvNamespace.get(keyName);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${keyName} not found`);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;

  // Try to fetch chunk
  let response = await fetch(directUrl);

  // If URL expired, refresh it
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
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`
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
          console.log(`✅ URL refreshed for chunk ${index}`);
          break;
        }

      } catch (refreshError) {
        console.error(`Failed to refresh chunk ${index}:`, refreshError);
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

// Parse Range header
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) return null;

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}

// Legacy file support
async function handleLegacyFile(request, kvNamespaces, masterMetadata, extension, env) {
  console.log('Serving legacy file format');
  return new Response('Legacy format - please re-upload for better performance', { status: 501 });
}