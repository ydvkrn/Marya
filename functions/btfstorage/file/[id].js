// MIME type mapping
const MIME_TYPES = {
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
  'mov': 'video/quicktime', 'avi': 'video/x-msvideo', 'm4v': 'video/x-m4v',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json',
  'zip': 'application/zip', 'rar': 'application/vnd.rar',
  '7z': 'application/x-7z-compressed'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('=== ULTRA-FAST MICRO-CHUNK SERVE ===');
  console.log('Custom File ID:', fileId);

  try {
    // Extract actual ID (MSM format) and extension
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

    // Get master metadata from primary KV
    const masterMetadataString = await kvNamespaces.FILES_KV.get(actualId);

    if (!masterMetadataString) {
      return new Response('File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    console.log(`File found: ${masterMetadata.filename} (${masterMetadata.totalChunks} micro-chunks)`);

    // Handle micro-chunked files
    if (masterMetadata.type === 'micro_chunked_keys') {
      return await handleMicroChunkedFile(request, kvNamespaces, masterMetadata, extension, env);
    } else {
      // Legacy support
      return await handleLegacyFile(request, kvNamespaces, masterMetadata, extension, env);
    }

  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

// Handle micro-chunked files (YouTube/Instagram style)
async function handleMicroChunkedFile(request, kvNamespaces, masterMetadata, extension, env) {
  const { totalChunks, chunks, filename, size, chunkSize } = masterMetadata;
  console.log(`Serving micro-chunked file: ${filename} (${totalChunks} chunks × ${Math.round(chunkSize/1024)}KB)`);

  // Handle Range requests for ultra-smooth streaming
  const range = request.headers.get('Range');
  if (range) {
    return await handleRangeRequestMicro(request, kvNamespaces, masterMetadata, extension, range, env);
  }

  // Get all micro-chunks in parallel batches (like YouTube)
  const batchSize = 10; // Process 10 chunks simultaneously
  const allChunkData = new Array(totalChunks);

  for (let batchStart = 0; batchStart < totalChunks; batchStart += batchSize) {
    const batchEnd = Math.min(batchStart + batchSize, totalChunks);
    const batchPromises = [];

    for (let i = batchStart; i < batchEnd; i++) {
      const chunkInfo = chunks[i];
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      const keyName = chunkInfo.keyName;
      batchPromises.push(getMicroChunkFromKey(kvNamespace, keyName, chunkInfo, env, i));
    }

    console.log(`Fetching batch ${Math.floor(batchStart/batchSize) + 1}/${Math.ceil(totalChunks/batchSize)}`);
    const batchResults = await Promise.all(batchPromises);
    
    // Store results in correct order
    batchResults.forEach(result => {
      allChunkData[result.index] = result.data;
    });
  }

  // Combine all micro-chunks
  const totalSize = allChunkData.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalSize);

  let offset = 0;
  for (const chunkData of allChunkData) {
    combinedBuffer.set(new Uint8Array(chunkData), offset);
    offset += chunkData.byteLength;
  }

  // Response headers for optimal streaming
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
    if (mimeType.startsWith('video/') || mimeType.startsWith('audio/')) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log('✅ Ultra-fast micro-chunked file served successfully');
  return new Response(combinedBuffer, { status: 200, headers });
}

// Get micro-chunk from KV key with auto-refresh
async function getMicroChunkFromKey(kvNamespace, keyName, chunkInfo, env, index) {
  const chunkMetadataString = await kvNamespace.get(keyName);
  if (!chunkMetadataString) {
    throw new Error(`Micro-chunk key ${keyName} not found`);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;

  // Try to fetch micro-chunk
  let response = await fetch(directUrl);

  // If URL expired, refresh it (faster than before)
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for micro-chunk ${index}, refreshing...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    // Try refresh with first available bot token
    for (const BOT_TOKEN of botTokens) {
      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`
        );

        if (!getFileResponse.ok) continue;

        const getFileData = await getFileResponse.json();
        if (!getFileData.ok || !getFileData.result?.file_path) continue;

        // Create fresh URL
        const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;

        // Update KV key with fresh URL
        const updatedMetadata = {
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        };

        await kvNamespace.put(keyName, JSON.stringify(updatedMetadata));

        // Try with fresh URL
        response = await fetch(freshUrl);
        if (response.ok) break;

      } catch (refreshError) {
        console.error(`Failed to refresh micro-chunk ${index}:`, refreshError);
        continue;
      }
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch micro-chunk ${index}: ${response.status}`);
  }

  return {
    index: index,
    data: await response.arrayBuffer()
  };
}

// Handle Range requests for ultra-smooth video streaming
async function handleRangeRequestMicro(request, kvNamespaces, masterMetadata, extension, range, env) {
  console.log('Handling Range request for micro-chunks:', range);

  const { size, chunkSize, chunks } = masterMetadata;
  const ranges = parseRange(range, size);

  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { status: 416 });
  }

  const { start, end } = ranges[0];
  const requestedSize = end - start + 1;

  // Determine which micro-chunks are needed
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  // Get needed micro-chunks in parallel
  const chunkPromises = neededChunks.map(async (chunkInfo) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const keyName = chunkInfo.keyName;
    return await getMicroChunkFromKey(kvNamespace, keyName, chunkInfo, env, chunkInfo.index);
  });

  const chunkResults = await Promise.all(chunkPromises);
  chunkResults.sort((a, b) => a.index - b.index);

  // Combine and extract range
  const combinedSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(combinedSize);

  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  const rangeStart = start - (startChunk * chunkSize);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + requestedSize);

  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');

  return new Response(rangeBuffer, { status: 206, headers });
}

// Parse Range header
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) return null;

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}

// Legacy support
async function handleLegacyFile(request, kvNamespaces, metadata, extension, env) {
  console.log('Serving legacy file format');
  return new Response('Legacy format - please re-upload', { status: 501 });
}
