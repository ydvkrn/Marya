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

  console.log('=== LIGHTNING FAST SERVE ===');
  console.log('File ID:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    if (!actualId.startsWith('MSM')) {
      return new Response('Invalid file ID format', { status: 404 });
    }

    const kvNamespaces = {
      FILES_KV: env.FILES_KV,
      FILES_KV2: env.FILES_KV2,
      FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4,
      FILES_KV5: env.FILES_KV5,
      FILES_KV6: env.FILES_KV6,
      FILES_KV7: env.FILES_KV7
    };

    const masterMetadataString = await kvNamespaces.FILES_KV.get(actualId);
    if (!masterMetadataString) {
      return new Response('File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    console.log(`File found: ${masterMetadata.filename} (${masterMetadata.strategy})`);

    // Handle direct uploads (small files)
    if (masterMetadata.type === 'direct_upload') {
      return await handleDirectServe(request, masterMetadata, extension, env);
    }
    
    // Handle chunked uploads
    if (masterMetadata.type === 'chunked_upload') {
      return await handleChunkedServe(request, kvNamespaces, masterMetadata, extension, env);
    }

    return new Response('Unknown file type', { status: 500 });

  } catch (error) {
    console.error('Serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

// Serve direct uploaded files (super fast)
async function handleDirectServe(request, metadata, extension, env) {
  console.log('Serving direct upload');

  let directUrl = metadata.directUrl;

  // Check if URL is still valid
  let response = await fetch(directUrl, { method: 'HEAD' });

  if (!response.ok) {
    console.log('Refreshing expired URL...');
    
    const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
    
    for (const botToken of botTokens) {
      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${metadata.telegramFileId}`
        );
        const getFileData = await getFileResponse.json();
        
        if (getFileData.ok) {
          directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
          break;
        }
      } catch (e) {
        continue;
      }
    }
  }

  // Proxy the file
  response = await fetch(directUrl);
  if (!response.ok) {
    return new Response('File not accessible', { status: 404 });
  }

  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', metadata.size.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=31536000');

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
  }

  return new Response(response.body, { headers });
}

// Serve chunked files (optimized)
async function handleChunkedServe(request, kvNamespaces, metadata, extension, env) {
  console.log(`Serving chunked file: ${metadata.totalChunks} chunks`);

  const { chunks, totalChunks } = metadata;

  // Handle Range requests
  const range = request.headers.get('Range');
  if (range) {
    return await handleRangeRequest(request, kvNamespaces, metadata, extension, range, env);
  }

  // Get all chunks in small batches
  const BATCH_SIZE = 10;
  const allChunkData = new Array(totalChunks);

  for (let batchStart = 0; batchStart < totalChunks; batchStart += BATCH_SIZE) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE, totalChunks);
    const batchPromises = [];

    for (let i = batchStart; i < batchEnd; i++) {
      const chunkInfo = chunks[i];
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      batchPromises.push(getChunkData(kvNamespace, chunkInfo, env, i));
    }

    const batchResults = await Promise.all(batchPromises);
    batchResults.forEach(result => {
      allChunkData[result.index] = result.data;
    });
  }

  // Combine chunks
  const totalSize = allChunkData.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalSize);

  let offset = 0;
  for (const chunkData of allChunkData) {
    combinedBuffer.set(new Uint8Array(chunkData), offset);
    offset += chunkData.byteLength;
  }

  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', totalSize.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${metadata.filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
  }

  return new Response(combinedBuffer, { headers });
}

// Get chunk data with auto-refresh
async function getChunkData(kvNamespace, chunkInfo, env, index) {
  const chunkMetadataString = await kvNamespace.get(chunkInfo.keyName);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${index} not found`);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  let response = await fetch(chunkMetadata.directUrl);

  if (!response.ok) {
    // Refresh URL
    const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
    
    for (const botToken of botTokens) {
      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${chunkMetadata.telegramFileId}`
        );
        const getFileData = await getFileResponse.json();
        
        if (getFileData.ok) {
          const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
          response = await fetch(freshUrl);
          if (response.ok) break;
        }
      } catch (e) {
        continue;
      }
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${index}`);
  }

  return {
    index: index,
    data: await response.arrayBuffer()
  };
}

// Handle Range requests for video streaming
async function handleRangeRequest(request, kvNamespaces, metadata, extension, range, env) {
  console.log('Handling Range request:', range);

  const { size, chunkSize, chunks } = metadata;
  const ranges = parseRange(range, size);

  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { status: 416 });
  }

  const { start, end } = ranges[0];
  const requestedSize = end - start + 1;

  // Determine needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  // Get needed chunks
  const chunkPromises = neededChunks.map(async (chunkInfo) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    return await getChunkData(kvNamespace, chunkInfo, env, chunkInfo.index);
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

function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) return null;

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}
