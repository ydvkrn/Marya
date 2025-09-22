// Perfect browser-compatible MIME types
const MIME_TYPES = {
  'mp4': 'video/mp4',
  'webm': 'video/webm',
  'mkv': 'video/mp4', // Serve as MP4 for browser compatibility
  'mov': 'video/mp4',
  'avi': 'video/mp4',
  'm4v': 'video/mp4',
  'wmv': 'video/mp4',
  'flv': 'video/mp4',
  '3gp': 'video/mp4',
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'flac': 'audio/mpeg',
  'aac': 'audio/mp4',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg',
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp',
  'pdf': 'application/pdf',
  'txt': 'text/plain',
  'zip': 'application/zip'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

function isStreamable(mimeType) {
  return mimeType.startsWith('video/') || 
         mimeType.startsWith('audio/') || 
         mimeType.startsWith('image/') ||
         mimeType === 'application/pdf';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 ULTIMATE MEMORY-SAFE STREAMING:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    if (!actualId.startsWith('MSM')) {
      return new Response('❌ Invalid file ID format', { status: 404 });
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
      return new Response('🔍 File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    const { filename, size, totalChunks } = masterMetadata;
    
    console.log(`📁 File: ${filename} (${Math.round(size/1024/1024)}MB, ${totalChunks} chunks)`);

    // Always use memory-safe streaming for ALL files
    return await handleMemorySafeStreaming(request, kvNamespaces, masterMetadata, extension, env);

  } catch (error) {
    console.error('💥 Error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

// Memory-safe streaming that NEVER loads full file into memory
async function handleMemorySafeStreaming(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);
  
  console.log(`🎬 Memory-safe streaming: ${filename} (Type: ${mimeType})`);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';
  
  console.log(`📺 Mode: ${isDownload ? 'DOWNLOAD' : 'STREAM'}`);

  // Handle Range requests (essential for video streaming)
  const range = request.headers.get('Range');
  if (range && !isDownload) {
    console.log('📺 Range request:', range);
    return await handleRangeStreaming(request, kvNamespaces, masterMetadata, extension, range, env, mimeType);
  }

  // Memory-safe ReadableStream for ALL files (no memory limits)
  console.log('🌊 Creating memory-safe stream...');
  
  const readable = new ReadableStream({
    async start(controller) {
      try {
        console.log(`🌊 Starting chunk-by-chunk streaming (${chunks.length} chunks)...`);
        
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          console.log(`📦 Loading chunk ${i + 1}/${chunks.length}...`);
          
          // Load chunk data
          const chunkData = await getChunkSafely(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
          
          // Stream chunk immediately (no accumulation)
          controller.enqueue(new Uint8Array(chunkData));
          
          // Force garbage collection by nullifying the chunk data
          chunkData = null;
        }
        
        console.log('✅ All chunks streamed successfully');
        controller.close();
        
      } catch (error) {
        console.error('💥 Streaming error:', error);
        controller.error(error);
      }
    }
  });

  // Perfect headers for streaming
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges');
  headers.set('Cache-Control', 'public, max-age=86400');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
      // Essential for video streaming
      headers.set('X-Content-Type-Options', 'nosniff');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log(`🚀 Starting memory-safe ${isDownload ? 'download' : 'stream'} as ${mimeType}`);
  return new Response(readable, { status: 200, headers });
}

// Handle Range requests for video seeking (YouTube-style)
async function handleRangeStreaming(request, kvNamespaces, masterMetadata, extension, range, env, mimeType) {
  const { size, chunks } = masterMetadata;
  const chunkSize = masterMetadata.chunkSize || Math.ceil(size / chunks.length);
  
  const ranges = parseRange(range, size);
  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: { 
        'Content-Range': `bytes */${size}`,
        'Accept-Ranges': 'bytes'
      }
    });
  }

  const { start, end } = ranges[0];
  const requestedSize = end - start + 1;

  console.log(`📺 Range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  // Memory-efficient range handling
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Need chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // Create streaming response for range
  const readable = new ReadableStream({
    async start(controller) {
      try {
        let totalProcessed = 0;
        const rangeStart = start - (startChunk * chunkSize);
        
        for (let i = 0; i < neededChunks.length; i++) {
          const chunkInfo = neededChunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          const actualIndex = startChunk + i;
          
          // Load chunk
          const chunkData = await getChunkSafely(kvNamespace, chunkInfo.keyName, chunkInfo, env, actualIndex);
          
          let chunkSlice;
          
          if (i === 0 && neededChunks.length === 1) {
            // Single chunk, extract exact range
            chunkSlice = chunkData.slice(rangeStart, rangeStart + requestedSize);
          } else if (i === 0) {
            // First chunk, start from rangeStart
            chunkSlice = chunkData.slice(rangeStart);
          } else if (i === neededChunks.length - 1) {
            // Last chunk, take only what we need
            const remainingBytes = requestedSize - totalProcessed;
            chunkSlice = chunkData.slice(0, remainingBytes);
          } else {
            // Middle chunk, take all
            chunkSlice = chunkData;
          }
          
          controller.enqueue(new Uint8Array(chunkSlice));
          totalProcessed += chunkSlice.byteLength;
          
          // Clean up memory
          chunkData = null;
          chunkSlice = null;
        }
        
        controller.close();
        
      } catch (error) {
        console.error('Range streaming error:', error);
        controller.error(error);
      }
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=86400');
  headers.set('Content-Disposition', 'inline');

  console.log(`✅ Range streaming: ${Math.round(requestedSize/1024/1024)}MB`);
  return new Response(readable, { status: 206, headers });
}

// Safe chunk loading with auto-refresh
async function getChunkSafely(kvNamespace, keyName, chunkInfo, env, index) {
  console.log(`📦 Loading chunk ${index}: ${keyName}`);

  const chunkMetadataString = await kvNamespace.get(keyName);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${keyName} not found`);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;

  let response = await fetch(directUrl);

  // Auto-refresh expired URLs
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${index}, refreshing...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    for (const botToken of botTokens) {
      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`
        );

        if (!getFileResponse.ok) continue;

        const getFileData = await getFileResponse.json();
        if (!getFileData.ok || !getFileData.result?.file_path) continue;

        const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

        // Update KV
        const updatedMetadata = {
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now()
        };

        await kvNamespace.put(keyName, JSON.stringify(updatedMetadata));

        response = await fetch(freshUrl);
        if (response.ok) {
          console.log(`✅ URL refreshed for chunk ${index}`);
          break;
        }

      } catch (refreshError) {
        continue;
      }
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${index}: ${response.status}`);
  }

  return await response.arrayBuffer();
}

function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) return null;

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}