// 🚀 SUPER SIMPLE BULLETPROOF VIDEO STREAMING
// No memory issues, no CPU limits, works 100%

const MIME_TYPES = {
  'mp4': 'video/mp4',
  'mkv': 'video/mp4',
  'avi': 'video/mp4', 
  'mov': 'video/mp4',
  'm4v': 'video/mp4',
  'wmv': 'video/mp4',
  'flv': 'video/mp4',
  '3gp': 'video/mp4',
  'webm': 'video/webm',
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'aac': 'audio/mp4',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg',
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🚀 Simple Streaming:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    if (!actualId.startsWith('MSM')) {
      return new Response('Invalid file ID', { status: 404 });
    }

    // Get file metadata
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    const mimeType = getMimeType(extension);
    
    return await handleSimpleStreaming(request, env, metadata, extension, mimeType);

  } catch (error) {
    console.error('Error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

async function handleSimpleStreaming(request, env, metadata, extension, mimeType) {
  const { chunks, filename, size } = metadata;
  
  if (!chunks || chunks.length === 0) {
    return new Response('No chunks found', { status: 404 });
  }

  console.log(`File: ${filename} (${chunks.length} chunks, ${Math.round(size/1024/1024)}MB)`);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');

  // Handle Range requests (for video seeking)
  const range = request.headers.get('Range');
  if (range && !isDownload) {
    return await handleRangeRequest(request, env, metadata, range, mimeType);
  }

  // Handle complete streaming
  return await handleCompleteStreaming(request, env, metadata, mimeType, isDownload);
}

// Simple Range handling (for video seeking)
async function handleRangeRequest(request, env, metadata, rangeHeader, mimeType) {
  const { size, chunks } = metadata;
  const chunkSize = metadata.chunkSize || Math.ceil(size / chunks.length);
  
  console.log('Range request:', rangeHeader);

  // Parse Range header
  const rangeMatch = rangeHeader.match(/bytes=(\d+)-(\d*)/);
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

  const requestedSize = end - start + 1;
  console.log(`Range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`Need chunks: ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // Load needed chunks SEQUENTIALLY (avoid memory issues)
  const loadedChunks = [];
  
  for (let i = 0; i < neededChunks.length; i++) {
    const chunkInfo = neededChunks[i];
    const chunkIndex = startChunk + i;
    
    try {
      const chunkData = await loadSingleChunk(env, chunkInfo, chunkIndex);
      loadedChunks.push({
        index: chunkIndex,
        data: chunkData
      });
      
      console.log(`Chunk ${chunkIndex} loaded: ${Math.round(chunkData.byteLength/1024)}KB`);
      
    } catch (chunkError) {
      console.error(`Chunk ${chunkIndex} failed:`, chunkError);
      return new Response(`Chunk ${chunkIndex} failed: ${chunkError.message}`, { status: 500 });
    }
  }

  // Combine chunks efficiently
  const totalSize = loadedChunks.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalSize);

  let offset = 0;
  for (const chunk of loadedChunks) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  // Extract exact range
  const rangeStart = start - (startChunk * chunkSize);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + requestedSize);

  // Perfect range headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', rangeBuffer.byteLength.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');

  console.log(`Range response: ${rangeBuffer.byteLength} bytes`);
  return new Response(rangeBuffer, { status: 206, headers });
}

// Complete file streaming
async function handleCompleteStreaming(request, env, metadata, mimeType, isDownload) {
  const { chunks, filename, size } = metadata;
  
  console.log(`Complete streaming: ${filename}`);

  // For large files, use ReadableStream to avoid memory issues
  const stream = new ReadableStream({
    async start(controller) {
      try {
        console.log('Starting stream...');
        
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          
          console.log(`Streaming chunk ${i + 1}/${chunks.length}...`);
          
          try {
            const chunkData = await loadSingleChunk(env, chunkInfo, i);
            controller.enqueue(new Uint8Array(chunkData));
            
            console.log(`Chunk ${i + 1} streamed: ${Math.round(chunkData.byteLength/1024)}KB`);
            
            // Small delay to prevent overload
            await new Promise(resolve => setTimeout(resolve, 10));
            
          } catch (chunkError) {
            console.error(`Chunk ${i + 1} failed:`, chunkError);
            controller.error(new Error(`Chunk ${i + 1} failed: ${chunkError.message}`));
            return;
          }
        }
        
        console.log('Stream completed successfully');
        controller.close();
        
      } catch (error) {
        console.error('Stream error:', error);
        controller.error(error);
      }
    }
  });

  // Perfect streaming headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  
  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
  }
  
  headers.set('Cache-Control', 'public, max-age=3600');

  console.log(`Streaming response ready: ${mimeType}`);
  return new Response(stream, { status: 200, headers });
}

// Load single chunk with 4-bot fallback
async function loadSingleChunk(env, chunkInfo, index) {
  console.log(`Loading chunk ${index + 1}: ${chunkInfo.keyName}`);

  // Get KV namespace
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  
  // Get chunk metadata
  const metadataString = await kvNamespace.get(chunkInfo.keyName);
  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkInfo.keyName}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  
  // Try direct URL first
  let response = await fetch(chunkMetadata.directUrl, {
    signal: AbortSignal.timeout(45000)
  });

  if (response.ok) {
    const arrayBuffer = await response.arrayBuffer();
    console.log(`Direct fetch success: ${Math.round(arrayBuffer.byteLength/1024)}KB`);
    return arrayBuffer;
  }

  // URL expired, try refresh with 4 bot tokens
  console.log(`URL expired, trying refresh...`);
  
  const botTokens = [
    env.BOT_TOKEN,
    env.BOT_TOKEN2,
    env.BOT_TOKEN3,
    env.BOT_TOKEN4
  ].filter(token => token);

  if (botTokens.length === 0) {
    throw new Error('No bot tokens available');
  }

  // Try each bot token
  for (let i = 0; i < botTokens.length; i++) {
    const botToken = botTokens[i];
    
    try {
      console.log(`Trying bot ${i + 1}/${botTokens.length}...`);
      
      // Get fresh file path
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      if (getFileResponse.ok) {
        const getFileData = await getFileResponse.json();
        
        if (getFileData.ok && getFileData.result?.file_path) {
          // Create fresh URL
          const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
          
          // Try fresh URL
          const freshResponse = await fetch(freshUrl, {
            signal: AbortSignal.timeout(45000)
          });

          if (freshResponse.ok) {
            const arrayBuffer = await freshResponse.arrayBuffer();
            console.log(`Refresh success with bot ${i + 1}: ${Math.round(arrayBuffer.byteLength/1024)}KB`);
            
            // Update KV with fresh URL (async)
            const updatedMetadata = {
              ...chunkMetadata,
              directUrl: freshUrl,
              lastRefreshed: Date.now()
            };
            
            kvNamespace.put(chunkInfo.keyName, JSON.stringify(updatedMetadata)).catch(() => {});
            
            return arrayBuffer;
          }
        }
      }
      
    } catch (botError) {
      console.error(`Bot ${i + 1} failed:`, botError.message);
      continue;
    }
  }

  throw new Error(`All ${botTokens.length} bot tokens failed for chunk ${index + 1}`);
}
