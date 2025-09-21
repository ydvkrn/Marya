// Memory-efficient MIME types
const EFFICIENT_MIME_TYPES = {
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
  'm4a': 'audio/mp4'
};

function getEfficientMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return EFFICIENT_MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🔥 LIGHTWEIGHT STREAMING:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    if (!actualId.startsWith('MSM')) {
      return new Response('Invalid file ID', { status: 404 });
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

    const metadataString = await kvNamespaces.FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    
    return await handleLightweightStreaming(request, kvNamespaces, metadata, extension, env);

  } catch (error) {
    console.error('Error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

async function handleLightweightStreaming(request, kvNamespaces, metadata, extension, env) {
  const { chunks, filename, size } = metadata;
  const mimeType = getEfficientMimeType(extension);
  
  console.log(`🔥 Processing: ${filename} (${Math.round(size/1024/1024)}MB, ${chunks.length} chunks)`);

  const url = new URL(request.url);
  const forceDownload = url.searchParams.has('dl');
  
  // Handle Range requests
  const range = request.headers.get('Range');
  if (range && !forceDownload) {
    return await handleLightweightRange(request, kvNamespaces, metadata, range, env, mimeType);
  }

  // Handle streaming with ReadableStream (memory efficient)
  return await handleEfficientStreaming(request, kvNamespaces, metadata, env, mimeType, forceDownload);
}

// Memory-efficient streaming with ReadableStream
async function handleEfficientStreaming(request, kvNamespaces, metadata, env, mimeType, forceDownload) {
  const { chunks, filename, size } = metadata;
  
  console.log(`🔥 Memory-efficient streaming: ${filename}`);

  const stream = new ReadableStream({
    async start(controller) {
      try {
        console.log('🔥 Starting efficient chunk streaming...');
        
        // Stream chunks one by one (memory efficient)
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          console.log(`🔥 Streaming chunk ${i + 1}/${chunks.length}...`);
          
          try {
            // Load single chunk (low memory usage)
            const chunkData = await loadSingleChunkEfficient(kvNamespace, chunkInfo, env, i);
            
            if (chunkData && chunkData.byteLength > 0) {
              // Stream immediately and release from memory
              controller.enqueue(new Uint8Array(chunkData));
              console.log(`✅ Chunk ${i + 1} streamed: ${Math.round(chunkData.byteLength/1024)}KB`);
              
              // Small delay to prevent memory buildup
              if (i < chunks.length - 1) {
                await new Promise(resolve => setTimeout(resolve, 50));
              }
            } else {
              console.error(`❌ Empty chunk ${i + 1}`);
              // Continue with next chunk instead of failing
              continue;
            }
            
          } catch (chunkError) {
            console.error(`❌ Chunk ${i + 1} failed:`, chunkError.message);
            
            // For video/audio, try to continue with next chunks
            if (mimeType.startsWith('video/') || mimeType.startsWith('audio/')) {
              console.log(`⚠️ Continuing stream despite chunk ${i + 1} failure`);
              continue;
            } else {
              // For downloads, we need all chunks
              throw chunkError;
            }
          }
        }
        
        console.log('✅ Efficient streaming completed');
        controller.close();
        
      } catch (criticalError) {
        console.error('🔥 Streaming error:', criticalError);
        controller.error(criticalError);
      }
    }
  });

  // Efficient headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Range');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
  
  if (forceDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
  }
  
  headers.set('Cache-Control', 'public, max-age=3600');
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('X-Streaming-Mode', 'lightweight-efficient');

  console.log(`🔥 Efficient streaming response ready`);
  return new Response(stream, { status: 200, headers });
}

// Lightweight range handling
async function handleLightweightRange(request, kvNamespaces, metadata, range, env, mimeType) {
  const { size, chunks } = metadata;
  const chunkSize = metadata.chunkSize || Math.ceil(size / chunks.length);
  
  console.log(`🔥 Lightweight range: ${range}`);

  const ranges = parseSimpleRange(range, size);
  if (!ranges) {
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: { 'Content-Range': `bytes */${size}` }
    });
  }

  const { start, end } = ranges[0];
  const requestedSize = end - start + 1;

  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`🔥 Range needs chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  try {
    // Load chunks sequentially (memory efficient)
    const chunkBuffers = [];
    
    for (let i = 0; i < neededChunks.length; i++) {
      const chunkInfo = neededChunks[i];
      const chunkIndex = startChunk + i;
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      
      const chunkData = await loadSingleChunkEfficient(kvNamespace, chunkInfo, env, chunkIndex);
      chunkBuffers.push(chunkData);
      
      console.log(`🔥 Range chunk ${chunkIndex} loaded: ${Math.round(chunkData.byteLength/1024)}KB`);
    }

    // Combine chunks efficiently
    const totalSize = chunkBuffers.reduce((sum, buffer) => sum + buffer.byteLength, 0);
    const combinedBuffer = new Uint8Array(totalSize);

    let offset = 0;
    for (const buffer of chunkBuffers) {
      combinedBuffer.set(new Uint8Array(buffer), offset);
      offset += buffer.byteLength;
    }

    // Extract exact range
    const rangeStart = start - (startChunk * chunkSize);
    const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + requestedSize);

    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', rangeBuffer.byteLength.toString());
    headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');

    console.log(`✅ Range response: ${rangeBuffer.byteLength} bytes`);
    return new Response(rangeBuffer, { status: 206, headers });

  } catch (error) {
    console.error('🔥 Range error:', error);
    return new Response(`Range error: ${error.message}`, { status: 500 });
  }
}

// Memory-efficient single chunk loader
async function loadSingleChunkEfficient(kvNamespace, chunkInfo, env, index) {
  const keyName = chunkInfo.keyName;
  
  console.log(`🔥 Loading efficient chunk ${index + 1}: ${keyName}`);

  // Get metadata
  let chunkMetadata;
  try {
    const metadataString = await kvNamespace.get(keyName);
    if (!metadataString) {
      throw new Error(`Chunk metadata not found: ${keyName}`);
    }
    chunkMetadata = JSON.parse(metadataString);
  } catch (kvError) {
    throw new Error(`KV error: ${kvError.message}`);
  }

  // Try direct URL first
  let directUrl = chunkMetadata.directUrl;
  
  try {
    console.log(`🔥 Direct fetch chunk ${index + 1}...`);
    
    const response = await fetch(directUrl, {
      signal: AbortSignal.timeout(30000)
    });

    if (response.ok) {
      const arrayBuffer = await response.arrayBuffer();
      console.log(`✅ Direct success: ${Math.round(arrayBuffer.byteLength/1024)}KB`);
      return arrayBuffer;
    } else {
      throw new Error(`HTTP ${response.status}`);
    }
    
  } catch (directError) {
    console.log(`❌ Direct failed: ${directError.message}`);
    
    // Quick single bot refresh (efficient)
    const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4]
      .filter(token => token);

    if (botTokens.length === 0) {
      throw new Error('No bot tokens available');
    }

    // Try first available bot
    const botToken = botTokens[0];
    
    try {
      console.log(`🔄 Quick refresh for chunk ${index + 1}...`);
      
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      if (getFileResponse.ok) {
        const getFileData = await getFileResponse.json();
        
        if (getFileData.ok && getFileData.result?.file_path) {
          const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

          const freshResponse = await fetch(freshUrl, {
            signal: AbortSignal.timeout(30000)
          });

          if (freshResponse.ok) {
            const arrayBuffer = await freshResponse.arrayBuffer();
            console.log(`✅ Refresh success: ${Math.round(arrayBuffer.byteLength/1024)}KB`);
            
            // Update KV async
            const updatedMetadata = {
              ...chunkMetadata,
              directUrl: freshUrl,
              lastRefreshed: Date.now()
            };
            
            kvNamespace.put(keyName, JSON.stringify(updatedMetadata)).catch(() => {});
            
            return arrayBuffer;
          }
        }
      }
    } catch (refreshError) {
      console.error(`🔄 Refresh failed: ${refreshError.message}`);
    }

    throw new Error(`Chunk ${index + 1} load failed completely`);
  }
}

// Simple range parser
function parseSimpleRange(range, size) {
  const match = range.match(/bytes=(\d+)-(\d*)/);
  if (!match) return null;

  const start = parseInt(match[1], 10);
  const end = match[2] ? parseInt(match[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}

console.log('🔥 LIGHTWEIGHT STREAMING SYSTEM READY!');
