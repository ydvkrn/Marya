// Simple MIME types for instant browser support
const MIME_TYPES = {
  'mp4': 'video/mp4',
  'mkv': 'video/mp4', // Serve MKV as MP4 for instant play
  'mov': 'video/mp4',
  'avi': 'video/mp4',
  'webm': 'video/webm',
  '3gp': 'video/mp4',
  'flv': 'video/mp4',
  'wmv': 'video/mp4',
  'm4v': 'video/mp4',
  
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'aac': 'audio/mp4',
  'm4a': 'audio/mp4',
  'flac': 'audio/mpeg',
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
  return mimeType.startsWith('video/') || mimeType.startsWith('audio/') || mimeType.startsWith('image/');
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 ULTIMATE STREAMING:', fileId);

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

    return await handleUltimateStreaming(request, kvNamespaces, masterMetadata, extension, env);

  } catch (error) {
    console.error('💥 Error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

async function handleUltimateStreaming(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);
  
  console.log(`🎬 Ultimate streaming: ${filename} (Type: ${mimeType})`);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';
  
  console.log(`📺 Mode: ${isDownload ? 'DOWNLOAD' : 'STREAM'}`);

  // Handle Range requests for video seeking
  const range = request.headers.get('Range');
  if (range && !isDownload) {
    console.log('📺 Range request:', range);
    return await handleRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env, mimeType);
  }

  // Progressive streaming
  if (!isDownload && isStreamable(mimeType)) {
    console.log('🚀 Progressive streaming...');
    return await handleProgressiveStream(request, kvNamespaces, masterMetadata, extension, env, mimeType);
  }

  // Full download
  console.log('💾 Full download...');
  return await handleFullDownload(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload);
}

// Progressive streaming for instant video playback
async function handleProgressiveStream(request, kvNamespaces, masterMetadata, extension, env, mimeType) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`🚀 Progressive streaming: ${filename}`);

  const readable = new ReadableStream({
    async start(controller) {
      try {
        // First load 3 chunks immediately for instant start
        console.log('📺 Loading first 3 chunks for instant playback...');
        
        const firstChunks = chunks.slice(0, Math.min(3, chunks.length));
        
        for (let i = 0; i < firstChunks.length; i++) {
          const chunkInfo = firstChunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          const chunkData = await getChunk(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
          controller.enqueue(new Uint8Array(chunkData));
          
          console.log(`⚡ Instant chunk ${i + 1}/${firstChunks.length} streamed`);
        }

        // Now load remaining chunks progressively
        console.log('📺 Loading remaining chunks progressively...');
        
        for (let i = 3; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          try {
            const chunkData = await getChunk(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
            controller.enqueue(new Uint8Array(chunkData));
            
            console.log(`📺 Progressive chunk ${i + 1}/${chunks.length} streamed`);
            
            // Small delay for smooth streaming
            await new Promise(resolve => setTimeout(resolve, 200));
            
          } catch (chunkError) {
            console.error(`❌ Chunk ${i} failed, continuing:`, chunkError);
            continue;
          }
        }
        
        console.log('✅ Progressive streaming completed');
        controller.close();
        
      } catch (error) {
        console.error('💥 Progressive streaming error:', error);
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
  headers.set('Cache-Control', 'public, max-age=86400');
  
  // Force inline for video playback
  headers.set('Content-Disposition', 'inline');
  headers.set('X-Content-Type-Options', 'nosniff');

  console.log(`🎬 Progressive stream started as ${mimeType}`);
  return new Response(readable, { status: 200, headers });
}

// Range requests for video seeking
async function handleRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env, mimeType) {
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

  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Loading chunks ${startChunk}-${endChunk} for seeking`);

  // Load chunks in small batches
  const chunkResults = [];
  const BATCH_SIZE = 4; // 4 chunks at a time
  
  for (let i = 0; i < neededChunks.length; i += BATCH_SIZE) {
    const batchChunks = neededChunks.slice(i, Math.min(i + BATCH_SIZE, neededChunks.length));
    
    const batchPromises = batchChunks.map(async (chunkInfo, batchIndex) => {
      const actualIndex = startChunk + i + batchIndex;
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      
      const chunkData = await getChunk(kvNamespace, chunkInfo.keyName, chunkInfo, env, actualIndex);
      return {
        index: actualIndex,
        data: chunkData
      };
    });
    
    const batchResults = await Promise.all(batchPromises);
    chunkResults.push(...batchResults);
    
    // Small delay between batches
    if (i + BATCH_SIZE < neededChunks.length) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  // Sort and combine chunks
  chunkResults.sort((a, b) => a.index - b.index);

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
  headers.set('Cache-Control', 'public, max-age=86400');
  headers.set('Content-Disposition', 'inline');

  console.log(`✅ Range served: ${Math.round(requestedSize/1024/1024)}MB`);
  return new Response(rangeBuffer, { status: 206, headers });
}

// Full download with high speed
async function handleFullDownload(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`💾 Full download: ${filename}`);

  const readable = new ReadableStream({
    async start(controller) {
      try {
        // Load chunks in parallel batches for speed
        const PARALLEL_SIZE = 5; // 5 chunks at once
        
        for (let batchStart = 0; batchStart < chunks.length; batchStart += PARALLEL_SIZE) {
          const batchEnd = Math.min(batchStart + PARALLEL_SIZE, chunks.length);
          const batchChunks = chunks.slice(batchStart, batchEnd);
          
          console.log(`💾 Download batch ${Math.floor(batchStart/PARALLEL_SIZE) + 1}/${Math.ceil(chunks.length/PARALLEL_SIZE)}`);
          
          // Load batch in parallel
          const batchPromises = batchChunks.map(async (chunkInfo, index) => {
            const actualIndex = batchStart + index;
            const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
            
            return {
              index: actualIndex,
              data: await getChunk(kvNamespace, chunkInfo.keyName, chunkInfo, env, actualIndex)
            };
          });
          
          const batchResults = await Promise.all(batchPromises);
          batchResults.sort((a, b) => a.index - b.index);
          
          // Stream batch immediately
          for (const result of batchResults) {
            controller.enqueue(new Uint8Array(result.data));
            console.log(`💾 Downloaded chunk ${result.index + 1}/${chunks.length}`);
          }
        }
        
        controller.close();
        
      } catch (error) {
        controller.error(error);
      }
    }
  });

  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=86400');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
  }

  return new Response(readable, { status: 200, headers });
}

// Simple chunk loader with auto-refresh
async function getChunk(kvNamespace, keyName, chunkInfo, env, index) {
  console.log(`📦 Loading chunk ${index}: ${keyName}`);

  let chunkMetadata;
  try {
    const chunkMetadataString = await kvNamespace.get(keyName);
    if (!chunkMetadataString) {
      throw new Error(`Chunk ${keyName} not found`);
    }
    chunkMetadata = JSON.parse(chunkMetadataString);
  } catch (kvError) {
    throw new Error(`KV error for chunk ${keyName}: ${kvError.message}`);
  }

  let directUrl = chunkMetadata.directUrl;
  let response = await fetch(directUrl);

  // Auto-refresh if URL expired
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 Refreshing URL for chunk ${index}...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    if (botTokens.length > 0) {
      const botToken = botTokens[0];
      
      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`
        );

        if (getFileResponse.ok) {
          const getFileData = await getFileResponse.json();
          if (getFileData.ok && getFileData.result?.file_path) {
            const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

            // Update KV
            const updatedMetadata = {
              ...chunkMetadata,
              directUrl: freshUrl,
              lastRefreshed: Date.now()
            };
            
            kvNamespace.put(keyName, JSON.stringify(updatedMetadata)).catch(() => {});

            response = await fetch(freshUrl);
            
            if (response.ok) {
              console.log(`✅ URL refreshed for chunk ${index}`);
            }
          }
        }
      } catch (refreshError) {
        console.error(`❌ Failed to refresh chunk ${index}:`, refreshError.message);
      }
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${index}: HTTP ${response.status}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  console.log(`✅ Chunk ${index} loaded: ${Math.round(arrayBuffer.byteLength/1024)}KB`);

  return arrayBuffer;
}

// Simple range parser
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) return null;

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}
