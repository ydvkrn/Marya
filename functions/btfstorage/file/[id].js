// Ultra-optimized for instant video/image display
const MIME_TYPES = {
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
  'mov': 'video/quicktime', 'avi': 'video/x-msvideo', 'm4v': 'video/x-m4v',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'pdf': 'application/pdf', 'txt': 'text/plain'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params, waitUntil } = context;
  const fileId = params.id;

  console.log('=== INSTANT MEDIA SERVING ===');

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';
    
    // ✅ STEP 1: Lightning-fast cache check
    const cacheKey = new Request(`https://instant-cache.marya.vault/${fileId}`, request);
    const cache = caches.default;
    
    let cachedResponse = await cache.match(cacheKey);
    if (cachedResponse) {
      console.log('🚀 INSTANT CACHE HIT - Serving immediately!');
      
      const headers = new Headers(cachedResponse.headers);
      headers.set('X-Cache', 'INSTANT-HIT');
      headers.set('X-Speed', 'LIGHTNING');
      
      return new Response(cachedResponse.body, {
        status: cachedResponse.status,
        headers: headers
      });
    }

    // ✅ STEP 2: Get metadata and start streaming immediately
    const kvNamespaces = {
      FILES_KV: env.FILES_KV, FILES_KV2: env.FILES_KV2, FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4, FILES_KV5: env.FILES_KV5, FILES_KV6: env.FILES_KV6, FILES_KV7: env.FILES_KV7
    };

    const masterMetadataString = await kvNamespaces.FILES_KV.get(actualId);
    if (!masterMetadataString) {
      return new Response('File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    const mimeType = getMimeType(extension);
    
    console.log(`⚡ Starting instant streaming: ${masterMetadata.filename}`);

    // ✅ STEP 3: Choose optimal streaming strategy
    let response;
    
    if (masterMetadata.type === 'multi_kv_chunked') {
      response = await instantChunkedStreaming(request, kvNamespaces, masterMetadata, extension, env, waitUntil);
    } else {
      response = await instantSingleFileStreaming(request, kvNamespaces.FILES_KV, actualId, extension, masterMetadata, env);
    }

    // ✅ STEP 4: Cache for instant future access
    if (response.ok) {
      const responseToCache = response.clone();
      const cacheHeaders = new Headers(responseToCache.headers);
      
      // Ultra-aggressive caching for instant access
      cacheHeaders.set('Cache-Control', 'public, max-age=31536000, immutable');
      cacheHeaders.set('CDN-Cache-Control', 'public, max-age=31536000, immutable');
      cacheHeaders.set('X-Cache', 'STORED');
      
      const cachedResponseFinal = new Response(responseToCache.body, {
        status: responseToCache.status,
        headers: cacheHeaders
      });
      
      // Background cache - no blocking
      waitUntil(cache.put(cacheKey, cachedResponseFinal.clone()));
    }

    return response;

  } catch (error) {
    console.error('❌ Instant serving error:', error);
    return new Response(`Error: ${error.message}`, { status: 500 });
  }
}

// ✅ INSTANT chunked file streaming with progressive loading
async function instantChunkedStreaming(request, kvNamespaces, masterMetadata, extension, env, waitUntil) {
  const { filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);
  
  console.log(`🎬 INSTANT chunked streaming: ${filename}`);

  // ✅ Handle Range requests for instant video seeking
  const range = request.headers.get('Range');
  if (range) {
    return await instantRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env);
  }

  // ✅ Create instant streaming response
  const readable = new ReadableStream({
    async start(controller) {
      try {
        console.log('🚀 Starting progressive chunk streaming...');
        
        // ✅ Load chunks progressively - start streaming immediately
        const chunks = masterMetadata.chunks;
        
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          const chunkKey = chunkInfo.chunkKey || `${actualId}_chunk_${i}`;
          
          console.log(`📦 Streaming chunk ${i}/${chunks.length}`);
          
          try {
            // ✅ Get chunk with instant fallback
            const chunkData = await instantGetChunk(kvNamespace, chunkKey, chunkInfo, env, waitUntil);
            
            // ✅ Stream chunk immediately - no buffering
            controller.enqueue(new Uint8Array(chunkData));
            
            // Small delay to prevent overwhelming the stream
            if (i < chunks.length - 1) {
              await new Promise(resolve => setTimeout(resolve, 10));
            }
            
          } catch (chunkError) {
            console.error(`❌ Chunk ${i} error:`, chunkError);
            // Continue with other chunks instead of failing completely
            continue;
          }
        }
        
        controller.close();
        console.log('✅ Progressive streaming completed');
        
      } catch (error) {
        console.error('❌ Streaming error:', error);
        controller.error(error);
      }
    }
  });

  // ✅ Instant response headers - optimized for immediate playback
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  
  // ✅ Critical for video streaming performance
  if (mimeType.startsWith('video/')) {
    headers.set('X-Media-Type', 'video');
    headers.set('Content-Disposition', 'inline');
    // Enable partial content support
    headers.set('Accept-Ranges', 'bytes');
  } else if (mimeType.startsWith('image/')) {
    headers.set('X-Media-Type', 'image');
    headers.set('Content-Disposition', 'inline');
  }
  
  if (size) {
    headers.set('Content-Length', size.toString());
  }

  // ✅ Download vs inline display
  const url = new URL(request.url);
  if (url.searchParams.has('dl')) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  }

  console.log('🚀 INSTANT streaming response ready!');
  
  return new Response(readable, {
    status: 200,
    headers: headers
  });
}

// ✅ Get chunk with multiple fallback strategies for instant access
async function instantGetChunk(kvNamespace, chunkKey, chunkInfo, env, waitUntil) {
  console.log(`⚡ Getting chunk instantly: ${chunkKey}`);
  
  const chunkMetadataString = await kvNamespace.get(chunkKey);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${chunkKey} not found`);
  }
  
  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;
  
  // ✅ Strategy 1: Try cached version first (fastest)
  const cachedChunk = await caches.default.match(directUrl);
  if (cachedChunk && cachedChunk.ok) {
    console.log(`📦 Serving cached chunk: ${chunkKey}`);
    return await cachedChunk.arrayBuffer();
  }
  
  // ✅ Strategy 2: Fetch with optimized settings
  let response = await fetch(directUrl, {
    cf: {
      cacheEverything: true,
      cacheTtl: 86400, // 1 day cache
      minify: false // Don't minify media files
    },
    headers: {
      'User-Agent': 'Marya-Vault-Instant/1.0'
    }
  });
  
  // ✅ Strategy 3: Background URL refresh if expired
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for ${chunkKey}, refreshing...`);
    
    // Background refresh - non-blocking
    waitUntil(instantRefreshChunkUrl(kvNamespace, chunkKey, chunkMetadata, env));
    
    // Try to get fresh URL immediately
    const freshUrl = await getFreshTelegramUrl(chunkMetadata.telegramFileId, env.BOT_TOKEN);
    if (freshUrl) {
      response = await fetch(freshUrl, {
        cf: { cacheEverything: true, cacheTtl: 86400 }
      });
    }
  }
  
  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${chunkKey}: ${response.status}`);
  }
  
  const chunkData = await response.arrayBuffer();
  
  // ✅ Background cache for future instant access
  waitUntil(caches.default.put(directUrl, new Response(chunkData)));
  
  return chunkData;
}

// ✅ INSTANT Range request handling - critical for video seeking
async function instantRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env) {
  console.log('🎯 INSTANT Range request for video seeking:', range);
  
  const { size } = masterMetadata;
  const ranges = parseRange(range, size);
  
  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: { 'Content-Range': `bytes */${size}` }
    });
  }
  
  const { start, end } = ranges[0];
  const chunkSize = end - start + 1;
  
  // ✅ Smart chunk calculation for instant seeking
  const CHUNK_SIZE = 20 * 1024 * 1024;
  const startChunk = Math.floor(start / CHUNK_SIZE);
  const endChunk = Math.floor(end / CHUNK_SIZE);
  
  const neededChunks = masterMetadata.chunks.slice(startChunk, endChunk + 1);
  
  console.log(`🎬 Video seek needs chunks ${startChunk}-${endChunk}`);
  
  // ✅ Parallel chunk fetching for instant seeking
  const chunkPromises = neededChunks.map(async (chunkInfo, index) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.chunkKey;
    const chunkData = await instantGetChunk(kvNamespace, chunkKey, chunkInfo, env, () => {});
    return {
      index: startChunk + index,
      data: chunkData
    };
  });
  
  const chunkResults = await Promise.all(chunkPromises);
  
  // ✅ Combine and extract exact range
  const totalChunkSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalChunkSize);
  
  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }
  
  const rangeStart = start - (startChunk * CHUNK_SIZE);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + chunkSize);
  
  // ✅ Instant range response headers
  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', chunkSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('X-Range-Type', 'instant');
  
  console.log('⚡ INSTANT Range response ready for video seeking!');
  
  return new Response(rangeBuffer, {
    status: 206, // Partial Content
    headers: headers
  });
}

// ✅ Single file instant streaming
async function instantSingleFileStreaming(request, kvNamespace, actualId, extension, metadata, env) {
  console.log('⚡ Single file instant streaming');
  
  const directUrl = await kvNamespace.get(actualId);
  if (!directUrl) {
    return new Response('File not found', { status: 404 });
  }
  
  // ✅ Try cached version first for instant access
  const cachedResponse = await caches.default.match(directUrl);
  if (cachedResponse && cachedResponse.ok) {
    console.log('📦 Serving cached single file instantly');
    return cachedResponse;
  }
  
  // ✅ Fetch with streaming optimization
  const response = await fetch(directUrl, {
    cf: {
      cacheEverything: true,
      cacheTtl: 86400
    }
  });
  
  if (!response.ok) {
    // Try refresh if expired
    const freshUrl = await getFreshTelegramUrl(metadata?.telegramFileId, env.BOT_TOKEN);
    if (freshUrl) {
      const freshResponse = await fetch(freshUrl);
      if (freshResponse.ok) {
        // Update KV with fresh URL
        await kvNamespace.put(actualId, freshUrl, { metadata });
        return createInstantMediaResponse(freshResponse, extension, metadata, request);
      }
    }
    return new Response(`File not accessible: ${response.status}`, { status: response.status });
  }
  
  return createInstantMediaResponse(response, extension, metadata, request);
}

// ✅ Create optimized media response for instant display
function createInstantMediaResponse(response, extension, metadata, request) {
  const headers = new Headers();
  const mimeType = getMimeType(extension);
  
  // Copy essential headers
  if (response.headers.get('content-length')) {
    headers.set('Content-Length', response.headers.get('content-length'));
  }
  
  headers.set('Content-Type', mimeType);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('X-Served-By', 'Instant-Media');
  
  // ✅ Optimize for media type
  if (mimeType.startsWith('video/')) {
    headers.set('Content-Disposition', 'inline');
    headers.set('X-Media-Type', 'video');
  } else if (mimeType.startsWith('image/')) {
    headers.set('Content-Disposition', 'inline');
    headers.set('X-Media-Type', 'image');
  } else if (mimeType.startsWith('audio/')) {
    headers.set('Content-Disposition', 'inline');
    headers.set('X-Media-Type', 'audio');
  }
  
  // Handle download parameter
  const url = new URL(request.url);
  if (url.searchParams.has('dl')) {
    const filename = metadata?.filename || 'download';
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  }
  
  return new Response(response.body, {
    status: response.status,
    headers: headers
  });
}

// ✅ Get fresh Telegram URL instantly
async function getFreshTelegramUrl(telegramFileId, botToken) {
  if (!botToken || !telegramFileId) return null;
  
  try {
    const response = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
      {
        cf: { cacheEverything: false } // Don't cache API calls
      }
    );
    
    if (response.ok) {
      const data = await response.json();
      if (data.ok && data.result?.file_path) {
        return `https://api.telegram.org/file/bot${botToken}/${data.result.file_path}`;
      }
    }
  } catch (error) {
    console.error('❌ Failed to get fresh URL:', error);
  }
  
  return null;
}

// ✅ Background chunk URL refresh
async function instantRefreshChunkUrl(kvNamespace, chunkKey, chunkMetadata, env) {
  try {
    const freshUrl = await getFreshTelegramUrl(chunkMetadata.telegramFileId, env.BOT_TOKEN);
    if (freshUrl) {
      const updatedMetadata = {
        ...chunkMetadata,
        directUrl: freshUrl,
        lastRefreshed: Date.now()
      };
      
      await kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata));
      console.log(`✅ Background refresh completed for ${chunkKey}`);
    }
  } catch (error) {
    console.error(`❌ Background refresh failed for ${chunkKey}:`, error);
  }
}

// ✅ Parse Range header
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) return null;
  
  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;
  
  if (start >= size || end >= size || start > end) return null;
  
  return [{ start, end }];
}
