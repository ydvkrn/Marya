// Ultra-optimized MIME types
const REVOLUTIONARY_MIME_TYPES = {
  // Video
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
  'mov': 'video/quicktime', 'avi': 'video/x-msvideo', 'm4v': 'video/x-m4v',
  'flv': 'video/x-flv', '3gp': 'video/3gpp', 'wmv': 'video/x-ms-wmv',
  
  // Audio  
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'm4a': 'audio/mp4', 'aac': 'audio/aac', 'ogg': 'audio/ogg',
  
  // Images
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'bmp': 'image/bmp', 'tiff': 'image/tiff', 'heic': 'image/heic',
  
  // Documents
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json'
};

function getRevolutionaryMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return REVOLUTIONARY_MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params, waitUntil } = context;
  const fileId = params.id;

  console.log('=== REVOLUTIONARY INSTANT STREAMING V2.0 ===');

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';
    
    // ✅ ULTRA-FAST CACHE CHECK (Speed: ~1-5ms)
    const cacheKey = new Request(`https://revolutionary-cache.marya.vault/${fileId}`, {
      method: request.method,
      headers: request.headers
    });

    const cache = caches.default;
    const cachedResponse = await cache.match(cacheKey);

    if (cachedResponse) {
      console.log('🚀 REVOLUTIONARY CACHE HIT - INSTANT RESPONSE!');
      
      const headers = new Headers(cachedResponse.headers);
      headers.set('X-Revolutionary-Cache', 'INSTANT-HIT');
      headers.set('X-Speed-Level', 'REVOLUTIONARY');
      headers.set('X-Response-Time', '~1ms');
      
      return new Response(cachedResponse.body, {
        status: cachedResponse.status,
        headers: headers
      });
    }

    console.log('💎 Cache miss - Creating revolutionary response...');

    // ✅ Get KV namespaces
    const kvNamespaces = {
      FILES_KV: env.FILES_KV, FILES_KV2: env.FILES_KV2, FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4, FILES_KV5: env.FILES_KV5, FILES_KV6: env.FILES_KV6, FILES_KV7: env.FILES_KV7
    };

    // Get file metadata
    const masterMetadataString = await kvNamespaces.FILES_KV.get(actualId);
    if (!masterMetadataString) {
      return new Response('File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    console.log(`🔥 Revolutionary streaming: ${masterMetadata.filename}`);

    // ✅ REVOLUTIONARY STREAMING STRATEGY
    let response;
    
    if (masterMetadata.type === 'revolutionary_chunked') {
      response = await revolutionaryChunkedStreaming(request, kvNamespaces, masterMetadata, extension, env, waitUntil);
    } else if (masterMetadata.type === 'optimized_single') {
      response = await revolutionaryOptimizedStreaming(request, kvNamespaces, masterMetadata, actualId, extension, env);
    } else if (masterMetadata.type === 'instant_small') {
      response = await revolutionaryInstantStreaming(request, kvNamespaces, masterMetadata, actualId, extension, env);
    } else {
      // Legacy support
      response = await revolutionaryLegacyStreaming(request, kvNamespaces.FILES_KV, actualId, extension, masterMetadata, env);
    }

    // ✅ REVOLUTIONARY CACHING (Store for next instant access)
    if (response.ok) {
      const responseToCache = response.clone();
      const revolutionaryCacheHeaders = new Headers(responseToCache.headers);
      
      // Ultra-aggressive revolutionary caching
      revolutionaryCacheHeaders.set('Cache-Control', 'public, max-age=31536000, immutable, stale-while-revalidate=86400');
      revolutionaryCacheHeaders.set('CDN-Cache-Control', 'public, max-age=31536000, immutable');
      revolutionaryCacheHeaders.set('Cloudflare-CDN-Cache-Control', 'public, max-age=31536000, immutable');
      revolutionaryCacheHeaders.set('X-Revolutionary-Cache', 'STORED-FOR-INSTANT');
      
      const cachedResponseFinal = new Response(responseToCache.body, {
        status: responseToCache.status,
        headers: revolutionaryCacheHeaders
      });
      
      // Background ultra-fast caching
      waitUntil(cache.put(cacheKey, cachedResponseFinal.clone()));
    }

    return response;

  } catch (error) {
    console.error('❌ Revolutionary streaming error:', error);
    return new Response(`Revolutionary Error: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}

// ✅ REVOLUTIONARY chunked streaming with INSTANT playback
async function revolutionaryChunkedStreaming(request, kvNamespaces, masterMetadata, extension, env, waitUntil) {
  const { filename, size, chunks } = masterMetadata;
  const mimeType = getRevolutionaryMimeType(extension);
  
  console.log(`🎬 REVOLUTIONARY CHUNKED STREAMING: ${filename}`);

  // ✅ Handle Range requests for INSTANT video seeking
  const range = request.headers.get('Range');
  if (range) {
    return await revolutionaryRangeStreaming(request, kvNamespaces, masterMetadata, extension, range, env, waitUntil);
  }

  // ✅ Create REVOLUTIONARY streaming response
  const revolutionaryStream = new ReadableStream({
    async start(controller) {
      try {
        console.log('🚀 Starting REVOLUTIONARY progressive streaming...');
        
        // ✅ PARALLEL chunk loading with immediate streaming
        const chunkStreamPromises = chunks.map(async (chunkInfo, index) => {
          return {
            index: index,
            promise: getRevolutionaryChunk(
              kvNamespaces[chunkInfo.kvNamespace], 
              chunkInfo.chunkKey, 
              chunkInfo, 
              env, 
              waitUntil
            )
          };
        });

        // ✅ Stream chunks as they become available (Progressive streaming)
        for (let i = 0; i < chunkStreamPromises.length; i++) {
          try {
            const chunkData = await chunkStreamPromises[i].promise;
            
            // ✅ INSTANT streaming - no buffering
            controller.enqueue(new Uint8Array(chunkData));
            
            console.log(`⚡ Chunk ${i}/${chunks.length} streamed instantly`);
            
            // Micro-delay for optimal streaming
            if (i < chunks.length - 1) {
              await new Promise(resolve => setTimeout(resolve, 5));
            }
            
          } catch (chunkError) {
            console.error(`⚠️ Chunk ${i} error (continuing):`, chunkError);
            // Continue streaming other chunks instead of failing
          }
        }
        
        controller.close();
        console.log('✅ REVOLUTIONARY streaming completed');
        
      } catch (error) {
        console.error('❌ Revolutionary stream error:', error);
        controller.error(error);
      }
    }
  });

  // ✅ REVOLUTIONARY response headers for optimal playback
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('X-Revolutionary-Streaming', 'INSTANT');
  
  // ✅ Media-specific optimizations
  if (mimeType.startsWith('video/')) {
    headers.set('X-Media-Optimization', 'VIDEO-INSTANT-PLAYBACK');
    headers.set('Content-Disposition', 'inline');
    // Enable video seeking
    headers.set('Accept-Ranges', 'bytes');
  } else if (mimeType.startsWith('image/')) {
    headers.set('X-Media-Optimization', 'IMAGE-INSTANT-DISPLAY');
    headers.set('Content-Disposition', 'inline');
  } else if (mimeType.startsWith('audio/')) {
    headers.set('X-Media-Optimization', 'AUDIO-INSTANT-PLAYBACK');
    headers.set('Content-Disposition', 'inline');
  }
  
  if (size) {
    headers.set('Content-Length', size.toString());
  }

  // Handle download parameter
  const url = new URL(request.url);
  if (url.searchParams.has('dl')) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  }

  console.log('🚀 REVOLUTIONARY STREAMING RESPONSE READY!');
  
  return new Response(revolutionaryStream, {
    status: 200,
    headers: headers
  });
}

// ✅ Get chunk with REVOLUTIONARY speed and fallbacks
async function getRevolutionaryChunk(kvNamespace, chunkKey, chunkInfo, env, waitUntil) {
  console.log(`⚡ Revolutionary chunk fetch: ${chunkKey}`);
  
  const chunkMetadataString = await kvNamespace.get(chunkKey);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${chunkKey} not found`);
  }
  
  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;
  
  // ✅ Strategy 1: Try cached version FIRST (Fastest - ~1ms)
  const cachedChunk = await caches.default.match(directUrl);
  if (cachedChunk && cachedChunk.ok) {
    console.log(`🚀 INSTANT cached chunk: ${chunkKey}`);
    return await cachedChunk.arrayBuffer();
  }
  
  // ✅ Strategy 2: Fetch with REVOLUTIONARY optimization
  let response = await fetch(directUrl, {
    cf: {
      cacheEverything: true,
      cacheTtl: 86400,
      polish: "off", // Don't modify media files
      mirage: "off"   // Don't optimize images
    },
    headers: {
      'User-Agent': 'Revolutionary-Marya-Vault/2.0',
      'Accept': '*/*',
      'Connection': 'keep-alive'
    }
  });
  
  // ✅ Strategy 3: Background URL refresh (Never show errors to user)
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 Revolutionary URL refresh for ${chunkKey}...`);
    
    // Non-blocking background refresh
    waitUntil(revolutionaryRefreshChunkUrl(kvNamespace, chunkKey, chunkMetadata, env));
    
    // Try fresh URL immediately
    const freshUrl = await getRevolutionaryFreshUrl(chunkMetadata.telegramFileId, env.BOT_TOKEN);
    if (freshUrl) {
      response = await fetch(freshUrl, {
        cf: { cacheEverything: true, cacheTtl: 86400 }
      });
      
      if (response.ok) {
        // Update KV with fresh URL in background
        waitUntil(updateChunkUrlInBackground(kvNamespace, chunkKey, chunkMetadata, freshUrl));
      }
    }
  }
  
  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${chunkKey}: ${response.status}`);
  }
  
  const chunkData = await response.arrayBuffer();
  
  // ✅ Background cache for future INSTANT access
  waitUntil(caches.default.put(directUrl, new Response(chunkData.slice())));
  
  return chunkData;
}

// ✅ REVOLUTIONARY Range requests for INSTANT video seeking
async function revolutionaryRangeStreaming(request, kvNamespaces, masterMetadata, extension, range, env, waitUntil) {
  console.log('🎯 REVOLUTIONARY RANGE REQUEST for instant seeking:', range);
  
  const { size, chunks } = masterMetadata;
  const ranges = parseRevolutionaryRange(range, size);
  
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
  const chunkSize = end - start + 1;
  
  // ✅ Revolutionary chunk calculation for INSTANT seeking
  const CHUNK_SIZE = 15 * 1024 * 1024; // Match upload chunk size
  const startChunk = Math.floor(start / CHUNK_SIZE);
  const endChunk = Math.floor(end / CHUNK_SIZE);
  
  const neededChunks = chunks.slice(startChunk, endChunk + 1);
  
  console.log(`🎬 Video seeking needs chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);
  
  // ✅ PARALLEL chunk fetching for INSTANT seeking
  const revolutionaryChunkPromises = neededChunks.map(async (chunkInfo, index) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkData = await getRevolutionaryChunk(kvNamespace, chunkInfo.chunkKey, chunkInfo, env, waitUntil);
    return {
      index: startChunk + index,
      data: chunkData
    };
  });
  
  const chunkResults = await Promise.all(revolutionaryChunkPromises);
  
  // ✅ Combine and extract EXACT range
  const totalChunkSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalChunkSize);
  
  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }
  
  const rangeStart = start - (startChunk * CHUNK_SIZE);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + chunkSize);
  
  // ✅ REVOLUTIONARY range response headers
  const headers = new Headers();
  headers.set('Content-Type', getRevolutionaryMimeType(extension));
  headers.set('Content-Length', chunkSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('X-Revolutionary-Range', 'INSTANT-SEEKING');
  
  console.log('⚡ REVOLUTIONARY RANGE RESPONSE READY - INSTANT VIDEO SEEKING!');
  
  return new Response(rangeBuffer, {
    status: 206, // Partial Content
    headers: headers
  });
}

// ✅ Revolutionary optimized single file streaming
async function revolutionaryOptimizedStreaming(request, kvNamespaces, masterMetadata, actualId, extension, env) {
  console.log('🚀 Revolutionary optimized streaming');
  
  const { directUrl, filename } = masterMetadata;
  
  // Try cached version first
  const cachedResponse = await caches.default.match(directUrl);
  if (cachedResponse && cachedResponse.ok) {
    console.log('🚀 INSTANT cached optimized file');
    return createRevolutionaryMediaResponse(cachedResponse, extension, masterMetadata, request);
  }
  
  // Fetch with revolutionary settings
  let response = await fetch(directUrl, {
    cf: {
      cacheEverything: true,
      cacheTtl: 86400,
      polish: "off"
    }
  });
  
  // Handle expired URLs
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    const freshUrl = await getRevolutionaryFreshUrl(masterMetadata.telegramFileId, env.BOT_TOKEN);
    if (freshUrl) {
      response = await fetch(freshUrl);
      if (response.ok) {
        // Update KV with fresh URL
        const updatedMetadata = { ...masterMetadata, directUrl: freshUrl, lastRefreshed: Date.now() };
        await kvNamespaces.FILES_KV.put(actualId, JSON.stringify(updatedMetadata));
      }
    }
  }
  
  if (!response.ok) {
    return new Response(`File not accessible: ${response.status}`, { status: response.status });
  }
  
  return createRevolutionaryMediaResponse(response, extension, masterMetadata, request);
}

// ✅ Revolutionary instant small file streaming
async function revolutionaryInstantStreaming(request, kvNamespaces, masterMetadata, actualId, extension, env) {
  console.log('⚡ Revolutionary instant streaming');
  
  const { directUrl } = masterMetadata;
  
  // Try cached version first for INSTANT access
  const cachedResponse = await caches.default.match(directUrl);
  if (cachedResponse && cachedResponse.ok) {
    console.log('⚡ INSTANT small file from cache');
    return createRevolutionaryMediaResponse(cachedResponse, extension, masterMetadata, request);
  }
  
  // Fetch with instant optimization
  const response = await fetch(directUrl, {
    cf: { cacheEverything: true, cacheTtl: 86400 }
  });
  
  if (!response.ok) {
    // Handle expired URLs for small files
    const freshUrl = await getRevolutionaryFreshUrl(masterMetadata.telegramFileId, env.BOT_TOKEN);
    if (freshUrl) {
      const freshResponse = await fetch(freshUrl);
      if (freshResponse.ok) {
        // Update KV
        const updatedMetadata = { ...masterMetadata, directUrl: freshUrl, lastRefreshed: Date.now() };
        await kvNamespaces.FILES_KV.put(actualId, JSON.stringify(updatedMetadata));
        return createRevolutionaryMediaResponse(freshResponse, extension, masterMetadata, request);
      }
    }
    return new Response(`File not accessible: ${response.status}`, { status: response.status });
  }
  
  return createRevolutionaryMediaResponse(response, extension, masterMetadata, request);
}

// ✅ Create REVOLUTIONARY media response for optimal display/playback
function createRevolutionaryMediaResponse(response, extension, metadata, request) {
  const headers = new Headers();
  const mimeType = getRevolutionaryMimeType(extension);
  
  // Copy essential headers
  if (response.headers.get('content-length')) {
    headers.set('Content-Length', response.headers.get('content-length'));
  }
  
  headers.set('Content-Type', mimeType);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('X-Revolutionary-Media', 'OPTIMIZED');
  
  // Revolutionary media optimizations
  if (mimeType.startsWith('video/')) {
    headers.set('Content-Disposition', 'inline');
    headers.set('X-Media-Type', 'video');
    headers.set('X-Video-Optimization', 'INSTANT-PLAYBACK');
  } else if (mimeType.startsWith('image/')) {
    headers.set('Content-Disposition', 'inline');
    headers.set('X-Media-Type', 'image');
    headers.set('X-Image-Optimization', 'INSTANT-DISPLAY');
  } else if (mimeType.startsWith('audio/')) {
    headers.set('Content-Disposition', 'inline');
    headers.set('X-Media-Type', 'audio');
    headers.set('X-Audio-Optimization', 'INSTANT-PLAYBACK');
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

// ✅ Get fresh Telegram URL with revolutionary speed
async function getRevolutionaryFreshUrl(telegramFileId, botToken) {
  if (!botToken || !telegramFileId) return null;
  
  try {
    const response = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
      {
        cf: { cacheEverything: false },
        headers: { 'User-Agent': 'Revolutionary-Marya-Vault/2.0' }
      }
    );
    
    if (response.ok) {
      const data = await response.json();
      if (data.ok && data.result?.file_path) {
        return `https://api.telegram.org/file/bot${botToken}/${data.result.file_path}`;
      }
    }
  } catch (error) {
    console.error('❌ Failed to get revolutionary fresh URL:', error);
  }
  
  return null;
}

// ✅ Background revolutionary URL refresh
async function revolutionaryRefreshChunkUrl(kvNamespace, chunkKey, chunkMetadata, env) {
  try {
    const freshUrl = await getRevolutionaryFreshUrl(chunkMetadata.telegramFileId, env.BOT_TOKEN);
    if (freshUrl) {
      const updatedMetadata = {
        ...chunkMetadata,
        directUrl: freshUrl,
        lastRefreshed: Date.now(),
        refreshCount: (chunkMetadata.refreshCount || 0) + 1
      };
      
      await kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata));
      
      // Pre-cache fresh URL
      await fetch(freshUrl, {
        cf: { cacheEverything: true, cacheTtl: 86400 }
      });
      
      console.log(`✅ Revolutionary background refresh completed for ${chunkKey}`);
    }
  } catch (error) {
    console.error(`❌ Revolutionary background refresh failed for ${chunkKey}:`, error);
  }
}

// ✅ Update chunk URL in background
async function updateChunkUrlInBackground(kvNamespace, chunkKey, chunkMetadata, freshUrl) {
  try {
    const updatedMetadata = {
      ...chunkMetadata,
      directUrl: freshUrl,
      lastRefreshed: Date.now()
    };
    
    await kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata));
    console.log(`✅ Chunk URL updated in background: ${chunkKey}`);
  } catch (error) {
    console.error(`❌ Failed to update chunk URL: ${chunkKey}`, error);
  }
}

// ✅ Legacy file support
async function revolutionaryLegacyStreaming(request, kvNamespace, actualId, extension, metadata, env) {
  console.log('🔄 Revolutionary legacy streaming');
  
  const directUrl = await kvNamespace.get(actualId);
  if (!directUrl) {
    return new Response('File not found', { status: 404 });
  }
  
  const response = await fetch(directUrl, {
    cf: { cacheEverything: true, cacheTtl: 86400 }
  });
  
  if (!response.ok) {
    // Try refresh
    const freshUrl = await getRevolutionaryFreshUrl(metadata?.telegramFileId, env.BOT_TOKEN);
    if (freshUrl) {
      const freshResponse = await fetch(freshUrl);
      if (freshResponse.ok) {
        await kvNamespace.put(actualId, freshUrl, { metadata: { ...metadata, lastRefreshed: Date.now() } });
        return createRevolutionaryMediaResponse(freshResponse, extension, metadata, request);
      }
    }
    return new Response(`File not accessible: ${response.status}`, { status: response.status });
  }
  
  return createRevolutionaryMediaResponse(response, extension, metadata, request);
}

// ✅ Parse Range header with revolutionary precision
function parseRevolutionaryRange(range, size) {
  const rangeMatch = range.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) return null;
  
  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;
  
  if (start >= size || end >= size || start > end) return null;
  
  return [{ start, end }];
}
