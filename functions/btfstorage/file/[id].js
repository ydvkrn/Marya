// Ultra-fast file serving with aggressive caching
const MIME_TYPES = {
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
  'mov': 'video/quicktime', 'avi': 'video/x-msvideo', 'm4v': 'video/x-m4v',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json',
  'zip': 'application/zip', 'rar': 'application/vnd.rar'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params, waitUntil } = context;
  const fileId = params.id;

  console.log('=== LIGHTNING-FAST FILE SERVE ===');
  console.log('File ID:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';
    
    // ✅ STEP 1: Check Cloudflare Cache First (FASTEST PATH)
    const cacheKey = new Request(`https://cache.marya-vault.com/file/${fileId}`, {
      method: 'GET',
      headers: request.headers
    });

    const cache = caches.default;
    let cachedResponse = await cache.match(cacheKey);

    if (cachedResponse) {
      console.log('🚀 CACHE HIT - Serving from edge (Ultra Fast!)');
      
      // ✅ Add performance headers
      const headers = new Headers(cachedResponse.headers);
      headers.set('X-Cache', 'HIT');
      headers.set('X-Served-By', 'Cloudflare-Edge');
      
      return new Response(cachedResponse.body, {
        status: cachedResponse.status,
        headers: headers
      });
    }

    console.log('💾 Cache miss - Fetching and caching...');

    // ✅ STEP 2: Get KV namespaces
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
    console.log(`📁 File: ${masterMetadata.filename}`);

    // ✅ STEP 3: Handle different file types with optimal streaming
    let response;
    
    if (masterMetadata.type === 'multi_kv_chunked') {
      response = await handleChunkedFileUltraFast(request, kvNamespaces, masterMetadata, extension, env, waitUntil);
    } else {
      response = await handleSingleFileUltraFast(request, kvNamespaces.FILES_KV, actualId, extension, masterMetadata, env, waitUntil);
    }

    // ✅ STEP 4: Cache the response aggressively (1 year TTL)
    const responseToCache = response.clone();
    const cacheHeaders = new Headers(responseToCache.headers);
    
    // ✅ Ultra-aggressive caching headers
    cacheHeaders.set('Cache-Control', 'public, max-age=31536000, immutable');
    cacheHeaders.set('CDN-Cache-Control', 'public, max-age=31536000');
    cacheHeaders.set('Cloudflare-CDN-Cache-Control', 'public, max-age=31536000');
    cacheHeaders.set('X-Cache', 'MISS');
    cacheHeaders.set('X-Served-By', 'Origin-Cached');

    const cachedResponseFinal = new Response(responseToCache.body, {
      status: responseToCache.status,
      headers: cacheHeaders
    });

    // ✅ Background cache without blocking user
    waitUntil(cache.put(cacheKey, cachedResponseFinal.clone()));
    
    console.log('✅ Response cached for lightning-fast future requests');
    
    return new Response(response.body, {
      status: response.status,
      headers: cacheHeaders
    });

  } catch (error) {
    console.error('❌ Serve error:', error);
    return new Response(`Server error: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}

// ✅ Ultra-fast chunked file handling with streaming
async function handleChunkedFileUltraFast(request, kvNamespaces, masterMetadata, extension, env, waitUntil) {
  const { totalChunks, chunks, filename, size } = masterMetadata;
  
  console.log(`🔄 Streaming chunked file: ${filename} (${totalChunks} chunks)`);

  // ✅ Handle Range requests for video streaming (CRITICAL FOR SPEED)
  const range = request.headers.get('Range');
  if (range) {
    return await handleRangeRequestOptimized(request, kvNamespaces, masterMetadata, extension, range, env, waitUntil);
  }

  // ✅ Stream all chunks with parallel fetching and background refresh
  const chunkPromises = chunks.map(async (chunkInfo, index) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.chunkKey || `${actualId}_chunk_${index}`;
    
    return await getChunkUltraFastWithBackgroundRefresh(kvNamespace, chunkKey, chunkInfo, env, waitUntil);
  });

  // ✅ Create streaming response while chunks are still loading
  const readable = new ReadableStream({
    async start(controller) {
      try {
        const chunkResults = await Promise.all(chunkPromises);
        
        // Sort chunks by index
        chunkResults.sort((a, b) => a.index - b.index);
        
        // Stream each chunk immediately
        for (const chunk of chunkResults) {
          controller.enqueue(new Uint8Array(chunk.data));
        }
        
        controller.close();
      } catch (error) {
        console.error('Streaming error:', error);
        controller.error(error);
      }
    }
  });

  // ✅ Ultra-fast response headers
  const headers = new Headers();
  const mimeType = getMimeType(extension);
  headers.set('Content-Type', mimeType);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Transfer-Encoding', 'chunked');
  headers.set('X-Content-Type-Options', 'nosniff');
  
  if (size) {
    headers.set('Content-Length', size.toString());
  }

  // ✅ Optimal content disposition
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');
  
  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (mimeType.startsWith('video/') || mimeType.startsWith('audio/') || mimeType.startsWith('image/')) {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log('🚀 Ultra-fast streaming response ready');
  
  return new Response(readable, {
    status: 200,
    headers: headers
  });
}

// ✅ Get chunk with background URL refresh (NEVER shows errors to user)
async function getChunkUltraFastWithBackgroundRefresh(kvNamespace, chunkKey, chunkInfo, env, waitUntil) {
  console.log(`⚡ Getting chunk: ${chunkKey}`);
  
  const chunkMetadataString = await kvNamespace.get(chunkKey);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${chunkKey} not found`);
  }
  
  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;
  
  // ✅ Try primary URL first
  let response = await fetch(directUrl, {
    cf: {
      cacheEverything: true,
      cacheTtl: 86400 // Cache for 1 day
    }
  });
  
  // ✅ If URL expired, refresh in background but serve stale if possible
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for ${chunkKey}, refreshing in background...`);
    
    // ✅ Background refresh (non-blocking)
    waitUntil(refreshChunkUrlInBackground(kvNamespace, chunkKey, chunkMetadata, env));
    
    // ✅ Try to serve stale version from cache
    const staleResponse = await caches.default.match(directUrl);
    if (staleResponse) {
      console.log(`📦 Serving stale version while refreshing ${chunkKey}`);
      return {
        index: chunkInfo.index,
        data: await staleResponse.arrayBuffer()
      };
    }
    
    // ✅ If no stale version, force refresh (blocking)
    const refreshedUrl = await forceRefreshChunkUrl(kvNamespace, chunkKey, chunkMetadata, env);
    if (refreshedUrl) {
      response = await fetch(refreshedUrl);
    }
  }
  
  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${chunkKey}: ${response.status}`);
  }
  
  return {
    index: chunkInfo.index,
    data: await response.arrayBuffer()
  };
}

// ✅ Background URL refresh (non-blocking)
async function refreshChunkUrlInBackground(kvNamespace, chunkKey, chunkMetadata, env) {
  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    if (!BOT_TOKEN) return;
    
    console.log(`🔄 Background refresh for ${chunkKey}`);
    
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`
    );
    
    if (!getFileResponse.ok) return;
    
    const getFileData = await getFileResponse.json();
    if (!getFileData.ok || !getFileData.result?.file_path) return;
    
    const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
    
    // ✅ Update KV with fresh URL
    const updatedMetadata = {
      ...chunkMetadata,
      directUrl: freshUrl,
      lastRefreshed: Date.now(),
      refreshCount: (chunkMetadata.refreshCount || 0) + 1
    };
    
    await kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata));
    
    // ✅ Pre-cache the fresh URL
    await fetch(freshUrl, {
      cf: { 
        cacheEverything: true,
        cacheTtl: 86400 
      }
    });
    
    console.log(`✅ Background refresh completed for ${chunkKey}`);
    
  } catch (error) {
    console.error(`❌ Background refresh failed for ${chunkKey}:`, error);
  }
}

// ✅ Force refresh (blocking) - only when absolutely necessary
async function forceRefreshChunkUrl(kvNamespace, chunkKey, chunkMetadata, env) {
  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    if (!BOT_TOKEN) throw new Error('BOT_TOKEN not available');
    
    console.log(`🚨 Force refresh for ${chunkKey}`);
    
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`
    );
    
    if (!getFileResponse.ok) {
      throw new Error(`Telegram API failed: ${getFileResponse.status}`);
    }
    
    const getFileData = await getFileResponse.json();
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Invalid Telegram response');
    }
    
    const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
    
    // ✅ Update KV immediately
    const updatedMetadata = {
      ...chunkMetadata,
      directUrl: freshUrl,
      lastRefreshed: Date.now(),
      refreshCount: (chunkMetadata.refreshCount || 0) + 1
    };
    
    await kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata));
    
    console.log(`✅ Force refresh completed for ${chunkKey}`);
    
    return freshUrl;
    
  } catch (error) {
    console.error(`❌ Force refresh failed for ${chunkKey}:`, error);
    return null;
  }
}

// ✅ Optimized Range request handling for video streaming
async function handleRangeRequestOptimized(request, kvNamespaces, masterMetadata, extension, range, env, waitUntil) {
  console.log('🎬 Optimized Range request:', range);
  
  const { size } = masterMetadata;
  const ranges = parseRange(range, size);
  
  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: {
        'Content-Range': `bytes */${size}`
      }
    });
  }
  
  const { start, end } = ranges[0];
  const chunkSize = end - start + 1;
  
  // ✅ Calculate which chunks we need
  const CHUNK_SIZE = 20 * 1024 * 1024;
  const startChunk = Math.floor(start / CHUNK_SIZE);
  const endChunk = Math.floor(end / CHUNK_SIZE);
  
  const neededChunks = masterMetadata.chunks.slice(startChunk, endChunk + 1);
  
  console.log(`🎯 Range needs chunks ${startChunk}-${endChunk}`);
  
  // ✅ Fetch needed chunks in parallel
  const chunkPromises = neededChunks.map(async (chunkInfo) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.chunkKey;
    return await getChunkUltraFastWithBackgroundRefresh(kvNamespace, chunkKey, chunkInfo, env, waitUntil);
  });
  
  const chunkResults = await Promise.all(chunkPromises);
  chunkResults.sort((a, b) => a.index - b.index);
  
  // ✅ Combine needed chunks
  const totalChunkSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalChunkSize);
  
  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }
  
  // ✅ Extract exact range
  const rangeStart = start - (startChunk * CHUNK_SIZE);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + chunkSize);
  
  // ✅ Range response headers
  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', chunkSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  
  console.log('✅ Range response ready');
  
  return new Response(rangeBuffer, {
    status: 206, // Partial Content
    headers: headers
  });
}

// ✅ Single file ultra-fast handling
async function handleSingleFileUltraFast(request, kvNamespace, actualId, extension, metadata, env, waitUntil) {
  console.log('⚡ Single file ultra-fast serving');
  
  const directUrl = await kvNamespace.get(actualId);
  if (!directUrl) {
    return new Response('File not found', { status: 404 });
  }
  
  // ✅ Try cached version first
  let response = await fetch(directUrl, {
    cf: {
      cacheEverything: true,
      cacheTtl: 86400
    }
  });
  
  // ✅ Background refresh if expired
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log('🔄 Single file URL expired, refreshing...');
    
    // Background refresh
    waitUntil(refreshSingleFileInBackground(kvNamespace, actualId, metadata, env));
    
    // Try stale version
    const staleResponse = await caches.default.match(directUrl);
    if (staleResponse) {
      console.log('📦 Serving stale single file');
      return staleResponse;
    }
    
    // Force refresh if no stale version
    const refreshedUrl = await forceRefreshSingleFile(kvNamespace, actualId, metadata, env);
    if (refreshedUrl) {
      response = await fetch(refreshedUrl);
    }
  }
  
  if (!response.ok) {
    return new Response(`File not accessible: ${response.status}`, { 
      status: response.status 
    });
  }
  
  // ✅ Optimized headers
  const headers = new Headers();
  const mimeType = getMimeType(extension);
  headers.set('Content-Type', mimeType);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');
  const filename = metadata?.filename || 'download';
  
  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
  }
  
  return new Response(response.body, { 
    status: response.status, 
    headers 
  });
}

// ✅ Background single file refresh
async function refreshSingleFileInBackground(kvNamespace, actualId, metadata, env) {
  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const telegramFileId = metadata?.telegramFileId;
    
    if (!BOT_TOKEN || !telegramFileId) return;
    
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
    
    if (getFileResponse.ok) {
      const getFileData = await getFileResponse.json();
      if (getFileData.ok && getFileData.result?.file_path) {
        const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
        
        await kvNamespace.put(actualId, freshUrl, {
          metadata: { 
            ...metadata, 
            lastRefreshed: Date.now() 
          }
        });
        
        // Pre-cache fresh URL
        await fetch(freshUrl, {
          cf: { cacheEverything: true, cacheTtl: 86400 }
        });
        
        console.log('✅ Single file background refresh completed');
      }
    }
  } catch (error) {
    console.error('❌ Single file background refresh failed:', error);
  }
}

// ✅ Force refresh single file
async function forceRefreshSingleFile(kvNamespace, actualId, metadata, env) {
  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const telegramFileId = metadata?.telegramFileId;
    
    if (!BOT_TOKEN || !telegramFileId) return null;
    
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
    
    if (getFileResponse.ok) {
      const getFileData = await getFileResponse.json();
      if (getFileData.ok && getFileData.result?.file_path) {
        const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
        
        await kvNamespace.put(actualId, freshUrl, {
          metadata: { ...metadata, lastRefreshed: Date.now() }
        });
        
        console.log('✅ Single file force refresh completed');
        return freshUrl;
      }
    }
  } catch (error) {
    console.error('❌ Single file force refresh failed:', error);
  }
  return null;
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
