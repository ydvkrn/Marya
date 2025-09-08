// ✅ PERFORMANCE-OPTIMIZED SERVE FUNCTION
const MIME_TYPES = {
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
  'mov': 'video/quicktime', 'avi': 'video/x-msvideo', 'm4v': 'video/x-m4v',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json',
  'zip': 'application/zip', '7z': 'application/x-7z-compressed'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('=== ULTRA-FAST SERVE REQUEST ===');
  console.log('File ID:', fileId);
  console.log('User-Agent:', request.headers.get('User-Agent'));

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    // ✅ Generate cache key for CDN caching
    const cacheKey = new Request(request.url, request);
    const cache = caches.default;

    // ✅ Try to serve from Cloudflare CDN cache first
    let response = await cache.match(cacheKey);
    if (response) {
      console.log('✅ Served from CDN cache');
      return response;
    }

    // ✅ KV namespaces
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
    console.log(`File found: ${masterMetadata.filename} (${masterMetadata.totalChunks} chunks)`);

    // ✅ Handle different file types with ultra-fast optimization
    if (masterMetadata.type === 'multi_kv_chunked') {
      response = await handleUltraFastChunkedFile(request, kvNamespaces, masterMetadata, extension, env);
    } else {
      response = await handleUltraFastSingleFile(request, kvNamespaces.FILES_KV, actualId, extension, masterMetadata, env);
    }

    // ✅ Cache response in CDN for ultra-fast future requests
    if (response && response.status === 200) {
      const cacheableResponse = new Response(response.body.tee ? response.body.tee() : response.body, {
        status: response.status,
        headers: response.headers
      });
      
      // ✅ Add aggressive caching headers
      cacheableResponse.headers.set('Cache-Control', 'public, max-age=31536000, immutable');
      cacheableResponse.headers.set('CDN-Cache-Control', 'max-age=31536000');
      cacheableResponse.headers.set('Cloudflare-CDN-Cache-Control', 'max-age=31536000');
      
      // Cache in CDN for next requests
      context.waitUntil(cache.put(cacheKey, cacheableResponse.clone()));
    }

    return response;

  } catch (error) {
    console.error('Ultra-fast serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

// ✅ Ultra-fast chunked file handler with parallel processing
async function handleUltraFastChunkedFile(request, kvNamespaces, masterMetadata, extension, env) {
  const { totalChunks, chunks, filename, size } = masterMetadata;
  
  console.log(`🚀 Ultra-fast serving: ${filename} (${totalChunks} chunks)`);

  // ✅ Advanced Range request handling for video streaming
  const range = request.headers.get('Range');
  if (range) {
    return await handleUltraFastRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env);
  }

  // ✅ Parallel chunk fetching with connection pooling
  const chunkPromises = chunks.map(async (chunkInfo, index) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.chunkKey || `${masterMetadata.id || actualId}_chunk_${index}`;
    
    return await getUltraFastChunk(kvNamespace, chunkKey, chunkInfo, env, index);
  });

  // ✅ Parallel execution with Promise.all for maximum speed
  const chunkResults = await Promise.all(chunkPromises);
  
  // ✅ Sort and combine with optimized memory usage
  chunkResults.sort((a, b) => a.index - b.index);
  
  // ✅ Stream-based response for large files (no memory limit)
  const readable = new ReadableStream({
    start(controller) {
      let currentChunk = 0;
      
      const enqueueNextChunk = () => {
        if (currentChunk >= chunkResults.length) {
          controller.close();
          return;
        }
        
        const chunk = chunkResults[currentChunk];
        controller.enqueue(new Uint8Array(chunk.data));
        currentChunk++;
        
        // Use setTimeout to prevent blocking
        setTimeout(enqueueNextChunk, 0);
      };
      
      enqueueNextChunk();
    }
  });

  // ✅ Ultra-optimized response headers
  const headers = new Headers();
  const mimeType = getMimeType(extension);
  
  // Core headers
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes');
  
  // ✅ Performance optimization headers
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('CDN-Cache-Control', 'max-age=31536000');
  headers.set('Cloudflare-CDN-Cache-Control', 'max-age=31536000');
  headers.set('Vary', 'Accept-Encoding');
  
  // ✅ CORS and security headers
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type');
  headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
  
  // ✅ Speed optimization headers
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('X-Frame-Options', 'ALLOWALL');
  headers.set('Referrer-Policy', 'no-referrer-when-downgrade');

  // ✅ Content disposition based on file type
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');
  
  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    // ✅ Force inline for media files for faster loading
    if (mimeType.startsWith('video/') || mimeType.startsWith('audio/') || mimeType.startsWith('image/')) {
      headers.set('Content-Disposition', 'inline');
    } else if (mimeType === 'application/pdf') {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log('✅ Ultra-fast chunked file served with streaming');
  return new Response(readable, { status: 200, headers });
}

// ✅ Ultra-fast chunk retrieval with connection pooling
async function getUltraFastChunk(kvNamespace, chunkKey, chunkInfo, env, index) {
  console.log(`🚀 Ultra-fast chunk ${index}: ${chunkKey}`);
  
  const chunkMetadataString = await kvNamespace.get(chunkKey);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${chunkKey} not found`);
  }
  
  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;
  
  // ✅ Ultra-fast fetch with optimized headers
  const fetchOptions = {
    method: 'GET',
    headers: {
      'User-Agent': 'MaryaVault-UltraFast/1.0',
      'Accept': '*/*',
      'Accept-Encoding': 'gzip, deflate, br',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    },
    // ✅ Enable HTTP/3 and modern protocols
    cf: {
      cacheTtl: 300,
      cacheEverything: true,
      minify: {
        javascript: false,
        css: false,
        html: false
      }
    }
  };

  let response = await fetch(directUrl, fetchOptions);
  
  // ✅ Auto-refresh expired URLs
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 Refreshing expired URL for chunk ${index}`);
    
    const BOT_TOKEN = env.BOT_TOKEN;
    if (!BOT_TOKEN) {
      throw new Error('BOT_TOKEN not available for URL refresh');
    }
    
    try {
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        fetchOptions
      );
      
      if (!getFileResponse.ok) {
        throw new Error(`Telegram getFile failed: ${getFileResponse.status}`);
      }
      
      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) {
        throw new Error('Invalid Telegram getFile response');
      }
      
      const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
      
      // ✅ Update KV with fresh URL
      const updatedMetadata = {
        ...chunkMetadata,
        directUrl: freshUrl,
        lastRefreshed: Date.now(),
        refreshCount: (chunkMetadata.refreshCount || 0) + 1
      };
      
      await kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata));
      console.log(`✅ URL refreshed for chunk ${index}`);
      
      response = await fetch(freshUrl, fetchOptions);
      
    } catch (refreshError) {
      console.error(`❌ Failed to refresh URL for chunk ${index}:`, refreshError);
      throw new Error(`Failed to refresh expired URL: ${refreshError.message}`);
    }
  }
  
  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${index}: ${response.status}`);
  }
  
  return {
    index: chunkInfo.index,
    data: await response.arrayBuffer()
  };
}

// ✅ Ultra-fast Range request handler for video streaming
async function handleUltraFastRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env) {
  console.log('🎥 Ultra-fast Range request:', range);
  
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
  
  const { start, end } = ranges;
  const chunkSize = end - start + 1;
  
  // ✅ Determine needed chunks efficiently
  const CHUNK_SIZE = 20 * 1024 * 1024;
  const startChunk = Math.floor(start / CHUNK_SIZE);
  const endChunk = Math.floor(end / CHUNK_SIZE);
  
  console.log(`🎯 Range needs chunks ${startChunk} to ${endChunk}`);
  
  const neededChunks = masterMetadata.chunks.slice(startChunk, endChunk + 1);
  
  // ✅ Parallel chunk fetching for range
  const chunkPromises = neededChunks.map(async (chunkInfo) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.chunkKey;
    return await getUltraFastChunk(kvNamespace, chunkKey, chunkInfo, env, chunkInfo.index);
  });
  
  const chunkResults = await Promise.all(chunkPromises);
  chunkResults.sort((a, b) => a.index - b.index);
  
  // ✅ Combine and extract range efficiently
  const combinedSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(combinedSize);
  
  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }
  
  const rangeStart = start - (startChunk * CHUNK_SIZE);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + chunkSize);
  
  // ✅ Ultra-optimized Range response headers
  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', chunkSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  
  // ✅ Performance headers for video streaming
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('CDN-Cache-Control', 'max-age=31536000');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
  
  console.log(`✅ Ultra-fast Range served: ${chunkSize} bytes`);
  
  return new Response(rangeBuffer, {
    status: 206,
    headers: headers
  });
}

// ✅ Ultra-fast single file handler
async function handleUltraFastSingleFile(request, kvNamespace, actualId, extension, metadata, env) {
  console.log('🚀 Ultra-fast single file serve');
  
  const directUrl = await kvNamespace.get(actualId);
  if (!directUrl) {
    return new Response('File not found', { status: 404 });
  }
  
  // ✅ Ultra-fast fetch with optimization
  const fetchOptions = {
    method: 'GET',
    headers: {
      'User-Agent': 'MaryaVault-UltraFast/1.0',
      'Accept': '*/*',
      'Accept-Encoding': 'gzip, deflate, br',
      'Range': request.headers.get('Range') || undefined
    },
    cf: {
      cacheTtl: 31536000,
      cacheEverything: true
    }
  };

  let response = await fetch(directUrl, fetchOptions);
  
  // ✅ Auto-refresh if expired
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log('🔄 Single file URL expired, refreshing...');
    
    const BOT_TOKEN = env.BOT_TOKEN;
    const telegramFileId = metadata?.telegramFileId;
    
    if (BOT_TOKEN && telegramFileId) {
      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
          fetchOptions
        );
        
        if (getFileResponse.ok) {
          const getFileData = await getFileResponse.json();
          if (getFileData.ok && getFileData.result?.file_path) {
            const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
            
            await kvNamespace.put(actualId, freshUrl, { 
              metadata: { ...metadata, lastRefreshed: Date.now() } 
            });
            
            console.log('✅ Single file URL refreshed');
            response = await fetch(freshUrl, fetchOptions);
          }
        }
      } catch (refreshError) {
        console.error('Failed to refresh single file URL:', refreshError);
      }
    }
  }
  
  if (!response.ok) {
    return new Response(`File not accessible: ${response.status}`, { status: response.status });
  }
  
  // ✅ Ultra-optimized headers
  const headers = new Headers();
  const mimeType = getMimeType(extension);
  
  // Copy important headers from response
  for (const [key, value] of response.headers.entries()) {
    const lowerKey = key.toLowerCase();
    if (['content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag'].includes(lowerKey)) {
      headers.set(key, value);
    }
  }
  
  headers.set('Content-Type', mimeType);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  headers.set('CDN-Cache-Control', 'max-age=31536000');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
  
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

// ✅ Parse Range header
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) return null;
  
  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[31] ? parseInt(rangeMatch[31], 10) : size - 1;
  
  if (start >= size || end >= size || start > end) return null;
  
  return [{ start, end }];
}
