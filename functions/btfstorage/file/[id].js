// 🎬 ADAPTIVE STREAMING SYSTEM - Netflix/YouTube Style
// Smart Progressive Loading + Intelligent Cache Management

const MIME_TYPES = {
  'mp4': 'video/mp4', 'mkv': 'video/mp4', 'avi': 'video/mp4', 'mov': 'video/mp4',
  'm4v': 'video/mp4', 'wmv': 'video/mp4', 'flv': 'video/mp4', '3gp': 'video/mp4',
  'webm': 'video/webm', 'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'aac': 'audio/mp4',
  'm4a': 'audio/mp4', 'ogg': 'audio/ogg'
};

// Smart Cache - Worker memory ke andar chunk cache
class SmartVideoCache {
  constructor() {
    this.cache = new Map();
    this.accessTime = new Map();
    this.maxSize = 50 * 1024 * 1024; // 50MB cache max
    this.currentSize = 0;
    this.hitCount = 0;
    this.missCount = 0;
  }

  get(key) {
    if (this.cache.has(key)) {
      this.accessTime.set(key, Date.now());
      this.hitCount++;
      console.log(`📚 CACHE HIT: ${key} (${this.getHitRate()}% hit rate)`);
      return this.cache.get(key);
    }
    this.missCount++;
    console.log(`❌ CACHE MISS: ${key} (${this.getHitRate()}% hit rate)`);
    return null;
  }

  set(key, data) {
    const dataSize = data.byteLength;
    
    // Free space if needed
    while (this.currentSize + dataSize > this.maxSize && this.cache.size > 0) {
      this.evictOldest();
    }

    // Store in cache
    this.cache.set(key, data);
    this.accessTime.set(key, Date.now());
    this.currentSize += dataSize;
    
    console.log(`💾 CACHE SET: ${key} (${Math.round(dataSize/1024)}KB) - Total: ${Math.round(this.currentSize/1024/1024)}MB`);
  }

  evictOldest() {
    let oldestKey = null;
    let oldestTime = Date.now();
    
    for (const [key, time] of this.accessTime.entries()) {
      if (time < oldestTime) {
        oldestTime = time;
        oldestKey = key;
      }
    }
    
    if (oldestKey) {
      const data = this.cache.get(oldestKey);
      this.currentSize -= data.byteLength;
      this.cache.delete(oldestKey);
      this.accessTime.delete(oldestKey);
      console.log(`🗑️ CACHE EVICT: ${oldestKey} (freed ${Math.round(data.byteLength/1024)}KB)`);
    }
  }

  getHitRate() {
    const total = this.hitCount + this.missCount;
    return total > 0 ? Math.round((this.hitCount / total) * 100) : 0;
  }

  getStats() {
    return {
      size: this.cache.size,
      currentMB: Math.round(this.currentSize / 1024 / 1024),
      maxMB: Math.round(this.maxSize / 1024 / 1024),
      hitRate: this.getHitRate()
    };
  }
}

// Global smart cache instance
const smartCache = new SmartVideoCache();

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 ADAPTIVE STREAMING ENGINE:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    if (!actualId.startsWith('MSM')) {
      return new Response('Invalid file ID', { status: 404 });
    }

    // Get metadata
    const metadataString = await env.FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { status: 404 });
    }

    const metadata = JSON.parse(metadataString);
    const mimeType = MIME_TYPES[extension.toLowerCase().replace('.', '')] || 'application/octet-stream';

    console.log(`📺 ${metadata.filename} (${metadata.chunks?.length || 0} chunks, ${Math.round(metadata.size/1024/1024)}MB)`);

    // Handle single files (best performance)
    if (metadata.telegramFileId && !metadata.chunks) {
      return await handleSingleFile(request, env, metadata.telegramFileId, mimeType, metadata.filename);
    }

    // Handle chunked files with adaptive streaming
    if (metadata.chunks && metadata.chunks.length > 0) {
      return await handleAdaptiveStreaming(request, env, metadata, mimeType);
    }

    return new Response('Invalid file format', { status: 400 });

  } catch (error) {
    console.error('🎬 Streaming error:', error);
    return new Response(`Streaming error: ${error.message}`, { status: 500 });
  }
}

// Single file - Direct proxy (zero CPU usage)
async function handleSingleFile(request, env, telegramFileId, mimeType, filename) {
  console.log('🚀 Single file streaming');
  
  const botToken = env.BOT_TOKEN || env.BOT_TOKEN2 || env.BOT_TOKEN3 || env.BOT_TOKEN4;
  
  try {
    // Get fresh Telegram URL
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(telegramFileId)}`,
      { signal: AbortSignal.timeout(10000) }
    );

    const getFileData = await getFileResponse.json();
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Telegram API error');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
    
    // Proxy with proper headers
    const telegramResponse = await fetch(directUrl, {
      headers: request.headers.get('Range') ? { 'Range': request.headers.get('Range') } : {}
    });

    const headers = new Headers(telegramResponse.headers);
    headers.set('Content-Type', mimeType);
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Content-Disposition', 'inline');

    return new Response(telegramResponse.body, {
      status: telegramResponse.status,
      headers: headers
    });

  } catch (error) {
    console.error('Single file error:', error);
    return new Response(`Single file error: ${error.message}`, { status: 500 });
  }
}

// Adaptive streaming for chunked files
async function handleAdaptiveStreaming(request, env, metadata, mimeType) {
  const chunks = metadata.chunks;
  const size = metadata.size;
  const filename = metadata.filename;
  const chunkSize = metadata.chunkSize || Math.ceil(size / chunks.length);

  console.log(`🎬 ADAPTIVE STREAMING: ${chunks.length} chunks, ${Math.round(chunkSize/1024/1024)}MB per chunk`);

  // Handle Range requests (video seeking)
  const range = request.headers.get('Range');
  if (range) {
    return await handleAdaptiveRange(request, env, metadata, range, mimeType, chunkSize);
  }

  // For non-range: Start adaptive streaming
  return await startAdaptiveStreaming(request, env, metadata, mimeType, chunkSize);
}

// Start adaptive streaming (Netflix/YouTube style)
async function startAdaptiveStreaming(request, env, metadata, mimeType, chunkSize) {
  const chunks = metadata.chunks;
  const size = metadata.size;
  const filename = metadata.filename;

  console.log('🎬 Starting adaptive streaming...');

  // Calculate initial streaming window (first 3-4 chunks for instant play)
  const initialChunks = Math.min(4, chunks.length);
  const initialSize = Math.min(initialChunks * chunkSize, 10 * 1024 * 1024); // Max 10MB initial

  console.log(`⚡ INSTANT PLAY: Loading first ${initialChunks} chunks (${Math.round(initialSize/1024/1024)}MB)`);

  try {
    // Load initial chunks for instant playback
    const initialBuffer = await loadInitialChunks(env, chunks.slice(0, initialChunks), 0);

    // Send 206 response with initial buffer to start playback
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', initialBuffer.byteLength.toString());
    headers.set('Content-Range', `bytes 0-${initialBuffer.byteLength - 1}/${size}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    
    // Netflix-style headers
    headers.set('X-Streaming-Mode', 'adaptive');
    headers.set('X-Initial-Chunks', initialChunks.toString());
    headers.set('X-Cache-Stats', JSON.stringify(smartCache.getStats()));

    console.log(`✅ INSTANT PLAY READY: ${Math.round(initialBuffer.byteLength/1024/1024)}MB buffered`);
    
    // Start background preloading (async, non-blocking)
    scheduleBackgroundPreloading(env, chunks, initialChunks);

    return new Response(initialBuffer, { status: 206, headers });

  } catch (error) {
    console.error('🎬 Adaptive streaming start error:', error);
    return new Response(`Adaptive streaming error: ${error.message}`, { status: 500 });
  }
}

// Load initial chunks for instant playback
async function loadInitialChunks(env, chunkInfos, startIndex) {
  console.log(`⚡ Loading ${chunkInfos.length} initial chunks...`);
  
  const parts = [];
  let totalSize = 0;

  for (let i = 0; i < chunkInfos.length; i++) {
    const chunkInfo = chunkInfos[i];
    const chunkIndex = startIndex + i;
    const cacheKey = `chunk_${chunkIndex}_${chunkInfo.keyName || chunkInfo.chunkKey}`;
    
    console.log(`⚡ Initial chunk ${chunkIndex + 1}...`);
    
    try {
      // Check smart cache first
      let chunkData = smartCache.get(cacheKey);
      
      if (!chunkData) {
        // Load from storage
        chunkData = await loadSingleChunk(env, chunkInfo, chunkIndex);
        // Store in smart cache
        smartCache.set(cacheKey, chunkData);
      }
      
      parts.push(new Uint8Array(chunkData));
      totalSize += chunkData.byteLength;
      
      console.log(`✅ Initial chunk ${chunkIndex + 1}: ${Math.round(chunkData.byteLength/1024)}KB (${smartCache.cache.has(cacheKey) ? 'CACHED' : 'LOADED'})`);
      
    } catch (err) {
      console.error(`❌ Initial chunk ${chunkIndex + 1} failed:`, err);
      // Don't fail completely, continue with available chunks
      continue;
    }
  }

  // Combine chunks
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.byteLength;
  }

  console.log(`⚡ INITIAL BUFFER READY: ${Math.round(totalSize/1024/1024)}MB`);
  return combined;
}

// Background preloading (Netflix/YouTube strategy)
async function scheduleBackgroundPreloading(env, chunks, startIndex) {
  console.log(`🔄 BACKGROUND PRELOAD: Scheduling preload for chunks ${startIndex + 1}-${chunks.length}`);
  
  // Don't await this - let it run in background
  setTimeout(async () => {
    const remainingChunks = chunks.slice(startIndex);
    const batchSize = 3; // Preload 3 chunks at a time
    
    for (let i = 0; i < remainingChunks.length; i += batchSize) {
      const batch = remainingChunks.slice(i, i + batchSize);
      
      try {
        await preloadChunkBatch(env, batch, startIndex + i);
        
        // Small delay between batches to be nice to the system
        await new Promise(resolve => setTimeout(resolve, 500));
        
      } catch (error) {
        console.error(`🔄 Batch ${i}-${i + batch.length} preload failed:`, error);
        // Continue with next batch
        continue;
      }
    }
    
    console.log(`🔄 BACKGROUND PRELOAD COMPLETED`);
  }, 1000); // Start after 1 second
}

// Preload chunk batch
async function preloadChunkBatch(env, chunkInfos, startIndex) {
  console.log(`🔄 Preloading batch: chunks ${startIndex + 1}-${startIndex + chunkInfos.length}`);
  
  for (let i = 0; i < chunkInfos.length; i++) {
    const chunkInfo = chunkInfos[i];
    const chunkIndex = startIndex + i;
    const cacheKey = `chunk_${chunkIndex}_${chunkInfo.keyName || chunkInfo.chunkKey}`;
    
    // Skip if already cached
    if (smartCache.get(cacheKey)) {
      console.log(`🔄 Chunk ${chunkIndex + 1} already cached, skipping`);
      continue;
    }
    
    try {
      console.log(`🔄 Preloading chunk ${chunkIndex + 1}...`);
      const chunkData = await loadSingleChunk(env, chunkInfo, chunkIndex);
      smartCache.set(cacheKey, chunkData);
      
      console.log(`✅ Preloaded chunk ${chunkIndex + 1}: ${Math.round(chunkData.byteLength/1024)}KB`);
      
    } catch (error) {
      console.error(`🔄 Preload failed for chunk ${chunkIndex + 1}:`, error);
      // Continue with next chunk
      continue;
    }
  }
}

// Handle adaptive range requests (video seeking)
async function handleAdaptiveRange(request, env, metadata, rangeHeader, mimeType, chunkSize) {
  const chunks = metadata.chunks;
  const size = metadata.size;

  console.log('🎬 ADAPTIVE RANGE:', rangeHeader);

  // Parse range
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
  console.log(`🎬 Range: ${start}-${end} (${Math.round(requestedSize/1024)}KB)`);

  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Range needs chunks: ${startChunk}-${endChunk} (${neededChunks.length})`);

  // Adaptive loading strategy
  if (neededChunks.length > 6) {
    // Too many chunks, serve partial range to stay within limits
    console.log('⚠️ Range too large, serving partial');
    const partialChunks = neededChunks.slice(0, 6);
    const partialEndChunk = startChunk + 5;
    const partialEnd = Math.min(end, (partialEndChunk + 1) * chunkSize - 1);
    
    const rangeData = await loadRangeChunks(env, partialChunks, startChunk, start, partialEnd, chunkSize);
    return createRangeResponse(rangeData, start, partialEnd, size, mimeType);
  }

  // Normal range loading with smart cache
  const rangeData = await loadRangeChunks(env, neededChunks, startChunk, start, end, chunkSize);
  return createRangeResponse(rangeData, start, end, size, mimeType);
}

// Load chunks for range with smart caching
async function loadRangeChunks(env, chunkInfos, startChunk, rangeStart, rangeEnd, chunkSize) {
  console.log(`📦 Loading ${chunkInfos.length} chunks for range...`);
  
  const parts = [];
  let totalSize = 0;

  // Load chunks with smart cache
  for (let i = 0; i < chunkInfos.length; i++) {
    const chunkInfo = chunkInfos[i];
    const chunkIndex = startChunk + i;
    const cacheKey = `chunk_${chunkIndex}_${chunkInfo.keyName || chunkInfo.chunkKey}`;
    
    try {
      // Try smart cache first
      let chunkData = smartCache.get(cacheKey);
      
      if (!chunkData) {
        // Load from storage and cache
        console.log(`📦 Loading chunk ${chunkIndex + 1} from storage...`);
        chunkData = await loadSingleChunk(env, chunkInfo, chunkIndex);
        smartCache.set(cacheKey, chunkData);
      }
      
      parts.push(new Uint8Array(chunkData));
      totalSize += chunkData.byteLength;
      
      console.log(`✅ Range chunk ${chunkIndex + 1}: ${Math.round(chunkData.byteLength/1024)}KB (${smartCache.cache.has(cacheKey) ? 'CACHE' : 'STORAGE'})`);
      
    } catch (error) {
      console.error(`❌ Range chunk ${chunkIndex + 1} failed:`, error);
      throw error;
    }
  }

  // Combine and extract exact range
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.byteLength;
  }

  // Extract exact requested range
  const rangeStartInBuffer = rangeStart - (startChunk * chunkSize);
  const requestedSize = rangeEnd - rangeStart + 1;
  const exactRange = combined.slice(rangeStartInBuffer, rangeStartInBuffer + requestedSize);

  console.log(`🎯 EXACT RANGE EXTRACTED: ${exactRange.byteLength} bytes`);
  return exactRange;
}

// Create range response
function createRangeResponse(data, start, end, totalSize, mimeType) {
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', data.byteLength.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Content-Disposition', 'inline');
  
  // Smart cache stats
  headers.set('X-Cache-Stats', JSON.stringify(smartCache.getStats()));

  console.log(`✅ RANGE RESPONSE: ${data.byteLength} bytes (Cache: ${smartCache.getHitRate()}% hit rate)`);
  return new Response(data, { status: 206, headers });
}

// Load single chunk with 4-bot fallback
async function loadSingleChunk(env, chunkInfo, index) {
  const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;
  const kvNamespace = env[chunkInfo.kvNamespace] || env.FILES_KV;
  
  console.log(`📥 Loading chunk ${index + 1}: ${chunkKey}`);
  
  // Get chunk metadata
  const metadataString = await kvNamespace.get(chunkKey);
  if (!metadataString) {
    throw new Error(`Chunk metadata not found: ${chunkKey}`);
  }

  const chunkMetadata = JSON.parse(metadataString);
  
  // Try direct URL first
  let response = await fetch(chunkMetadata.directUrl, { signal: AbortSignal.timeout(30000) });
  
  if (response.ok) {
    return response.arrayBuffer();
  }

  // URL refresh with 4-bot fallback
  console.log(`🔄 Refreshing chunk ${index + 1}...`);
  
  const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
  
  for (const botToken of botTokens) {
    try {
      const getFileResponse = await fetch(
        `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
        { signal: AbortSignal.timeout(15000) }
      );

      const getFileData = await getFileResponse.json();
      if (!getFileData.ok || !getFileData.result?.file_path) continue;

      const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
      response = await fetch(freshUrl, { signal: AbortSignal.timeout(30000) });
      
      if (response.ok) {
        // Update KV async
        kvNamespace.put(chunkKey, JSON.stringify({
          ...chunkMetadata,
          directUrl: freshUrl,
          refreshed: Date.now()
        })).catch(() => {});

        return response.arrayBuffer();
      }
      
    } catch (botError) {
      continue;
    }
  }

  throw new Error(`All bot tokens failed for chunk ${index + 1}`);
}
