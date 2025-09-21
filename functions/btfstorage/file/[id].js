// Browser-compatible MIME types
const MIME_TYPES = {
  'mp4': 'video/mp4',
  'webm': 'video/webm',
  'mkv': 'video/mp4', // Serve as MP4 for instant browser play
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

// Smart cache for chunks (in-memory cache per worker)
const chunkCache = new Map();
const CACHE_TTL = 30 * 60 * 1000; // 30 minutes
const MAX_CACHE_SIZE = 50; // Max 50 chunks cached

function getCachedChunk(key) {
  const cached = chunkCache.get(key);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    console.log(`💾 Cache hit for chunk: ${key}`);
    return cached.data;
  }
  return null;
}

function setCachedChunk(key, data) {
  // Clean old cache entries if full
  if (chunkCache.size >= MAX_CACHE_SIZE) {
    const oldestKey = chunkCache.keys().next().value;
    chunkCache.delete(oldestKey);
  }
  
  chunkCache.set(key, {
    data: data,
    timestamp: Date.now()
  });
  console.log(`💾 Cached chunk: ${key} (${chunkCache.size}/${MAX_CACHE_SIZE})`);
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 PROGRESSIVE STREAMING WITH CACHE:', fileId);

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

    // Progressive streaming with smart caching
    return await handleProgressiveStreaming(request, kvNamespaces, masterMetadata, extension, env);

  } catch (error) {
    console.error('💥 Error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

// Progressive streaming (YouTube-style - only load what's needed)
async function handleProgressiveStreaming(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);
  
  console.log(`🎬 Progressive streaming: ${filename} (Type: ${mimeType}, ${chunks.length} chunks)`);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';
  
  console.log(`📺 Mode: ${isDownload ? 'DOWNLOAD' : 'PROGRESSIVE_STREAM'}`);

  // Handle Range requests (video seeking/skipping)
  const range = request.headers.get('Range');
  if (range && !isDownload) {
    console.log('📺 Range request (smart seeking):', range);
    return await handleSmartRangeStreaming(request, kvNamespaces, masterMetadata, extension, range, env, mimeType);
  }

  // Progressive streaming - start playing immediately, load chunks on demand
  if (!isDownload && isStreamable(mimeType)) {
    console.log('🚀 Starting progressive video streaming...');
    return await handleVideoProgressiveStream(request, kvNamespaces, masterMetadata, extension, env, mimeType);
  }

  // Full download mode
  console.log('💾 Full file download mode...');
  return await handleFullDownload(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload);
}

// Smart video progressive streaming (YouTube-style)
async function handleVideoProgressiveStream(request, kvNamespaces, masterMetadata, extension, env, mimeType) {
  const { chunks, filename, size } = masterMetadata;
  const chunkSize = masterMetadata.chunkSize || Math.ceil(size / chunks.length);
  
  console.log(`🚀 Video progressive stream: ${filename}`);

  // Create a readable stream that loads chunks progressively
  const readable = new ReadableStream({
    async start(controller) {
      try {
        // Start with first few chunks for instant playback
        const INITIAL_CHUNKS = Math.min(5, chunks.length); // Load first 5 chunks immediately
        
        console.log(`🚀 Loading initial ${INITIAL_CHUNKS} chunks for instant playback...`);
        
        // Load initial chunks for smooth start
        const initialPromises = [];
        for (let i = 0; i < INITIAL_CHUNKS; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          const promise = getChunkWithCache(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
          initialPromises.push(promise);
        }

        // Wait for initial chunks and stream them
        const initialResults = await Promise.all(initialPromises);
        
        for (let i = 0; i < initialResults.length; i++) {
          console.log(`📺 Streaming initial chunk ${i + 1}/${INITIAL_CHUNKS}`);
          controller.enqueue(new Uint8Array(initialResults[i]));
        }

        // Now progressively load and stream remaining chunks
        for (let i = INITIAL_CHUNKS; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          console.log(`📺 Progressive loading chunk ${i + 1}/${chunks.length}...`);
          
          try {
            const chunkData = await getChunkWithCache(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
            controller.enqueue(new Uint8Array(chunkData));
            
            // Small delay for smooth streaming
            await new Promise(resolve => setTimeout(resolve, 100));
            
          } catch (chunkError) {
            console.error(`❌ Progressive chunk ${i} failed, continuing...`, chunkError);
            // Continue streaming other chunks instead of failing
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

  // Perfect headers for progressive video streaming
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges');
  headers.set('Cache-Control', 'public, max-age=86400');
  
  // Force inline for instant video playback
  headers.set('Content-Disposition', 'inline');
  
  // Video streaming optimizations
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('Cross-Origin-Resource-Policy', 'cross-origin');

  console.log(`🎬 Progressive video stream started as ${mimeType}`);
  return new Response(readable, { status: 200, headers });
}

// Smart range streaming (for video seeking/skipping)
async function handleSmartRangeStreaming(request, kvNamespaces, masterMetadata, extension, range, env, mimeType) {
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

  console.log(`📺 Smart range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB) - Video seeking`);

  // Determine which chunks are needed for this range
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Smart loading chunks ${startChunk}-${endChunk} for seek operation`);

  // Load only the required chunks (not all chunks)
  const chunkResults = [];
  
  // Load chunks in small batches for seeking
  const SEEK_BATCH_SIZE = 3; // Only 3 chunks at a time for fast seeking
  
  for (let i = 0; i < neededChunks.length; i += SEEK_BATCH_SIZE) {
    const batchChunks = neededChunks.slice(i, Math.min(i + SEEK_BATCH_SIZE, neededChunks.length));
    
    console.log(`📦 Loading seek batch ${Math.floor(i/SEEK_BATCH_SIZE) + 1} (${batchChunks.length} chunks)...`);
    
    const batchPromises = batchChunks.map(async (chunkInfo, batchIndex) => {
      const actualIndex = startChunk + i + batchIndex;
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      
      const chunkData = await getChunkWithCache(kvNamespace, chunkInfo.keyName, chunkInfo, env, actualIndex);
      return {
        index: actualIndex,
        data: chunkData
      };
    });
    
    const batchResults = await Promise.all(batchPromises);
    chunkResults.push(...batchResults);
  }

  // Sort results by index
  chunkResults.sort((a, b) => a.index - b.index);

  // Combine chunks
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

  // Perfect range headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=86400');
  headers.set('Content-Disposition', 'inline');

  console.log(`✅ Smart range streaming: ${Math.round(requestedSize/1024/1024)}MB for seek`);
  return new Response(rangeBuffer, { status: 206, headers });
}

// Full download for non-streamable files
async function handleFullDownload(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`💾 Full download: ${filename} (${chunks.length} chunks)`);

  // Use sequential loading to avoid subrequest limits
  const readable = new ReadableStream({
    async start(controller) {
      try {
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          console.log(`💾 Download loading chunk ${i + 1}/${chunks.length}...`);
          
          const chunkData = await getChunkWithCache(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
          controller.enqueue(new Uint8Array(chunkData));
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

// Enhanced chunk loading with smart caching
async function getChunkWithCache(kvNamespace, keyName, chunkInfo, env, index) {
  const cacheKey = `${keyName}_${chunkInfo.telegramFileId}`;
  
  // Check cache first
  const cachedData = getCachedChunk(cacheKey);
  if (cachedData) {
    return cachedData;
  }

  console.log(`📦 Loading chunk ${index}: ${keyName}`);

  let chunkMetadata;
  try {
    const chunkMetadataString = await kvNamespace.get(keyName);
    if (!chunkMetadataString) {
      throw new Error(`Chunk ${keyName} not found in KV`);
    }
    chunkMetadata = JSON.parse(chunkMetadataString);
  } catch (kvError) {
    throw new Error(`KV error for chunk ${keyName}: ${kvError.message}`);
  }

  let directUrl = chunkMetadata.directUrl;
  let response = await fetch(directUrl, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; MaryaVault/1.0)'
    }
  });

  // Auto-refresh expired URLs
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${index}, refreshing...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    if (botTokens.length > 0) {
      const botToken = botTokens[0]; // Use first available bot
      
      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { signal: AbortSignal.timeout(15000) }
        );

        if (getFileResponse.ok) {
          const getFileData = await getFileResponse.json();
          if (getFileData.ok && getFileData.result?.file_path) {
            const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

            // Update KV with fresh URL (fire and forget)
            const updatedMetadata = {
              ...chunkMetadata,
              directUrl: freshUrl,
              lastRefreshed: Date.now()
            };
            
            kvNamespace.put(keyName, JSON.stringify(updatedMetadata)).catch(() => {});

            // Try with fresh URL
            response = await fetch(freshUrl, {
              headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; MaryaVault/1.0)'
              }
            });
            
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

  // Cache the chunk
  setCachedChunk(cacheKey, arrayBuffer);

  return arrayBuffer;
}

// Parse Range header
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) return null;

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}
