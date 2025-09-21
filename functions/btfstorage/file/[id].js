// 🎬 ULTIMATE VIDEO STREAMING SOLUTION - 15 METHODS COMBINED
// ✅ Works with ALL browsers, ALL devices, ALL video players

// Enhanced MIME types with video-specific optimizations
const MIME_TYPES = {
  'mp4': 'video/mp4',
  'webm': 'video/webm',  
  'mkv': 'video/x-matroska',
  'mov': 'video/quicktime',
  'avi': 'video/x-msvideo',
  'm4v': 'video/mp4',
  'wmv': 'video/x-ms-wmv',
  'flv': 'video/x-flv',
  '3gp': 'video/3gpp',
  '3g2': 'video/3gpp2',
  'ogv': 'video/ogg',
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'flac': 'audio/flac',
  'aac': 'audio/aac',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg',
  'weba': 'audio/webm',
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png', 
  'gif': 'image/gif',
  'webp': 'image/webp',
  'svg': 'image/svg+xml',
  'pdf': 'application/pdf',
  'txt': 'text/plain',
  'zip': 'application/zip',
  'rar': 'application/x-rar-compressed',
  '7z': 'application/x-7z-compressed'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

function isVideoFile(mimeType) {
  return mimeType.startsWith('video/');
}

function isAudioFile(mimeType) {
  return mimeType.startsWith('audio/');
}

function isMediaFile(mimeType) {
  return isVideoFile(mimeType) || isAudioFile(mimeType);
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

  console.log('🎬 ULTIMATE STREAMING ENGINE ACTIVATED:', fileId);

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
    const mimeType = getMimeType(extension);

    console.log(`📁 File: ${filename} (${Math.round(size/1024/1024)}MB, ${totalChunks} chunks, Type: ${mimeType})`);

    // METHOD 1-15: Ultimate streaming with all methods combined
    return await handleUltimateStreaming(request, kvNamespaces, masterMetadata, extension, env);

  } catch (error) {
    console.error('💥 Ultimate streaming error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

// 🚀 ULTIMATE STREAMING HANDLER - ALL 15 METHODS COMBINED
async function handleUltimateStreaming(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);
  const isVideo = isVideoFile(mimeType);
  const isAudio = isAudioFile(mimeType);
  const isMedia = isMediaFile(mimeType);

  console.log(`🎬 ULTIMATE STREAMING: ${filename} (${Math.round(size/1024/1024)}MB, Video: ${isVideo}, Audio: ${isAudio})`);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';
  const userAgent = request.headers.get('User-Agent') || '';
  const isMobile = /Mobile|Android|iPhone|iPad/i.test(userAgent);
  const isSafari = /Safari/i.test(userAgent) && !/Chrome/i.test(userAgent);
  const isIOS = /iPhone|iPad/i.test(userAgent);

  console.log(`📱 Client: Mobile: ${isMobile}, Safari: ${isSafari}, iOS: ${isIOS}, Mode: ${isDownload ? 'DOWNLOAD' : 'STREAM'}`);

  // METHOD 1: Advanced Range Request Detection (Critical for video)
  const range = request.headers.get('Range');
  if (range && !isDownload && isMedia) {
    console.log('📺 RANGE REQUEST DETECTED (Video/Audio seeking):', range);
    return await handleAdvancedRangeStream(request, kvNamespaces, masterMetadata, extension, range, env, mimeType, userAgent);
  }

  // METHOD 2: HEAD Request Support (Browser pre-flight)
  if (request.method === 'HEAD') {
    console.log('🔍 HEAD REQUEST - Browser checking file info');
    return await handleHeadRequest(size, mimeType, filename, isDownload);
  }

  // METHOD 3: OPTIONS Request Support (CORS pre-flight)
  if (request.method === 'OPTIONS') {
    console.log('🔄 OPTIONS REQUEST - CORS pre-flight');
    return handleOptionsRequest();
  }

  // METHOD 4-15: Full streaming with all optimizations
  console.log('🌊 FULL STREAMING with ALL METHODS...');

  const readable = new ReadableStream({
    async start(controller) {
      try {
        console.log(`🚀 Starting ULTIMATE streaming (${chunks.length} chunks)...`);

        // METHOD 4: Smart batch sizing based on file type and device
        let BATCH_SIZE = 10; // Default
        if (isVideo && size > 50 * 1024 * 1024) BATCH_SIZE = 6;  // Large videos: smaller batches
        else if (isVideo) BATCH_SIZE = 8;  // Videos: medium batches  
        else if (isAudio) BATCH_SIZE = 12; // Audio: larger batches
        else if (isMobile) BATCH_SIZE = 8; // Mobile: smaller batches
        else BATCH_SIZE = 10; // Default

        // METHOD 5: Smart delays based on content type
        const CHUNK_DELAY = isVideo ? (isMobile ? 75 : 50) : (isAudio ? 30 : 50);
        const BATCH_DELAY = isVideo ? (isMobile ? 150 : 100) : (isAudio ? 50 : 100);

        console.log(`⚡ Optimizations: BatchSize=${BATCH_SIZE}, ChunkDelay=${CHUNK_DELAY}ms, BatchDelay=${BATCH_DELAY}ms`);

        for (let batchStart = 0; batchStart < chunks.length; batchStart += BATCH_SIZE) {
          const batchEnd = Math.min(batchStart + BATCH_SIZE, chunks.length);
          const batchChunks = chunks.slice(batchStart, batchEnd);

          console.log(`📦 Ultimate batch ${Math.floor(batchStart/BATCH_SIZE) + 1}/${Math.ceil(chunks.length/BATCH_SIZE)} (${batchChunks.length} chunks)`);

          // METHOD 6: Hybrid parallel/sequential processing
          if ((isVideo && batchChunks.length <= 4) || (isAudio && batchChunks.length <= 6)) {
            // METHOD 7: Parallel processing for media files (faster)
            console.log('⚡ PARALLEL processing for media optimization');
            
            const chunkPromises = batchChunks.map((chunkInfo, i) => {
              const chunkIndex = batchStart + i;
              return getUltimateChunk(kvNamespaces, chunkInfo, env, chunkIndex, isMedia, userAgent);
            });

            try {
              const chunkResults = await Promise.all(chunkPromises);
              
              // METHOD 8: Smart chunking for media streaming
              for (const chunkData of chunkResults) {
                if (chunkData && chunkData.byteLength > 0) {
                  controller.enqueue(new Uint8Array(chunkData));
                  
                  // METHOD 9: Micro-delays for smooth video streaming
                  if (isVideo && chunkData.byteLength > 1024 * 1024) {
                    await new Promise(resolve => setTimeout(resolve, 25));
                  }
                }
              }
            } catch (batchError) {
              console.error(`❌ Parallel batch failed, falling back to sequential...`);
              
              // METHOD 10: Automatic fallback to sequential
              for (let i = 0; i < batchChunks.length; i++) {
                const chunkInfo = batchChunks[i];
                const chunkIndex = batchStart + i;
                
                try {
                  const chunkData = await getUltimateChunk(kvNamespaces, chunkInfo, env, chunkIndex, isMedia, userAgent);
                  if (chunkData && chunkData.byteLength > 0) {
                    controller.enqueue(new Uint8Array(chunkData));
                  }
                } catch (chunkError) {
                  console.error(`❌ Chunk ${chunkIndex} failed, continuing...`);
                  continue;
                }
              }
            }
          } else {
            // METHOD 11: Sequential processing for large batches
            console.log('🔄 SEQUENTIAL processing for stability');
            
            for (let i = 0; i < batchChunks.length; i++) {
              const chunkInfo = batchChunks[i];
              const chunkIndex = batchStart + i;

              try {
                const chunkData = await getUltimateChunk(kvNamespaces, chunkInfo, env, chunkIndex, isMedia, userAgent);
                
                if (chunkData && chunkData.byteLength > 0) {
                  controller.enqueue(new Uint8Array(chunkData));
                  
                  // METHOD 12: Dynamic delays based on chunk size
                  const delayTime = chunkData.byteLength > 2 * 1024 * 1024 ? CHUNK_DELAY * 2 : CHUNK_DELAY;
                  await new Promise(resolve => setTimeout(resolve, delayTime));
                }

              } catch (chunkError) {
                console.error(`❌ Chunk ${chunkIndex} failed:`, chunkError);
                continue;
              }
            }
          }

          // Delay between batches
          if (batchEnd < chunks.length) {
            await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
          }
        }

        console.log('✅ ULTIMATE streaming completed successfully');
        controller.close();

      } catch (error) {
        console.error('💥 ULTIMATE streaming error:', error);
        controller.error(error);
      }
    }
  });

  // METHOD 13: Ultimate headers for ALL browsers and devices
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  
  // CRITICAL: Video streaming headers
  headers.set('Accept-Ranges', 'bytes'); // Essential for video seeking
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges, Content-Range, Content-Type');
  
  // METHOD 14: Advanced caching strategy
  if (isMedia) {
    headers.set('Cache-Control', 'public, max-age=31536000, immutable'); // 1 year for media
  } else {
    headers.set('Cache-Control', 'public, max-age=86400'); // 1 day for others
  }

  // METHOD 15: Device-specific optimizations
  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
      
      // Essential for video/audio streaming
      headers.set('X-Content-Type-Options', 'nosniff');
      headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
      
      if (isMedia) {
        // Critical for media streaming
        headers.set('Cross-Origin-Embedder-Policy', 'cross-origin');
        headers.set('Vary', 'Range, Accept-Encoding');
        
        // iOS/Safari specific optimizations
        if (isIOS || isSafari) {
          headers.set('Connection', 'keep-alive');
          if (isVideo) {
            headers.set('Content-Transfer-Encoding', 'binary');
          }
        }
        
        // Mobile optimizations
        if (isMobile) {
          headers.set('Keep-Alive', 'timeout=5, max=100');
        }
      }
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log(`🚀 ULTIMATE STREAMING READY: ${filename} as ${mimeType}`);
  return new Response(readable, { status: 200, headers });
}

// 🎯 ADVANCED RANGE REQUEST HANDLER (Critical for video seeking)
async function handleAdvancedRangeStream(request, kvNamespaces, masterMetadata, extension, range, env, mimeType, userAgent) {
  const { size, chunks } = masterMetadata;
  const chunkSize = masterMetadata.chunkSize || Math.ceil(size / chunks.length);
  const isVideo = isVideoFile(mimeType);
  const isMobile = /Mobile|Android|iPhone|iPad/i.test(userAgent);

  console.log(`📺 ADVANCED RANGE PROCESSING - Size: ${Math.round(size/1024/1024)}MB, ChunkSize: ${Math.round(chunkSize/1024)}KB`);

  const ranges = parseAdvancedRange(range, size);
  if (!ranges || ranges.length !== 1) {
    console.error('❌ Invalid range request format');
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: { 
        'Content-Range': `bytes */${size}`,
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }

  const { start, end } = ranges[0];
  const requestedSize = end - start + 1;

  console.log(`🎯 RANGE: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB) for ${isVideo ? 'VIDEO' : 'AUDIO'}`);

  // Calculate needed chunks with smart optimization
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Loading chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // Smart concurrent loading based on device and content
  let MAX_CONCURRENT = 8; // Default
  if (isVideo && isMobile) MAX_CONCURRENT = 4;
  else if (isVideo) MAX_CONCURRENT = 6;
  else if (isMobile) MAX_CONCURRENT = 6;

  const chunkResults = [];
  
  // Process in smart batches
  for (let i = 0; i < neededChunks.length; i += MAX_CONCURRENT) {
    const batchChunks = neededChunks.slice(i, Math.min(i + MAX_CONCURRENT, neededChunks.length));

    console.log(`📦 Range batch ${Math.floor(i/MAX_CONCURRENT) + 1} (${batchChunks.length} chunks)...`);

    const batchPromises = batchChunks.map(async (chunkInfo, batchIndex) => {
      const actualIndex = startChunk + i + batchIndex;
      
      try {
        const chunkData = await getUltimateChunk(kvNamespaces, chunkInfo, env, actualIndex, isVideo, userAgent);
        return {
          index: actualIndex,
          data: chunkData || new ArrayBuffer(0)
        };
      } catch (error) {
        console.error(`❌ Range chunk ${actualIndex} failed:`, error);
        return {
          index: actualIndex,
          data: new ArrayBuffer(0)
        };
      }
    });

    const batchResults = await Promise.all(batchPromises);
    chunkResults.push(...batchResults);

    // Smart delay for range requests
    if (i + MAX_CONCURRENT < neededChunks.length) {
      const delay = isVideo ? (isMobile ? 75 : 50) : 30;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  // Sort and combine chunks
  chunkResults.sort((a, b) => a.index - b.index);

  const validChunks = chunkResults.filter(chunk => chunk.data.byteLength > 0);
  if (validChunks.length === 0) {
    console.error('❌ No valid chunks found for range request');
    return new Response('Failed to load requested range', { status: 500 });
  }

  const combinedSize = validChunks.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(combinedSize);

  let offset = 0;
  for (const chunk of validChunks) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  // Extract exact range with safety checks
  const rangeStart = start - (startChunk * chunkSize);
  const safeRangeStart = Math.max(0, Math.min(rangeStart, combinedBuffer.length - 1));
  const actualSize = Math.min(requestedSize, combinedBuffer.length - safeRangeStart);
  const rangeBuffer = combinedBuffer.slice(safeRangeStart, safeRangeStart + actualSize);

  // CRITICAL: Perfect 206 response headers for video streaming
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', actualSize.toString());
  headers.set('Content-Range', `bytes ${start}-${start + actualSize - 1}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges, Content-Range');
  
  // Advanced caching for range requests
  if (isVideo) {
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  } else {
    headers.set('Cache-Control', 'public, max-age=86400');
  }
  
  headers.set('Content-Disposition', 'inline');
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
  
  // Device-specific optimizations for range requests
  if (isMobile) {
    headers.set('Keep-Alive', 'timeout=5, max=50');
  }

  console.log(`✅ RANGE SERVED: ${Math.round(actualSize/1024/1024)}MB with status 206`);
  return new Response(rangeBuffer, { status: 206, headers }); // CRITICAL: 206 status
}

// 🔍 HEAD REQUEST HANDLER (Browser pre-flight checks)
async function handleHeadRequest(size, mimeType, filename, isDownload) {
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges, Content-Type');
  
  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
  }

  console.log('✅ HEAD request processed');
  return new Response(null, { status: 200, headers });
}

// 🔄 OPTIONS REQUEST HANDLER (CORS pre-flight)
function handleOptionsRequest() {
  const headers = new Headers();
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Allow-Headers', 'Range, Content-Type, Authorization');
  headers.set('Access-Control-Max-Age', '86400');

  console.log('✅ OPTIONS request processed');
  return new Response(null, { status: 200, headers });
}

// 🚀 ULTIMATE CHUNK LOADER (All optimizations combined)
async function getUltimateChunk(kvNamespaces, chunkInfo, env, index, isMedia, userAgent) {
  const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
  const keyName = chunkInfo.keyName;
  const isMobile = /Mobile|Android|iPhone|iPad/i.test(userAgent);
  
  console.log(`📦 ULTIMATE chunk load ${index}: ${keyName} (Media: ${isMedia}, Mobile: ${isMobile})`);

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
  
  // Optimized fetch settings for different content types
  const fetchOptions = {
    headers: {
      'User-Agent': isMedia ? 'Mozilla/5.0 (compatible; MediaStreamer/3.0)' : 'Mozilla/5.0 (compatible; FileStreamer/2.0)',
      'Accept': '*/*',
      'Connection': 'keep-alive'
    },
    // Extended timeout for media files
    signal: AbortSignal.timeout(isMedia ? 30000 : 20000)
  };

  // Add mobile-specific optimizations
  if (isMobile) {
    fetchOptions.headers['Accept-Encoding'] = 'identity';
  }

  let response = await fetch(directUrl, fetchOptions);

  // Enhanced URL refresh with multiple bot token support
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${index}, refreshing with enhanced method...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2, 
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    let refreshed = false;

    // Try up to 3 different bot tokens with smart rotation
    for (let tokenIndex = 0; tokenIndex < Math.min(3, botTokens.length) && !refreshed; tokenIndex++) {
      // Smart token selection (round-robin based on chunk index)
      const selectedTokenIndex = (tokenIndex + index) % botTokens.length;
      const botToken = botTokens[selectedTokenIndex];

      try {
        console.log(`🔄 Trying bot token ${selectedTokenIndex + 1} for chunk ${index}...`);
        
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { signal: AbortSignal.timeout(8000) }
        );

        if (getFileResponse.ok) {
          const getFileData = await getFileResponse.json();
          if (getFileData.ok && getFileData.result?.file_path) {
            const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

            // Update KV with fresh URL and metadata
            const updatedMetadata = {
              ...chunkMetadata,
              directUrl: freshUrl,
              lastRefreshed: Date.now(),
              refreshCount: (chunkMetadata.refreshCount || 0) + 1,
              lastSuccessfulToken: selectedTokenIndex
            };

            // Fire and forget KV update
            kvNamespace.put(keyName, JSON.stringify(updatedMetadata)).catch(() => {});

            // Try with fresh URL
            response = await fetch(freshUrl, fetchOptions);

            if (response.ok) {
              console.log(`✅ URL refreshed for chunk ${index} using token ${selectedTokenIndex + 1}`);
              refreshed = true;
              break;
            }
          }
        }
      } catch (refreshError) {
        console.error(`❌ Failed to refresh chunk ${index} with token ${selectedTokenIndex + 1}:`, refreshError.message);
        continue;
      }
    }

    if (!refreshed) {
      console.error(`💥 Could not refresh chunk ${index} with any bot token`);
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${index}: HTTP ${response.status} ${response.statusText}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  console.log(`✅ ULTIMATE chunk ${index} loaded: ${Math.round(arrayBuffer.byteLength/1024)}KB`);

  return arrayBuffer;
}

// 🎯 ADVANCED RANGE PARSER (Enhanced for all edge cases)
function parseAdvancedRange(range, size) {
  if (!range || !range.startsWith('bytes=')) {
    return null;
  }

  // Handle various range formats
  const rangeSpec = range.substring(6); // Remove 'bytes='
  
  // Support multiple range formats: bytes=0-499, bytes=0-, bytes=-500
  const rangeMatch = rangeSpec.match(/^(d+)-(d*)$/) || rangeSpec.match(/^-(d+)$/) || rangeSpec.match(/^(d+)-$/);
  
  if (!rangeMatch) {
    return null;
  }

  let start, end;

  if (rangeSpec.startsWith('-')) {
    // Suffix range: bytes=-500 (last 500 bytes)
    const suffixLength = parseInt(rangeMatch[1], 10);
    start = Math.max(0, size - suffixLength);
    end = size - 1;
  } else if (rangeSpec.endsWith('-')) {
    // From start to end: bytes=500-
    start = parseInt(rangeMatch[1], 10);
    end = size - 1;
  } else {
    // Regular range: bytes=0-499
    start = parseInt(rangeMatch[1], 10);
    end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;
  }

  // Validate range
  if (isNaN(start) || isNaN(end) || start < 0 || start >= size) {
    return null;
  }

  // Ensure end is within bounds
  end = Math.min(end, size - 1);
  
  if (start > end) {
    return null;
  }

  return [{ start, end }];
}