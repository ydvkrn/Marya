// Browser-compatible MIME types - ENHANCED
const MIME_TYPES = {
  'mp4': 'video/mp4',
  'webm': 'video/webm',
  'mkv': 'video/mp4',
  'mov': 'video/quicktime',
  'avi': 'video/mp4',
  'm4v': 'video/mp4',
  'wmv': 'video/mp4',
  'flv': 'video/mp4',
  '3gp': 'video/3gpp',
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

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 OPTIMIZED STREAMING INITIATED:', fileId);

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

    // IMPROVED: Always use optimized streaming for ALL files
    return await handleLargeFileOptimized(request, kvNamespaces, masterMetadata, extension, env);

  } catch (error) {
    console.error('💥 Error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

// ENHANCED: Much better optimized streaming
async function handleLargeFileOptimized(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);
  const isVideo = mimeType.startsWith('video/');

  console.log(`🎬 Optimized streaming: ${filename} (Type: ${mimeType}, ${chunks.length} chunks, Video: ${isVideo})`);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';

  console.log(`📺 Mode: ${isDownload ? 'DOWNLOAD' : 'STREAM'}`);

  // IMPROVED: Better range request handling especially for videos
  const range = request.headers.get('Range');
  if (range && !isDownload) {
    console.log('📺 Range request with enhanced optimization:', range);
    return await handleOptimizedRangeStream(request, kvNamespaces, masterMetadata, extension, range, env, mimeType);
  }

  // ENHANCED: Better sequential chunk streaming (optimized for all file types)
  console.log('🌊 Enhanced sequential streaming...');

  const readable = new ReadableStream({
    async start(controller) {
      try {
        console.log(`🌊 Starting enhanced streaming (${chunks.length} chunks)...`);

        // OPTIMIZED: Better batch processing based on file type
        const BATCH_SIZE = isVideo ? 8 : 12; // Videos need more careful handling
        const CHUNK_DELAY = isVideo ? 30 : 50; // Faster for videos

        for (let batchStart = 0; batchStart < chunks.length; batchStart += BATCH_SIZE) {
          const batchEnd = Math.min(batchStart + BATCH_SIZE, chunks.length);
          const batchChunks = chunks.slice(batchStart, batchEnd);

          console.log(`📦 Processing enhanced batch ${Math.floor(batchStart/BATCH_SIZE) + 1}/${Math.ceil(chunks.length/BATCH_SIZE)} (${batchChunks.length} chunks)`);

          // IMPROVED: Parallel processing within batch for better speed
          if (isVideo || batchChunks.length <= 4) {
            // For videos or small batches, use parallel loading for speed
            const chunkPromises = batchChunks.map((chunkInfo, i) => {
              const chunkIndex = batchStart + i;
              return getChunkOptimized(kvNamespaces, chunkInfo, env, chunkIndex);
            });

            try {
              const chunkResults = await Promise.all(chunkPromises);
              
              // Stream all chunks from this batch
              for (const chunkData of chunkResults) {
                if (chunkData) {
                  controller.enqueue(new Uint8Array(chunkData));
                }
              }
            } catch (batchError) {
              console.error(`❌ Batch ${Math.floor(batchStart/BATCH_SIZE) + 1} had errors, continuing...`);
              
              // Fallback: Process sequentially if parallel fails
              for (let i = 0; i < batchChunks.length; i++) {
                const chunkInfo = batchChunks[i];
                const chunkIndex = batchStart + i;
                
                try {
                  const chunkData = await getChunkOptimized(kvNamespaces, chunkInfo, env, chunkIndex);
                  controller.enqueue(new Uint8Array(chunkData));
                } catch (chunkError) {
                  console.error(`❌ Chunk ${chunkIndex} failed, skipping...`);
                  continue;
                }
              }
            }
          } else {
            // For non-videos with large batches, use sequential for stability
            for (let i = 0; i < batchChunks.length; i++) {
              const chunkInfo = batchChunks[i];
              const chunkIndex = batchStart + i;

              try {
                const chunkData = await getChunkOptimized(kvNamespaces, chunkInfo, env, chunkIndex);
                controller.enqueue(new Uint8Array(chunkData));
                
                // Micro delay for stability
                await new Promise(resolve => setTimeout(resolve, 20));

              } catch (chunkError) {
                console.error(`❌ Chunk ${chunkIndex} failed:`, chunkError);
                continue;
              }
            }
          }

          // Delay between batches - shorter for videos
          if (batchEnd < chunks.length) {
            await new Promise(resolve => setTimeout(resolve, CHUNK_DELAY));
          }
        }

        console.log('✅ All chunks streamed successfully');
        controller.close();

      } catch (error) {
        console.error('💥 Enhanced streaming error:', error);
        controller.error(error);
      }
    }
  });

  // ENHANCED: Perfect headers for better streaming
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes'); // CRITICAL for video seeking
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges, Content-Range');
  
  // IMPROVED: Better caching strategy
  if (isVideo) {
    headers.set('Cache-Control', 'public, max-age=31536000, immutable'); // 1 year for videos
  } else {
    headers.set('Cache-Control', 'public, max-age=86400'); // 1 day for others
  }

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
      // ENHANCED: Essential headers for perfect video playback
      headers.set('X-Content-Type-Options', 'nosniff');
      headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
      
      // Additional video-specific headers
      if (isVideo) {
        headers.set('Cross-Origin-Embedder-Policy', 'cross-origin');
      }
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log(`🚀 Starting optimized ${isDownload ? 'download' : 'stream'} as ${mimeType}`);
  return new Response(readable, { status: 200, headers });
}

// ENHANCED: Much better range streaming with video optimization
async function handleOptimizedRangeStream(request, kvNamespaces, masterMetadata, extension, range, env, mimeType) {
  const { size, chunks } = masterMetadata;
  const chunkSize = masterMetadata.chunkSize || Math.ceil(size / chunks.length);
  const isVideo = mimeType.startsWith('video/');

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

  console.log(`📺 Enhanced range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB, Video: ${isVideo})`);

  // Determine needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Need chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // ENHANCED: Optimized concurrent chunk fetches
  const MAX_CONCURRENT = Math.min(neededChunks.length, isVideo ? 6 : 8);
  const chunkResults = [];

  // IMPROVED: Better batch processing for range requests
  for (let i = 0; i < neededChunks.length; i += MAX_CONCURRENT) {
    const batchChunks = neededChunks.slice(i, Math.min(i + MAX_CONCURRENT, neededChunks.length));

    console.log(`📦 Loading range batch ${Math.floor(i/MAX_CONCURRENT) + 1} (${batchChunks.length} chunks)...`);

    const batchPromises = batchChunks.map(async (chunkInfo, batchIndex) => {
      const actualIndex = startChunk + i + batchIndex;
      
      try {
        const chunkData = await getChunkOptimized(kvNamespaces, chunkInfo, env, actualIndex);
        return {
          index: actualIndex,
          data: chunkData
        };
      } catch (error) {
        console.error(`❌ Range chunk ${actualIndex} failed:`, error);
        return {
          index: actualIndex,
          data: new ArrayBuffer(0) // Empty data to maintain order
        };
      }
    });

    const batchResults = await Promise.all(batchPromises);
    chunkResults.push(...batchResults);

    // OPTIMIZED: Shorter delay for videos
    if (i + MAX_CONCURRENT < neededChunks.length) {
      await new Promise(resolve => setTimeout(resolve, isVideo ? 50 : 100));
    }
  }

  // Sort results by index
  chunkResults.sort((a, b) => a.index - b.index);

  // Combine chunks
  const combinedSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(combinedSize);

  let offset = 0;
  for (const chunk of chunkResults) {
    if (chunk.data.byteLength > 0) {
      combinedBuffer.set(new Uint8Array(chunk.data), offset);
      offset += chunk.data.byteLength;
    }
  }

  // Extract exact range
  const rangeStart = start - (startChunk * chunkSize);
  const actualRequestedSize = Math.min(requestedSize, combinedBuffer.length - rangeStart);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + actualRequestedSize);

  // ENHANCED: Perfect streaming headers for range requests
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', actualRequestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${start + actualRequestedSize - 1}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges, Content-Range');
  
  // ENHANCED: Better caching for range requests
  if (isVideo) {
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
  } else {
    headers.set('Cache-Control', 'public, max-age=86400');
  }
  
  headers.set('Content-Disposition', 'inline');
  
  // Video-specific headers for range requests
  if (isVideo) {
    headers.set('X-Content-Type-Options', 'nosniff');
    headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
  }

  console.log(`✅ Range streaming: ${Math.round(actualRequestedSize/1024/1024)}MB`);
  return new Response(rangeBuffer, { status: 206, headers });
}

// ENHANCED: Much better chunk loading with optimization
async function getChunkOptimized(kvNamespaces, chunkInfo, env, index) {
  const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
  const keyName = chunkInfo.keyName;
  
  console.log(`📦 Optimized load chunk ${index}: ${keyName}`);

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
  
  // ENHANCED: Better fetch with optimized settings
  let response = await fetch(directUrl, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; MaryaStreamer/2.0)',
      'Accept': '*/*',
      'Connection': 'keep-alive'
    },
    // IMPROVED: Better timeout handling
    signal: AbortSignal.timeout(20000) // 20 seconds
  });

  // ENHANCED: Smarter URL refresh with better error handling
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${index}, refreshing...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    let refreshed = false;

    // IMPROVED: Try up to 2 different bot tokens for better reliability
    for (let tokenIndex = 0; tokenIndex < Math.min(2, botTokens.length) && !refreshed; tokenIndex++) {
      const botToken = botTokens[tokenIndex];

      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { signal: AbortSignal.timeout(8000) }
        );

        if (getFileResponse.ok) {
          const getFileData = await getFileResponse.json();
          if (getFileData.ok && getFileData.result?.file_path) {
            const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

            // ENHANCED: Update KV with fresh URL (fire and forget)
            const updatedMetadata = {
              ...chunkMetadata,
              directUrl: freshUrl,
              lastRefreshed: Date.now(),
              refreshCount: (chunkMetadata.refreshCount || 0) + 1
            };

            kvNamespace.put(keyName, JSON.stringify(updatedMetadata)).catch(() => {});

            // Try with fresh URL
            response = await fetch(freshUrl, {
              headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; MaryaStreamer/2.0)',
                'Accept': '*/*',
                'Connection': 'keep-alive'
              }
            });

            if (response.ok) {
              console.log(`✅ URL refreshed for chunk ${index} using token ${tokenIndex + 1}`);
              refreshed = true;
            }
          }
        }
      } catch (refreshError) {
        console.error(`❌ Failed to refresh chunk ${index} with token ${tokenIndex + 1}:`, refreshError.message);
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
  console.log(`✅ Chunk ${index} loaded: ${Math.round(arrayBuffer.byteLength/1024)}KB`);

  return arrayBuffer;
}

// ENHANCED: Better Range parser with improved error handling
function parseRange(range, size) {
  if (!range || !range.startsWith('bytes=')) {
    return null;
  }

  // Support multiple range formats
  const rangeMatch = range.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) {
    return null;
  }

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  // ENHANCED: Better validation
  if (isNaN(start) || isNaN(end) || start < 0 || start >= size) {
    return null;
  }

  // IMPROVED: Handle edge cases
  const actualEnd = Math.min(end, size - 1);
  
  if (start > actualEnd) {
    return null;
  }

  return [{ start, end: actualEnd }];
}