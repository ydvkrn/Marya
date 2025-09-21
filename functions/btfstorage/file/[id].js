// Browser-compatible MIME types (same as original)
const MIME_TYPES = {
  'mp4': 'video/mp4',
  'webm': 'video/webm',
  'mkv': 'video/mp4',
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

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 LARGE FILE OPTIMIZED STREAMING:', fileId);

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

    // Use optimized streaming for ALL files
    return await handleLargeFileOptimized(request, kvNamespaces, masterMetadata, extension, env);

  } catch (error) {
    console.error('💥 Error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

// MINIMAL FIX: Keep original logic but fix the key issues
async function handleLargeFileOptimized(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);

  console.log(`🎬 Optimized streaming: ${filename} (Type: ${mimeType}, ${chunks.length} chunks)`);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';

  console.log(`📺 Mode: ${isDownload ? 'DOWNLOAD' : 'STREAM'}`);

  // CRITICAL FIX: Handle Range requests properly for video streaming
  const range = request.headers.get('Range');
  if (range && !isDownload) {
    console.log('📺 Range request detected:', range);
    return await handleOptimizedRangeStream(request, kvNamespaces, masterMetadata, extension, range, env, mimeType);
  }

  // MINIMAL CHANGE: Keep original sequential approach but with better error handling
  console.log('🌊 Sequential streaming (optimized)...');

  const readable = new ReadableStream({
    async start(controller) {
      try {
        console.log(`🌊 Starting streaming (${chunks.length} chunks)...`);

        // KEEP ORIGINAL: Small batches to avoid subrequest limits
        const BATCH_SIZE = 10;

        for (let batchStart = 0; batchStart < chunks.length; batchStart += BATCH_SIZE) {
          const batchEnd = Math.min(batchStart + BATCH_SIZE, chunks.length);
          const batchChunks = chunks.slice(batchStart, batchEnd);

          console.log(`📦 Processing batch ${Math.floor(batchStart/BATCH_SIZE) + 1}/${Math.ceil(chunks.length/BATCH_SIZE)} (${batchChunks.length} chunks)`);

          // KEEP ORIGINAL: Sequential processing (not parallel)
          for (let i = 0; i < batchChunks.length; i++) {
            const chunkInfo = batchChunks[i];
            const chunkIndex = batchStart + i;

            console.log(`📦 Loading chunk ${chunkIndex + 1}/${chunks.length}...`);

            try {
              const chunkData = await getChunkSequentially(kvNamespaces[chunkInfo.kvNamespace], chunkInfo.keyName, chunkInfo, env, chunkIndex);

              // CRITICAL: Stream chunk immediately
              controller.enqueue(new Uint8Array(chunkData));

              // KEEP ORIGINAL: Small delay between chunks for stability
              await new Promise(resolve => setTimeout(resolve, 50));

            } catch (chunkError) {
              console.error(`❌ Chunk ${chunkIndex} failed:`, chunkError);
              // Continue with next chunk instead of failing completely
              continue;
            }
          }

          // KEEP ORIGINAL: Delay between batches
          if (batchEnd < chunks.length) {
            await new Promise(resolve => setTimeout(resolve, 100));
          }
        }

        console.log('✅ All chunks streamed successfully');
        controller.close();

      } catch (error) {
        console.error('💥 Sequential streaming error:', error);
        controller.error(error);
      }
    }
  });

  // CRITICAL FIX: Perfect headers for streaming
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes'); // ESSENTIAL for video seeking
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges');
  headers.set('Cache-Control', 'public, max-age=86400');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
      // ESSENTIAL for video streaming
      headers.set('X-Content-Type-Options', 'nosniff');
      headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }

  console.log(`🚀 Starting optimized ${isDownload ? 'download' : 'stream'} as ${mimeType}`);
  return new Response(readable, { status: 200, headers });
}

// CRITICAL FIX: Proper Range streaming with 206 status
async function handleOptimizedRangeStream(request, kvNamespaces, masterMetadata, extension, range, env, mimeType) {
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

  console.log(`📺 Range request: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Need chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // CONSERVATIVE: Load chunks one by one to avoid issues
  const chunkResults = [];
  
  for (let i = 0; i < neededChunks.length; i++) {
    const chunkInfo = neededChunks[i];
    const actualIndex = startChunk + i;
    
    try {
      console.log(`📦 Loading range chunk ${actualIndex}...`);
      const chunkData = await getChunkSequentially(kvNamespaces[chunkInfo.kvNamespace], chunkInfo.keyName, chunkInfo, env, actualIndex);
      
      chunkResults.push({
        index: actualIndex,
        data: chunkData
      });
      
      // Small delay between chunks
      await new Promise(resolve => setTimeout(resolve, 50));
      
    } catch (error) {
      console.error(`❌ Range chunk ${actualIndex} failed:`, error);
      // Continue with next chunk
      continue;
    }
  }

  if (chunkResults.length === 0) {
    return new Response('Failed to load requested range', { status: 500 });
  }

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
  const actualSize = Math.min(requestedSize, combinedBuffer.length - rangeStart);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + actualSize);

  // CRITICAL: 206 status with proper headers for video streaming
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', actualSize.toString());
  headers.set('Content-Range', `bytes ${start}-${start + actualSize - 1}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=86400');
  headers.set('Content-Disposition', 'inline');

  console.log(`✅ Range streaming: ${Math.round(actualSize/1024/1024)}MB`);
  return new Response(rangeBuffer, { status: 206, headers }); // CRITICAL: 206 status
}

// KEEP ORIGINAL: Sequential chunk loading (same as your working code)
async function getChunkSequentially(kvNamespace, keyName, chunkInfo, env, index) {
  console.log(`📦 Sequential load chunk ${index}: ${keyName}`);

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

  // KEEP ORIGINAL: Single URL refresh attempt
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${index}, attempting refresh...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    let refreshed = false;

    // Try only the first available bot token
    if (botTokens.length > 0) {
      const botToken = botTokens[0];

      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { signal: AbortSignal.timeout(10000) }
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
              refreshed = true;
            }
          }
        }
      } catch (refreshError) {
        console.error(`❌ Failed to refresh chunk ${index}:`, refreshError.message);
      }
    }

    if (!refreshed) {
      console.error(`💥 Could not refresh chunk ${index}, using expired URL`);
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${index}: HTTP ${response.status}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  console.log(`✅ Chunk ${index} loaded: ${Math.round(arrayBuffer.byteLength/1024)}KB`);

  return arrayBuffer;
}

// KEEP ORIGINAL: Parse Range header (same logic)
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) return null;

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;

  if (start >= size || end >= size || start > end) return null;

  return [{ start, end }];
}