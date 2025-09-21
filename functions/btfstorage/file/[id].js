// Browser-compatible MIME type mapping
const MIME_TYPES = {
  // Video formats (browser optimized)
  'mp4': 'video/mp4',
  'webm': 'video/webm', 
  'mkv': 'video/mp4', // ← Serve MKV as MP4 for browser compatibility
  'mov': 'video/mp4', // ← MOV as MP4 too
  'avi': 'video/mp4', // ← AVI as MP4 too
  'm4v': 'video/mp4',
  'wmv': 'video/mp4',
  'flv': 'video/mp4',
  '3gp': 'video/mp4',
  'mpg': 'video/mpeg',
  'mpeg': 'video/mpeg',
  
  // Audio formats (browser optimized)
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
  'flac': 'audio/mpeg', // ← FLAC as MP3 for compatibility
  'aac': 'audio/mp4',
  'm4a': 'audio/mp4',
  'ogg': 'audio/ogg',
  'wma': 'audio/mpeg',
  
  // Image formats
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp',
  'svg': 'image/svg+xml',
  'bmp': 'image/jpeg',
  'tiff': 'image/jpeg',
  
  // Document formats
  'pdf': 'application/pdf',
  'txt': 'text/plain',
  'json': 'application/json',
  'html': 'text/html',
  'css': 'text/css',
  'js': 'application/javascript',
  
  // Archive formats
  'zip': 'application/zip',
  'rar': 'application/vnd.rar',
  '7z': 'application/x-7z-compressed',
  'tar': 'application/x-tar',
  'gz': 'application/gzip'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

// Check if file should be streamed inline
function isStreamable(mimeType) {
  return mimeType.startsWith('video/') || 
         mimeType.startsWith('audio/') || 
         mimeType.startsWith('image/') ||
         mimeType === 'application/pdf' ||
         mimeType.startsWith('text/');
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎬 ULTIMATE BROWSER STREAMING:', fileId);

  try {
    // Extract file ID and extension
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    // Validate MSM ID format
    if (!actualId.startsWith('MSM')) {
      return new Response('❌ Invalid file ID format', { status: 404 });
    }

    // All KV namespaces
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
      return new Response('🔍 File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    const { filename, size, totalChunks } = masterMetadata;
    
    console.log(`📁 File: ${filename} (${Math.round(size/1024/1024)}MB, ${totalChunks} chunks)`);

    // Handle all chunked files with browser-optimized streaming
    if (masterMetadata.chunks && masterMetadata.chunks.length > 0) {
      return await handleBrowserOptimizedStreaming(request, kvNamespaces, masterMetadata, extension, env);
    } else {
      return new Response('❌ Unsupported file format', { status: 501 });
    }

  } catch (error) {
    console.error('💥 File serve error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

// Browser-optimized streaming with perfect headers
async function handleBrowserOptimizedStreaming(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);
  
  console.log(`🎬 Browser streaming: ${filename} (Type: ${mimeType})`);

  // Check URL parameters
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';
  
  console.log(`📺 Mode: ${isDownload ? 'DOWNLOAD' : 'STREAM'}`);

  // Handle Range requests for video streaming (Netflix/YouTube style)
  const range = request.headers.get('Range');
  if (range && !isDownload && isStreamable(mimeType)) {
    console.log('📺 Range streaming request:', range);
    return await handleVideoStreamingRange(request, kvNamespaces, masterMetadata, extension, range, env, mimeType);
  }

  // For large files without range, stream progressively
  if (size > 50 * 1024 * 1024 && !isDownload) {
    console.log('🌊 Progressive streaming for large file');
    return await handleProgressiveStreaming(request, kvNamespaces, masterMetadata, extension, env, mimeType);
  }

  // Regular file serving
  console.log('📄 Regular file serving');
  return await handleCompleteFileServing(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload);
}

// Handle video streaming with Range support (YouTube-style)
async function handleVideoStreamingRange(request, kvNamespaces, masterMetadata, extension, range, env, mimeType) {
  const { size, chunks } = masterMetadata;
  const chunkSize = masterMetadata.chunkSize || Math.ceil(size / chunks.length);
  
  // Parse range header
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

  console.log(`📺 Streaming range: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  // Determine which chunks we need
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Loading chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // Get chunks in parallel with auto-refresh
  const chunkPromises = neededChunks.map(async (chunkInfo, index) => {
    const actualIndex = startChunk + index;
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    return await getChunkWithAutoRefresh(kvNamespace, chunkInfo.keyName, chunkInfo, env, actualIndex);
  });

  const chunkResults = await Promise.all(chunkPromises);
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

  // Perfect streaming headers for browser compatibility
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', requestedSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
  headers.set('Cache-Control', 'public, max-age=3600, s-maxage=86400');
  
  // FORCE INLINE STREAMING (no download)
  headers.set('Content-Disposition', 'inline');
  
  // Additional browser compatibility headers
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
  
  console.log(`✅ Streaming ${Math.round(requestedSize/1024/1024)}MB range as ${mimeType}`);
  return new Response(rangeBuffer, { status: 206, headers });
}

// Progressive streaming for large files without range requests
async function handleProgressiveStreaming(request, kvNamespaces, masterMetadata, extension, env, mimeType) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`🌊 Progressive streaming: ${filename} (${Math.round(size/1024/1024)}MB)`);

  // Create readable stream that loads chunks on demand
  const readable = new ReadableStream({
    async start(controller) {
      try {
        console.log('🌊 Starting progressive chunk streaming...');
        
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          
          console.log(`🌊 Streaming chunk ${i + 1}/${chunks.length} (${Math.round(chunkInfo.size/1024)}KB)...`);
          
          const chunkData = await getChunkWithAutoRefresh(kvNamespace, chunkInfo.keyName, chunkInfo, env, i);
          
          // Send chunk to browser
          controller.enqueue(new Uint8Array(chunkData.data));
          
          // Small delay to prevent overwhelming the browser
          if (i < chunks.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 10));
          }
        }
        
        console.log('✅ All chunks streamed successfully');
        controller.close();
        
      } catch (error) {
        console.error('💥 Progressive streaming error:', error);
        controller.error(error);
      }
    }
  });

  // Perfect progressive streaming headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges');
  headers.set('Cache-Control', 'public, max-age=3600, s-maxage=86400');
  
  // FORCE INLINE STREAMING
  headers.set('Content-Disposition', 'inline');
  
  // Browser compatibility
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
  
  // Streaming hints for browser
  headers.set('Transfer-Encoding', 'chunked');

  console.log(`🌊 Starting progressive stream as ${mimeType}`);
  return new Response(readable, { status: 200, headers });
}

// Complete file serving for smaller files and downloads
async function handleCompleteFileServing(request, kvNamespaces, masterMetadata, extension, env, mimeType, isDownload) {
  const { chunks, filename, size } = masterMetadata;
  
  console.log(`📄 ${isDownload ? 'Download' : 'Stream'} serving: ${filename} (${chunks.length} chunks)`);

  // Get all chunks in parallel
  const chunkPromises = chunks.map(async (chunkInfo, index) => {
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    return await getChunkWithAutoRefresh(kvNamespace, chunkInfo.keyName, chunkInfo, env, index);
  });

  const chunkResults = await Promise.all(chunkPromises);
  chunkResults.sort((a, b) => a.index - b.index);

  // Combine all chunks
  const totalSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalSize);

  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  // Perfect headers based on mode
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges');
  headers.set('Cache-Control', 'public, max-age=3600, s-maxage=86400');

  if (isDownload) {
    // Force download
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    console.log(`💾 Download mode: ${filename}`);
  } else {
    // Force streaming/inline
    if (isStreamable(mimeType)) {
      headers.set('Content-Disposition', 'inline');
      headers.set('X-Content-Type-Options', 'nosniff');
      headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
      console.log(`📺 Stream mode: ${filename} as ${mimeType}`);
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      console.log(`💾 Non-streamable file, forcing download: ${filename}`);
    }
  }

  console.log(`✅ File served: ${Math.round(totalSize/1024/1024)}MB`);
  return new Response(combinedBuffer, { status: 200, headers });
}

// Enhanced chunk loading with auto-refresh and multi-bot fallback
async function getChunkWithAutoRefresh(kvNamespace, keyName, chunkInfo, env, index) {
  console.log(`📦 Loading chunk ${index}: ${keyName}`);

  // Get chunk metadata from KV
  const chunkMetadataString = await kvNamespace.get(keyName);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${keyName} not found in KV`);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;

  // Try to fetch the chunk
  let response = await fetch(directUrl, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; MaryaVault/1.0)'
    }
  });

  // If URL expired, refresh with multiple bot tokens
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${index} (status: ${response.status}), refreshing...`);

    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token);

    let refreshed = false;

    // Try each bot token
    for (const botToken of botTokens) {
      try {
        console.log(`🔄 Trying to refresh with bot ending ...${botToken.slice(-4)}`);
        
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { 
            signal: AbortSignal.timeout(15000),
            headers: {
              'User-Agent': 'Mozilla/5.0 (compatible; MaryaVault/1.0)'
            }
          }
        );

        if (!getFileResponse.ok) {
          console.log(`❌ GetFile failed with bot ${botToken.slice(-4)}: ${getFileResponse.status}`);
          continue;
        }

        const getFileData = await getFileResponse.json();
        if (!getFileData.ok || !getFileData.result?.file_path) {
          console.log(`❌ Invalid GetFile response from bot ${botToken.slice(-4)}`);
          continue;
        }

        // Create fresh URL
        const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

        // Test the fresh URL
        const testResponse = await fetch(freshUrl, {
          method: 'HEAD',
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; MaryaVault/1.0)'
          }
        });

        if (testResponse.ok) {
          // Update KV with fresh URL
          const updatedMetadata = {
            ...chunkMetadata,
            directUrl: freshUrl,
            lastRefreshed: Date.now(),
            refreshCount: (chunkMetadata.refreshCount || 0) + 1,
            refreshedWith: botToken.slice(-4)
          };

          await kvNamespace.put(keyName, JSON.stringify(updatedMetadata));

          // Use the fresh URL
          response = await fetch(freshUrl, {
            headers: {
              'User-Agent': 'Mozilla/5.0 (compatible; MaryaVault/1.0)'
            }
          });
          
          if (response.ok) {
            console.log(`✅ URL refreshed successfully for chunk ${index} with bot ...${botToken.slice(-4)}`);
            refreshed = true;
            break;
          }
        } else {
          console.log(`❌ Fresh URL test failed for bot ${botToken.slice(-4)}: ${testResponse.status}`);
        }

      } catch (refreshError) {
        console.error(`❌ Failed to refresh chunk ${index} with bot ${botToken.slice(-4)}:`, refreshError.message);
        continue;
      }
    }

    if (!refreshed) {
      console.error(`💥 Failed to refresh chunk ${index} with any bot token`);
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${index}: HTTP ${response.status} - ${response.statusText}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  console.log(`✅ Chunk ${index} loaded: ${Math.round(arrayBuffer.byteLength/1024)}KB`);

  return {
    index: index,
    data: arrayBuffer
  };
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
