// 🎯 SUPER SIMPLE VIDEO STREAMING - BACK TO BASICS
// ✅ Focus on core functionality only

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
  'flac': 'audio/flac',
  'aac': 'audio/aac',
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

function isVideoFile(mimeType) {
  return mimeType.startsWith('video/');
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🎯 BASIC STREAMING:', fileId);

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
    const { filename, size, chunks } = masterMetadata;
    const mimeType = getMimeType(extension);
    const isVideo = isVideoFile(mimeType);

    console.log(`📁 ${filename} (${Math.round(size/1024/1024)}MB, ${chunks.length} chunks, Video: ${isVideo})`);

    return await handleBasicStreaming(request, kvNamespaces, masterMetadata, extension, env);

  } catch (error) {
    console.error('💥 Error:', error);
    return new Response(`❌ Server error: ${error.message}`, { status: 500 });
  }
}

// 🎯 BASIC STREAMING - SIMPLE AND RELIABLE
async function handleBasicStreaming(request, kvNamespaces, masterMetadata, extension, env) {
  const { chunks, filename, size } = masterMetadata;
  const mimeType = getMimeType(extension);
  const isVideo = isVideoFile(mimeType);

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';

  console.log(`🎯 Mode: ${isDownload ? 'DOWNLOAD' : (isVideo ? 'VIDEO STREAM' : 'FILE STREAM')}`);

  // CRITICAL: Handle range requests for video (MUST return 206)
  const range = request.headers.get('Range');
  if (range && !isDownload) {
    console.log('📺 RANGE REQUEST:', range);
    return await handleBasicRange(request, kvNamespaces, masterMetadata, range, env, mimeType);
  }

  // BASIC: Simple full file streaming
  console.log('🌊 BASIC FULL STREAMING...');

  // Method 1: Try to load all chunks first (for small files like 2MB)
  if (size <= 10 * 1024 * 1024) { // 10MB or less - load completely first
    console.log('📦 SMALL FILE - Loading all chunks first...');
    
    try {
      const allChunks = [];
      
      // Load all chunks sequentially for reliability
      for (let i = 0; i < chunks.length; i++) {
        const chunkInfo = chunks[i];
        console.log(`📦 Loading chunk ${i + 1}/${chunks.length}...`);
        
        try {
          const chunkData = await loadBasicChunk(kvNamespaces, chunkInfo, env, i);
          allChunks.push(new Uint8Array(chunkData));
        } catch (chunkError) {
          console.error(`❌ Chunk ${i} failed:`, chunkError);
          return new Response(`❌ Failed to load chunk ${i}: ${chunkError.message}`, { status: 500 });
        }
      }

      // Combine all chunks
      const totalSize = allChunks.reduce((sum, chunk) => sum + chunk.length, 0);
      const combinedFile = new Uint8Array(totalSize);
      
      let offset = 0;
      for (const chunk of allChunks) {
        combinedFile.set(chunk, offset);
        offset += chunk.length;
      }

      console.log(`✅ Small file completely loaded: ${Math.round(totalSize/1024/1024)}MB`);

      // Return complete file with proper headers
      const headers = createBasicHeaders(mimeType, totalSize, filename, isDownload, isVideo);
      return new Response(combinedFile, { status: 200, headers });

    } catch (error) {
      console.error('💥 Small file loading failed, falling back to streaming:', error);
      // Fall through to streaming method
    }
  }

  // Method 2: Streaming for larger files
  console.log('🌊 STREAMING mode for large file...');
  
  const readable = new ReadableStream({
    async start(controller) {
      try {
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          console.log(`📦 Streaming chunk ${i + 1}/${chunks.length}...`);
          
          try {
            const chunkData = await loadBasicChunk(kvNamespaces, chunkInfo, env, i);
            controller.enqueue(new Uint8Array(chunkData));
            
            // Small delay between chunks
            await new Promise(resolve => setTimeout(resolve, 100));
            
          } catch (chunkError) {
            console.error(`❌ Chunk ${i} failed:`, chunkError);
            // For videos, we can't continue with missing chunks
            if (isVideo) {
              controller.error(new Error(`Video chunk ${i} failed: ${chunkError.message}`));
              return;
            }
            // For other files, continue with next chunk
            continue;
          }
        }

        console.log('✅ Streaming completed');
        controller.close();

      } catch (error) {
        console.error('💥 Streaming error:', error);
        controller.error(error);
      }
    }
  });

  const headers = createBasicHeaders(mimeType, size, filename, isDownload, isVideo);
  return new Response(readable, { status: 200, headers });
}

// 🎯 BASIC RANGE HANDLER (Essential for video seeking)
async function handleBasicRange(request, kvNamespaces, masterMetadata, range, env, mimeType) {
  const { size, chunks } = masterMetadata;
  const chunkSize = masterMetadata.chunkSize || Math.ceil(size / chunks.length);
  const isVideo = isVideoFile(mimeType);

  console.log(`📺 RANGE: File size ${Math.round(size/1024/1024)}MB, Chunk size ${Math.round(chunkSize/1024)}KB`);

  // Parse range request
  const rangeMatch = range.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) {
    console.error('❌ Invalid range format:', range);
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: { 
        'Content-Range': `bytes */${size}`,
        'Accept-Ranges': 'bytes'
      }
    });
  }

  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;
  const requestedSize = end - start + 1;

  console.log(`🎯 Range request: ${start}-${end} (${Math.round(requestedSize/1024/1024)}MB)`);

  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`📦 Need chunks ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // Load needed chunks
  const chunkData = [];
  for (let i = 0; i < neededChunks.length; i++) {
    const chunkInfo = neededChunks[i];
    const chunkIndex = startChunk + i;
    
    console.log(`📦 Loading range chunk ${chunkIndex}...`);
    
    try {
      const data = await loadBasicChunk(kvNamespaces, chunkInfo, env, chunkIndex);
      chunkData.push(new Uint8Array(data));
    } catch (error) {
      console.error(`❌ Range chunk ${chunkIndex} failed:`, error);
      return new Response('Failed to load requested range', { status: 500 });
    }
  }

  // Combine chunks
  const totalChunkSize = chunkData.reduce((sum, chunk) => sum + chunk.length, 0);
  const combinedBuffer = new Uint8Array(totalChunkSize);
  
  let offset = 0;
  for (const chunk of chunkData) {
    combinedBuffer.set(chunk, offset);
    offset += chunk.length;
  }

  // Extract exact range
  const rangeStart = start - (startChunk * chunkSize);
  const actualSize = Math.min(requestedSize, combinedBuffer.length - rangeStart);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + actualSize);

  console.log(`✅ Range served: ${Math.round(actualSize/1024/1024)}MB`);

  // CRITICAL: Return 206 status with proper headers
  const headers = new Headers();
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', actualSize.toString());
  headers.set('Content-Range', `bytes ${start}-${start + actualSize - 1}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=86400');
  headers.set('Content-Disposition', 'inline');

  return new Response(rangeBuffer, { status: 206, headers }); // CRITICAL: 206 status
}

// 🎯 BASIC CHUNK LOADER (Simple and reliable)
async function loadBasicChunk(kvNamespaces, chunkInfo, env, index) {
  const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
  const keyName = chunkInfo.keyName;
  
  console.log(`📦 Loading chunk ${index}: ${keyName}`);

  // Get chunk metadata
  const chunkMetadataString = await kvNamespace.get(keyName);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${keyName} not found in KV`);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;

  // Try to fetch chunk
  let response = await fetch(directUrl);

  // If URL expired, refresh it
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${index}, refreshing...`);

    const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(Boolean);
    
    if (botTokens.length > 0) {
      const botToken = botTokens[0]; // Use first available token

      try {
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`
        );

        if (getFileResponse.ok) {
          const getFileData = await getFileResponse.json();
          if (getFileData.ok && getFileData.result?.file_path) {
            const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
            response = await fetch(freshUrl);
            
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
    throw new Error(`HTTP ${response.status} for chunk ${index}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  console.log(`✅ Chunk ${index} loaded: ${Math.round(arrayBuffer.byteLength/1024)}KB`);

  return arrayBuffer;
}

// 🎯 CREATE BASIC HEADERS (Essential for video playback)
function createBasicHeaders(mimeType, size, filename, isDownload, isVideo) {
  const headers = new Headers();
  
  // CRITICAL headers for video playback
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  
  // ESSENTIAL for video seeking
  headers.set('Accept-Ranges', 'bytes');
  
  // CORS headers
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  headers.set('Access-Control-Expose-Headers', 'Content-Length, Accept-Ranges, Content-Range');
  
  // Caching
  headers.set('Cache-Control', isVideo ? 'public, max-age=31536000' : 'public, max-age=86400');

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
    
    if (isVideo) {
      // CRITICAL for video streaming
      headers.set('X-Content-Type-Options', 'nosniff');
      headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
    }
  }

  return headers;
}