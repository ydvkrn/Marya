// Simple MIME types (purana system jaisa)
const MIME_TYPES = {
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/mp4', // MKV ko MP4 serve karte hain
  'mov': 'video/mp4', 'avi': 'video/mp4', 'm4v': 'video/mp4',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json',
  'zip': 'application/zip', 'rar': 'application/vnd.rar'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('=== SIMPLE FILE SERVE ===');
  console.log('File ID:', fileId);

  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    // KV namespaces (purane system jaisa)
    const kvNamespaces = {
      FILES_KV: env.FILES_KV,
      FILES_KV2: env.FILES_KV2,
      FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4,
      FILES_KV5: env.FILES_KV5,
      FILES_KV6: env.FILES_KV6,
      FILES_KV7: env.FILES_KV7
    };

    // Get master metadata (purane system jaisa)
    const masterMetadataString = await kvNamespaces.FILES_KV.get(actualId);
    if (!masterMetadataString) {
      return new Response('File not found', { status: 404 });
    }

    const masterMetadata = JSON.parse(masterMetadataString);
    console.log(`File found: ${masterMetadata.filename} (${masterMetadata.totalChunks || masterMetadata.chunks?.length || 0} chunks)`);

    // Handle chunked files (purane system se inspired but fixed)
    if (masterMetadata.type === 'multi_kv_chunked' || masterMetadata.chunks) {
      return await handleChunkedFile(request, kvNamespaces, masterMetadata, extension, env);
    } else {
      // Single file support
      return await handleSingleFile(request, kvNamespaces.FILES_KV, actualId, extension, masterMetadata, env);
    }

  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

// Handle chunked files (FIXED - no Promise.all overload)
async function handleChunkedFile(request, kvNamespaces, masterMetadata, extension, env) {
  const chunks = masterMetadata.chunks || [];
  const filename = masterMetadata.filename;
  const size = masterMetadata.size;

  console.log(`Serving chunked file: ${filename} (${chunks.length} chunks)`);

  // Handle Range requests (for video seeking)
  const range = request.headers.get('Range');
  if (range) {
    return await handleRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env);
  }

  // Stream all chunks SEQUENTIALLY (no Promise.all overload)
  const readable = new ReadableStream({
    async start(controller) {
      try {
        console.log('Starting sequential chunk streaming...');
        
        // Load chunks ONE BY ONE (no memory overload)
        for (let i = 0; i < chunks.length; i++) {
          const chunkInfo = chunks[i];
          const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
          const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey || `${masterMetadata.id || actualId}_chunk_${i}`;
          
          console.log(`Loading chunk ${i + 1}/${chunks.length}: ${chunkKey}`);
          
          try {
            const chunkData = await getChunkWithRefresh(kvNamespace, chunkKey, chunkInfo, env);
            controller.enqueue(new Uint8Array(chunkData));
            
            console.log(`✅ Chunk ${i + 1} streamed: ${Math.round(chunkData.byteLength/1024)}KB`);
            
            // Small delay to prevent overload
            await new Promise(resolve => setTimeout(resolve, 50));
            
          } catch (chunkError) {
            console.error(`❌ Chunk ${i + 1} failed:`, chunkError);
            // Continue with next chunk instead of failing completely
            continue;
          }
        }
        
        console.log('✅ Sequential streaming completed');
        controller.close();
        
      } catch (error) {
        console.error('Streaming error:', error);
        controller.error(error);
      }
    }
  });

  // Response headers (purane system jaisa)
  const headers = new Headers();
  const mimeType = getMimeType(extension);
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', size.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=3600');

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

  console.log('✅ Chunked file streaming response ready');
  return new Response(readable, { status: 200, headers });
}

// Get chunk with URL refresh (purane system jaisa but better error handling)
async function getChunkWithRefresh(kvNamespace, chunkKey, chunkInfo, env) {
  console.log(`Getting chunk: ${chunkKey}`);
  
  const chunkMetadataString = await kvNamespace.get(chunkKey);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${chunkKey} not found`);
  }

  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;

  // Try to fetch chunk
  let response = await fetch(directUrl, { signal: AbortSignal.timeout(30000) });

  // If URL expired, refresh it (purane system jaisa)
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${chunkKey}, refreshing...`);
    
    // Try all available bot tokens
    const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(token => token);
    
    if (botTokens.length === 0) {
      throw new Error('No BOT_TOKEN available for URL refresh');
    }

    let refreshed = false;
    
    for (const BOT_TOKEN of botTokens) {
      try {
        // Get fresh URL from Telegram
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { signal: AbortSignal.timeout(15000) }
        );

        if (!getFileResponse.ok) {
          console.log(`Bot token failed: ${BOT_TOKEN.slice(-4)}`);
          continue;
        }

        const getFileData = await getFileResponse.json();
        if (!getFileData.ok || !getFileData.result?.file_path) {
          console.log(`Invalid response from bot: ${BOT_TOKEN.slice(-4)}`);
          continue;
        }

        // Create fresh URL
        const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;

        // Update KV with fresh URL (purane system jaisa)
        const updatedMetadata = {
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now(),
          refreshCount: (chunkMetadata.refreshCount || 0) + 1
        };

        await kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata));
        console.log(`✅ URL refreshed for chunk ${chunkKey}`);

        // Try with fresh URL
        response = await fetch(freshUrl, { signal: AbortSignal.timeout(30000) });
        
        if (response.ok) {
          refreshed = true;
          break;
        }
        
      } catch (refreshError) {
        console.error(`❌ Failed to refresh with bot ${BOT_TOKEN.slice(-4)}:`, refreshError);
        continue;
      }
    }

    if (!refreshed) {
      throw new Error(`Failed to refresh expired URL for chunk ${chunkKey}`);
    }
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${chunkKey}: ${response.status}`);
  }

  return await response.arrayBuffer();
}

// Handle Range requests (purane system se inspired)
async function handleRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env) {
  console.log('Handling Range request:', range);
  
  const { size } = masterMetadata;
  const ranges = parseRange(range, size);
  
  if (!ranges || ranges.length !== 1) {
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: { 'Content-Range': `bytes */${size}` }
    });
  }

  const { start, end } = ranges[0];
  const chunkSize = end - start + 1;
  console.log(`Range: ${start}-${end} (${Math.round(chunkSize/1024)}KB)`);

  // Determine which chunks are needed (purane system jaisa)
  const CHUNK_SIZE = masterMetadata.chunkSize || (20 * 1024 * 1024); // 20MB default
  const startChunk = Math.floor(start / CHUNK_SIZE);
  const endChunk = Math.floor(end / CHUNK_SIZE);
  const neededChunks = masterMetadata.chunks.slice(startChunk, endChunk + 1);

  console.log(`Need chunks: ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  // Load needed chunks SEQUENTIALLY (no Promise.all)
  const chunkResults = [];
  
  for (let i = 0; i < neededChunks.length; i++) {
    const chunkInfo = neededChunks[i];
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.keyName || chunkInfo.chunkKey;
    
    try {
      const chunkData = await getChunkWithRefresh(kvNamespace, chunkKey, chunkInfo, env);
      chunkResults.push({
        index: startChunk + i,
        data: chunkData
      });
      
      console.log(`✅ Range chunk ${startChunk + i} loaded`);
      
    } catch (chunkError) {
      console.error(`❌ Range chunk ${startChunk + i} failed:`, chunkError);
      return new Response(`Range chunk ${startChunk + i} failed: ${chunkError.message}`, { status: 500 });
    }
  }

  // Combine and extract range (purane system jaisa)
  const combinedSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(combinedSize);
  
  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }

  const rangeStart = start - (startChunk * CHUNK_SIZE);
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + chunkSize);

  // Range response headers
  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', chunkSize.toString());
  headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');

  console.log(`✅ Range response ready: ${rangeBuffer.byteLength} bytes`);
  return new Response(rangeBuffer, { status: 206, headers });
}

// Single file support (purane system jaisa)
async function handleSingleFile(request, kvNamespace, actualId, extension, metadata, env) {
  console.log('Serving single file (legacy)');
  
  const directUrl = await kvNamespace.get(actualId);
  if (!directUrl) {
    return new Response('File not found', { status: 404 });
  }

  let response = await fetch(directUrl);
  
  // Auto-refresh single file URL if expired
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log('🔄 Single file URL expired, refreshing...');
    
    const BOT_TOKEN = env.BOT_TOKEN;
    const telegramFileId = metadata?.telegramFileId;
    
    if (BOT_TOKEN && telegramFileId) {
      try {
        const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);
        if (getFileResponse.ok) {
          const getFileData = await getFileResponse.json();
          if (getFileData.ok && getFileData.result?.file_path) {
            const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
            await kvNamespace.put(actualId, freshUrl);
            console.log('✅ Single file URL refreshed');
            response = await fetch(freshUrl);
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

  const headers = new Headers();
  const mimeType = getMimeType(extension);
  headers.set('Content-Type', mimeType);
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Cache-Control', 'public, max-age=3600');

  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');
  const filename = metadata?.filename || 'download';

  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    headers.set('Content-Disposition', 'inline');
  }

  return new Response(response.body, { status: response.status, headers });
}

// Parse Range header (purane system jaisa)
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(\d+)-(\d*)/);
  if (!rangeMatch) return null;
  
  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;
  
  if (start >= size || end >= size || start > end) return null;
  
  return [{ start, end }];
}
