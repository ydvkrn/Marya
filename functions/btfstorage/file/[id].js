// MIME type mapping
const MIME_TYPES = {
  'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
  'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
  'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
  'mov': 'video/quicktime', 'avi': 'video/x-msvideo', 'm4v': 'video/x-m4v',
  'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'flac': 'audio/flac',
  'pdf': 'application/pdf', 'txt': 'text/plain', 'json': 'application/json',
  'zip': 'application/zip', 'rar': 'application/vnd.rar',
  '7z': 'application/x-7z-compressed'
};

function getMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;
  
  console.log('=== MULTI-KV FILE SERVE WITH AUTO-REFRESH ===');
  console.log('File ID:', fileId);
  
  try {
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';
    
    // ✅ All KV namespaces
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
    console.log(`File found: ${masterMetadata.filename} (${masterMetadata.totalChunks || masterMetadata.chunks?.length || 0} chunks)`);
    
    // ✅ Handle chunked files with auto-refresh
    if (masterMetadata.type === 'multi_kv_chunked' || masterMetadata.chunks) {
      return await handleChunkedFileWithAutoRefresh(request, kvNamespaces, masterMetadata, extension, env);
    } else {
      // Legacy single file support
      return await handleSingleFile(request, kvNamespaces.FILES_KV, actualId, extension, masterMetadata, env);
    }
    
  } catch (error) {
    console.error('File serve error:', error);
    return new Response(`Server error: ${error.message}`, { status: 500 });
  }
}

// ✅ Handle chunked files with auto URL refresh - FIXED Promise.all()
async function handleChunkedFileWithAutoRefresh(request, kvNamespaces, masterMetadata, extension, env) {
  const { totalChunks, chunks, filename, size } = masterMetadata;
  
  console.log(`Serving chunked file: ${filename} (${chunks?.length || totalChunks} chunks)`);
  
  // ✅ Handle Range requests for video streaming
  const range = request.headers.get('Range');
  if (range) {
    return await handleRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env);
  }
  
  // ✅ FIXED: Load chunks SEQUENTIALLY instead of Promise.all()
  const chunkResults = [];
  
  // Load first 3 chunks only (for instant play + no CPU overload)
  const maxInitialChunks = Math.min(3, chunks.length);
  
  for (let i = 0; i < maxInitialChunks; i++) {
    try {
      const chunkInfo = chunks[i];
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      const chunkKey = chunkInfo.chunkKey || `${masterMetadata.id || actualId}_chunk_${i}`;
      
      console.log(`Loading chunk ${i + 1}/${maxInitialChunks}...`);
      const chunkResult = await getChunkWithAutoRefresh(kvNamespace, chunkKey, chunkInfo, env, i);
      chunkResults.push(chunkResult);
      
      console.log(`✅ Chunk ${i + 1} loaded: ${Math.round(chunkResult.data.byteLength/1024/1024)}MB`);
      
    } catch (chunkError) {
      console.error(`❌ Chunk ${i + 1} failed:`, chunkError);
      // Continue with available chunks (don't fail completely)
      continue;
    }
  }
  
  if (chunkResults.length === 0) {
    throw new Error('No chunks could be loaded');
  }
  
  // Sort and combine loaded chunks only
  chunkResults.sort((a, b) => a.index - b.index);
  const totalSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(totalSize);
  
  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }
  
  // ✅ Response headers
  const headers = new Headers();
  const mimeType = getMimeType(extension);
  headers.set('Content-Type', mimeType);
  headers.set('Content-Length', totalSize.toString());
  headers.set('Access-Control-Allow-Origin', '*');
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Cache-Control', 'public, max-age=3600');
  
  // Send as partial content to enable Range requests
  if (chunkResults.length < chunks.length) {
    headers.set('Content-Range', `bytes 0-${totalSize - 1}/${size}`);
  }
  
  const url = new URL(request.url);
  const isDownload = url.searchParams.has('dl');
  
  if (isDownload) {
    headers.set('Content-Disposition', `attachment; filename="${filename}"`);
  } else {
    if (mimeType.startsWith('image/') || mimeType.startsWith('video/') ||
        mimeType.startsWith('audio/') || mimeType === 'application/pdf') {
      headers.set('Content-Disposition', 'inline');
    } else {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
    }
  }
  
  console.log(`✅ File served: ${chunkResults.length} chunks, ${Math.round(totalSize/1024/1024)}MB`);
  
  return new Response(combinedBuffer, { 
    status: chunkResults.length < chunks.length ? 206 : 200, 
    headers 
  });
}

// ✅ Get chunk with automatic URL refresh and cleanup
async function getChunkWithAutoRefresh(kvNamespace, chunkKey, chunkInfo, env, index) {
  console.log(`Getting chunk: ${chunkKey}`);
  
  const chunkMetadataString = await kvNamespace.get(chunkKey);
  if (!chunkMetadataString) {
    throw new Error(`Chunk ${chunkKey} not found`);
  }
  
  const chunkMetadata = JSON.parse(chunkMetadataString);
  let directUrl = chunkMetadata.directUrl;
  
  // ✅ Try to fetch chunk
  let response = await fetch(directUrl, { signal: AbortSignal.timeout(30000) });
  
  // ✅ If URL expired (403, 404, 410), refresh it
  if (!response.ok && (response.status === 403 || response.status === 404 || response.status === 410)) {
    console.log(`🔄 URL expired for chunk ${chunkKey}, refreshing...`);
    
    // Try multiple bot tokens
    const botTokens = [env.BOT_TOKEN, env.BOT_TOKEN2, env.BOT_TOKEN3, env.BOT_TOKEN4].filter(t => t);
    
    if (botTokens.length === 0) {
      throw new Error('BOT_TOKEN not available for URL refresh');
    }
    
    let refreshed = false;
    for (const BOT_TOKEN of botTokens) {
      try {
        // Get fresh URL from Telegram
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { signal: AbortSignal.timeout(15000) }
        );
        
        if (!getFileResponse.ok) continue;
        
        const getFileData = await getFileResponse.json();
        if (!getFileData.ok || !getFileData.result?.file_path) continue;
        
        // ✅ Create new URL
        const freshUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
        
        // ✅ Update KV with fresh URL
        const updatedMetadata = {
          ...chunkMetadata,
          directUrl: freshUrl,
          lastRefreshed: Date.now(),
          refreshCount: (chunkMetadata.refreshCount || 0) + 1
        };
        
        // Store updated metadata (don't await to avoid blocking)
        kvNamespace.put(chunkKey, JSON.stringify(updatedMetadata)).catch(err => 
          console.error('Failed to update chunk metadata:', err)
        );
        
        console.log(`✅ URL refreshed for chunk ${chunkKey}`);
        
        // Try with fresh URL
        response = await fetch(freshUrl, { signal: AbortSignal.timeout(30000) });
        
        if (response.ok) {
          refreshed = true;
          break;
        }
        
      } catch (refreshError) {
        console.error(`❌ Failed to refresh URL with bot token:`, refreshError);
        continue;
      }
    }
    
    if (!refreshed) {
      throw new Error(`Failed to refresh expired URL for all bot tokens: ${chunkKey}`);
    }
  }
  
  if (!response.ok) {
    throw new Error(`Failed to fetch chunk ${chunkKey}: ${response.status}`);
  }
  
  return {
    index: index,
    data: await response.arrayBuffer()
  };
}

// ✅ Handle Range requests for video streaming - FIXED: Max 3 chunks
async function handleRangeRequest(request, kvNamespaces, masterMetadata, extension, range, env) {
  console.log('Handling Range request:', range);
  
  const { size, chunks } = masterMetadata;
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
  
  // Determine which chunks are needed
  const CHUNK_SIZE = masterMetadata.chunkSize || 20 * 1024 * 1024; // 20MB default
  const startChunk = Math.floor(start / CHUNK_SIZE);
  const endChunk = Math.floor(end / CHUNK_SIZE);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);
  
  console.log(`Need chunks: ${startChunk}-${endChunk} (${neededChunks.length})`);
  
  // LIMIT: Max 3 chunks for Range requests (avoid CPU overload)
  const maxRangeChunks = Math.min(3, neededChunks.length);
  const limitedChunks = neededChunks.slice(0, maxRangeChunks);
  
  if (maxRangeChunks < neededChunks.length) {
    console.log(`⚠️ Range limited to ${maxRangeChunks} chunks to avoid CPU overload`);
    // Adjust end position
    const limitedEnd = Math.min(end, (startChunk + maxRangeChunks) * CHUNK_SIZE - 1);
    return await processRangeChunks(kvNamespaces, limitedChunks, startChunk, start, limitedEnd, CHUNK_SIZE, extension, size, env);
  }
  
  return await processRangeChunks(kvNamespaces, limitedChunks, startChunk, start, end, CHUNK_SIZE, extension, size, env);
}

// Process range chunks sequentially
async function processRangeChunks(kvNamespaces, neededChunks, startChunk, start, end, CHUNK_SIZE, extension, totalSize, env) {
  // Get needed chunks SEQUENTIALLY (no Promise.all)
  const chunkResults = [];
  
  for (let i = 0; i < neededChunks.length; i++) {
    const chunkInfo = neededChunks[i];
    const chunkIndex = startChunk + i;
    const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
    const chunkKey = chunkInfo.chunkKey || chunkInfo.keyName;
    
    try {
      console.log(`Loading range chunk ${chunkIndex + 1}...`);
      const chunkResult = await getChunkWithAutoRefresh(kvNamespace, chunkKey, chunkInfo, env, chunkIndex);
      chunkResults.push(chunkResult);
      
      console.log(`✅ Range chunk ${chunkIndex + 1}: ${Math.round(chunkResult.data.byteLength/1024)}KB`);
    } catch (error) {
      console.error(`❌ Range chunk ${chunkIndex + 1} failed:`, error);
      return new Response(`Range chunk ${chunkIndex + 1} failed: ${error.message}`, { status: 500 });
    }
  }
  
  // Sort and combine
  chunkResults.sort((a, b) => a.index - b.index);
  const combinedSize = chunkResults.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
  const combinedBuffer = new Uint8Array(combinedSize);
  
  let offset = 0;
  for (const chunk of chunkResults) {
    combinedBuffer.set(new Uint8Array(chunk.data), offset);
    offset += chunk.data.byteLength;
  }
  
  // Extract exact range
  const rangeStart = start - (startChunk * CHUNK_SIZE);
  const requestedSize = end - start + 1;
  const rangeBuffer = combinedBuffer.slice(rangeStart, rangeStart + requestedSize);
  
  const headers = new Headers();
  headers.set('Content-Type', getMimeType(extension));
  headers.set('Content-Length', rangeBuffer.byteLength.toString());
  headers.set('Content-Range', `bytes ${start}-${start + rangeBuffer.byteLength - 1}/${totalSize}`);
  headers.set('Accept-Ranges', 'bytes');
  headers.set('Access-Control-Allow-Origin', '*');
  
  console.log(`✅ Range response: ${rangeBuffer.byteLength} bytes`);
  
  return new Response(rangeBuffer, { status: 206, headers });
}

// ✅ Legacy single file support
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
            // Update KV with fresh URL
            await kvNamespace.put(actualId, freshUrl, { metadata: { ...metadata, lastRefreshed: Date.now() } });
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

// ✅ Parse Range header
function parseRange(range, size) {
  const rangeMatch = range.match(/bytes=(d+)-(d*)/);
  if (!rangeMatch) return null;
  
  const start = parseInt(rangeMatch[1], 10);
  const end = rangeMatch[2] ? parseInt(rangeMatch[2], 10) : size - 1;
  
  if (start >= size || end >= size || start > end) return null;
  
  return [{ start, end }];
}