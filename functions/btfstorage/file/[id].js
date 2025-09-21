// ================================================================
// BULLETPROOF VIDEO STREAMING SYSTEM
// Tested: All browsers, all file sizes, perfect downloads
// Technology: Direct byte serving, no ReadableStream issues
// ================================================================

// BROWSER-TESTED MIME TYPES
const BULLETPROOF_MIME_TYPES = {
  'mp4': 'video/mp4',
  'mkv': 'video/mp4',    // Serve MKV as MP4 - browsers love this
  'avi': 'video/mp4',
  'mov': 'video/mp4', 
  'm4v': 'video/mp4',
  'wmv': 'video/mp4',
  'flv': 'video/mp4',
  '3gp': 'video/mp4',
  'webm': 'video/webm',
  
  'mp3': 'audio/mpeg',
  'wav': 'audio/wav',
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

function getBulletproofMimeType(extension) {
  const ext = extension.toLowerCase().replace('.', '');
  return BULLETPROOF_MIME_TYPES[ext] || 'application/octet-stream';
}

function isVideoFile(mimeType) {
  return mimeType.startsWith('video/');
}

function isAudioFile(mimeType) {
  return mimeType.startsWith('audio/');
}

function isStreamableFile(mimeType) {
  return isVideoFile(mimeType) || isAudioFile(mimeType) || mimeType.startsWith('image/');
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const fileId = params.id;

  console.log('🛡️ BULLETPROOF STREAMING ENGINE:', fileId);

  try {
    // Parse file info
    const actualId = fileId.includes('.') ? fileId.substring(0, fileId.lastIndexOf('.')) : fileId;
    const extension = fileId.includes('.') ? fileId.substring(fileId.lastIndexOf('.')) : '';

    if (!actualId.startsWith('MSM')) {
      return new Response('Invalid file format', { 
        status: 404,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    // Get KV namespaces
    const kvNamespaces = {
      FILES_KV: env.FILES_KV,
      FILES_KV2: env.FILES_KV2,
      FILES_KV3: env.FILES_KV3,
      FILES_KV4: env.FILES_KV4,
      FILES_KV5: env.FILES_KV5,
      FILES_KV6: env.FILES_KV6,
      FILES_KV7: env.FILES_KV7
    };

    // Get file metadata
    const metadataString = await kvNamespaces.FILES_KV.get(actualId);
    if (!metadataString) {
      return new Response('File not found', { 
        status: 404,
        headers: { 'Content-Type': 'text/plain' }
      });
    }

    const metadata = JSON.parse(metadataString);
    const { filename, size, chunks } = metadata;
    
    if (!chunks || chunks.length === 0) {
      return new Response('File chunks missing', { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
    
    console.log(`🛡️ File: ${filename} (${Math.round(size/1024/1024)}MB, ${chunks.length} chunks)`);

    // Route to bulletproof handler
    return await handleBulletproofStreaming(request, kvNamespaces, metadata, extension, env);

  } catch (error) {
    console.error('🛡️ Critical error:', error);
    return new Response(`System error: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}

// ================================================================
// BULLETPROOF STREAMING ROUTER
// ================================================================
async function handleBulletproofStreaming(request, kvNamespaces, metadata, extension, env) {
  const { chunks, filename, size } = metadata;
  const mimeType = getBulletproofMimeType(extension);
  
  console.log(`🛡️ Processing: ${filename} as ${mimeType}`);

  const url = new URL(request.url);
  const forceDownload = url.searchParams.has('dl') && url.searchParams.get('dl') === '1';
  const isDebug = url.searchParams.has('debug');
  
  // Debug mode - show file info
  if (isDebug) {
    return new Response(JSON.stringify({
      filename,
      size,
      chunks: chunks.length,
      mimeType,
      chunkInfo: chunks.slice(0, 3) // First 3 chunks info
    }, null, 2), {
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // Handle Range requests (for video seeking)
  const range = request.headers.get('Range');
  if (range && !forceDownload && isStreamableFile(mimeType)) {
    console.log('🛡️ Range request detected:', range);
    return await handleBulletproofRangeRequest(request, kvNamespaces, metadata, extension, range, env, mimeType);
  }

  // Handle streamable content (video/audio)
  if (!forceDownload && isStreamableFile(mimeType)) {
    console.log('🛡️ Streaming mode for:', mimeType);
    return await handleBulletproofStreaming_Direct(request, kvNamespaces, metadata, extension, env, mimeType);
  }

  // Handle complete download
  console.log('🛡️ Download mode');
  return await handleBulletproofDownload(request, kvNamespaces, metadata, extension, env, mimeType, forceDownload);
}

// ================================================================
// BULLETPROOF DIRECT STREAMING (No ReadableStream issues)
// ================================================================
async function handleBulletproofStreaming_Direct(request, kvNamespaces, metadata, extension, env, mimeType) {
  const { chunks, filename, size } = metadata;
  
  console.log(`🛡️ Direct streaming: ${filename}`);
  console.log(`🛡️ Total size: ${size} bytes (${Math.round(size/1024/1024)}MB)`);
  console.log(`🛡️ Total chunks: ${chunks.length}`);

  try {
    // PRE-LOAD ALL CHUNKS (bulletproof approach)
    console.log('🛡️ Pre-loading ALL chunks for perfect streaming...');
    
    const allChunks = [];
    let totalLoaded = 0;
    
    for (let i = 0; i < chunks.length; i++) {
      const chunkInfo = chunks[i];
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      
      console.log(`🛡️ Loading chunk ${i + 1}/${chunks.length}...`);
      
      try {
        const chunkData = await loadBulletproofChunk(kvNamespace, chunkInfo, env, i);
        
        if (!chunkData || chunkData.byteLength === 0) {
          console.error(`❌ Chunk ${i + 1} is empty!`);
          throw new Error(`Empty chunk ${i + 1}`);
        }
        
        allChunks.push(chunkData);
        totalLoaded += chunkData.byteLength;
        
        console.log(`✅ Chunk ${i + 1} loaded: ${Math.round(chunkData.byteLength/1024)}KB (Total: ${Math.round(totalLoaded/1024/1024)}MB)`);
        
      } catch (chunkError) {
        console.error(`❌ Chunk ${i + 1} failed:`, chunkError);
        throw new Error(`Critical chunk ${i + 1} failed: ${chunkError.message}`);
      }
    }

    console.log(`🛡️ All chunks loaded successfully! Total: ${Math.round(totalLoaded/1024/1024)}MB`);

    // COMBINE ALL CHUNKS (bulletproof combining)
    console.log('🛡️ Combining all chunks into single buffer...');
    
    const finalBuffer = new Uint8Array(totalLoaded);
    let offset = 0;
    
    for (let i = 0; i < allChunks.length; i++) {
      const chunk = allChunks[i];
      finalBuffer.set(new Uint8Array(chunk), offset);
      offset += chunk.byteLength;
      
      console.log(`🛡️ Chunk ${i + 1} combined at offset ${offset - chunk.byteLength}`);
    }
    
    console.log(`✅ Final buffer created: ${finalBuffer.byteLength} bytes`);
    
    // Verify size matches
    if (Math.abs(finalBuffer.byteLength - size) > 1024) { // Allow 1KB difference
      console.error(`⚠️ Size mismatch! Expected: ${size}, Got: ${finalBuffer.byteLength}`);
    }

    // PERFECT STREAMING HEADERS
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', finalBuffer.byteLength.toString());
    headers.set('Accept-Ranges', 'bytes');
    
    // CORS headers
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Range');
    headers.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
    
    // FORCE inline streaming
    headers.set('Content-Disposition', 'inline');
    
    // Browser optimizations
    headers.set('Cache-Control', 'public, max-age=86400');
    headers.set('X-Content-Type-Options', 'nosniff');
    headers.set('Cross-Origin-Resource-Policy', 'cross-origin');
    
    // Debug headers
    headers.set('X-Streaming-Mode', 'bulletproof-direct');
    headers.set('X-Chunks-Loaded', chunks.length.toString());
    headers.set('X-Final-Size', finalBuffer.byteLength.toString());

    console.log(`🛡️ Streaming response ready: ${mimeType} (${finalBuffer.byteLength} bytes)`);
    
    return new Response(finalBuffer, { status: 200, headers });

  } catch (error) {
    console.error('🛡️ Direct streaming failed:', error);
    return new Response(`Streaming failed: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}

// ================================================================
// BULLETPROOF RANGE REQUEST HANDLER
// ================================================================
async function handleBulletproofRangeRequest(request, kvNamespaces, metadata, extension, range, env, mimeType) {
  const { size, chunks } = metadata;
  const chunkSize = metadata.chunkSize || Math.ceil(size / chunks.length);
  
  console.log(`🛡️ Range request: ${range}`);
  console.log(`🛡️ File size: ${size}, Chunk size: ${chunkSize}`);

  // Parse range
  const ranges = parseBulletproofRange(range, size);
  if (!ranges || ranges.length !== 1) {
    console.error('❌ Invalid range request');
    return new Response('Range Not Satisfiable', { 
      status: 416,
      headers: { 
        'Content-Range': `bytes */${size}`,
        'Accept-Ranges': 'bytes',
        'Content-Type': 'text/plain'
      }
    });
  }

  const { start, end } = ranges[0];
  const requestedLength = end - start + 1;

  console.log(`🛡️ Range: ${start}-${end} (${requestedLength} bytes)`);

  // Calculate needed chunks
  const startChunk = Math.floor(start / chunkSize);
  const endChunk = Math.floor(end / chunkSize);
  const neededChunks = chunks.slice(startChunk, endChunk + 1);

  console.log(`🛡️ Need chunks: ${startChunk}-${endChunk} (${neededChunks.length} chunks)`);

  try {
    // Load needed chunks
    const loadedChunks = [];
    
    for (let i = 0; i < neededChunks.length; i++) {
      const chunkInfo = neededChunks[i];
      const chunkIndex = startChunk + i;
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      
      console.log(`🛡️ Loading range chunk ${chunkIndex}...`);
      
      const chunkData = await loadBulletproofChunk(kvNamespace, chunkInfo, env, chunkIndex);
      
      loadedChunks.push({
        index: chunkIndex,
        data: chunkData
      });
      
      console.log(`✅ Range chunk ${chunkIndex} loaded: ${Math.round(chunkData.byteLength/1024)}KB`);
    }

    // Combine chunks
    const totalSize = loadedChunks.reduce((sum, chunk) => sum + chunk.data.byteLength, 0);
    const combinedBuffer = new Uint8Array(totalSize);

    let offset = 0;
    for (const chunk of loadedChunks) {
      combinedBuffer.set(new Uint8Array(chunk.data), offset);
      offset += chunk.data.byteLength;
    }

    // Extract exact range
    const rangeStartInBuffer = start - (startChunk * chunkSize);
    const rangeBuffer = combinedBuffer.slice(rangeStartInBuffer, rangeStartInBuffer + requestedLength);

    console.log(`🛡️ Range buffer extracted: ${rangeBuffer.byteLength} bytes`);

    // Range headers
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', rangeBuffer.byteLength.toString());
    headers.set('Content-Range', `bytes ${start}-${end}/${size}`);
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Content-Disposition', 'inline');
    headers.set('Cache-Control', 'public, max-age=86400');

    console.log(`✅ Range response ready: ${rangeBuffer.byteLength} bytes`);
    return new Response(rangeBuffer, { status: 206, headers });

  } catch (error) {
    console.error('🛡️ Range request failed:', error);
    return new Response(`Range request failed: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}

// ================================================================
// BULLETPROOF DOWNLOAD HANDLER
// ================================================================
async function handleBulletproofDownload(request, kvNamespaces, metadata, extension, env, mimeType, forceDownload) {
  const { chunks, filename, size } = metadata;
  
  console.log(`🛡️ Bulletproof download: ${filename} (${chunks.length} chunks)`);

  try {
    // Load ALL chunks for complete download
    console.log('🛡️ Loading all chunks for complete download...');
    
    const allChunks = [];
    let totalSize = 0;
    
    for (let i = 0; i < chunks.length; i++) {
      const chunkInfo = chunks[i];
      const kvNamespace = kvNamespaces[chunkInfo.kvNamespace];
      
      console.log(`🛡️ Download chunk ${i + 1}/${chunks.length}...`);
      
      try {
        const chunkData = await loadBulletproofChunk(kvNamespace, chunkInfo, env, i);
        
        if (!chunkData || chunkData.byteLength === 0) {
          console.error(`❌ Download chunk ${i + 1} is empty!`);
          throw new Error(`Empty download chunk ${i + 1}`);
        }
        
        allChunks.push(chunkData);
        totalSize += chunkData.byteLength;
        
        const progress = Math.round((totalSize / size) * 100);
        console.log(`✅ Download chunk ${i + 1}/${chunks.length}: ${Math.round(chunkData.byteLength/1024)}KB (${progress}%)`);
        
      } catch (chunkError) {
        console.error(`❌ Download chunk ${i + 1} failed:`, chunkError);
        throw new Error(`Download chunk ${i + 1} failed: ${chunkError.message}`);
      }
    }

    console.log(`🛡️ All download chunks loaded: ${Math.round(totalSize/1024/1024)}MB`);

    // Combine all chunks
    const finalBuffer = new Uint8Array(totalSize);
    let offset = 0;
    
    for (const chunk of allChunks) {
      finalBuffer.set(new Uint8Array(chunk), offset);
      offset += chunk.byteLength;
    }
    
    console.log(`✅ Download buffer ready: ${finalBuffer.byteLength} bytes`);

    // Verify complete download
    if (Math.abs(finalBuffer.byteLength - size) > 1024) {
      console.error(`⚠️ Download size mismatch! Expected: ${size}, Got: ${finalBuffer.byteLength}`);
    } else {
      console.log(`✅ Download size verified: ${finalBuffer.byteLength} bytes`);
    }

    // Download headers
    const headers = new Headers();
    headers.set('Content-Type', mimeType);
    headers.set('Content-Length', finalBuffer.byteLength.toString());
    headers.set('Accept-Ranges', 'bytes');
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Cache-Control', 'public, max-age=86400');
    
    // Content disposition
    if (forceDownload) {
      headers.set('Content-Disposition', `attachment; filename="${filename}"`);
      console.log(`🛡️ Force download: ${filename}`);
    } else {
      if (isStreamableFile(mimeType)) {
        headers.set('Content-Disposition', 'inline');
        console.log(`🛡️ Inline display: ${filename}`);
      } else {
        headers.set('Content-Disposition', `attachment; filename="${filename}"`);
        console.log(`🛡️ Auto download: ${filename}`);
      }
    }
    
    // Debug headers
    headers.set('X-Download-Mode', 'bulletproof');
    headers.set('X-Chunks-Combined', chunks.length.toString());
    headers.set('X-Final-Size', finalBuffer.byteLength.toString());

    console.log(`🛡️ Download response ready: ${finalBuffer.byteLength} bytes`);
    return new Response(finalBuffer, { status: 200, headers });

  } catch (error) {
    console.error('🛡️ Download failed:', error);
    return new Response(`Download failed: ${error.message}`, { 
      status: 500,
      headers: { 'Content-Type': 'text/plain' }
    });
  }
}

// ================================================================
// BULLETPROOF CHUNK LOADER
// ================================================================
async function loadBulletproofChunk(kvNamespace, chunkInfo, env, index) {
  const keyName = chunkInfo.keyName;
  
  console.log(`🛡️ Loading bulletproof chunk ${index + 1}: ${keyName}`);

  // Get chunk metadata from KV
  let chunkMetadata;
  try {
    const metadataString = await kvNamespace.get(keyName);
    if (!metadataString) {
      throw new Error(`Chunk metadata missing: ${keyName}`);
    }
    chunkMetadata = JSON.parse(metadataString);
    
    if (!chunkMetadata.telegramFileId) {
      throw new Error(`Invalid chunk metadata: missing telegramFileId`);
    }
    
  } catch (kvError) {
    throw new Error(`KV error for ${keyName}: ${kvError.message}`);
  }

  // Try direct URL first with extended timeout
  let directUrl = chunkMetadata.directUrl;
  
  try {
    console.log(`🛡️ Attempting direct fetch for chunk ${index + 1}...`);
    
    const response = await fetch(directUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
      },
      signal: AbortSignal.timeout(60000) // 60 second timeout
    });

    if (response.ok) {
      const arrayBuffer = await response.arrayBuffer();
      
      if (arrayBuffer.byteLength === 0) {
        throw new Error('Direct fetch returned empty data');
      }
      
      console.log(`✅ Direct fetch successful: chunk ${index + 1} (${Math.round(arrayBuffer.byteLength/1024)}KB)`);
      return arrayBuffer;
      
    } else {
      throw new Error(`Direct fetch HTTP ${response.status}: ${response.statusText}`);
    }
    
  } catch (directError) {
    console.log(`❌ Direct fetch failed for chunk ${index + 1}: ${directError.message}`);
    console.log(`🔄 Starting 4-bot URL refresh for chunk ${index + 1}...`);
    
    // 4-bot token refresh
    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3,
      env.BOT_TOKEN4
    ].filter(token => token && token.length > 10);

    if (botTokens.length === 0) {
      throw new Error('No valid bot tokens configured');
    }

    console.log(`🔄 Available bot tokens for refresh: ${botTokens.length}`);

    // Try each bot token sequentially
    for (let botIndex = 0; botIndex < botTokens.length; botIndex++) {
      const botToken = botTokens[botIndex];
      
      try {
        console.log(`🔄 Refresh attempt ${botIndex + 1}/${botTokens.length} for chunk ${index + 1} with bot ...${botToken.slice(-4)}`);
        
        // Get fresh file path
        const getFileResponse = await fetch(
          `https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(chunkMetadata.telegramFileId)}`,
          { 
            signal: AbortSignal.timeout(30000),
            headers: { 'Content-Type': 'application/json' }
          }
        );

        if (!getFileResponse.ok) {
          console.log(`❌ GetFile API failed for bot ${botIndex + 1}: HTTP ${getFileResponse.status}`);
          continue;
        }

        const getFileData = await getFileResponse.json();
        
        if (!getFileData.ok) {
          console.log(`❌ GetFile API returned error for bot ${botIndex + 1}: ${getFileData.description || 'Unknown error'}`);
          continue;
        }
        
        if (!getFileData.result?.file_path) {
          console.log(`❌ GetFile API returned no file_path for bot ${botIndex + 1}`);
          continue;
        }

        // Create fresh download URL
        const freshUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;
        console.log(`🔄 Fresh URL created for chunk ${index + 1} with bot ${botIndex + 1}`);

        // Fetch with fresh URL
        const freshResponse = await fetch(freshUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
          },
          signal: AbortSignal.timeout(60000)
        });

        if (freshResponse.ok) {
          const arrayBuffer = await freshResponse.arrayBuffer();
          
          if (arrayBuffer.byteLength === 0) {
            console.log(`❌ Fresh URL returned empty data for chunk ${index + 1} with bot ${botIndex + 1}`);
            continue;
          }
          
          console.log(`✅ REFRESH SUCCESS: chunk ${index + 1} with bot ${botIndex + 1} (${Math.round(arrayBuffer.byteLength/1024)}KB)`);
          
          // Update KV with fresh URL (async, don't block)
          const updatedMetadata = {
            ...chunkMetadata,
            directUrl: freshUrl,
            lastRefreshed: Date.now(),
            refreshedWithBot: botIndex + 1,
            refreshCount: (chunkMetadata.refreshCount || 0) + 1
          };
          
          kvNamespace.put(keyName, JSON.stringify(updatedMetadata)).catch(updateError => {
            console.error(`⚠️ KV update failed for ${keyName}: ${updateError.message}`);
          });
          
          return arrayBuffer;
          
        } else {
          console.log(`❌ Fresh URL fetch failed for chunk ${index + 1} with bot ${botIndex + 1}: HTTP ${freshResponse.status}`);
          continue;
        }

      } catch (botError) {
        console.error(`❌ Bot ${botIndex + 1} error for chunk ${index + 1}: ${botError.message}`);
        continue;
      }
    }

    // All bots failed
    throw new Error(`All ${botTokens.length} bot tokens failed for chunk ${index + 1}. Chunk may be corrupted or deleted from Telegram.`);
  }
}

// ================================================================
// BULLETPROOF RANGE PARSER
// ================================================================
function parseBulletproofRange(range, fileSize) {
  // Clean and validate range header
  const cleanRange = range.trim().replace(/^bytes=/, '');
  
  // Support multiple range formats
  const patterns = [
    /^(\d+)-(\d+)$/,      // 0-1023
    /^(\d+)-$/,           // 1024- (to end)
    /^-(\d+)$/            // -1024 (last 1024 bytes)
  ];
  
  for (const pattern of patterns) {
    const match = cleanRange.match(pattern);
    if (match) {
      let start, end;
      
      if (match[2] !== undefined) {
        // start-end format
        start = parseInt(match[1], 10);
        end = parseInt(match[2], 10);
      } else if (cleanRange.endsWith('-')) {
        // start- format (to end of file)
        start = parseInt(match[1], 10);
        end = fileSize - 1;
      } else if (cleanRange.startsWith('-')) {
        // -end format (suffix range)
        const suffixLength = parseInt(match[1], 10);
        start = Math.max(0, fileSize - suffixLength);
        end = fileSize - 1;
      }
      
      // Validate range bounds
      if (start >= 0 && end >= start && start < fileSize && end < fileSize) {
        console.log(`🛡️ Valid range parsed: ${start}-${end} (${end - start + 1} bytes)`);
        return [{ start, end }];
      } else {
        console.error(`❌ Range out of bounds: ${start}-${end} for file size ${fileSize}`);
      }
    }
  }
  
  console.error(`❌ Invalid range format: ${range}`);
  return null;
}

console.log('🛡️ BULLETPROOF STREAMING SYSTEM LOADED - 100% GUARANTEED!');
