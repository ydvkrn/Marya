export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { url } = await request.json();
    
    if (!url || !url.startsWith('http')) {
      throw new Error('Invalid URL provided');
    }

    console.log('📥 Importing from URL:', url);

    // Enhanced URL fetching with proper headers
    const response = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
      }
    });

    if (!response.ok) {
      throw new Error(`Failed to download: HTTP ${response.status}`);
    }

    // Enhanced filename extraction
    let filename = extractFilename(url, response);
    
    // Enhanced content type detection
    let contentType = detectContentType(response, filename, url);

    // Ensure filename has proper extension
    filename = ensureProperExtension(filename, contentType);

    console.log(`📦 File imported: ${filename} (Type: ${contentType})`);

    // Convert to File object
    const fileBuffer = await response.arrayBuffer();
    const file = new File([fileBuffer], filename, { type: contentType });

    const fileSizeMB = Math.round(file.size / 1024 / 1024);
    console.log(`📊 File size: ${fileSizeMB}MB`);

    // Check size limits (2GB max)
    if (file.size > 2 * 1024 * 1024 * 1024) {
      throw new Error(`File too large: ${fileSizeMB}MB (max 2GB)`);
    }

    // Generate MSM ID
    const msmId = generateMSMId();
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    // Environment setup
    const botTokens = [
      env.BOT_TOKEN,
      env.BOT_TOKEN2,
      env.BOT_TOKEN3, 
      env.BOT_TOKEN4
    ].filter(token => token);

    const CHANNEL_ID = env.CHANNEL_ID;

    const kvNamespaces = [
      { kv: env.FILES_KV, name: 'FILES_KV' },
      { kv: env.FILES_KV2, name: 'FILES_KV2' },
      { kv: env.FILES_KV3, name: 'FILES_KV3' },
      { kv: env.FILES_KV4, name: 'FILES_KV4' },
      { kv: env.FILES_KV5, name: 'FILES_KV5' },
      { kv: env.FILES_KV6, name: 'FILES_KV6' },
      { kv: env.FILES_KV7, name: 'FILES_KV7' }
    ].filter(item => item.kv);

    // Smart chunking based on file size
    let chunkSize;
    if (file.size <= 50 * 1024 * 1024) {
      chunkSize = 10 * 1024 * 1024; // 10MB for smaller files
    } else if (file.size <= 200 * 1024 * 1024) {
      chunkSize = 20 * 1024 * 1024; // 20MB for medium files  
    } else {
      chunkSize = 50 * 1024 * 1024; // 50MB for large files
    }

    const totalChunks = Math.ceil(file.size / chunkSize);
    const chunkResults = [];

    console.log(`🚀 Starting chunked import: ${totalChunks} chunks × ${Math.round(chunkSize/1024/1024)}MB`);

    // Upload chunks with proper error handling
    for (let i = 0; i < totalChunks; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, file.size);
      const chunk = file.slice(start, end);

      const kvIndex = Math.floor(i / 40);
      const targetKV = kvNamespaces[kvIndex];
      const botToken = botTokens[i % botTokens.length];

      console.log(`⬆️ Importing chunk ${i + 1}/${totalChunks} (${Math.round(chunk.size/1024/1024)}MB)...`);

      try {
        const chunkResult = await uploadImportedChunk(
          chunk, msmId, i, kvIndex, i % 40,
          botToken, CHANNEL_ID, targetKV, filename
        );

        chunkResults.push(chunkResult);
      } catch (chunkError) {
        console.error(`Chunk ${i + 1} failed:`, chunkError);
        throw new Error(`Upload failed at chunk ${i + 1}: ${chunkError.message}`);
      }
    }

    // Store final metadata with enhanced info
    const finalMetadata = {
      filename: filename,
      size: file.size,
      contentType: contentType,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'url_import',
      originalUrl: url,
      totalChunks: totalChunks,
      chunkSize: chunkSize,
      strategy: 'url_imported_chunked',
      neverExpires: true,
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.keyName,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    await kvNamespaces[0].kv.put(msmId, JSON.stringify(finalMetadata));

    const baseUrl = new URL(request.url).origin;

    console.log(`✅ URL import completed: ${filename} (${fileSizeMB}MB)`);

    return new Response(JSON.stringify({
      success: true,
      filename: filename,
      size: file.size,
      contentType: contentType,
      url: `${baseUrl}/btfstorage/file/${msmId}${extension}`,
      download: `${baseUrl}/btfstorage/file/${msmId}${extension}?dl=1`,
      id: msmId,
      strategy: 'url_imported',
      originalUrl: url,
      chunks: totalChunks,
      chunkSize: `${Math.round(chunkSize/1024/1024)}MB`,
      lifetime: 'Permanent (Never Expires)'
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('URL import error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Enhanced filename extraction
function extractFilename(url, response) {
  // Try Content-Disposition first
  const contentDisposition = response.headers.get('Content-Disposition');
  if (contentDisposition) {
    const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
    if (filenameMatch) {
      let filename = filenameMatch[1].replace(/['"]/g, '');
      if (filename && filename !== 'undefined') {
        return decodeURIComponent(filename);
      }
    }
  }

  // Extract from URL
  let filename = url.split('/').pop().split('?')[0];
  
  // Decode URL encoding
  try {
    filename = decodeURIComponent(filename);
  } catch (e) {
    // Keep original if decode fails
  }

  // Fallback if no filename
  if (!filename || filename.length < 1) {
    filename = 'imported_file';
  }

  return filename;
}

// Enhanced content type detection
function detectContentType(response, filename, url) {
  // Try response headers first
  let contentType = response.headers.get('Content-Type');
  if (contentType) {
    // Clean up content type (remove charset etc)
    contentType = contentType.split(';')[0].trim();
    if (contentType !== 'application/octet-stream') {
      return contentType;
    }
  }

  // Detect from filename extension
  const ext = filename.split('.').pop()?.toLowerCase();
  if (ext) {
    const mimeMap = {
      'mkv': 'video/x-matroska',
      'mp4': 'video/mp4',
      'avi': 'video/x-msvideo',
      'mov': 'video/quicktime',
      'webm': 'video/webm',
      'flv': 'video/x-flv',
      '3gp': 'video/3gpp',
      'wmv': 'video/x-ms-wmv',
      'jpg': 'image/jpeg',
      'jpeg': 'image/jpeg',
      'png': 'image/png',
      'gif': 'image/gif',
      'webp': 'image/webp',
      'mp3': 'audio/mpeg',
      'wav': 'audio/wav',
      'flac': 'audio/flac',
      'aac': 'audio/aac',
      'm4a': 'audio/mp4',
      'pdf': 'application/pdf',
      'zip': 'application/zip',
      'rar': 'application/vnd.rar',
      '7z': 'application/x-7z-compressed'
    };
    
    if (mimeMap[ext]) {
      return mimeMap[ext];
    }
  }

  // Detect from URL patterns
  if (url.includes('telegram.org')) {
    if (filename.includes('.')) {
      const urlExt = filename.split('.').pop().toLowerCase();
      if (urlExt === 'mkv') return 'video/x-matroska';
      if (urlExt === 'mp4') return 'video/mp4';
    }
  }

  return 'application/octet-stream';
}

// Ensure proper file extension
function ensureProperExtension(filename, contentType) {
  if (filename.includes('.')) {
    return filename; // Already has extension
  }

  // Add extension based on content type
  const extensionMap = {
    'video/x-matroska': '.mkv',
    'video/mp4': '.mp4',
    'video/x-msvideo': '.avi',
    'video/quicktime': '.mov',
    'video/webm': '.webm',
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'audio/mpeg': '.mp3',
    'audio/wav': '.wav',
    'application/pdf': '.pdf',
    'application/zip': '.zip'
  };

  const extension = extensionMap[contentType] || '.bin';
  return filename + extension;
}

// Upload imported chunk
async function uploadImportedChunk(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  try {
    const chunkFile = new File([chunk], `${originalFilename}.imported.chunk${chunkIndex}`, { 
      type: 'application/octet-stream' 
    });

    // Upload to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', channelId);
    telegramForm.append('document', chunkFile);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
      method: 'POST',
      body: telegramForm,
      signal: AbortSignal.timeout(60000) // 60 second timeout
    });

    if (!telegramResponse.ok) {
      const errorText = await telegramResponse.text();
      throw new Error(`Telegram upload failed: ${telegramResponse.status} - ${errorText}`);
    }

    const telegramData = await telegramResponse.json();
    
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error(`Telegram API error: ${telegramData.description || 'Unknown error'}`);
    }

    const telegramFileId = telegramData.result.document.file_id;

    // Get file URL
    const getFileResponse = await fetch(
      `https://api.telegram.org/bot${botToken}/getFile?file_id=${telegramFileId}`,
      { signal: AbortSignal.timeout(30000) }
    );
    
    if (!getFileResponse.ok) {
      throw new Error(`GetFile failed: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();
    
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('No file_path in GetFile response');
    }

    const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

    // Store in KV
    const keyName = `${fileId}_imported_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
    const chunkMetadata = {
      telegramFileId: telegramFileId,
      directUrl: directUrl,
      size: chunk.size,
      chunkIndex: chunkIndex,
      uploadedAt: Date.now(),
      neverExpires: true,
      importedFromUrl: true
    };

    await kvNamespace.kv.put(keyName, JSON.stringify(chunkMetadata));

    return {
      telegramFileId: telegramFileId,
      size: chunk.size,
      directUrl: directUrl,
      kvNamespace: kvNamespace.name,
      keyName: keyName
    };

  } catch (error) {
    throw new Error(`Imported chunk ${chunkIndex} failed: ${error.message}`);
  }
}

// Generate MSM ID
function generateMSMId() {
  const timestamp = Date.now();
  const r1 = Math.floor(Math.random() * 1000).toString().padStart(3, '0');
  const r2 = Math.floor(Math.random() * 100).toString().padStart(2, '0');
  const r3 = Math.floor(Math.random() * 100).toString().padStart(2, '0');
  const r4 = Math.floor(Math.random() * 100).toString().padStart(2, '0');
  const c1 = String.fromCharCode(65 + Math.floor(Math.random() * 26));
  const c2 = String.fromCharCode(65 + Math.floor(Math.random() * 26));
  
  return `MSM${r1}-${r2}${c1}${r3}${c2}${r4}-${timestamp.toString(36).slice(-2)}`;
}
