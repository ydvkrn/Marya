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

    // Handle Telegram URLs specially
    if (url.includes('t.me/') || url.includes('telegram.org/')) {
      return await handleTelegramURL(url, env, request, corsHeaders);
    }

    // Regular URL handling
    return await handleRegularURL(url, env, request, corsHeaders);

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

// Handle Telegram URLs (t.me links)
async function handleTelegramURL(url, env, request, corsHeaders) {
  console.log('📱 Processing Telegram URL...');
  
  // Extract file info from Telegram URL
  const telegramMatch = url.match(/t.me/c/(-?d+)/(d+)/);
  if (!telegramMatch) {
    throw new Error('Invalid Telegram URL format. Use direct file URLs instead of t.me links.');
  }

  const [, chatId, messageId] = telegramMatch;
  console.log(`📱 Telegram: Chat ${chatId}, Message ${messageId}`);

  // Note: t.me URLs can't be directly downloaded
  // We need the user to provide the direct file URL instead
  return new Response(JSON.stringify({
    success: false,
    error: 'Telegram t.me URLs cannot be directly imported. Please:
1. Right-click on the file in Telegram
2. Copy the direct download link (not t.me link)
3. Use that URL instead.

Or upload the file directly to get a permanent URL.',
    suggestion: 'Upload the file directly for better performance and permanent storage.'
  }), {
    status: 400,
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// Handle regular URLs with enhanced error handling
async function handleRegularURL(url, env, request, corsHeaders) {
  // Enhanced URL fetching with multiple user agents
  const userAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
  ];

  let response;
  let lastError;

  for (const userAgent of userAgents) {
    try {
      console.log(`🔄 Trying with user agent: ${userAgent.slice(0, 50)}...`);
      
      response = await fetch(url, {
        headers: {
          'User-Agent': userAgent,
          'Accept': '*/*',
          'Accept-Encoding': 'gzip, deflate, br',
          'Accept-Language': 'en-US,en;q=0.9',
          'Connection': 'keep-alive',
          'Upgrade-Insecure-Requests': '1'
        },
        signal: AbortSignal.timeout(30000)
      });

      if (response.ok) {
        console.log('✅ Successfully fetched with user agent');
        break;
      }
      
      lastError = `HTTP ${response.status}`;
      console.log(`❌ Failed with status: ${response.status}`);
      
    } catch (fetchError) {
      lastError = fetchError.message;
      console.log(`❌ Fetch error: ${fetchError.message}`);
      continue;
    }
  }

  if (!response || !response.ok) {
    throw new Error(`Failed to download from URL: ${lastError}`);
  }

  // Enhanced filename and content type detection
  let filename = extractFilename(url, response);
  let contentType = detectContentType(response, filename, url);
  filename = ensureProperExtension(filename, contentType);

  console.log(`📦 File imported: ${filename} (Type: ${contentType})`);

  const fileBuffer = await response.arrayBuffer();
  const file = new File([fileBuffer], filename, { type: contentType });

  const fileSizeMB = Math.round(file.size / 1024 / 1024);
  console.log(`📊 File size: ${fileSizeMB}MB`);

  if (file.size > 2 * 1024 * 1024 * 1024) {
    throw new Error(`File too large: ${fileSizeMB}MB (max 2GB)`);
  }

  // Generate MSM ID and process upload
  const msmId = generateMSMId();
  const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

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

  // Smart chunking
  let chunkSize;
  if (file.size <= 50 * 1024 * 1024) {
    chunkSize = 10 * 1024 * 1024; // 10MB
  } else if (file.size <= 200 * 1024 * 1024) {
    chunkSize = 20 * 1024 * 1024; // 20MB
  } else {
    chunkSize = 30 * 1024 * 1024; // 30MB for large files
  }

  const totalChunks = Math.ceil(file.size / chunkSize);
  const chunkResults = [];

  console.log(`🚀 Starting chunked import: ${totalChunks} chunks × ${Math.round(chunkSize/1024/1024)}MB`);

  for (let i = 0; i < totalChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, file.size);
    const chunk = file.slice(start, end);

    const kvIndex = Math.floor(i / 40);
    const targetKV = kvNamespaces[kvIndex];
    const botToken = botTokens[i % botTokens.length];

    console.log(`⬆️ Importing chunk ${i + 1}/${totalChunks} (${Math.round(chunk.size/1024/1024)}MB)...`);

    try {
      const chunkResult = await uploadImportedChunkWithRetry(
        chunk, msmId, i, kvIndex, i % 40,
        botToken, CHANNEL_ID, targetKV, filename
      );

      chunkResults.push(chunkResult);
      
      // Small delay to avoid rate limits
      if (i < totalChunks - 1) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
      
    } catch (chunkError) {
      console.error(`Chunk ${i + 1} failed:`, chunkError);
      throw new Error(`Upload failed at chunk ${i + 1}: ${chunkError.message}`);
    }
  }

  // Store final metadata
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
    strategy: 'url_imported_streaming',
    neverExpires: true,
    streamable: true,
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
    stream: `${baseUrl}/btfstorage/file/${msmId}${extension}?stream=1`,
    id: msmId,
    strategy: 'url_imported',
    originalUrl: url,
    chunks: totalChunks,
    chunkSize: `${Math.round(chunkSize/1024/1024)}MB`,
    streamable: true,
    lifetime: 'Permanent (Never Expires)'
  }), {
    headers: { 'Content-Type': 'application/json', ...corsHeaders }
  });
}

// Upload chunk with retry for URL imports
async function uploadImportedChunkWithRetry(chunk, fileId, chunkIndex, kvIndex, keyIndex, botToken, channelId, kvNamespace, originalFilename) {
  const maxRetries = 2;
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      if (attempt > 0) {
        const delay = 2000 + (Math.random() * 3000);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
      
      const chunkFile = new File([chunk], `${originalFilename}.imported.chunk${chunkIndex}`, { 
        type: 'application/octet-stream' 
      });

      const telegramForm = new FormData();
      telegramForm.append('chat_id', channelId);
      telegramForm.append('document', chunkFile);

      const telegramResponse = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
        method: 'POST',
        body: telegramForm,
        signal: AbortSignal.timeout(90000)
      });

      if (!telegramResponse.ok) {
        if (telegramResponse.status === 429) {
          const retryAfter = telegramResponse.headers.get('Retry-After');
          const delay = retryAfter ? parseInt(retryAfter) * 1000 : 30000;
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        
        const errorText = await telegramResponse.text();
        throw new Error(`Telegram upload failed: ${telegramResponse.status} - ${errorText}`);
      }

      const telegramData = await telegramResponse.json();
      
      if (!telegramData.ok || !telegramData.result?.document?.file_id) {
        throw new Error(`Telegram API error: ${telegramData.description || 'Unknown error'}`);
      }

      const telegramFileId = telegramData.result.document.file_id;

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

      const keyName = `${fileId}_imported_chunk_${chunkIndex}_kv${kvIndex}_key${keyIndex}`;
      const chunkMetadata = {
        telegramFileId: telegramFileId,
        directUrl: directUrl,
        size: chunk.size,
        chunkIndex: chunkIndex,
        uploadedAt: Date.now(),
        neverExpires: true,
        importedFromUrl: true,
        streamable: true
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
      if (attempt === maxRetries - 1) {
        throw new Error(`Imported chunk ${chunkIndex} failed: ${error.message}`);
      }
    }
  }
}

// Helper functions (same as before)
function extractFilename(url, response) {
  const contentDisposition = response.headers.get('Content-Disposition');
  if (contentDisposition) {
    const filenameMatch = contentDisposition.match(/filename[^;=
]*=((['"]).*?\u0002|[^;
]*)/);
    if (filenameMatch) {
      let filename = filenameMatch[1].replace(/['"]/g, '');
      if (filename && filename !== 'undefined') {
        return decodeURIComponent(filename);
      }
    }
  }

  let filename = url.split('/').pop().split('?')[0];
  
  try {
    filename = decodeURIComponent(filename);
  } catch (e) {
    // Keep original if decode fails
  }

  if (!filename || filename.length < 1) {
    filename = 'imported_file';
  }

  return filename;
}

function detectContentType(response, filename, url) {
  let contentType = response.headers.get('Content-Type');
  if (contentType) {
    contentType = contentType.split(';')[0].trim();
    if (contentType !== 'application/octet-stream') {
      return contentType;
    }
  }

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

  return 'application/octet-stream';
}

function ensureProperExtension(filename, contentType) {
  if (filename.includes('.')) {
    return filename;
  }

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