
// functions/upload-from-url.js
// Simplified but robust URL handler for complex Workers URLs

export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== ROBUST URL UPLOAD START ===');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed'
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
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

    if (!BOT_TOKEN || !CHANNEL_ID || kvNamespaces.length === 0) {
      throw new Error('Missing configuration');
    }

    const { url } = await request.json();

    if (!url || !url.trim()) {
      throw new Error('No URL provided');
    }

    const cleanUrl = url.trim();
    console.log('Processing URL:', cleanUrl.substring(0, 50) + '...');

    // Basic URL validation
    if (!cleanUrl.startsWith('http://') && !cleanUrl.startsWith('https://')) {
      throw new Error('URL must start with http:// or https://');
    }

    // Step 1: Try to get file info first (quick check)
    let fileSize = 0;
    let contentType = 'application/octet-stream';

    try {
      console.log('Getting file info...');
      const headResp = await fetch(cleanUrl, { 
        method: 'HEAD',
        signal: AbortSignal.timeout(15000) // 15 seconds
      });

      if (headResp.ok) {
        fileSize = parseInt(headResp.headers.get('Content-Length') || '0');
        contentType = headResp.headers.get('Content-Type') || contentType;
        console.log(`File info: ${Math.round(fileSize/1024/1024)}MB, type: ${contentType}`);

        if (fileSize > 2000 * 1024 * 1024) {
          throw new Error(`File too large: ${Math.round(fileSize/1024/1024)}MB (max 2000MB)`);
        }
      }
    } catch (headError) {
      console.log('Head request failed, continuing with download:', headError.message);
    }

    // Step 2: Download the file
    console.log('Starting file download...');

    const downloadResp = await fetch(cleanUrl, {
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; FileUploader/1.0)',
        'Accept': '*/*'
      },
      // No timeout here - let it run as long as needed
    });

    if (!downloadResp.ok) {
      throw new Error(`Download failed: ${downloadResp.status} ${downloadResp.statusText}`);
    }

    // Step 3: Get filename
    let filename = 'download.file';

    // Try to extract filename from URL path
    try {
      const urlPath = new URL(cleanUrl).pathname;
      const pathParts = urlPath.split('/');
      const lastPart = pathParts[pathParts.length - 1];

      if (lastPart && lastPart.includes('.')) {
        filename = decodeURIComponent(lastPart);
      }
    } catch (e) {
      console.log('Could not extract filename from URL, using default');
    }

    // Try Content-Disposition header
    const disposition = downloadResp.headers.get('Content-Disposition');
    if (disposition && disposition.includes('filename')) {
      const filenameMatch = disposition.match(/filename[*]?=([^;]+)/);
      if (filenameMatch) {
        filename = filenameMatch[1].replace(/['"]/g, '').trim();
        filename = decodeURIComponent(filename);
      }
    }

    // Clean filename
    filename = filename.replace(/[<>:"/\|?*]/g, '_');
    console.log('Using filename:', filename);

    // Step 4: Read file content
    console.log('Reading file content...');
    const arrayBuffer = await downloadResp.arrayBuffer();

    console.log(`Downloaded: ${Math.round(arrayBuffer.byteLength/1024/1024)}MB`);

    // Size check
    if (arrayBuffer.byteLength > 2000 * 1024 * 1024) {
      throw new Error(`File too large: ${Math.round(arrayBuffer.byteLength/1024/1024)}MB (max 2000MB)`);
    }

    // Step 5: Create file and process chunks
    const file = new File([arrayBuffer], filename, { type: contentType });

    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = filename.includes('.') ? filename.slice(filename.lastIndexOf('.')) : '';

    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    if (totalChunks > 100) {
      throw new Error(`File requires ${totalChunks} chunks, maximum 100 supported`);
    }

    console.log(`Creating ${totalChunks} chunks...`);

    // Step 6: Upload chunks
    const chunkPromises = [];
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const chunkFile = new File([chunk], `${filename}.part${i}`, { type: contentType });
      const targetKV = kvNamespaces[i % kvNamespaces.length];

      chunkPromises.push(uploadChunk(chunkFile, fileId, i, BOT_TOKEN, CHANNEL_ID, targetKV));
    }

    console.log('Uploading all chunks...');
    const chunkResults = await Promise.all(chunkPromises);
    console.log('All chunks uploaded successfully');

    // Step 7: Store metadata
    const masterMetadata = {
      filename: filename,
      size: file.size,
      contentType: contentType,
      extension: extension,
      uploadedAt: Date.now(),
      type: 'url_upload_chunked',
      totalChunks: totalChunks,
      chunkSize: CHUNK_SIZE,
      sourceUrl: cleanUrl.length > 100 ? cleanUrl.substring(0, 100) + '...' : cleanUrl,
      chunks: chunkResults.map((result, index) => ({
        index: index,
        kvNamespace: result.kvNamespace,
        keyName: result.chunkKey,
        telegramFileId: result.telegramFileId,
        size: result.size
      }))
    };

    await kvNamespaces[0].kv.put(fileId, JSON.stringify(masterMetadata));

    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;

    const result = {
      success: true,
      filename: filename,
      size: file.size,
      contentType: contentType,
      url: customUrl,
      download: downloadUrl,
      id: fileId,
      strategy: 'url_upload_chunked',
      chunks: totalChunks,
      sourceUrl: cleanUrl.length > 50 ? cleanUrl.substring(0, 50) + '...' : cleanUrl
    };

    console.log('URL upload completed successfully:', {
      filename: result.filename,
      size: `${Math.round(result.size/1024/1024)}MB`,
      chunks: result.chunks
    });

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('URL upload error:', error);

    let errorMsg = error.message;
    if (errorMsg.includes('AbortError') || errorMsg.includes('timeout')) {
      errorMsg = 'Download timeout - file may be too large or server too slow';
    }

    return new Response(JSON.stringify({
      success: false,
      error: errorMsg
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}

// Simple chunk upload function
async function uploadChunk(chunkFile, fileId, chunkIndex, botToken, channelId, kvNamespace) {
  console.log(`Uploading chunk ${chunkIndex}...`);

  const formData = new FormData();
  formData.append('chat_id', channelId);
  formData.append('document', chunkFile);

  const telegramResp = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, {
    method: 'POST',
    body: formData
  });

  if (!telegramResp.ok) {
    throw new Error(`Telegram upload failed for chunk ${chunkIndex}: ${telegramResp.status}`);
  }

  const telegramData = await telegramResp.json();
  if (!telegramData.ok || !telegramData.result?.document?.file_id) {
    throw new Error(`Invalid Telegram response for chunk ${chunkIndex}`);
  }

  const fileId_telegram = telegramData.result.document.file_id;

  const getFileResp = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${encodeURIComponent(fileId_telegram)}`);
  if (!getFileResp.ok) {
    throw new Error(`GetFile failed for chunk ${chunkIndex}: ${getFileResp.status}`);
  }

  const getFileData = await getFileResp.json();
  if (!getFileData.ok || !getFileData.result?.file_path) {
    throw new Error(`No file_path for chunk ${chunkIndex}`);
  }

  const directUrl = `https://api.telegram.org/file/bot${botToken}/${getFileData.result.file_path}`;

  const chunkKey = `${fileId}_chunk_${chunkIndex}`;
  const chunkMetadata = {
    telegramFileId: fileId_telegram,
    directUrl: directUrl,
    size: chunkFile.size,
    index: chunkIndex,
    parentFileId: fileId,
    kvNamespace: kvNamespace.name,
    uploadedAt: Date.now(),
    lastRefreshed: Date.now()
  };

  await kvNamespace.kv.put(chunkKey, JSON.stringify(chunkMetadata));

  return {
    telegramFileId: fileId_telegram,
    size: chunkFile.size,
    directUrl: directUrl,
    kvNamespace: kvNamespace.name,
    chunkKey: chunkKey
  };
}