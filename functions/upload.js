// functions/upload.js
// ðŸš€ ULTRA-ROBUST CHUNKED UPLOAD - HTTP 500 FIX

export async function onRequest(context) {
  const { request, env } = context;

  console.log('ðŸš€ UPLOAD STARTED:', new Date().toISOString());

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return jsonResponse({ success: false, error: 'Only POST allowed' }, 405, corsHeaders);
  }

  try {
    // Validate environment first
    if (!env.BOT_TOKEN || !env.CHAT_ID || !env.FILES_KV) {
      console.error('âŒ Missing env vars:', {
        BOT_TOKEN: !!env.BOT_TOKEN,
        CHAT_ID: !!env.CHAT_ID,
        FILES_KV: !!env.FILES_KV
      });
      throw new Error('Server configuration error - Missing credentials');
    }

    console.log('âœ… Environment validated');

    // Get file with timeout protection
    let formData;
    try {
      formData = await Promise.race([
        request.formData(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('Form data timeout')), 30000))
      ]);
    } catch (error) {
      console.error('âŒ FormData error:', error.message);
      throw new Error('Failed to parse upload data: ' + error.message);
    }

    const file = formData.get('file');

    if (!file || !file.name || file.size === 0) {
      console.error('âŒ Invalid file:', { 
        exists: !!file, 
        name: file?.name, 
        size: file?.size 
      });
      throw new Error('No valid file provided');
    }

    console.log('ðŸ“ File received:', file.name, formatBytes(file.size));

    // Validate file size
    if (file.size > 2147483648) {
      throw new Error(`File too large: ${formatBytes(file.size)} (max 2GB)`);
    }

    // Generate unique filename
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const fileId = `${timestamp}${random}`;

    const ext = file.name.includes('.') ? file.name.substring(file.name.lastIndexOf('.')) : '';
    const baseName = file.name.substring(0, file.name.lastIndexOf('.') || file.name.length);
    const sanitized = baseName.toLowerCase().replace(/[^a-z0-9-]/g, '_').substring(0, 40);
    const finalFilename = `${sanitized}_${fileId}${ext}`;

    console.log('ðŸ†” Generated filename:', finalFilename);

    // Determine upload strategy
    const CHUNK_THRESHOLD = 50 * 1024 * 1024; // 50MB (reduced for better reliability)
    const CHUNK_SIZE = 10 * 1024 * 1024; // 10MB chunks (smaller for stability)
    const needsChunking = file.size > CHUNK_THRESHOLD;

    console.log(`ðŸ“Š Upload strategy: ${needsChunking ? 'CHUNKED' : 'SINGLE'} (${formatBytes(file.size)})`);

    let result;

    if (needsChunking) {
      result = await uploadChunked(file, finalFilename, env, CHUNK_SIZE);
    } else {
      result = await uploadSingle(file, finalFilename, env);
    }

    console.log('âœ… Upload completed successfully');

    return jsonResponse({
      success: true,
      filename: finalFilename,
      id: fileId,
      originalName: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      uploadType: needsChunking ? 'chunked' : 'single',
      chunks: result.chunks || 0,
      uploadedAt: new Date().toISOString()
    }, 200, corsHeaders);

  } catch (error) {
    console.error('âŒ UPLOAD ERROR:', error.message);
    console.error('Stack:', error.stack);

    return jsonResponse({
      success: false,
      error: error.message || 'Upload failed',
      details: error.stack?.split('\n')[0]
    }, 500, corsHeaders);
  }
}

// Single file upload with retry
async function uploadSingle(file, filename, env) {
  console.log('ðŸ“¤ Starting SINGLE upload...');

  const maxRetries = 3;
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`ðŸ”„ Upload attempt ${attempt}/${maxRetries}`);

      // Create form data
      const formData = new FormData();
      formData.append('chat_id', env.CHAT_ID);
      formData.append('document', file);
      formData.append('caption', `ðŸ“ ${file.name}\nSize: ${formatBytes(file.size)}`);

      // Upload with timeout
      const uploadPromise = fetch(
        `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
        {
          method: 'POST',
          body: formData
        }
      );

      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Upload timeout (120s)')), 120000)
      );

      const response = await Promise.race([uploadPromise, timeoutPromise]);

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Telegram API error: ${response.status} - ${errorText.substring(0, 200)}`);
      }

      const data = await response.json();

      if (!data.ok || !data.result?.document?.file_id) {
        throw new Error(`Invalid Telegram response: ${JSON.stringify(data).substring(0, 200)}`);
      }

      const telegramFileId = data.result.document.file_id;

      // Store metadata
      const metadata = {
        filename: filename,
        originalName: file.name,
        size: file.size,
        contentType: file.type || 'application/octet-stream',
        uploadType: 'single',
        telegramFileId: telegramFileId,
        uploadedAt: Date.now()
      };

      await env.FILES_KV.put(filename, JSON.stringify(metadata));

      console.log('âœ… Single upload successful');
      return { chunks: 0 };

    } catch (error) {
      console.error(`âŒ Attempt ${attempt} failed:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        const delay = Math.min(1000 * Math.pow(2, attempt), 5000);
        console.log(`â³ Retrying in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  throw new Error(`Upload failed after ${maxRetries} attempts: ${lastError.message}`);
}

// Chunked upload with advanced retry
async function uploadChunked(file, filename, env, chunkSize) {
  console.log('ðŸ§© Starting CHUNKED upload...');

  const totalChunks = Math.ceil(file.size / chunkSize);
  console.log(`ðŸ“Š Total chunks: ${totalChunks}, size: ${formatBytes(chunkSize)}`);

  if (totalChunks > 200) {
    throw new Error(`Too many chunks: ${totalChunks} (max 200). Try smaller file.`);
  }

  const chunks = [];

  // Upload chunks with retry logic
  for (let i = 0; i < totalChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, file.size);
    const chunkBlob = file.slice(start, end);

    console.log(`ðŸ“¤ Uploading chunk ${i + 1}/${totalChunks} (${formatBytes(chunkBlob.size)})`);

    const chunkFilename = `${filename}.chunk${String(i).padStart(4, '0')}`;
    const chunkFile = new File([chunkBlob], chunkFilename, { type: 'application/octet-stream' });

    const chunkResult = await uploadChunkWithRetry(chunkFile, i, filename, env, 3);
    chunks.push(chunkResult);

    // Small delay to avoid rate limits
    if (i < totalChunks - 1) {
      await new Promise(resolve => setTimeout(resolve, 300));
    }
  }

  // Store master metadata
  const masterMetadata = {
    filename: filename,
    originalName: file.name,
    size: file.size,
    contentType: file.type || 'application/octet-stream',
    uploadType: 'chunked',
    totalChunks: totalChunks,
    chunkSize: chunkSize,
    chunks: chunks,
    uploadedAt: Date.now()
  };

  await env.FILES_KV.put(filename, JSON.stringify(masterMetadata));

  console.log('âœ… All chunks uploaded');
  return { chunks: totalChunks };
}

// Upload single chunk with retry
async function uploadChunkWithRetry(chunkFile, index, parentFilename, env, maxRetries) {
  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const formData = new FormData();
      formData.append('chat_id', env.CHAT_ID);
      formData.append('document', chunkFile);
      formData.append('caption', `ðŸ§© Chunk ${index + 1}`);

      const response = await fetch(
        `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
        {
          method: 'POST',
          body: formData
        }
      );

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();

      if (!data.ok || !data.result?.document?.file_id) {
        throw new Error('Invalid response');
      }

      const telegramFileId = data.result.document.file_id;

      // Store chunk metadata
      const chunkKey = `${parentFilename}_chunk_${String(index).padStart(4, '0')}`;
      const chunkMeta = {
        parentFile: parentFilename,
        index: index,
        size: chunkFile.size,
        telegramFileId: telegramFileId,
        uploadedAt: Date.now()
      };

      await env.FILES_KV.put(chunkKey, JSON.stringify(chunkMeta));

      console.log(`âœ… Chunk ${index + 1} uploaded`);

      return {
        index: index,
        size: chunkFile.size,
        telegramFileId: telegramFileId
      };

    } catch (error) {
      console.error(`âŒ Chunk ${index + 1} attempt ${attempt} failed:`, error.message);
      lastError = error;

      if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
      }
    }
  }

  throw new Error(`Chunk ${index + 1} failed after ${maxRetries} attempts: ${lastError.message}`);
}

// Utilities
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function jsonResponse(data, status, headers) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { ...headers, 'Content-Type': 'application/json' }
  });
}