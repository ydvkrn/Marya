// functions/upload.js
// ðŸš€ MARYA VAULT - Advanced Chunked Upload with FIXED URL Response

export async function onRequest(context) {
  const { request, env } = context;

  console.log('ðŸš€ CHUNKED UPLOAD HANDLER STARTED');

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ 
      success: false, 
      error: 'Only POST allowed' 
    }), {
      status: 405,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  try {
    // Get file from form data
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || file.size === 0) {
      throw new Error('No file provided');
    }

    console.log(`ðŸ“ File: ${file.name} (${formatBytes(file.size)})`);

    // Validate file size (2GB max)
    if (file.size > 2147483648) {
      throw new Error('File too large (max 2GB)');
    }

    // Generate unique file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const fileId = `${timestamp}_${random}`;

    // Sanitize filename for URL
    const ext = file.name.includes('.') ? file.name.substring(file.name.lastIndexOf('.')) : '';
    const baseName = file.name.substring(0, file.name.lastIndexOf('.') || file.name.length);
    const sanitized = baseName.toLowerCase().replace(/[^a-z0-9-]/g, '_').substring(0, 50);
    const finalFilename = `${sanitized}_${fileId}${ext}`;

    console.log(`ðŸ†” File ID: ${fileId}`);
    console.log(`ðŸ“ Filename: ${finalFilename}`);

    // Check environment
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHAT_ID = env.CHAT_ID;

    if (!BOT_TOKEN || !CHAT_ID) {
      throw new Error('Telegram credentials not configured');
    }

    // Determine upload strategy: single or chunked
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB chunks
    const needsChunking = file.size > 500 * 1024 * 1024; // 500MB threshold

    let uploadResult;

    if (needsChunking) {
      console.log('ðŸ§© Using CHUNKED upload strategy');
      uploadResult = await handleChunkedUpload(file, fileId, finalFilename, env, BOT_TOKEN, CHAT_ID, CHUNK_SIZE);
    } else {
      console.log('ðŸ“¤ Using SINGLE upload strategy');
      uploadResult = await handleSingleUpload(file, fileId, finalFilename, env, BOT_TOKEN, CHAT_ID);
    }

    console.log('âœ… Upload completed successfully');

    // âœ… CRITICAL FIX - Return proper response with filename
    return new Response(JSON.stringify({
      success: true,
      filename: finalFilename,  // âœ… Used by frontend to build URL
      id: fileId,
      originalName: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      uploadType: needsChunking ? 'chunked' : 'single',
      chunks: uploadResult.chunks || 0,
      uploadedAt: new Date().toISOString()
    }), {
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (error) {
    console.error('âŒ Upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}

// Handle single file upload (< 500MB)
async function handleSingleUpload(file, fileId, filename, env, botToken, chatId) {
  console.log('ðŸ“¤ Uploading single file to Telegram...');

  const formData = new FormData();
  formData.append('chat_id', chatId);
  formData.append('document', file);
  formData.append('caption', `ðŸ“ ${file.name}\nID: ${fileId}\nSize: ${formatBytes(file.size)}`);

  const response = await fetch(
    `https://api.telegram.org/bot${botToken}/sendDocument`,
    {
      method: 'POST',
      body: formData,
    }
  );

  const data = await response.json();

  if (!data.ok) {
    throw new Error(`Telegram error: ${data.description || 'Unknown'}`);
  }

  const telegramFileId = data.result.document.file_id;

  // Store metadata in KV
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

  console.log('âœ… Single upload completed');

  return { chunks: 0 };
}

// Handle chunked upload (>= 500MB)
async function handleChunkedUpload(file, fileId, filename, env, botToken, chatId, chunkSize) {
  console.log('ðŸ§© Starting chunked upload...');

  const totalChunks = Math.ceil(file.size / chunkSize);
  console.log(`ðŸ“Š Total chunks: ${totalChunks}`);

  const chunkMetadata = [];

  // Upload each chunk
  for (let i = 0; i < totalChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, file.size);
    const chunk = file.slice(start, end);

    console.log(`ðŸ“¤ Uploading chunk ${i + 1}/${totalChunks} (${formatBytes(chunk.size)})`);

    // Create chunk file
    const chunkFilename = `${filename}.chunk${String(i).padStart(3, '0')}`;
    const chunkFile = new File([chunk], chunkFilename, { 
      type: 'application/octet-stream' 
    });

    // Upload chunk to Telegram
    const formData = new FormData();
    formData.append('chat_id', chatId);
    formData.append('document', chunkFile);
    formData.append('caption', `ðŸ§© Chunk ${i + 1}/${totalChunks}\nParent: ${filename}\nID: ${fileId}`);

    const response = await fetch(
      `https://api.telegram.org/bot${botToken}/sendDocument`,
      {
        method: 'POST',
        body: formData,
      }
    );

    const data = await response.json();

    if (!data.ok) {
      throw new Error(`Chunk ${i} upload failed: ${data.description}`);
    }

    const chunkFileId = data.result.document.file_id;

    // Store chunk metadata
    chunkMetadata.push({
      index: i,
      size: chunk.size,
      telegramFileId: chunkFileId,
      filename: chunkFilename
    });

    // Store chunk info in separate KV entry
    const chunkKey = `${filename}_chunk_${String(i).padStart(3, '0')}`;
    await env.FILES_KV.put(chunkKey, JSON.stringify({
      parentFile: filename,
      index: i,
      size: chunk.size,
      telegramFileId: chunkFileId,
      uploadedAt: Date.now()
    }));

    console.log(`âœ… Chunk ${i + 1}/${totalChunks} uploaded`);

    // Small delay to avoid rate limits
    await new Promise(resolve => setTimeout(resolve, 500));
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
    chunks: chunkMetadata,
    uploadedAt: Date.now()
  };

  await env.FILES_KV.put(filename, JSON.stringify(masterMetadata));

  console.log('âœ… All chunks uploaded and metadata stored');

  return { chunks: totalChunks };
}

// Utility function
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}