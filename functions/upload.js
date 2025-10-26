// functions/upload.js
// ðŸš€ ADVANCED FILE UPLOAD with Chunking & Retry Logic

export async function onRequest(context) {
  const { request, env } = context;

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || file.size === 0) {
      throw new Error('No file provided');
    }

    console.log(`ðŸ“ Uploading: ${file.name} (${formatBytes(file.size)})`);

    // Generate unique filename
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const ext = file.name.includes('.') ? file.name.substring(file.name.lastIndexOf('.')) : '';
    const baseName = file.name.substring(0, file.name.lastIndexOf('.') || file.name.length);
    const sanitized = baseName.toLowerCase().replace(/[^a-z0-9-]/g, '_').substring(0, 40);
    const finalFilename = `${sanitized}_${timestamp}${random}${ext}`;

    // Chunking strategy for large files
    const CHUNK_THRESHOLD = 100 * 1024 * 1024; // 100MB
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB chunks

    let result;

    if (file.size > CHUNK_THRESHOLD) {
      console.log(`ðŸ§© Using chunked upload (${Math.ceil(file.size / CHUNK_SIZE)} chunks)`);
      result = await uploadChunked(file, finalFilename, env, CHUNK_SIZE);
    } else {
      console.log('ðŸ“¤ Using single upload');
      result = await uploadSingle(file, finalFilename, env);
    }

    const baseUrl = new URL(request.url).origin;

    return new Response(JSON.stringify({
      success: true,
      filename: finalFilename,
      id: timestamp + random,
      originalName: file.name,
      size: file.size,
      contentType: file.type || 'application/octet-stream',
      url: `${baseUrl}/btfstorage/file/${finalFilename}`,
      download: `${baseUrl}/btfstorage/file/${finalFilename}?dl=1`,
      uploadType: result.type,
      chunks: result.chunks || 0,
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

// Single file upload with retry
async function uploadSingle(file, filename, env) {
  const maxRetries = 3;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const formData = new FormData();
      formData.append('chat_id', env.CHAT_ID);
      formData.append('document', file);
      formData.append('caption', `ðŸ“ ${file.name}\n${formatBytes(file.size)}`);

      const response = await fetch(
        `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
        { method: 'POST', body: formData }
      );

      const data = await response.json();

      if (!data.ok) {
        throw new Error(`Telegram: ${data.description || 'Upload failed'}`);
      }

      // Store metadata
      await env.FILES_KV.put(filename, JSON.stringify({
        filename: filename,
        originalName: file.name,
        size: file.size,
        contentType: file.type || 'application/octet-stream',
        telegramFileId: data.result.document.file_id,
        uploadType: 'single',
        uploadedAt: Date.now()
      }));

      console.log(`âœ… Single upload complete`);
      return { type: 'single', chunks: 0 };

    } catch (error) {
      console.error(`âŒ Attempt ${attempt} failed:`, error.message);
      if (attempt === maxRetries) throw error;
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

// Advanced chunked upload
async function uploadChunked(file, filename, env, chunkSize) {
  const totalChunks = Math.ceil(file.size / chunkSize);
  console.log(`ðŸ§© Uploading ${totalChunks} chunks...`);

  const chunks = [];

  // Upload each chunk with retry
  for (let i = 0; i < totalChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, file.size);
    const chunkBlob = file.slice(start, end);

    console.log(`ðŸ“¤ Chunk ${i + 1}/${totalChunks} (${formatBytes(chunkBlob.size)})`);

    const chunkFilename = `${filename}.chunk${String(i).padStart(4, '0')}`;
    const chunkFile = new File([chunkBlob], chunkFilename, { 
      type: 'application/octet-stream' 
    });

    // Retry logic for each chunk
    let uploaded = false;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const formData = new FormData();
        formData.append('chat_id', env.CHAT_ID);
        formData.append('document', chunkFile);
        formData.append('caption', `ðŸ§© Chunk ${i + 1}/${totalChunks}`);

        const response = await fetch(
          `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
          { method: 'POST', body: formData }
        );

        const data = await response.json();

        if (!data.ok) {
          throw new Error(`Telegram: ${data.description}`);
        }

        // Store chunk metadata
        const chunkKey = `${filename}_chunk_${String(i).padStart(4, '0')}`;
        await env.FILES_KV.put(chunkKey, JSON.stringify({
          parentFile: filename,
          index: i,
          size: chunkBlob.size,
          telegramFileId: data.result.document.file_id,
          uploadedAt: Date.now()
        }));

        chunks.push({
          index: i,
          size: chunkBlob.size,
          telegramFileId: data.result.document.file_id
        });

        console.log(`âœ… Chunk ${i + 1} uploaded`);
        uploaded = true;
        break;

      } catch (error) {
        console.error(`âŒ Chunk ${i + 1} attempt ${attempt} failed:`, error.message);
        if (attempt === 3) throw new Error(`Chunk ${i + 1} failed after 3 attempts`);
        await new Promise(r => setTimeout(r, 1000 * attempt));
      }
    }

    if (!uploaded) {
      throw new Error(`Failed to upload chunk ${i + 1}`);
    }

    // Delay between chunks
    if (i < totalChunks - 1) {
      await new Promise(r => setTimeout(r, 300));
    }
  }

  // Store master metadata
  await env.FILES_KV.put(filename, JSON.stringify({
    filename: filename,
    originalName: file.name,
    size: file.size,
    contentType: file.type || 'application/octet-stream',
    uploadType: 'chunked',
    totalChunks: totalChunks,
    chunkSize: chunkSize,
    chunks: chunks,
    uploadedAt: Date.now()
  }));

  console.log(`âœ… All ${totalChunks} chunks uploaded`);
  return { type: 'chunked', chunks: totalChunks };
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}