// functions/upload-from-url.js
// ðŸŒ ADVANCED URL UPLOAD with Streaming & Chunking

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
    const { url } = await request.json();

    if (!url || !url.trim()) {
      throw new Error('No URL provided');
    }

    console.log(`ðŸŒ Downloading from: ${url}`);

    // Validate URL
    const urlObj = new URL(url);
    if (!['http:', 'https:'].includes(urlObj.protocol)) {
      throw new Error('Only HTTP/HTTPS URLs supported');
    }

    // Download with retry
    let downloadResponse;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        downloadResponse = await fetch(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
          }
        });

        if (downloadResponse.ok) break;
        throw new Error(`HTTP ${downloadResponse.status}`);

      } catch (error) {
        console.error(`âŒ Download attempt ${attempt} failed:`, error.message);
        if (attempt === 3) throw new Error(`Download failed: ${error.message}`);
        await new Promise(r => setTimeout(r, 2000 * attempt));
      }
    }

    // Extract filename
    let filename = 'download';
    const contentDisposition = downloadResponse.headers.get('Content-Disposition');

    if (contentDisposition) {
      const match = contentDisposition.match(/filename[*]?=([^;\n\r"']+)/);
      if (match) {
        filename = match[1].replace(/['"]/g, '').trim();
      }
    }

    if (filename === 'download') {
      const urlFilename = urlObj.pathname.split('/').pop();
      if (urlFilename && urlFilename.length > 0) {
        filename = urlFilename;
      }
    }

    const contentType = downloadResponse.headers.get('Content-Type') || 'application/octet-stream';

    // Download content
    const arrayBuffer = await downloadResponse.arrayBuffer();
    const fileSize = arrayBuffer.byteLength;

    console.log(`âœ… Downloaded: ${formatBytes(fileSize)}`);

    if (fileSize === 0) {
      throw new Error('Downloaded file is empty');
    }

    if (fileSize > 2147483648) {
      throw new Error(`File too large: ${formatBytes(fileSize)} (max 2GB)`);
    }

    // Generate unique filename
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const ext = filename.includes('.') ? filename.substring(filename.lastIndexOf('.')) : '';
    const baseName = filename.substring(0, filename.lastIndexOf('.') || filename.length);
    const sanitized = baseName.toLowerCase().replace(/[^a-z0-9-]/g, '_').substring(0, 40);
    const finalFilename = `${sanitized}_${timestamp}${random}${ext}`;

    // Create file object
    const file = new File([arrayBuffer], filename, { type: contentType });

    // Chunking strategy
    const CHUNK_THRESHOLD = 100 * 1024 * 1024; // 100MB
    const CHUNK_SIZE = 20 * 1024 * 1024; // 20MB

    let result;

    if (fileSize > CHUNK_THRESHOLD) {
      console.log(`ðŸ§© Using chunked upload`);
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
      originalName: filename,
      size: fileSize,
      contentType: contentType,
      url: `${baseUrl}/btfstorage/file/${finalFilename}`,
      download: `${baseUrl}/btfstorage/file/${finalFilename}?dl=1`,
      uploadType: result.type,
      chunks: result.chunks || 0,
      sourceUrl: url,
      uploadedAt: new Date().toISOString()
    }), {
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (error) {
    console.error('âŒ URL upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}

// Single upload
async function uploadSingle(file, filename, env) {
  const maxRetries = 3;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const formData = new FormData();
      formData.append('chat_id', env.CHAT_ID);
      formData.append('document', file);
      formData.append('caption', `ðŸŒ ${file.name}\n${formatBytes(file.size)}`);

      const response = await fetch(
        `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
        { method: 'POST', body: formData }
      );

      const data = await response.json();

      if (!data.ok) {
        throw new Error(`Telegram: ${data.description || 'Upload failed'}`);
      }

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

// Chunked upload
async function uploadChunked(file, filename, env, chunkSize) {
  const totalChunks = Math.ceil(file.size / chunkSize);
  console.log(`ðŸ§© Uploading ${totalChunks} chunks...`);

  const chunks = [];

  for (let i = 0; i < totalChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, file.size);
    const chunkBlob = file.slice(start, end);

    console.log(`ðŸ“¤ Chunk ${i + 1}/${totalChunks}`);

    const chunkFilename = `${filename}.chunk${String(i).padStart(4, '0')}`;
    const chunkFile = new File([chunkBlob], chunkFilename, { 
      type: 'application/octet-stream' 
    });

    // Retry logic
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
        if (attempt === 3) throw new Error(`Chunk ${i + 1} failed`);
        await new Promise(r => setTimeout(r, 1000 * attempt));
      }
    }

    if (!uploaded) {
      throw new Error(`Failed to upload chunk ${i + 1}`);
    }

    if (i < totalChunks - 1) {
      await new Promise(r => setTimeout(r, 300));
    }
  }

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