// functions/upload-from-url.js
export async function onRequest(context) {
  const { request, env } = context;

  // CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': '*',
  };

  // Handle preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: corsHeaders
    });
  }

  // Only allow POST
  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed. Use POST.'
    }), {
      status: 405,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  try {
    const { url } = await request.json();

    if (!url || !url.trim()) {
      throw new Error('No URL provided');
    }

    console.log(`ðŸŒ Downloading: ${url}`);

    // Validate URL
    const urlObj = new URL(url);
    if (!['http:', 'https:'].includes(urlObj.protocol)) {
      throw new Error('Only HTTP/HTTPS URLs allowed');
    }

    // Download file
    const downloadResponse = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });

    if (!downloadResponse.ok) {
      throw new Error(`Download failed: ${downloadResponse.status}`);
    }

    // Get filename
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
        filename = decodeURIComponent(urlFilename);
      }
    }

    const contentType = downloadResponse.headers.get('Content-Type') || 'application/octet-stream';
    const arrayBuffer = await downloadResponse.arrayBuffer();
    const fileSize = arrayBuffer.byteLength;

    console.log(`âœ… Downloaded: ${formatBytes(fileSize)}`);

    if (fileSize === 0) throw new Error('File is empty');
    if (fileSize > 2147483648) throw new Error('File too large (max 2GB)');

    // Generate unique filename
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 8);
    const ext = filename.includes('.') ? filename.substring(filename.lastIndexOf('.')) : '';
    const baseName = filename.substring(0, filename.lastIndexOf('.') || filename.length);
    const sanitized = baseName.toLowerCase().replace(/[^a-z0-9-]/g, '_').substring(0, 40);
    const finalFilename = `${sanitized}_${timestamp}${random}${ext}`;

    // Create file
    const file = new File([arrayBuffer], filename, { type: contentType });

    // Chunking strategy
    const CHUNK_THRESHOLD = 50 * 1024 * 1024; // 50MB
    const CHUNK_SIZE = 10 * 1024 * 1024; // 10MB

    let uploadType = 'single';
    let chunks = 0;

    if (fileSize > CHUNK_THRESHOLD) {
      console.log('ðŸ§© Using chunked upload');
      chunks = await uploadChunked(file, finalFilename, env, CHUNK_SIZE);
      uploadType = 'chunked';
    } else {
      console.log('ðŸ“¤ Using single upload');
      await uploadSingle(file, finalFilename, env);
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
      uploadType: uploadType,
      chunks: chunks,
      sourceUrl: url,
      uploadedAt: new Date().toISOString()
    }), {
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (error) {
    console.error('âŒ Error:', error.message);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
}

async function uploadSingle(file, filename, env) {
  const formData = new FormData();
  formData.append('chat_id', env.CHAT_ID);
  formData.append('document', file);
  formData.append('caption', `ðŸŒ ${file.name}`);

  const response = await fetch(
    `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
    { method: 'POST', body: formData }
  );

  const data = await response.json();
  if (!data.ok) throw new Error('Telegram upload failed');

  await env.FILES_KV.put(filename, JSON.stringify({
    filename,
    originalName: file.name,
    size: file.size,
    contentType: file.type || 'application/octet-stream',
    telegramFileId: data.result.document.file_id,
    uploadType: 'single',
    uploadedAt: Date.now()
  }));

  return 0;
}

async function uploadChunked(file, filename, env, chunkSize) {
  const totalChunks = Math.ceil(file.size / chunkSize);
  console.log(`ðŸ§© Total chunks: ${totalChunks}`);

  const chunks = [];

  for (let i = 0; i < totalChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, file.size);
    const chunkBlob = file.slice(start, end);

    console.log(`ðŸ“¤ Chunk ${i + 1}/${totalChunks}`);

    const chunkFilename = `${filename}.chunk${String(i).padStart(4, '0')}`;
    const chunkFile = new File([chunkBlob], chunkFilename, { type: 'application/octet-stream' });

    const formData = new FormData();
    formData.append('chat_id', env.CHAT_ID);
    formData.append('document', chunkFile);
    formData.append('caption', `ðŸ§© ${i + 1}/${totalChunks}`);

    const response = await fetch(
      `https://api.telegram.org/bot${env.BOT_TOKEN}/sendDocument`,
      { method: 'POST', body: formData }
    );

    const data = await response.json();
    if (!data.ok) throw new Error(`Chunk ${i + 1} failed`);

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

    if (i < totalChunks - 1) {
      await new Promise(r => setTimeout(r, 300));
    }
  }

  await env.FILES_KV.put(filename, JSON.stringify({
    filename,
    originalName: file.name,
    size: file.size,
    contentType: file.type || 'application/octet-stream',
    uploadType: 'chunked',
    totalChunks,
    chunkSize,
    chunks,
    uploadedAt: Date.now()
  }));

  return totalChunks;
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}