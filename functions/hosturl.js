import { BOT_TOKEN, CHANNEL_ID, MAX_SIZE } from './_config.js';

const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type'
};

export async function onRequest({ request, env }) {
  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: cors });
  }

  try {
    const url = new URL(request.url);
    const fileUrl = url.searchParams.get('url');
    
    if (!fileUrl) {
      return jsonResponse({ success: false, error: 'No URL provided' }, 400);
    }

    // Download file from URL
    const fileResponse = await fetch(fileUrl);
    if (!fileResponse.ok) {
      throw new Error('Failed to fetch file from URL');
    }

    const fileBlob = await fileResponse.blob();
    const filename = fileUrl.split('/').pop() || 'download';
    
    if (fileBlob.size > MAX_SIZE) {
      return jsonResponse({ success: false, error: 'File too large (max 2GB)' }, 400);
    }

    // Upload to Telegram
    const formData = new FormData();
    formData.append('chat_id', CHANNEL_ID);
    formData.append('document', fileBlob, filename);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: formData
    });

    const telegramResult = await telegramResponse.json();

    if (!telegramResult.ok) {
      throw new Error(telegramResult.description || 'Telegram upload failed');
    }

    // Get file path
    const fileId = telegramResult.result.document.file_id;
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const getFileResult = await getFileResponse.json();

    if (!getFileResult.ok) {
      throw new Error('Failed to get file path');
    }

    const filePath = getFileResult.result.file_path;
    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    // Generate slug and store
    const slug = generateSlug(filename);
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: filename,
        size: fileBlob.size,
        contentType: fileBlob.type,
        uploadedAt: Date.now()
      }
    });

    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/m/${slug}`;

    return jsonResponse({
      success: true,
      filename: filename,
      size: fileBlob.size,
      contentType: fileBlob.type,
      view_url: viewUrl,
      download_url: viewUrl + '?dl=1',
      stream_url: viewUrl
    });

  } catch (error) {
    console.error('URL upload error:', error);
    return jsonResponse({ 
      success: false, 
      error: error.message 
    }, 500);
  }
}

function generateSlug(filename) {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substr(2, 8);
  
  const lastDot = filename.lastIndexOf('.');
  const extension = lastDot !== -1 ? filename.substring(lastDot) : '';
  const nameWithoutExt = lastDot !== -1 ? filename.substring(0, lastDot) : filename;
  
  const cleanName = nameWithoutExt.replace(/[^a-zA-Z0-9]/g, '').substr(0, 15);
  
  return `${timestamp}-${random}-${cleanName}${extension}`.toLowerCase();
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 
      'Content-Type': 'application/json',
      ...cors
    }
  });
}
