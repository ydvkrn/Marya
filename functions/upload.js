import { BOT_TOKEN, CHANNEL_ID, MAX_SIZE } from './_config.js';

const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type'
};

export async function onRequest({ request, env }) {
  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: cors });
  }

  if (request.method !== 'POST') {
    return jsonResponse({ success: false, error: 'Method not allowed' }, 405);
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      return jsonResponse({ success: false, error: 'No file uploaded' }, 400);
    }

    if (file.size > MAX_SIZE) {
      return jsonResponse({ success: false, error: 'File too large (max 2GB)' }, 400);
    }

    console.log('Uploading:', file.name, 'Type:', file.type, 'Size:', file.size);

    // ✅ FIXED: Always use sendDocument for ALL file types (most reliable)
    const telegramFormData = new FormData();
    telegramFormData.append('chat_id', CHANNEL_ID);
    telegramFormData.append('document', file, file.name); // Important: include filename

    const telegramUrl = `https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`;
    const telegramResponse = await fetch(telegramUrl, {
      method: 'POST',
      body: telegramFormData
    });

    const telegramResult = await telegramResponse.json();
    console.log('Telegram response:', telegramResult);

    if (!telegramResult.ok) {
      console.error('Telegram error:', telegramResult);
      return jsonResponse({ 
        success: false, 
        error: telegramResult.description || 'Telegram upload failed' 
      }, 500);
    }

    // Get file_id from document
    const document = telegramResult.result.document;
    if (!document) {
      return jsonResponse({ success: false, error: 'No document in response' }, 500);
    }

    const fileId = document.file_id;
    const fileName = document.file_name || file.name;
    
    // Get file path from Telegram
    const getFileUrl = `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`;
    const fileResponse = await fetch(getFileUrl);
    const fileResult = await fileResponse.json();

    if (!fileResult.ok) {
      console.error('GetFile error:', fileResult);
      return jsonResponse({ success: false, error: 'Failed to get file path' }, 500);
    }

    const filePath = fileResult.result.file_path;
    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    // Generate slug with extension
    const slug = generateSlugWithExtension(fileName);
    
    // ✅ FIXED: Store as simple string, not JSON
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: fileName,
        size: file.size,
        contentType: file.type,
        uploadedAt: Date.now()
      }
    });

    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/m/${slug}`;

    return jsonResponse({
      success: true,
      filename: fileName,
      size: file.size,
      contentType: file.type,
      view_url: viewUrl,
      download_url: viewUrl + '?dl=1',
      stream_url: viewUrl
    });

  } catch (error) {
    console.error('Upload error:', error);
    return jsonResponse({ 
      success: false, 
      error: error.message || 'Server error' 
    }, 500);
  }
}

function generateSlugWithExtension(filename) {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substr(2, 8);
  
  // Preserve extension properly
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
