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

    // ✅ FIXED: Choose correct Telegram method based on file type
    const contentType = file.type.toLowerCase();
    let telegramMethod = 'sendDocument'; // Default fallback
    let fieldName = 'document';

    // Determine method and field name based on content type and size
    if (contentType.startsWith('image/') && file.size < 10 * 1024 * 1024) {
      telegramMethod = 'sendPhoto';
      fieldName = 'photo';
    } else if (contentType.startsWith('video/') && file.size < 50 * 1024 * 1024) {
      telegramMethod = 'sendVideo';
      fieldName = 'video';
    } else if (contentType.startsWith('audio/')) {
      telegramMethod = 'sendAudio';
      fieldName = 'audio';
    }

    // Upload to Telegram
    const telegramFormData = new FormData();
    telegramFormData.append('chat_id', CHANNEL_ID);
    telegramFormData.append(fieldName, file);

    // Add extra parameters for specific types
    if (telegramMethod === 'sendVideo') {
      telegramFormData.append('supports_streaming', 'true');
    }

    const telegramUrl = `https://api.telegram.org/bot${BOT_TOKEN}/${telegramMethod}`;
    const telegramResponse = await fetch(telegramUrl, {
      method: 'POST',
      body: telegramFormData
    });

    const telegramResult = await telegramResponse.json();
    console.log('Telegram response:', telegramResult);

    if (!telegramResult.ok) {
      return jsonResponse({ 
        success: false, 
        error: telegramResult.description || 'Telegram upload failed' 
      }, 500);
    }

    // ✅ FIXED: Extract file_id based on upload method
    let fileId = null;
    let fileName = file.name;

    if (telegramResult.result.document) {
      fileId = telegramResult.result.document.file_id;
      fileName = telegramResult.result.document.file_name || file.name;
    } else if (telegramResult.result.photo) {
      // For photos, get the largest size
      const photos = telegramResult.result.photo;
      fileId = photos[photos.length - 1].file_id;
    } else if (telegramResult.result.video) {
      fileId = telegramResult.result.video.file_id;
    } else if (telegramResult.result.audio) {
      fileId = telegramResult.result.audio.file_id;
    } else if (telegramResult.result.voice) {
      fileId = telegramResult.result.voice.file_id;
    }

    if (!fileId) {
      console.error('No file_id found in result:', telegramResult);
      return jsonResponse({ success: false, error: 'Failed to get file ID' }, 500);
    }

    // Get file path
    const getFileUrl = `https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`;
    const fileResponse = await fetch(getFileUrl);
    const fileResult = await fileResponse.json();

    if (!fileResult.ok) {
      return jsonResponse({ success: false, error: 'Failed to get file path' }, 500);
    }

    const filePath = fileResult.result.file_path;
    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    // Generate slug with extension
    const slug = generateSlugWithExtension(fileName);
    
    // Store in KV with detailed metadata
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: fileName,
        size: file.size,
        contentType: file.type,
        telegramMethod: telegramMethod,
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
  const random = Math.random().toString(36).substr(2, 6);
  
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
