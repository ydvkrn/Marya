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
    return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...cors }
    });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      return new Response(JSON.stringify({ success: false, error: 'No file uploaded' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...cors }
      });
    }

    if (file.size > MAX_SIZE) {
      return new Response(JSON.stringify({ success: false, error: 'File too large (max 2GB)' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...cors }
      });
    }

    // Upload to Telegram
    const telegramData = new FormData();
    telegramData.append('chat_id', CHANNEL_ID);
    telegramData.append('document', file, file.name);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramData
    });

    const telegramResult = await telegramResponse.json();

    if (!telegramResult.ok) {
      console.error('Telegram error:', telegramResult);
      return new Response(JSON.stringify({ 
        success: false, 
        error: telegramResult.description || 'Telegram upload failed' 
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...cors }
      });
    }

    // Get file info
    const document = telegramResult.result.document;
    const fileId = document.file_id;
    const fileName = document.file_name || file.name;

    // Get file URL from Telegram
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const getFileResult = await getFileResponse.json();

    if (!getFileResult.ok) {
      console.error('GetFile error:', getFileResult);
      return new Response(JSON.stringify({ success: false, error: 'Failed to get file URL' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json', ...cors }
      });
    }

    const filePath = getFileResult.result.file_path;
    const telegramFileUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    // Generate slug
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const lastDot = fileName.lastIndexOf('.');
    const extension = lastDot !== -1 ? fileName.substring(lastDot) : '';
    const nameWithoutExt = lastDot !== -1 ? fileName.substring(0, lastDot) : fileName;
    const cleanName = nameWithoutExt.replace(/[^a-zA-Z0-9]/g, '').substr(0, 15);
    const slug = `${timestamp}-${random}-${cleanName}${extension}`.toLowerCase();

    // Store in KV - SIMPLE STRING STORAGE
    await env.FILES_KV.put(slug, telegramFileUrl, {
      metadata: {
        filename: fileName,
        size: file.size,
        contentType: file.type,
        uploadedAt: Date.now()
      }
    });

    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/m/${slug}`;

    return new Response(JSON.stringify({
      success: true,
      filename: fileName,
      size: file.size,
      contentType: file.type,
      view_url: viewUrl,
      download_url: viewUrl + '?dl=1',
      stream_url: viewUrl
    }), {
      headers: { 'Content-Type': 'application/json', ...cors }
    });

  } catch (error) {
    console.error('Upload error:', error);
    return new Response(JSON.stringify({ 
      success: false, 
      error: error.message || 'Server error' 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...cors }
    });
  }
}
