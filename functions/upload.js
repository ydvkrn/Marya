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

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file uploaded');
    }

    if (file.size > MAX_SIZE) {
      throw new Error('File too large (max 2GB)');
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
      throw new Error(telegramResult.description || 'Telegram upload failed');
    }

    // Get file URL
    const fileId = telegramResult.result.document.file_id;
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const getFileResult = await getFileResponse.json();

    if (!getFileResult.ok) {
      throw new Error('Failed to get file URL');
    }

    const filePath = getFileResult.result.file_path;
    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    // Generate slug with extension
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const lastDot = file.name.lastIndexOf('.');
    const extension = lastDot !== -1 ? file.name.substring(lastDot) : '';
    const nameWithoutExt = lastDot !== -1 ? file.name.substring(0, lastDot) : file.name;
    const cleanName = nameWithoutExt.replace(/[^a-zA-Z0-9]/g, '').substr(0, 15);
    const slug = `${timestamp}-${random}-${cleanName}${extension}`.toLowerCase();

    // ✅ FIXED: Store URL as plain text (not JSON)
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: file.name,
        size: file.size,
        contentType: file.type,
        uploadedAt: Date.now()
      }
    });

    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/m/${slug}`;

    return new Response(JSON.stringify({
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      view_url: viewUrl,
      download_url: viewUrl + '?dl=1',
      stream_url: viewUrl
    }), {
      headers: { 'Content-Type': 'application/json', ...cors }
    });

  } catch (error) {
    return new Response(JSON.stringify({ 
      success: false, 
      error: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...cors }
    });
  }
}
