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

    if (!file || file.size === 0) {
      throw new Error('No file uploaded or file is empty');
    }

    if (file.size > MAX_SIZE) {
      throw new Error('File too large (max 2GB)');
    }

    console.log('Uploading file:', file.name, 'Size:', file.size);

    // Upload to Telegram
    const telegramData = new FormData();
    telegramData.append('chat_id', CHANNEL_ID);
    telegramData.append('document', file, file.name);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramData
    });

    const telegramResult = await telegramResponse.json();
    console.log('Telegram result:', telegramResult);

    if (!telegramResult.ok) {
      throw new Error(telegramResult.description || 'Telegram upload failed');
    }

    // Get file URL
    const fileId = telegramResult.result.document.file_id;
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const getFileResult = await getFileResponse.json();

    if (!getFileResult.ok) {
      throw new Error('Failed to get file URL from Telegram');
    }

    const filePath = getFileResult.result.file_path;
    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    console.log('Direct Telegram URL:', directUrl);

    // Generate simple slug
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop() : '';
    const slug = `${timestamp}${random}${extension}`.toLowerCase();

    // Store in KV (just the URL as string)
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: file.name,
        size: file.size,
        contentType: file.type,
        uploadedAt: Date.now()
      }
    });

    console.log('Stored in KV with slug:', slug);

    // Generate file ID for display
    const fileNumber = Math.floor(Math.random() * 100);
    const fileIdCode = `MSMfile${fileNumber}/${Math.random().toString(36).substr(2, 3)}-${Math.random().toString(36).substr(2, 3)}`;

    const baseUrl = new URL(request.url).origin;
    
    // Simplified URLs
    const streamUrl = `${baseUrl}/btf/${slug}/${fileIdCode}`;
    const downloadUrl = `${baseUrl}/btf/${slug}/${fileIdCode}?dl=1`;

    return new Response(JSON.stringify({
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      view_url: streamUrl,
      stream_url: streamUrl,
      download_url: downloadUrl,
      file_id: fileIdCode
    }), {
      headers: { 'Content-Type': 'application/json', ...cors }
    });

  } catch (error) {
    console.error('Upload error:', error);
    return new Response(JSON.stringify({ 
      success: false, 
      error: error.message 
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...cors }
    });
  }
}
