import { BOT_TOKEN, CHANNEL_ID, MAX_SIZE } from './_config.js';

export async function onRequest({ request, env }) {
  if (request.method === 'OPTIONS') {
    return new Response(null, { 
      headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST' }
    });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file || file.size > MAX_SIZE) {
      throw new Error('Invalid file or too large');
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
    if (!telegramResult.ok) throw new Error('Telegram upload failed');

    // Get file URL
    const fileId = telegramResult.result.document.file_id;
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const getFileResult = await getFileResponse.json();
    if (!getFileResult.ok) throw new Error('Failed to get file URL');

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileResult.result.file_path}`;

    // Generate simple slug
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 6);
    const extension = file.name.includes('.') ? file.name.split('.').pop() : '';
    const slug = `${timestamp}${random}${extension ? '.' + extension : ''}`;

    // Store in KV
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: file.name,
        size: file.size,
        type: file.type,
        uploadedAt: Date.now()
      }
    });

    const baseUrl = new URL(request.url).origin;
    const fileUrl = `${baseUrl}/m/${slug}`;

    return new Response(JSON.stringify({
      success: true,
      filename: file.name,
      size: file.size,
      url: fileUrl
    }), {
      headers: { 
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      }
    });

  } catch (error) {
    return new Response(JSON.stringify({ 
      success: false, 
      error: error.message 
    }), {
      status: 500,
      headers: { 
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }
}
