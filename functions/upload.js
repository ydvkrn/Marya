const BOT_TOKEN = '8360624116:AAEEJha8CRgL8TnrEKk5zOuCNXXRawmbuaE';
const CHANNEL_ID = '-1003071466750';

export async function onRequest(context) {
  const { request, env } = context;
  
  // CORS headers
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
      status: 405,
      headers: { ...headers, 'Content-Type': 'application/json' }
    });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    if (file.size > 2000000000) { // 2GB limit
      throw new Error('File too large (max 2GB)');
    }

    // Upload to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', file, file.name);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    const telegramData = await telegramResponse.json();

    if (!telegramData.ok) {
      throw new Error(telegramData.description || 'Telegram upload failed');
    }

    // Get file URL
    const fileId = telegramData.result.document.file_id;
    const fileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const fileData = await fileResponse.json();

    if (!fileData.ok) {
      throw new Error('Failed to get file URL');
    }

    const telegramURL = `https://api.telegram.org/file/bot${BOT_TOKEN}/${fileData.result.file_path}`;

    // Generate slug
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop() : '';
    const slug = `${timestamp}${random}${extension}`;

    // Store in KV
    await env.FILES_KV.put(slug, telegramURL, {
      metadata: {
        filename: file.name,
        size: file.size,
        type: file.type,
        uploaded: Date.now()
      }
    });

    const baseURL = new URL(request.url).origin;
    const viewURL = `${baseURL}/file/${slug}`;
    const downloadURL = `${baseURL}/file/${slug}?dl=1`;

    return new Response(JSON.stringify({
      success: true,
      filename: file.name,
      size: file.size,
      type: file.type,
      url: viewURL,
      download: downloadURL
    }), {
      headers: { ...headers, 'Content-Type': 'application/json' }
    });

  } catch (error) {
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { ...headers, 'Content-Type': 'application/json' }
    });
  }
}
