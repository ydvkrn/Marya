export async function onRequest(context) {
  const { request, env } = context;

  // 🔒 SECURE: Bot token from environment variables
  const BOT_TOKEN = env.BOT_TOKEN || env.TELEGRAM_BOT_TOKEN;
  const CHANNEL_ID = env.CHANNEL_ID || env.TELEGRAM_CHANNEL_ID;

  if (!BOT_TOKEN || !CHANNEL_ID) {
    return new Response(JSON.stringify({
      success: false,
      error: 'Server configuration error'
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      return new Response(JSON.stringify({ success: false, error: 'No file provided' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    const MAX_SIZE = 2147483648; // 2GB
    if (file.size > MAX_SIZE) {
      return new Response(JSON.stringify({ success: false, error: 'File too large (max 2GB)' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log('Uploading file:', file.name, 'Size:', file.size);

    // Upload to Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', file, file.name);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram API error: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    console.log('Telegram response:', telegramData.ok ? 'Success' : 'Failed');

    if (!telegramData.ok) {
      throw new Error(telegramData.description || 'Telegram upload failed');
    }

    if (!telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response structure');
    }

    const fileId = telegramData.result.document.file_id;

    // Get file URL
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    
    if (!getFileResponse.ok) {
      throw new Error(`Get file error: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Failed to get file URL from Telegram');
    }

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
    console.log('Direct URL obtained successfully');

    // Generate slug
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop().toLowerCase() : '';
    const slug = `${timestamp}${random}${extension}`;

    // Store in KV
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: file.name,
        size: file.size,
        contentType: file.type,
        uploadedAt: Date.now()
      }
    });

    console.log('File stored in KV with slug:', slug);

    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/f/${slug}`;
    const downloadUrl = `${baseUrl}/f/${slug}?dl=1`;

    return new Response(JSON.stringify({
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: viewUrl,
      download: downloadUrl
    }), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
