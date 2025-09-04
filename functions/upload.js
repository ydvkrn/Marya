const BOT_TOKEN = '8360624116:AAEEJha8CRgL8TnrEKk5zOuCNXXRawmbuaE';
const CHANNEL_ID = '-1003071466750';
const MAX_SIZE = 2147483648; // 2GB

export async function onRequest(context) {
  const { request, env } = context;

  // CORS headers
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

    if (file.size > MAX_SIZE) {
      return new Response(JSON.stringify({ success: false, error: 'File too large (max 2GB)' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    console.log('Uploading file:', file.name, 'Size:', file.size, 'Type:', file.type);

    // Create FormData for Telegram
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', file, file.name);

    // Upload to Telegram
    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    console.log('Telegram response status:', telegramResponse.status);

    if (!telegramResponse.ok) {
      throw new Error(`Telegram API error: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    console.log('Telegram data received:', JSON.stringify(telegramData, null, 2));

    if (!telegramData.ok) {
      throw new Error(telegramData.description || 'Telegram upload failed');
    }

    // ✅ FIXED: Proper error handling for file_id
    if (!telegramData.result || !telegramData.result.document) {
      throw new Error('No document in Telegram response');
    }

    const document = telegramData.result.document;
    const fileId = document.file_id;

    if (!fileId) {
      throw new Error('No file_id in Telegram response');
    }

    console.log('File uploaded to Telegram, file_id:', fileId);

    // Get file URL from Telegram
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    
    if (!getFileResponse.ok) {
      throw new Error(`Get file error: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();
    console.log('GetFile response:', JSON.stringify(getFileData, null, 2));

    if (!getFileData.ok) {
      throw new Error(getFileData.description || 'Failed to get file URL');
    }

    if (!getFileData.result || !getFileData.result.file_path) {
      throw new Error('No file_path in getFile response');
    }

    const filePath = getFileData.result.file_path;
    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${filePath}`;

    console.log('Direct Telegram URL:', directUrl);

    // Generate slug
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop().toLowerCase() : '';
    const slug = `${timestamp}${random}${extension}`;

    console.log('Generated slug:', slug);

    // ✅ FIXED: Store URL as plain string in KV
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: file.name,
        size: file.size,
        contentType: file.type,
        uploadedAt: Date.now()
      }
    });

    console.log('File stored in KV successfully');

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
