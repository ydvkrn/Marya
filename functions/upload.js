// functions/upload.js
export async function onRequest(context) {
  const { request, env } = context;

  console.log('ðŸ“¡ Upload request received:', request.method);

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({
      success: false,
      error: 'Method not allowed'
    }), { status: 405, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
  }

  try {
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    console.log('ðŸ”§ Bot credentials check:', !!BOT_TOKEN, !!CHANNEL_ID);

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing bot credentials');
    }

    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log(`ðŸ“ Processing file: ${file.name} (${file.size} bytes)`);

    // File validation
    const MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024; // 2GB
    if (file.size > MAX_FILE_SIZE) {
      throw new Error(`File too large: ${Math.round(file.size/1024/1024)}MB (max 2048MB)`);
    }

    // Generate unique file ID
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId = `id${timestamp}${random}`;
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    console.log(`ðŸ†” Generated file ID: ${fileId}`);

    // Upload to Telegram
    const telegramFormData = new FormData();
    telegramFormData.append('chat_id', CHANNEL_ID);
    telegramFormData.append('document', file);

    console.log('ðŸ“¤ Uploading to Telegram...');

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramFormData
    });

    if (!telegramResponse.ok) {
      throw new Error(`Telegram API error: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    console.log('ðŸ“‹ Telegram response:', telegramData.ok ? 'Success' : 'Failed');

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    const telegramFileId = telegramData.result.document.file_id;

    // Get file path from Telegram
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(telegramFileId)}`);

    if (!getFileResponse.ok) {
      throw new Error(`GetFile API error: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();

    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('No file_path in getFile response');
    }

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;

    console.log('âœ… File uploaded successfully');

    // Store metadata in KV (if available)
    if (env.FILES_KV) {
      const metadata = {
        filename: file.name,
        size: file.size,
        contentType: file.type,
        extension: extension,
        uploadedAt: Date.now(),
        telegramFileId: telegramFileId,
        directUrl: directUrl
      };

      await env.FILES_KV.put(fileId, JSON.stringify(metadata));
      console.log('ðŸ’¾ Metadata stored in KV');
    }

    // Generate public URLs
    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId}${extension}?dl=1`;

    console.log('ðŸ”— Generated URLs:', customUrl);

    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: customUrl,
      download: downloadUrl,
      id: fileId,
      telegramFileId: telegramFileId
    };

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('âŒ Upload error:', error);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}