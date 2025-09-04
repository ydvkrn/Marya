export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== MARYA VAULT UPLOAD START ===');

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
    }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    // ✅ Use environment variables from Cloudflare Pages
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;

    console.log('Environment check:', {
      BOT_TOKEN: !!BOT_TOKEN,
      CHANNEL_ID: !!CHANNEL_ID,
      FILES_KV: !!env.FILES_KV
    });

    if (!BOT_TOKEN || !CHANNEL_ID) {
      throw new Error('Missing bot credentials in environment variables');
    }

    if (!env.FILES_KV) {
      throw new Error('FILES_KV binding not found');
    }

    // Parse form data
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log('File received:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // Size validation (2GB)
    if (file.size > 2147483648) {
      throw new Error(`File too large: ${Math.round(file.size / 1024 / 1024)}MB (max 2GB)`);
    }

    // Upload to Telegram
    console.log('Uploading to Telegram...');
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', file, file.name);

    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    console.log('Telegram response status:', telegramResponse.status);

    if (!telegramResponse.ok) {
      const errorText = await telegramResponse.text();
      console.error('Telegram API error:', errorText);
      throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
    }

    // Parse Telegram response safely
    let telegramData;
    try {
      const responseText = await telegramResponse.text();
      telegramData = JSON.parse(responseText);
    } catch (parseError) {
      console.error('JSON parse error:', parseError);
      throw new Error('Invalid JSON response from Telegram');
    }

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      console.error('Invalid Telegram response:', telegramData);
      throw new Error('Invalid Telegram response structure');
    }

    const fileId = telegramData.result.document.file_id;
    console.log('File uploaded to Telegram, file_id:', fileId);

    // Get file URL
    console.log('Getting file URL...');
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${encodeURIComponent(fileId)}`);

    if (!getFileResponse.ok) {
      throw new Error(`GetFile API failed: ${getFileResponse.status}`);
    }

    let getFileData;
    try {
      const responseText = await getFileResponse.text();
      getFileData = JSON.parse(responseText);
    } catch (parseError) {
      console.error('GetFile JSON parse error:', parseError);
      throw new Error('Invalid JSON response from GetFile');
    }

    if (!getFileData.ok || !getFileData.result?.file_path) {
      console.error('Invalid GetFile response:', getFileData);
      throw new Error('No file_path in GetFile response');
    }

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
    console.log('Direct URL created');

    // ✅ Generate ID in your custom format: id + timestamp + random
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).slice(2, 8);
    const fileId_custom = `id${timestamp}${random}`;
    
    // Get file extension
    const extension = file.name.includes('.') ? file.name.slice(file.name.lastIndexOf('.')) : '';

    // Store in KV
    console.log('Storing in KV...');
    await env.FILES_KV.put(fileId_custom, directUrl, {
      metadata: {
        filename: file.name,
        size: file.size,
        contentType: file.type,
        extension: extension,
        uploadedAt: Date.now()
      }
    });

    console.log('File stored in KV with ID:', fileId_custom);

    // ✅ Return URLs in your custom format
    const baseUrl = new URL(request.url).origin;
    const customUrl = `${baseUrl}/btfstorage/file/${fileId_custom}${extension}`;
    const downloadUrl = `${baseUrl}/btfstorage/file/${fileId_custom}${extension}?dl=1`;

    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: customUrl,
      download: downloadUrl,
      id: fileId_custom
    };

    console.log('Upload completed successfully:', result);
    console.log('=== MARYA VAULT UPLOAD END ===');

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('=== UPLOAD ERROR ===');
    console.error('Error:', error.message);
    console.error('Stack:', error.stack);

    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
