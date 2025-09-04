export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== UPLOAD REQUEST START ===');
  console.log('Method:', request.method);
  console.log('URL:', request.url);

  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    console.log('OPTIONS request - returning CORS headers');
    return new Response(null, { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    console.log('Invalid method:', request.method);
    return new Response(JSON.stringify({ success: false, error: 'Method not allowed' }), {
      status: 405,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }

  try {
    // Check environment variables
    const BOT_TOKEN = env.BOT_TOKEN;
    const CHANNEL_ID = env.CHANNEL_ID;
    
    console.log('Environment check:');
    console.log('BOT_TOKEN exists:', !!BOT_TOKEN);
    console.log('CHANNEL_ID exists:', !!CHANNEL_ID);
    console.log('FILES_KV exists:', !!env.FILES_KV);

    if (!BOT_TOKEN) {
      throw new Error('BOT_TOKEN environment variable not found');
    }
    if (!CHANNEL_ID) {
      throw new Error('CHANNEL_ID environment variable not found');
    }
    if (!env.FILES_KV) {
      throw new Error('FILES_KV binding not found');
    }

    console.log('Parsing form data...');
    const formData = await request.formData();
    const file = formData.get('file');

    console.log('File received:', !!file);
    if (file) {
      console.log('File details - Name:', file.name, 'Size:', file.size, 'Type:', file.type);
    }

    if (!file) {
      throw new Error('No file provided in form data');
    }

    const MAX_SIZE = 2147483648; // 2GB
    if (file.size > MAX_SIZE) {
      throw new Error(`File too large: ${file.size} bytes (max 2GB)`);
    }

    console.log('Creating Telegram form data...');
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', file, file.name);

    console.log('Uploading to Telegram...');
    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    console.log('Telegram response status:', telegramResponse.status);
    console.log('Telegram response OK:', telegramResponse.ok);

    if (!telegramResponse.ok) {
      const errorText = await telegramResponse.text();
      console.error('Telegram API error:', errorText);
      throw new Error(`Telegram API error (${telegramResponse.status}): ${errorText}`);
    }

    const telegramData = await telegramResponse.json();
    console.log('Telegram upload result - OK:', telegramData.ok);

    if (!telegramData.ok) {
      console.error('Telegram returned not OK:', telegramData);
      throw new Error(telegramData.description || 'Telegram upload failed');
    }

    // Validate response structure
    if (!telegramData.result) {
      console.error('No result in Telegram response:', telegramData);
      throw new Error('Invalid Telegram API response: no result');
    }

    if (!telegramData.result.document) {
      console.error('No document in result:', telegramData.result);
      throw new Error('Invalid Telegram API response: no document');
    }

    const document = telegramData.result.document;
    const fileId = document.file_id;

    if (!fileId) {
      console.error('No file_id in document:', document);
      throw new Error('Invalid Telegram API response: no file_id');
    }

    console.log('File uploaded to Telegram successfully, file_id:', fileId);

    // Get file URL
    console.log('Getting file URL from Telegram...');
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    
    console.log('GetFile response status:', getFileResponse.status);
    
    if (!getFileResponse.ok) {
      const errorText = await getFileResponse.text();
      console.error('GetFile API error:', errorText);
      throw new Error(`GetFile API error (${getFileResponse.status}): ${errorText}`);
    }

    const getFileData = await getFileResponse.json();
    console.log('GetFile result - OK:', getFileData.ok);

    if (!getFileData.ok) {
      console.error('GetFile returned not OK:', getFileData);
      throw new Error(getFileData.description || 'Failed to get file URL');
    }

    if (!getFileData.result || !getFileData.result.file_path) {
      console.error('No file_path in getFile result:', getFileData);
      throw new Error('Invalid getFile response: no file_path');
    }

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
    console.log('Direct URL generated successfully');

    // Generate slug
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop().toLowerCase() : '';
    const slug = `${timestamp}${random}${extension}`;

    console.log('Generated slug:', slug);

    // Store in KV
    console.log('Storing in KV...');
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

    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: viewUrl,
      download: downloadUrl
    };

    console.log('Upload completed successfully');
    console.log('=== UPLOAD REQUEST END ===');

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('=== UPLOAD ERROR ===');
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);
    console.error('=== END ERROR ===');

    return new Response(JSON.stringify({
      success: false,
      error: error.message,
      debug: {
        hasEnv: {
          BOT_TOKEN: !!env?.BOT_TOKEN,
          CHANNEL_ID: !!env?.CHANNEL_ID,
          FILES_KV: !!env?.FILES_KV
        }
      }
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
