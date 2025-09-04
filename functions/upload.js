export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== UPLOAD REQUEST ===');
  console.log('Method:', request.method);
  console.log('Content-Type:', request.headers.get('content-type'));

  // CORS headers
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  // Handle preflight requests
  if (request.method === 'OPTIONS') {
    console.log('Handling OPTIONS request');
    return new Response(null, { headers: corsHeaders });
  }

  // Only allow POST
  if (request.method !== 'POST') {
    console.log('Invalid method:', request.method);
    return new Response(JSON.stringify({ 
      success: false, 
      error: 'Method not allowed' 
    }), {
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

    if (!BOT_TOKEN || !CHANNEL_ID || !env.FILES_KV) {
      throw new Error('Missing environment variables or KV binding');
    }

    // Parse form data
    console.log('Parsing form data...');
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      console.log('No file in form data');
      throw new Error('No file provided');
    }

    console.log('File details:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // Size check (2GB)
    if (file.size > 2147483648) {
      throw new Error(`File too large: ${Math.round(file.size / 1024 / 1024)}MB (max 2GB)`);
    }

    // Upload to Telegram
    console.log('Creating Telegram form...');
    const telegramForm = new FormData();
    telegramForm.append('chat_id', CHANNEL_ID);
    telegramForm.append('document', file, file.name);

    console.log('Uploading to Telegram...');
    const telegramResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendDocument`, {
      method: 'POST',
      body: telegramForm
    });

    console.log('Telegram response status:', telegramResponse.status);

    if (!telegramResponse.ok) {
      const errorText = await telegramResponse.text();
      console.error('Telegram error:', errorText);
      throw new Error(`Telegram upload failed: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    console.log('Telegram success:', telegramData.ok);

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    // Get file URL
    const fileId = telegramData.result.document.file_id;
    console.log('Getting file URL for:', fileId);

    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    
    if (!getFileResponse.ok) {
      throw new Error(`GetFile failed: ${getFileResponse.status}`);
    }

    const getFileData = await getFileResponse.json();
    
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Failed to get file path');
    }

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
    console.log('Direct URL created');

    // Generate slug
    const now = Date.now();
    const random = Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop().toLowerCase() : '';
    const slug = `${now.toString(36)}${random}${extension}`;

    // Store in KV
    console.log('Storing in KV with slug:', slug);
    await env.FILES_KV.put(slug, directUrl, {
      metadata: {
        filename: file.name,
        size: file.size,
        contentType: file.type,
        uploadedAt: now
      }
    });

    // Return success
    const baseUrl = new URL(request.url).origin;
    const result = {
      success: true,
      filename: file.name,
      size: file.size,
      contentType: file.type,
      url: `${baseUrl}/f/${slug}`,
      download: `${baseUrl}/f/${slug}?dl=1`
    };

    console.log('Upload completed successfully');
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });

  } catch (error) {
    console.error('Upload error:', error.message);
    return new Response(JSON.stringify({
      success: false,
      error: error.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json', ...corsHeaders }
    });
  }
}
