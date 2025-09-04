export async function onRequest(context) {
  const { request, env } = context;

  console.log('=== UPLOAD REQUEST ===');

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
    // Environment variables (use hardcoded for now)
    const BOT_TOKEN = '8360624116:AAEEJha8CRgL8TnrEKk5zOuCNXXRawmbuaE';
    const CHANNEL_ID = '-1003071466750';

    // Parse form data
    console.log('Getting form data...');
    const formData = await request.formData();
    const file = formData.get('file');

    if (!file) {
      throw new Error('No file provided');
    }

    console.log('File details:', {
      name: file.name,
      size: file.size,
      type: file.type
    });

    // Size check
    if (file.size > 2147483648) { // 2GB
      throw new Error('File too large (max 2GB)');
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
      throw new Error(`Telegram upload failed: ${telegramResponse.status} - ${errorText}`);
    }

    const telegramData = await telegramResponse.json();

    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    // Get file URL
    const fileId = telegramData.result.document.file_id;
    console.log('Getting file URL...');

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
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop().toLowerCase() : '';
    const slug = `${timestamp}${random}${extension}`;

    // Store in KV
    if (env.FILES_KV) {
      console.log('Storing in KV...');
      await env.FILES_KV.put(slug, directUrl, {
        metadata: {
          filename: file.name,
          size: file.size,
          contentType: file.type,
          uploadedAt: Date.now()
        }
      });
      console.log('Stored in KV successfully');
    }

    // Return result
    const baseUrl = new URL(request.url).origin;
    const viewUrl = `${baseUrl}/f/${slug}`;
    const downloadUrl = `${baseUrl}/f/${slug}?dl=1`;

    console.log('Generated URLs:', { viewUrl, downloadUrl });

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
