export async function onRequest(context) {
  console.log('=== UPLOAD STARTED ===');
  
  const { request, env } = context;
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Simple environment check
    const BOT_TOKEN = '8360624116:AAEEJha8CRgL8TnrEKk5zOuCNXXRawmbuaE';
    const CHANNEL_ID = '-1003071466750';
    
    console.log('Getting form data...');
    const formData = await request.formData();
    const file = formData.get('file');
    
    console.log('File:', file ? file.name : 'NOT FOUND');
    
    if (!file) {
      return new Response(JSON.stringify({ success: false, error: 'No file' }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
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

    console.log('Telegram status:', telegramResponse.status);
    
    if (!telegramResponse.ok) {
      throw new Error(`Telegram error: ${telegramResponse.status}`);
    }

    const telegramData = await telegramResponse.json();
    
    if (!telegramData.ok || !telegramData.result?.document?.file_id) {
      throw new Error('Invalid Telegram response');
    }

    // Get file URL
    const fileId = telegramData.result.document.file_id;
    const getFileResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/getFile?file_id=${fileId}`);
    const getFileData = await getFileResponse.json();
    
    if (!getFileData.ok || !getFileData.result?.file_path) {
      throw new Error('Failed to get file URL');
    }

    const directUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${getFileData.result.file_path}`;
    
    // Generate simple slug
    const slug = Date.now().toString(36) + Math.random().toString(36).substr(2, 8);
    const extension = file.name.includes('.') ? '.' + file.name.split('.').pop() : '';
    const finalSlug = slug + extension;

    // Store in KV (if available)
    if (env.FILES_KV) {
      await env.FILES_KV.put(finalSlug, directUrl, {
        metadata: { filename: file.name, size: file.size, uploadedAt: Date.now() }
      });
    }

    const baseUrl = new URL(request.url).origin;
    
    return new Response(JSON.stringify({
      success: true,
      filename: file.name,
      size: file.size,
      url: `${baseUrl}/f/${finalSlug}`,
      download: `${baseUrl}/f/${finalSlug}?dl=1`,
      direct: directUrl // For testing
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
